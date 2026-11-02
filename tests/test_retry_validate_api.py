import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest
import respx

from app.clients.hospital_api import HospitalApiClient
from app.models import JobStatus, RowResult, RowStatus
from app.progress_store import ProgressStore
from app.services.bulk_processor import BulkJobProcessor
from tests.conftest import FakeQueue
from tests.upstream import BASE_URL, FakeUpstream

CSV = b"name,address,phone\nCity Hospital,1 Main St,555-0100\nGeneral,2 Oak Ave,\nMercy,3 Elm St,\n"


async def _no_sleep(seconds: float) -> None:
    return None


@pytest.fixture
def upstream() -> AsyncIterator[FakeUpstream]:
    with respx.mock(assert_all_called=False) as router:
        yield FakeUpstream(router)


@pytest.fixture
async def processor(store, settings) -> AsyncIterator[BulkJobProcessor]:
    async with httpx.AsyncClient(base_url=BASE_URL) as http:
        yield BulkJobProcessor(store, HospitalApiClient(http, max_retries=1, sleep=_no_sleep), settings)


async def _upload(api, content: bytes = CSV) -> str:
    resp = await api.post("/hospitals/bulk", params={"wait": "false"}, files={"file": ("h.csv", content, "text/csv")})
    assert resp.status_code == 202, resp.text
    return resp.json()["batch_id"]


# --- POST /hospitals/bulk/validate ----------------------------------------------


async def test_validate_valid_csv_returns_rows_without_processing(api, redis, queue: FakeQueue) -> None:
    resp = await api.post("/hospitals/bulk/validate", files={"file": ("h.csv", CSV, "text/csv")})

    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is True
    assert body["total_rows"] == 3
    assert body["rows"][1] == {"row_number": 2, "name": "General", "address": "2 Oak Ave", "phone": None}
    assert queue.jobs == []
    assert await redis.keys("*") == []


async def test_validate_invalid_rows_returns_422(api) -> None:
    resp = await api.post("/hospitals/bulk/validate", files={"file": ("h.csv", b"name,address\n,X\n", "text/csv")})

    assert resp.status_code == 422
    assert resp.json()["errors"][0]["field"] == "name"


async def test_validate_bad_file_returns_400(api) -> None:
    resp = await api.post("/hospitals/bulk/validate", files={"file": ("h.json", b"{}", "application/json")})

    assert resp.status_code == 400


# --- POST /hospitals/bulk/{batch_id}/retry -------------------------------------


async def test_retry_unknown_batch_returns_404(api) -> None:
    resp = await api.post("/hospitals/bulk/nope/retry")

    assert resp.status_code == 404


@pytest.mark.parametrize("status", [JobStatus.QUEUED, JobStatus.PROCESSING])
async def test_retry_while_job_is_active_returns_409(api, store: ProgressStore, status: JobStatus) -> None:
    batch_id = await _upload(api)
    await store.update_meta(batch_id, status=status)

    resp = await api.post(f"/hospitals/bulk/{batch_id}/retry")

    assert resp.status_code == 409


async def test_retry_completed_job_returns_409(api, store: ProgressStore) -> None:
    batch_id = await _upload(api)
    await store.update_meta(batch_id, status=JobStatus.COMPLETED, batch_activated=True)

    resp = await api.post(f"/hospitals/bulk/{batch_id}/retry")

    assert resp.status_code == 409
    assert "nothing to retry" in resp.json()["detail"].lower()


async def test_retry_resets_failed_rows_and_requeues(api, store: ProgressStore, queue: FakeQueue) -> None:
    batch_id = await _upload(api)
    await store.record_result(batch_id, RowResult(row=1, name="City Hospital", status=RowStatus.CREATED, hospital_id=1))
    await store.record_result(batch_id, RowResult(row=2, name="General", status=RowStatus.FAILED, error="503"))
    await store.record_result(batch_id, RowResult(row=3, name="Mercy", status=RowStatus.CREATED, hospital_id=3))
    await store.update_meta(batch_id, status=JobStatus.PARTIAL_FAILURE)

    resp = await api.post(f"/hospitals/bulk/{batch_id}/retry")

    assert resp.status_code == 202
    body = resp.json()
    assert body["retried_rows"] == [2]
    assert body["status"] == "queued"
    assert body["status_url"] == f"/hospitals/bulk/{batch_id}/status"
    meta = await store.get_meta(batch_id)
    assert meta.status is JobStatus.QUEUED
    assert (meta.processed, meta.succeeded, meta.failed) == (2, 2, 0)
    assert len(queue.jobs) == 2
    retry_batch, retry_job_id = queue.jobs[1]
    assert retry_batch == batch_id
    assert retry_job_id != batch_id, "arq would drop a job id it has already seen"


async def test_retry_of_job_that_never_ran_is_allowed(api, store: ProgressStore, queue: FakeQueue) -> None:
    batch_id = await _upload(api)
    await store.update_meta(batch_id, status=JobStatus.FAILED, error="Could not enqueue job")

    resp = await api.post(f"/hospitals/bulk/{batch_id}/retry")

    assert resp.status_code == 202
    assert resp.json()["retried_rows"] == []
    assert (await store.get_meta(batch_id)).error is None


async def test_concurrent_retries_only_one_wins(api, store: ProgressStore, queue: FakeQueue) -> None:
    batch_id = await _upload(api)
    await store.update_meta(batch_id, status=JobStatus.PARTIAL_FAILURE)

    responses = await asyncio.gather(*(api.post(f"/hospitals/bulk/{batch_id}/retry") for _ in range(5)))

    assert sorted(r.status_code for r in responses) == [202, 409, 409, 409, 409]
    assert len(queue.jobs) == 2


async def test_retry_flow_end_to_end_activates_without_duplicates(
    api, store: ProgressStore, processor: BulkJobProcessor, upstream: FakeUpstream
) -> None:
    batch_id = await _upload(api)
    upstream.failures["General"] = [503, 503]  # exhausts both attempts on the first run

    await processor.run(batch_id)
    first = (await api.get(f"/hospitals/bulk/{batch_id}/status")).json()
    assert first["status"] == "partial_failure"
    assert first["batch_activated"] is False
    assert upstream.activated == []

    assert (await api.post(f"/hospitals/bulk/{batch_id}/retry")).status_code == 202
    await processor.run(batch_id)

    final = (await api.get(f"/hospitals/bulk/{batch_id}/status")).json()
    assert final["status"] == "completed"
    assert final["batch_activated"] is True
    assert (final["processed"], final["succeeded"], final["failed"]) == (3, 3, 0)
    assert sorted(upstream.created_names) == ["City Hospital", "General", "Mercy"]
    assert upstream.activated == [batch_id]


async def test_transition_status_is_compare_and_set(store: ProgressStore, api) -> None:
    batch_id = await _upload(api)

    assert await store.transition_status(batch_id, {JobStatus.QUEUED}, JobStatus.PROCESSING) is True
    assert await store.transition_status(batch_id, {JobStatus.QUEUED}, JobStatus.PROCESSING) is False
    assert await store.transition_status("missing", {JobStatus.QUEUED}, JobStatus.PROCESSING) is False
    assert (await store.get_meta(batch_id)).status is JobStatus.PROCESSING
