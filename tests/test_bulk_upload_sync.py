"""POST /hospitals/bulk waits for the result by default (wait=true); wait=false keeps the async 202."""

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest
import respx

from app.clients.hospital_api import HospitalApiClient
from app.models import JobStatus
from app.progress_store import ProgressStore
from app.services.bulk_processor import BulkJobProcessor
from tests.conftest import FakeQueue
from tests.upstream import BASE_URL, FakeUpstream

CSV = b"name,address,phone\nCity Hospital,1 Main St,555-0100\nGeneral,2 Oak Ave,\n"


async def _no_sleep(seconds: float) -> None:
    return None


class InlineWorkerQueue(FakeQueue):
    """Behaves like arq + a worker: every enqueued job is processed in the background."""

    def __init__(self, store: ProgressStore, processor: BulkJobProcessor) -> None:
        super().__init__(store)
        self._processor = processor
        self.tasks: list[asyncio.Task[None]] = []

    async def enqueue(self, batch_id: str, job_id: str) -> None:
        await super().enqueue(batch_id, job_id)
        self.tasks.append(asyncio.create_task(self._processor.run(batch_id)))


@pytest.fixture
def upstream() -> AsyncIterator[FakeUpstream]:
    with respx.mock(assert_all_called=False) as router:
        yield FakeUpstream(router)


@pytest.fixture
async def queue(store: ProgressStore, settings, upstream: FakeUpstream) -> AsyncIterator[InlineWorkerQueue]:
    async with httpx.AsyncClient(base_url=BASE_URL) as http:
        processor = BulkJobProcessor(store, HospitalApiClient(http, max_retries=1, sleep=_no_sleep), settings)
        inline = InlineWorkerQueue(store, processor)
        yield inline
        await asyncio.gather(*inline.tasks)


async def _upload(api, **params):
    return await api.post("/hospitals/bulk", params=params, files={"file": ("h.csv", CSV, "text/csv")})


async def test_default_waits_and_returns_full_result(api, upstream: FakeUpstream) -> None:
    resp = await _upload(api)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "completed"
    assert (body["total_rows"], body["processed"], body["succeeded"], body["failed"]) == (2, 2, 2, 0)
    assert body["batch_activated"] is True
    assert body["processing_time_seconds"] is not None
    assert [(r["row"], r["status"]) for r in body["results"]] == [(1, "created"), (2, "created")]
    assert all(r["hospital_id"] for r in body["results"])
    assert upstream.activated == [body["batch_id"]]


async def test_sync_body_matches_status_endpoint(api) -> None:
    body = (await _upload(api, wait="true")).json()

    status = (await api.get(f"/hospitals/bulk/{body['batch_id']}/status")).json()
    assert body == status


async def test_sync_partial_failure_returns_200_with_row_errors(api, upstream: FakeUpstream) -> None:
    upstream.failures["General"] = [422]

    resp = await _upload(api)

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "partial_failure"
    assert (body["processed"], body["succeeded"], body["failed"]) == (2, 1, 1)
    assert body["batch_activated"] is False
    failed = body["results"][1]
    assert failed["status"] == "failed"
    assert "422" in failed["error"]
    assert upstream.activated == []


async def test_sync_timeout_returns_202_and_job_keeps_running(
    api, settings, store: ProgressStore, queue: InlineWorkerQueue, upstream: FakeUpstream
) -> None:
    settings.sync_wait_timeout_seconds = 0.05
    upstream.delay_seconds = 0.3

    resp = await _upload(api)

    assert resp.status_code == 202
    body = resp.json()
    batch_id = body["batch_id"]
    assert body["status_url"] == f"/hospitals/bulk/{batch_id}/status"
    assert body["status"] in ("queued", "processing")
    # The job was not lost: it finishes in the background and is visible via the status URL.
    await asyncio.gather(*queue.tasks)
    assert (await store.get_meta(batch_id)).status is JobStatus.COMPLETED


async def test_wait_false_returns_202_immediately(api, queue: InlineWorkerQueue, upstream: FakeUpstream) -> None:
    upstream.delay_seconds = 0.3

    resp = await _upload(api, wait="false")

    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "queued"
    assert body["duplicate"] is False
    assert body["status_url"] == f"/hospitals/bulk/{body['batch_id']}/status"
    assert upstream.created == [], "the response did not wait for processing"


async def test_duplicate_upload_in_sync_mode_waits_for_existing_job(
    api, queue: InlineWorkerQueue, upstream: FakeUpstream
) -> None:
    upstream.delay_seconds = 0.05
    first = (await _upload(api, wait="false")).json()

    resp = await _upload(api)

    assert resp.status_code == 200
    body = resp.json()
    assert body["batch_id"] == first["batch_id"]
    assert body["status"] == "completed"
    assert len(queue.jobs) == 1
    assert sorted(upstream.created_names) == ["City Hospital", "General"], "no duplicate hospitals"


async def test_duplicate_of_completed_job_in_sync_mode_returns_result_at_once(
    api, queue: InlineWorkerQueue, upstream: FakeUpstream
) -> None:
    first = (await _upload(api)).json()

    second = await _upload(api)

    assert second.status_code == 200
    assert second.json() == first
    assert len(queue.jobs) == 1
