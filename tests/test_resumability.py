"""A worker can crash mid-job; when arq re-runs it, no hospital may be created twice."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx
import pytest
import respx

from app.clients.hospital_api import HospitalApiClient, UpstreamUnavailableError
from app.models import HospitalRow, JobMeta, JobStatus, RowResult, RowStatus
from app.progress_store import ProgressStore
from app.services.bulk_processor import BulkJobProcessor
from tests.upstream import BASE_URL, FakeUpstream

BATCH_ID = "3f2b8a1e-6c1d-4f7a-9b2e-0d5c8e7f1a23"


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


async def _seed(store: ProgressStore, n: int = 5) -> list[HospitalRow]:
    rows = [HospitalRow(row_number=i, name=f"H{i}", address=f"{i} Main St") for i in range(1, n + 1)]
    meta = JobMeta(
        batch_id=BATCH_ID, filename="h.csv", file_hash="x", total_rows=n, created_at=datetime(2026, 10, 30, tzinfo=UTC)
    )
    await store.create_job(meta, rows)
    return rows


async def _simulate_crash_after(store: ProgressStore, results: list[RowResult]) -> None:
    """State left behind by a worker that died mid-job."""
    await store.update_meta(BATCH_ID, status=JobStatus.PROCESSING, started_at=datetime(2026, 10, 30, tzinfo=UTC))
    for result in results:
        await store.record_result(BATCH_ID, result)


async def test_resume_skips_rows_already_created(store, processor, upstream: FakeUpstream) -> None:
    await _seed(store, 5)
    await _simulate_crash_after(
        store,
        [RowResult(row=i, name=f"H{i}", status=RowStatus.CREATED, hospital_id=500 + i) for i in (1, 2)],
    )

    await processor.run(BATCH_ID)

    assert sorted(upstream.created_names) == ["H3", "H4", "H5"]
    meta = await store.get_meta(BATCH_ID)
    assert meta.status is JobStatus.COMPLETED
    assert (meta.processed, meta.succeeded, meta.failed) == (5, 5, 0)
    assert (await store.get_results(BATCH_ID))[1].hospital_id == 501


async def test_resume_does_not_reprocess_recorded_failures(store, processor, upstream: FakeUpstream) -> None:
    await _seed(store, 3)
    await _simulate_crash_after(store, [RowResult(row=1, name="H1", status=RowStatus.FAILED, error="422")])

    await processor.run(BATCH_ID)

    assert sorted(upstream.created_names) == ["H2", "H3"]
    meta = await store.get_meta(BATCH_ID)
    assert (meta.processed, meta.succeeded, meta.failed) == (3, 2, 1)
    assert meta.status is JobStatus.PARTIAL_FAILURE


async def test_resume_reconciles_rows_created_upstream_but_not_recorded(
    store, processor, upstream: FakeUpstream
) -> None:
    """Crash window: upstream created H2, but the worker died before writing the result to Redis."""
    await _seed(store, 3)
    await _simulate_crash_after(store, [RowResult(row=1, name="H1", status=RowStatus.CREATED, hospital_id=501)])
    orphan = upstream.preexisting(BATCH_ID, "H2", "2 Main St")

    await processor.run(BATCH_ID)

    assert upstream.created_names.count("H2") == 1
    assert sorted(upstream.created_names) == ["H2", "H3"]
    result = (await store.get_results(BATCH_ID))[2]
    assert result.status is RowStatus.CREATED
    assert result.hospital_id == orphan["id"]
    meta = await store.get_meta(BATCH_ID)
    assert (meta.processed, meta.succeeded, meta.failed) == (3, 3, 0)


async def test_fresh_job_does_not_query_upstream_batch(store, processor, upstream: FakeUpstream) -> None:
    await _seed(store, 2)

    await processor.run(BATCH_ID)

    assert upstream.list_calls == 0


async def test_resume_aborts_if_reconciliation_impossible(store, processor, upstream: FakeUpstream) -> None:
    """Creating rows blind could duplicate hospitals, so the job fails and arq retries it later."""
    await _seed(store, 2)
    await _simulate_crash_after(store, [])
    upstream.list_failures = [503, 503]

    with pytest.raises(UpstreamUnavailableError):
        await processor.run(BATCH_ID)

    assert upstream.created == []
