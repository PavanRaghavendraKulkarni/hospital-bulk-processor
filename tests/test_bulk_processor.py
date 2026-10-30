import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta

import fakeredis
import httpx
import pytest
import respx

from app.clients.hospital_api import HospitalApiClient
from app.config import Settings
from app.models import HospitalRow, JobMeta, JobStatus, RowStatus
from app.progress_store import ProgressStore
from app.rate_limiter import GlobalRateLimiter
from app.services.bulk_processor import BulkJobProcessor
from tests.conftest import IDEMPOTENCY_TTL, TTL
from tests.upstream import BASE_URL, FakeUpstream

BATCH_ID = "3f2b8a1e-6c1d-4f7a-9b2e-0d5c8e7f1a23"


class StepClock:
    """Each call returns a time 1.5s after the previous one."""

    def __init__(self) -> None:
        self._t = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        self._t += timedelta(seconds=1.5)
        return self._t


async def _no_sleep(seconds: float) -> None:
    return None


@pytest.fixture
def upstream() -> AsyncIterator[FakeUpstream]:
    with respx.mock(assert_all_called=False) as router:
        yield FakeUpstream(router)


@pytest.fixture
async def http() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(base_url=BASE_URL) as client:
        yield client


def _client(http: httpx.AsyncClient, limiter: GlobalRateLimiter | None = None) -> HospitalApiClient:
    return HospitalApiClient(http, max_retries=2, sleep=_no_sleep, limiter=limiter)


def _processor(store, http, settings, limiter=None) -> BulkJobProcessor:
    return BulkJobProcessor(store, _client(http, limiter), settings, now=StepClock())


async def _seed(store: ProgressStore, names: list[str], batch_id: str = BATCH_ID) -> None:
    rows = [HospitalRow(row_number=i, name=n, address=f"{i} Main St") for i, n in enumerate(names, start=1)]
    meta = JobMeta(
        batch_id=batch_id,
        filename="h.csv",
        file_hash="x",
        total_rows=len(rows),
        created_at=datetime(2026, 10, 30, tzinfo=UTC),
    )
    await store.create_job(meta, rows)


# --- happy path ---------------------------------------------------------------


async def test_all_rows_created_then_batch_activated(store, http, settings, upstream: FakeUpstream) -> None:
    await _seed(store, ["A", "B", "C"])

    await _processor(store, http, settings).run(BATCH_ID)

    assert sorted(upstream.created_names) == ["A", "B", "C"]
    assert all(h["creation_batch_id"] == BATCH_ID for h in upstream.created)
    assert upstream.activated == [BATCH_ID]
    meta = await store.get_meta(BATCH_ID)
    assert meta.status is JobStatus.COMPLETED
    assert (meta.processed, meta.succeeded, meta.failed) == (3, 3, 0)
    assert meta.batch_activated is True
    assert meta.started_at is not None and meta.finished_at > meta.started_at
    assert meta.processing_time_seconds == (meta.finished_at - meta.started_at).total_seconds()
    results = await store.get_results(BATCH_ID)
    assert {r.status for r in results.values()} == {RowStatus.CREATED}
    assert sorted(r.hospital_id for r in results.values()) == [1, 2, 3]


async def test_transient_5xx_is_retried_and_row_succeeds(store, http, settings, upstream: FakeUpstream) -> None:
    await _seed(store, ["A", "B"])
    upstream.failures["A"] = [503, 502]

    await _processor(store, http, settings).run(BATCH_ID)

    meta = await store.get_meta(BATCH_ID)
    assert meta.status is JobStatus.COMPLETED
    assert sorted(upstream.created_names) == ["A", "B"]


# --- failures -----------------------------------------------------------------


async def test_partial_failure_does_not_activate(store, http, settings, upstream: FakeUpstream) -> None:
    await _seed(store, ["A", "B", "C"])
    upstream.failures["B"] = [422]

    await _processor(store, http, settings).run(BATCH_ID)

    meta = await store.get_meta(BATCH_ID)
    assert meta.status is JobStatus.PARTIAL_FAILURE
    assert (meta.processed, meta.succeeded, meta.failed) == (3, 2, 1)
    assert meta.batch_activated is False
    assert upstream.activated == []
    failed = (await store.get_results(BATCH_ID))[2]
    assert failed.status is RowStatus.FAILED
    assert "422" in failed.error


async def test_4xx_row_is_not_retried(store, http, settings, upstream: FakeUpstream) -> None:
    await _seed(store, ["A"])
    upstream.failures["A"] = [400, 400, 400]

    await _processor(store, http, settings).run(BATCH_ID)

    assert upstream.failures["A"] == [400, 400], "only one attempt should have been made"


async def test_exhausted_retries_marks_row_failed(store, http, settings, upstream: FakeUpstream) -> None:
    await _seed(store, ["A", "B"])
    upstream.failures["A"] = [503, 503, 503]

    await _processor(store, http, settings).run(BATCH_ID)

    meta = await store.get_meta(BATCH_ID)
    assert meta.status is JobStatus.PARTIAL_FAILURE
    assert "503" in (await store.get_results(BATCH_ID))[1].error


async def test_partial_failure_activates_when_policy_allows(store, http, upstream: FakeUpstream) -> None:
    settings = Settings(_env_file=None, activate_on_partial_failure=True)
    await _seed(store, ["A", "B"])
    upstream.failures["B"] = [422]

    await _processor(store, http, settings).run(BATCH_ID)

    meta = await store.get_meta(BATCH_ID)
    assert meta.status is JobStatus.PARTIAL_FAILURE
    assert meta.batch_activated is True
    assert upstream.activated == [BATCH_ID]


async def test_all_rows_failing_marks_job_failed(store, http, settings, upstream: FakeUpstream) -> None:
    await _seed(store, ["A", "B"])
    upstream.failures.update({"A": [400], "B": [409]})

    await _processor(store, http, settings).run(BATCH_ID)

    meta = await store.get_meta(BATCH_ID)
    assert meta.status is JobStatus.FAILED
    assert meta.batch_activated is False
    assert upstream.activated == []


async def test_activation_failure_marks_job_failed(store, http, settings, upstream: FakeUpstream) -> None:
    await _seed(store, ["A"])
    upstream.activate_failures = [503, 503, 503]

    await _processor(store, http, settings).run(BATCH_ID)

    meta = await store.get_meta(BATCH_ID)
    assert meta.status is JobStatus.FAILED
    assert meta.batch_activated is False
    assert "activat" in meta.error.lower()


async def test_unknown_batch_is_a_noop(store, http, settings, upstream: FakeUpstream) -> None:
    await _processor(store, http, settings).run("missing")

    assert upstream.created == []


async def test_mark_failed_records_error(store, http, settings) -> None:
    await _seed(store, ["A"])

    await _processor(store, http, settings).mark_failed(BATCH_ID, "worker gave up")

    meta = await store.get_meta(BATCH_ID)
    assert meta.status is JobStatus.FAILED
    assert meta.error == "worker gave up"
    assert meta.finished_at is not None


# --- concurrency control ------------------------------------------------------


async def test_per_job_concurrency_is_bounded(store, http, upstream: FakeUpstream) -> None:
    settings = Settings(_env_file=None, per_job_concurrency=3)
    await _seed(store, [f"H{i}" for i in range(12)])
    upstream.delay_seconds = 0.01

    await _processor(store, http, settings).run(BATCH_ID)

    assert upstream.peak_in_flight == 3
    assert len(upstream.created) == 12


async def test_global_rate_limit_respected_across_concurrent_jobs(
    fake_server, http, upstream: FakeUpstream
) -> None:
    """Two jobs on two 'workers' (separate Redis clients), each allowed 5 in parallel, share a global cap of 4."""
    settings = Settings(_env_file=None, per_job_concurrency=5)
    upstream.delay_seconds = 0.01
    processors = []
    for batch in ("11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"):
        client = fakeredis.FakeAsyncRedis(server=fake_server, decode_responses=True)
        store = ProgressStore(client, ttl_seconds=TTL, idempotency_ttl_seconds=IDEMPOTENCY_TTL)
        limiter = GlobalRateLimiter(client, max_in_flight=4, lease_seconds=30, poll_interval_seconds=0.001)
        await _seed(store, [f"{batch[:1]}-{i}" for i in range(10)], batch_id=batch)
        processors.append((_processor(store, http, settings, limiter), batch))

    await asyncio.gather(*(p.run(batch) for p, batch in processors))

    assert upstream.peak_in_flight == 4
    assert len(upstream.created) == 20
