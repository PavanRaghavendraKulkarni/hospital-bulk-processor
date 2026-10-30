import asyncio
from datetime import UTC, datetime

from app.models import HospitalRow, JobMeta, JobStatus, RowResult, RowStatus
from app.progress_store import ProgressStore, file_hash_key, meta_key, results_key, rows_key
from tests.conftest import IDEMPOTENCY_TTL, TTL

BATCH_ID = "3f2b8a1e-6c1d-4f7a-9b2e-0d5c8e7f1a23"
CREATED_AT = datetime(2026, 10, 30, 12, 0, tzinfo=UTC)


def _meta(total_rows: int = 3) -> JobMeta:
    return JobMeta(
        batch_id=BATCH_ID, filename="h.csv", file_hash="abc", total_rows=total_rows, created_at=CREATED_AT
    )


def _rows(n: int = 3) -> list[HospitalRow]:
    return [HospitalRow(row_number=i, name=f"H{i}", address=f"Addr {i}", phone=None) for i in range(1, n + 1)]


def _created(row: int) -> RowResult:
    return RowResult(row=row, name=f"H{row}", status=RowStatus.CREATED, hospital_id=100 + row)


def _failed(row: int) -> RowResult:
    return RowResult(row=row, name=f"H{row}", status=RowStatus.FAILED, error="boom")


async def test_create_job_round_trips_meta_and_rows(store: ProgressStore) -> None:
    await store.create_job(_meta(), _rows())

    meta = await store.get_meta(BATCH_ID)
    assert meta == _meta()
    assert meta.status is JobStatus.QUEUED
    assert (meta.processed, meta.succeeded, meta.failed) == (0, 0, 0)
    assert meta.batch_activated is False
    assert await store.get_rows(BATCH_ID) == _rows()


async def test_rows_come_back_in_row_order(store: ProgressStore) -> None:
    await store.create_job(_meta(12), _rows(12))

    assert [r.row_number for r in await store.get_rows(BATCH_ID)] == list(range(1, 13))


async def test_unknown_batch_returns_none(store: ProgressStore) -> None:
    assert await store.get_meta("does-not-exist") is None
    assert await store.get_rows("does-not-exist") == []
    assert await store.get_results("does-not-exist") == {}


async def test_all_job_keys_get_a_ttl(store: ProgressStore, redis) -> None:
    await store.create_job(_meta(), _rows())
    await store.record_result(BATCH_ID, _created(1))

    for key in (meta_key(BATCH_ID), rows_key(BATCH_ID), results_key(BATCH_ID)):
        ttl = await redis.ttl(key)
        assert 0 < ttl <= TTL, key


async def test_record_result_stores_result_and_updates_counters(store: ProgressStore) -> None:
    await store.create_job(_meta(), _rows())

    await store.record_result(BATCH_ID, _created(1))
    await store.record_result(BATCH_ID, _failed(2))

    meta = await store.get_meta(BATCH_ID)
    assert (meta.processed, meta.succeeded, meta.failed) == (2, 1, 1)
    results = await store.get_results(BATCH_ID)
    assert results == {1: _created(1), 2: _failed(2)}


async def test_concurrent_record_result_keeps_counters_exact(store: ProgressStore) -> None:
    n = 50
    await store.create_job(_meta(n), _rows(n))

    await asyncio.gather(
        *(store.record_result(BATCH_ID, _created(i) if i % 3 else _failed(i)) for i in range(1, n + 1))
    )

    meta = await store.get_meta(BATCH_ID)
    expected_failed = sum(1 for i in range(1, n + 1) if i % 3 == 0)
    assert (meta.processed, meta.succeeded, meta.failed) == (n, n - expected_failed, expected_failed)
    assert len(await store.get_results(BATCH_ID)) == n


async def test_update_meta_sets_and_clears_fields(store: ProgressStore) -> None:
    await store.create_job(_meta(), _rows())
    started = datetime(2026, 10, 30, 12, 1, tzinfo=UTC)

    await store.update_meta(
        BATCH_ID, status=JobStatus.PROCESSING, started_at=started, batch_activated=True, error="x"
    )
    meta = await store.get_meta(BATCH_ID)
    assert meta.status is JobStatus.PROCESSING
    assert meta.started_at == started
    assert meta.batch_activated is True
    assert meta.error == "x"

    await store.update_meta(BATCH_ID, error=None, batch_activated=False)
    meta = await store.get_meta(BATCH_ID)
    assert meta.error is None
    assert meta.batch_activated is False


async def test_reset_failed_rows_clears_only_failures_and_adjusts_counters(store: ProgressStore) -> None:
    await store.create_job(_meta(), _rows())
    await store.record_result(BATCH_ID, _created(1))
    await store.record_result(BATCH_ID, _failed(2))
    await store.record_result(BATCH_ID, _failed(3))

    reset = await store.reset_failed_rows(BATCH_ID)

    assert reset == [2, 3]
    meta = await store.get_meta(BATCH_ID)
    assert (meta.processed, meta.succeeded, meta.failed) == (1, 1, 0)
    assert list(await store.get_results(BATCH_ID)) == [1]


async def test_reset_failed_rows_with_no_failures_is_a_noop(store: ProgressStore) -> None:
    await store.create_job(_meta(), _rows())
    await store.record_result(BATCH_ID, _created(1))

    assert await store.reset_failed_rows(BATCH_ID) == []
    assert (await store.get_meta(BATCH_ID)).processed == 1


async def test_refresh_ttl_resets_expiry(store: ProgressStore, redis) -> None:
    await store.create_job(_meta(), _rows())
    await redis.expire(meta_key(BATCH_ID), 5)

    await store.refresh_ttl(BATCH_ID)

    assert await redis.ttl(meta_key(BATCH_ID)) > 5


async def test_claim_file_hash_first_wins(store: ProgressStore, redis) -> None:
    assert await store.claim_file_hash("sha", "batch-1") is None
    assert await store.claim_file_hash("sha", "batch-2") == "batch-1"
    assert 0 < await redis.ttl(file_hash_key("sha")) <= IDEMPOTENCY_TTL


async def test_replace_file_hash_points_to_new_batch(store: ProgressStore) -> None:
    await store.claim_file_hash("sha", "batch-1")

    await store.replace_file_hash("sha", "batch-2")

    assert await store.claim_file_hash("sha", "batch-3") == "batch-2"


async def test_delete_job_removes_all_job_keys(store: ProgressStore, redis) -> None:
    await store.create_job(_meta(), _rows())
    await store.record_result(BATCH_ID, _created(1))

    await store.delete_job(BATCH_ID)

    assert await redis.keys("bulk:*") == []
