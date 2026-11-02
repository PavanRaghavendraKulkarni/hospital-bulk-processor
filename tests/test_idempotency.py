import asyncio

import pytest

from app.models import JobStatus
from app.progress_store import ProgressStore
from tests.conftest import FakeQueue

CSV = b"name,address,phone\nCity Hospital,1 Main St,555-0100\nGeneral,2 Oak Ave,\n"


async def _upload(api, content: bytes = CSV):
    resp = await api.post("/hospitals/bulk", params={"wait": "false"}, files={"file": ("h.csv", content, "text/csv")})
    assert resp.status_code == 202, resp.text
    return resp.json()


async def test_same_file_while_queued_returns_existing_batch(api, redis, queue: FakeQueue) -> None:
    first = await _upload(api)
    second = await _upload(api)

    assert second["batch_id"] == first["batch_id"]
    assert second["duplicate"] is True
    assert first["duplicate"] is False
    assert len(queue.jobs) == 1
    assert len(await redis.keys("bulk:*:meta")) == 1


@pytest.mark.parametrize("status", [JobStatus.PROCESSING, JobStatus.COMPLETED])
async def test_same_file_while_processing_or_completed_returns_existing(
    api, store: ProgressStore, queue: FakeQueue, status: JobStatus
) -> None:
    first = await _upload(api)
    await store.update_meta(first["batch_id"], status=status)

    second = await _upload(api)

    assert second["batch_id"] == first["batch_id"]
    assert second["status"] == status.value
    assert len(queue.jobs) == 1


@pytest.mark.parametrize("status", [JobStatus.PARTIAL_FAILURE, JobStatus.FAILED])
async def test_same_file_after_failure_creates_new_batch(
    api, store: ProgressStore, queue: FakeQueue, status: JobStatus
) -> None:
    first = await _upload(api)
    await store.update_meta(first["batch_id"], status=status)

    second = await _upload(api)
    third = await _upload(api)

    assert second["batch_id"] != first["batch_id"]
    assert second["duplicate"] is False
    assert third["batch_id"] == second["batch_id"], "the hash now points at the new batch"
    assert len(queue.jobs) == 2


async def test_expired_job_behind_hash_creates_new_batch(api, store: ProgressStore, queue: FakeQueue) -> None:
    first = await _upload(api)
    await store.delete_job(first["batch_id"])

    second = await _upload(api)

    assert second["batch_id"] != first["batch_id"]
    assert len(queue.jobs) == 2


async def test_different_content_creates_different_batches(api, queue: FakeQueue) -> None:
    first = await _upload(api)
    second = await _upload(api, CSV + b"Third,3 Elm St,\n")

    assert first["batch_id"] != second["batch_id"]
    assert len(queue.jobs) == 2


async def test_concurrent_identical_uploads_create_one_batch(api, redis, queue: FakeQueue) -> None:
    responses = await asyncio.gather(*(_upload(api) for _ in range(5)))

    assert len({r["batch_id"] for r in responses}) == 1
    assert sum(not r["duplicate"] for r in responses) == 1
    assert len(queue.jobs) == 1
    assert len(await redis.keys("bulk:*:meta")) == 1
