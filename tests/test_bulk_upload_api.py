import hashlib
import uuid

from redis.exceptions import ConnectionError as RedisConnectionError

from app.models import JobStatus
from app.progress_store import ProgressStore
from tests.conftest import FakeQueue

VALID_CSV = b"name,address,phone\nCity Hospital,1 Main St,555-0100\nGeneral,2 Oak Ave,\n"


async def _upload(api, content: bytes, filename: str = "hospitals.csv", content_type: str = "text/csv"):
    return await api.post(
        "/hospitals/bulk", params={"wait": "false"}, files={"file": (filename, content, content_type)}
    )


async def test_valid_upload_returns_202_and_queues_job(api, store: ProgressStore, queue: FakeQueue) -> None:
    resp = await _upload(api, VALID_CSV)

    assert resp.status_code == 202
    body = resp.json()
    batch_id = body["batch_id"]
    assert uuid.UUID(batch_id).version == 4
    assert body["status"] == "queued"
    assert body["total_rows"] == 2
    assert body["duplicate"] is False
    assert body["status_url"] == f"/hospitals/bulk/{batch_id}/status"
    assert queue.jobs == [(batch_id, batch_id)]


async def test_valid_upload_stores_meta_and_rows_in_redis(api, store: ProgressStore) -> None:
    batch_id = (await _upload(api, VALID_CSV)).json()["batch_id"]

    meta = await store.get_meta(batch_id)
    assert meta.status is JobStatus.QUEUED
    assert meta.filename == "hospitals.csv"
    assert meta.total_rows == 2
    assert meta.file_hash == hashlib.sha256(VALID_CSV).hexdigest()
    assert meta.created_at is not None
    rows = await store.get_rows(batch_id)
    assert [(r.row_number, r.name, r.phone) for r in rows] == [(1, "City Hospital", "555-0100"), (2, "General", None)]


async def test_file_level_error_returns_400_and_nothing_is_queued(api, redis, queue: FakeQueue) -> None:
    resp = await _upload(api, VALID_CSV, filename="hospitals.txt")

    assert resp.status_code == 400
    assert "csv" in resp.json()["detail"].lower()
    assert queue.jobs == []
    assert await redis.keys("bulk:*") == []


async def test_row_errors_return_422_with_row_details(api, redis, queue: FakeQueue) -> None:
    resp = await _upload(api, b"name,address,phone\n,1 Main St,\nGeneral,,\n")

    assert resp.status_code == 422
    errors = resp.json()["errors"]
    assert [(e["row"], e["field"]) for e in errors] == [(1, "name"), (2, "address")]
    assert queue.jobs == []
    assert await redis.keys("bulk:*") == []


async def test_oversized_upload_returns_400(api, settings) -> None:
    big = b"name,address,phone\n" + b"A,B,\n" * (settings.max_upload_bytes // 4)

    resp = await _upload(api, big)

    assert resp.status_code == 400
    assert "size" in resp.json()["detail"].lower()


async def test_empty_file_returns_400(api) -> None:
    resp = await _upload(api, b"")

    assert resp.status_code == 400
    assert "empty" in resp.json()["detail"].lower()


async def test_missing_file_field_returns_422(api) -> None:
    resp = await api.post("/hospitals/bulk")

    assert resp.status_code == 422


async def test_queue_failure_returns_503_and_marks_job_failed(
    api, redis, store: ProgressStore, queue: FakeQueue
) -> None:
    queue.fail_with = RedisConnectionError("queue down")

    resp = await _upload(api, VALID_CSV)

    assert resp.status_code == 503
    keys = await redis.keys("bulk:*:meta")
    assert len(keys) == 1
    meta = await store.get_meta(keys[0].split(":")[1])
    assert meta.status is JobStatus.FAILED
    assert "enqueue" in meta.error
