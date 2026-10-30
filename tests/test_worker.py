import pytest
from arq.worker import Retry

from app.worker import process_bulk_job


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.ran: list[str] = []
        self.failed: list[tuple[str, str]] = []

    async def run(self, batch_id: str) -> None:
        self.ran.append(batch_id)
        if self.error:
            raise self.error

    async def mark_failed(self, batch_id: str, error: str) -> None:
        self.failed.append((batch_id, error))


async def test_runs_the_processor() -> None:
    processor = FakeProcessor()

    await process_bulk_job({"processor": processor, "job_try": 1}, "b1")

    assert processor.ran == ["b1"]


async def test_unexpected_error_is_retried_with_backoff() -> None:
    processor = FakeProcessor(ConnectionError("redis blip"))

    with pytest.raises(Retry):
        await process_bulk_job({"processor": processor, "job_try": 1}, "b1")

    assert processor.failed == []


async def test_last_attempt_marks_job_failed(settings) -> None:
    processor = FakeProcessor(ConnectionError("redis down"))

    with pytest.raises(ConnectionError):
        await process_bulk_job({"processor": processor, "job_try": settings.worker_max_tries}, "b1")

    assert processor.failed and processor.failed[0][0] == "b1"
    assert "redis down" in processor.failed[0][1]
