"""Use cases behind the /hospitals/bulk endpoints. Route handlers only translate HTTP <-> these calls."""

from collections.abc import Callable
from datetime import UTC, datetime

from pydantic import BaseModel

from app.config import Settings
from app.job_queue import JobQueue
from app.models import JobStatus
from app.progress_store import ProgressStore


class QueueUnavailableError(Exception):
    """The job was validated and stored but could not be enqueued."""


class BulkSubmission(BaseModel):
    batch_id: str
    status: JobStatus
    total_rows: int
    duplicate: bool = False


def _utcnow() -> datetime:
    return datetime.now(UTC)


class BulkService:
    def __init__(
        self,
        store: ProgressStore,
        queue: JobQueue,
        settings: Settings,
        now: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._store = store
        self._queue = queue
        self._settings = settings
        self._now = now

    async def submit(self, content: bytes, filename: str | None) -> BulkSubmission:
        raise NotImplementedError
