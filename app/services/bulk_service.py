"""Use cases behind the /hospitals/bulk endpoints. Route handlers only translate HTTP <-> these calls."""

import hashlib
import uuid
from collections.abc import Callable
from datetime import UTC, datetime

from pydantic import BaseModel

from app.config import Settings
from app.csv_validation import validate_csv
from app.job_queue import JobQueue
from app.logging_config import get_logger
from app.models import JobMeta, JobStatus
from app.progress_store import ProgressStore
from app.schemas import BulkStatusResponse

logger = get_logger(__name__)


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
        """Validate, persist and enqueue an upload. Raises CsvValidationError before touching Redis."""
        rows = validate_csv(
            content,
            filename=filename,
            max_rows=self._settings.max_csv_rows,
            max_bytes=self._settings.max_upload_bytes,
        )
        batch_id = str(uuid.uuid4())
        meta = JobMeta(
            batch_id=batch_id,
            filename=filename or "",
            file_hash=hashlib.sha256(content).hexdigest(),
            total_rows=len(rows),
            created_at=self._now(),
        )
        log = logger.bind(batch_id=batch_id)

        await self._store.create_job(meta, rows)
        await self._enqueue(batch_id, job_id=batch_id)
        log.info("bulk_job_queued", filename=meta.filename, total_rows=meta.total_rows)
        return BulkSubmission(batch_id=batch_id, status=JobStatus.QUEUED, total_rows=meta.total_rows)

    async def get_status(self, batch_id: str) -> BulkStatusResponse | None:
        raise NotImplementedError

    async def _enqueue(self, batch_id: str, job_id: str) -> None:
        try:
            await self._queue.enqueue(batch_id, job_id=job_id)
        except Exception as exc:
            logger.exception("enqueue_failed", batch_id=batch_id)
            await self._store.update_meta(batch_id, status=JobStatus.FAILED, error=f"Could not enqueue job: {exc}")
            raise QueueUnavailableError(str(exc)) from exc
