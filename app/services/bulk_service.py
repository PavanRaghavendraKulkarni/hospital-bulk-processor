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

# A re-upload of the same file maps to the existing job only if that job is alive or succeeded.
# After a (partial) failure the user may re-upload to start fresh, or use /retry on the old batch.
REUSABLE_STATUSES = frozenset({JobStatus.QUEUED, JobStatus.PROCESSING, JobStatus.COMPLETED})


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

        # Store first, then claim the hash: whoever loses the claim can rely on the winner's meta existing.
        await self._store.create_job(meta, rows)
        existing = await self._find_reusable_job(meta.file_hash, batch_id)
        if existing is not None:
            await self._store.delete_job(batch_id)
            log.info("duplicate_upload", existing_batch_id=existing.batch_id, existing_status=existing.status)
            return BulkSubmission(
                batch_id=existing.batch_id, status=existing.status, total_rows=existing.total_rows, duplicate=True
            )

        await self._enqueue(batch_id, job_id=batch_id)
        log.info("bulk_job_queued", filename=meta.filename, total_rows=meta.total_rows)
        return BulkSubmission(batch_id=batch_id, status=JobStatus.QUEUED, total_rows=meta.total_rows)

    async def get_status(self, batch_id: str) -> BulkStatusResponse | None:
        meta = await self._store.get_meta(batch_id)
        if meta is None:
            return None
        results = await self._store.get_results(batch_id)
        percent = round(meta.processed / meta.total_rows * 100, 1) if meta.total_rows else 0.0
        return BulkStatusResponse(
            **meta.model_dump(exclude={"file_hash"}),
            percent_complete=percent,
            results=list(results.values()),
        )

    async def _find_reusable_job(self, file_hash: str, batch_id: str) -> JobMeta | None:
        """Return the job a duplicate upload should map to, or None if this upload should run as new."""
        existing_id = await self._store.claim_file_hash(file_hash, batch_id)
        if existing_id is None:
            return None
        existing = await self._store.get_meta(existing_id)
        if existing is not None and existing.status in REUSABLE_STATUSES:
            return existing
        # Previous attempt failed or expired: this upload becomes the canonical batch for the hash.
        await self._store.replace_file_hash(file_hash, batch_id)
        return None

    async def _enqueue(self, batch_id: str, job_id: str) -> None:
        try:
            await self._queue.enqueue(batch_id, job_id=job_id)
        except Exception as exc:
            logger.exception("enqueue_failed", batch_id=batch_id)
            await self._store.update_meta(batch_id, status=JobStatus.FAILED, error=f"Could not enqueue job: {exc}")
            raise QueueUnavailableError(str(exc)) from exc
