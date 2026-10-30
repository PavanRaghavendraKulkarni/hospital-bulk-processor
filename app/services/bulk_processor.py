"""Processes one bulk job: creates each row upstream, records progress, then activates the batch."""

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime

import structlog

from app.clients.hospital_api import HospitalApiClient, UpstreamClientError, UpstreamError
from app.config import Settings
from app.logging_config import get_logger
from app.models import HospitalRow, JobMeta, JobStatus, RowResult, RowStatus
from app.progress_store import ProgressStore

logger = get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.now(UTC)


class BulkJobProcessor:
    def __init__(
        self,
        store: ProgressStore,
        client: HospitalApiClient,
        settings: Settings,
        now: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._store = store
        self._client = client
        self._settings = settings
        self._now = now

    async def run(self, batch_id: str) -> None:
        with structlog.contextvars.bound_contextvars(batch_id=batch_id):
            meta = await self._store.get_meta(batch_id)
            if meta is None:
                logger.warning("bulk_job_not_found")
                return

            started_at = self._now()
            await self._store.update_meta(
                batch_id, status=JobStatus.PROCESSING, started_at=started_at, finished_at=None, error=None
            )
            rows = await self._store.get_rows(batch_id)
            logger.info("bulk_job_started", total_rows=len(rows))

            semaphore = asyncio.Semaphore(self._settings.per_job_concurrency)
            await asyncio.gather(*(self._process_row(batch_id, row, semaphore) for row in rows))

            await self._finish(batch_id, started_at)

    async def mark_failed(self, batch_id: str, error: str) -> None:
        await self._store.update_meta(batch_id, status=JobStatus.FAILED, error=error, finished_at=self._now())

    async def _process_row(self, batch_id: str, row: HospitalRow, semaphore: asyncio.Semaphore) -> None:
        async with semaphore:
            result = await self._create(batch_id, row)
        await self._store.record_result(batch_id, result)
        logger.info("row_processed", row=row.row_number, status=result.status, error=result.error)

    async def _create(self, batch_id: str, row: HospitalRow) -> RowResult:
        failed = RowResult(row=row.row_number, name=row.name, status=RowStatus.FAILED)
        try:
            hospital = await self._client.create_hospital(
                name=row.name, address=row.address, phone=row.phone, batch_id=batch_id
            )
        except UpstreamClientError as exc:
            return failed.model_copy(update={"error": f"Rejected by upstream ({exc.status_code}): {exc.detail}"})
        except UpstreamError as exc:
            return failed.model_copy(update={"error": str(exc)})
        except Exception as exc:  # one bad row must not abort the whole job
            logger.exception("row_unexpected_error", row=row.row_number)
            return failed.model_copy(update={"error": f"Unexpected error: {exc}"})
        return RowResult(row=row.row_number, name=row.name, status=RowStatus.CREATED, hospital_id=hospital.id)

    async def _finish(self, batch_id: str, started_at: datetime) -> None:
        meta = await self._store.get_meta(batch_id)
        assert meta is not None
        activated, error = await self._maybe_activate(batch_id, meta)
        status = _final_status(meta, error)
        if status is JobStatus.FAILED and error is None:
            error = "All rows failed"

        finished_at = self._now()
        await self._store.update_meta(
            batch_id,
            status=status,
            finished_at=finished_at,
            processing_time_seconds=(finished_at - started_at).total_seconds(),
            batch_activated=activated,
            error=error,
        )
        logger.info(
            "bulk_job_finished",
            status=status,
            succeeded=meta.succeeded,
            failed=meta.failed,
            batch_activated=activated,
        )

    async def _maybe_activate(self, batch_id: str, meta: JobMeta) -> tuple[bool, str | None]:
        """Returns (batch_activated, error). Policy: never activate a partially created batch unless configured."""
        if meta.succeeded == 0:
            return meta.batch_activated, None
        if meta.failed and not self._settings.activate_on_partial_failure:
            logger.info("batch_activation_skipped", reason="partial_failure")
            return meta.batch_activated, None
        try:
            await self._client.activate_batch(batch_id)
        except UpstreamError as exc:
            logger.error("batch_activation_failed", error=str(exc))
            return meta.batch_activated, f"Batch activation failed: {exc}"
        return True, None


def _final_status(meta: JobMeta, activation_error: str | None) -> JobStatus:
    if activation_error or meta.succeeded == 0:
        return JobStatus.FAILED
    if meta.failed:
        return JobStatus.PARTIAL_FAILURE
    return JobStatus.COMPLETED
