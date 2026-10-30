"""Domain models shared by the API, the worker and the progress store."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel


class HospitalRow(BaseModel):
    """One validated CSV data row. row_number is 1-based and excludes the header."""

    row_number: int
    name: str
    address: str
    phone: str | None = None


class JobStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    PARTIAL_FAILURE = "partial_failure"
    FAILED = "failed"

    @property
    def is_final(self) -> bool:
        return self in (JobStatus.COMPLETED, JobStatus.PARTIAL_FAILURE, JobStatus.FAILED)


class RowStatus(StrEnum):
    CREATED = "created"
    FAILED = "failed"


class RowResult(BaseModel):
    row: int
    name: str
    status: RowStatus
    hospital_id: int | None = None
    error: str | None = None


class JobMeta(BaseModel):
    """Everything about a bulk job except its rows and per-row results. Stored as a Redis hash."""

    batch_id: str
    filename: str
    file_hash: str
    total_rows: int
    status: JobStatus = JobStatus.QUEUED
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    processing_time_seconds: float | None = None
    processed: int = 0
    succeeded: int = 0
    failed: int = 0
    batch_activated: bool = False
    error: str | None = None
