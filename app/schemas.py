"""Pydantic request/response models exposed by the API."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from app.csv_validation import RowError
from app.models import JobStatus, RowResult


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    redis: Literal["ok", "unavailable"]


class ErrorResponse(BaseModel):
    detail: str
    errors: list[RowError] = []


class BulkUploadResponse(BaseModel):
    batch_id: str
    status: JobStatus
    total_rows: int
    duplicate: bool = False
    status_url: str


class BulkStatusResponse(BaseModel):
    batch_id: str
    filename: str
    status: JobStatus
    total_rows: int
    processed: int
    succeeded: int
    failed: int
    percent_complete: float
    batch_activated: bool
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    processing_time_seconds: float | None = None
    error: str | None = None
    results: list[RowResult]
