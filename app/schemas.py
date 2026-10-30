"""Pydantic request/response models exposed by the API."""

from typing import Literal

from pydantic import BaseModel

from app.csv_validation import RowError
from app.models import JobStatus


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
