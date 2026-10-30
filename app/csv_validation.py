"""Synchronous CSV validation. Runs before anything is stored or queued."""

from pydantic import BaseModel

from app.models import HospitalRow


class RowError(BaseModel):
    row: int | None = None
    field: str | None = None
    message: str


class CsvValidationError(Exception):
    """Raised when an upload is rejected. status_code is 400 for file-level, 422 for row-level problems."""

    def __init__(self, message: str, *, status_code: int = 400, errors: list[RowError] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.errors = errors or []


def validate_csv(
    content: bytes,
    *,
    filename: str | None,
    max_rows: int,
    max_bytes: int,
) -> list[HospitalRow]:
    raise NotImplementedError
