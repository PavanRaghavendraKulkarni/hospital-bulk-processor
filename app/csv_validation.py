"""Synchronous CSV validation. Runs before anything is stored or queued."""

import csv
import io
import re

from pydantic import BaseModel

from app.models import HospitalRow

REQUIRED_COLUMNS = ("name", "address")
OPTIONAL_COLUMNS = ("phone",)
ALLOWED_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS
MAX_FIELD_LENGTH = 255
PHONE_PATTERN = re.compile(r"^[0-9+\-().\s/xX]{3,30}$")


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
    """Parse and validate an uploaded CSV, returning its rows or raising CsvValidationError."""
    if not filename or not filename.lower().endswith(".csv"):
        raise CsvValidationError("File must be a .csv file")
    if len(content) > max_bytes:
        raise CsvValidationError(f"File size exceeds the limit of {max_bytes} bytes")
    try:
        text = content.decode("utf-8-sig")  # utf-8-sig transparently strips a BOM
    except UnicodeDecodeError:
        raise CsvValidationError("File must be UTF-8 encoded") from None
    if not text.strip():
        raise CsvValidationError("File is empty")

    try:
        records = [r for r in csv.reader(io.StringIO(text), skipinitialspace=True) if any(cell.strip() for cell in r)]
    except csv.Error as exc:
        raise CsvValidationError(f"Malformed CSV: {exc}") from None

    columns = _parse_header(records[0])
    data = records[1:]
    if not data:
        raise CsvValidationError("File has a header but no data rows")
    if len(data) > max_rows:
        raise CsvValidationError(f"File has {len(data)} data rows; the maximum is {max_rows}")

    rows: list[HospitalRow] = []
    errors: list[RowError] = []
    seen: dict[tuple[str, str], int] = {}
    for row_number, record in enumerate(data, start=1):
        row, row_errors = _parse_row(row_number, record, columns)
        if row is not None:
            key = (row.name.casefold(), row.address.casefold())
            if key in seen:
                row_errors.append(
                    RowError(row=row_number, message=f"Duplicate of row {seen[key]} (same name and address)")
                )
            else:
                seen[key] = row_number
        errors.extend(row_errors)
        if row is not None and not row_errors:
            rows.append(row)

    if errors:
        raise CsvValidationError("CSV contains invalid rows", status_code=422, errors=errors)
    return rows


def _parse_header(header: list[str]) -> list[str]:
    columns = [c.strip().lower() for c in header]
    unknown = [c for c in columns if c not in ALLOWED_COLUMNS]
    duplicates = {c for c in columns if columns.count(c) > 1}
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if unknown or duplicates or missing:
        problems = []
        if missing:
            problems.append(f"missing {missing}")
        if unknown:
            problems.append(f"unknown {unknown}")
        if duplicates:
            problems.append(f"duplicated {sorted(duplicates)}")
        raise CsvValidationError(
            f"Invalid header ({'; '.join(problems)}). Expected columns: {','.join(ALLOWED_COLUMNS)}"
        )
    return columns


def _parse_row(row_number: int, record: list[str], columns: list[str]) -> tuple[HospitalRow | None, list[RowError]]:
    if len(record) > len(columns):
        return None, [RowError(row=row_number, message=f"Expected {len(columns)} columns, got {len(record)}")]

    values = {col: (record[i].strip() if i < len(record) else "") for i, col in enumerate(columns)}
    errors: list[RowError] = []
    for field in REQUIRED_COLUMNS:
        if not values[field]:
            errors.append(RowError(row=row_number, field=field, message=f"{field} is required"))
    for field, value in values.items():
        if len(value) > MAX_FIELD_LENGTH:
            errors.append(
                RowError(row=row_number, field=field, message=f"{field} exceeds {MAX_FIELD_LENGTH} characters")
            )
    phone = values.get("phone") or None
    if phone and not PHONE_PATTERN.match(phone):
        errors.append(RowError(row=row_number, field="phone", message="phone contains invalid characters"))

    if errors:
        return None, errors
    return HospitalRow(row_number=row_number, name=values["name"], address=values["address"], phone=phone), []
