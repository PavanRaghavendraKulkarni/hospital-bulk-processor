"""Domain models shared by the API, the worker and the progress store."""

from enum import StrEnum

from pydantic import BaseModel


class HospitalRow(BaseModel):
    """One validated CSV data row. row_number is 1-based and excludes the header."""

    row_number: int
    name: str
    address: str
    phone: str | None = None
