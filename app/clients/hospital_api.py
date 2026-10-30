"""Async client for the upstream Hospital Directory API."""

import asyncio
from collections.abc import Awaitable, Callable

import httpx
from pydantic import BaseModel


class UpstreamHospital(BaseModel):
    id: int
    name: str
    address: str
    phone: str | None = None
    creation_batch_id: str | None = None
    active: bool | None = None


class UpstreamError(Exception):
    """Base class for upstream API failures."""


class UpstreamClientError(UpstreamError):
    """Non-retryable 4xx response: the request itself is wrong."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(f"upstream returned {status_code}: {detail}")
        self.status_code = status_code
        self.detail = detail


class UpstreamUnavailableError(UpstreamError):
    """Retryable failure (network error, 429, 5xx) that persisted after all retries."""


class HospitalApiClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        max_retries: int = 3,
        backoff_base_seconds: float = 0.5,
        backoff_max_seconds: float = 8.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._http = http
        self._max_retries = max_retries
        self._backoff_base = backoff_base_seconds
        self._backoff_max = backoff_max_seconds
        self._sleep = sleep

    async def create_hospital(
        self, *, name: str, address: str, phone: str | None, batch_id: str
    ) -> UpstreamHospital:
        raise NotImplementedError

    async def activate_batch(self, batch_id: str) -> None:
        raise NotImplementedError
