"""Async client for the upstream Hospital Directory API."""

import asyncio
import random
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, nullcontext
from typing import Any, Protocol

import httpx
from pydantic import BaseModel

from app.logging_config import get_logger

logger = get_logger(__name__)


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


class UpstreamLimiter(Protocol):
    def slot(self) -> AbstractAsyncContextManager[None]: ...


class HospitalApiClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        max_retries: int = 3,
        backoff_base_seconds: float = 0.5,
        backoff_max_seconds: float = 8.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        limiter: UpstreamLimiter | None = None,
    ) -> None:
        self._http = http
        self._max_retries = max_retries
        self._backoff_base = backoff_base_seconds
        self._backoff_max = backoff_max_seconds
        self._sleep = sleep
        self._limiter = limiter

    async def create_hospital(
        self, *, name: str, address: str, phone: str | None, batch_id: str
    ) -> UpstreamHospital:
        payload = {"name": name, "address": address, "phone": phone, "creation_batch_id": batch_id}
        response = await self._request("POST", "/hospitals/", json=payload)
        return UpstreamHospital.model_validate(response.json())

    async def activate_batch(self, batch_id: str) -> None:
        await self._request("PATCH", f"/hospitals/batch/{batch_id}/activate")

    async def _request(self, method: str, path: str, json: Any = None) -> httpx.Response:
        """Send a request, retrying only on network errors, 429 and 5xx. 4xx raise immediately."""
        attempts = self._max_retries + 1
        for attempt in range(attempts):
            retry_after: float | None = None
            try:
                # Hold a global slot only for the HTTP call itself, never during backoff.
                async with self._limiter.slot() if self._limiter else nullcontext():
                    response = await self._http.request(method, path, json=json)
            except httpx.TransportError as exc:
                failure = f"{type(exc).__name__}: {exc}"
            else:
                if response.is_success:
                    return response
                if not _is_retryable(response.status_code):
                    raise UpstreamClientError(response.status_code, response.text[:500])
                failure = f"HTTP {response.status_code}"
                retry_after = _parse_retry_after(response)

            if attempt == attempts - 1:
                break
            delay = self._backoff_delay(attempt, retry_after)
            logger.warning(
                "upstream_retry",
                method=method,
                path=path,
                attempt=attempt + 1,
                failure=failure,
                delay_seconds=round(delay, 2),
            )
            await self._sleep(delay)

        raise UpstreamUnavailableError(f"{method} {path} failed after {attempts} attempts: {failure}")

    def _backoff_delay(self, attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return min(retry_after, self._backoff_max)
        nominal = min(self._backoff_max, self._backoff_base * 2**attempt)
        # Jitter spreads retries from concurrent rows so they don't hit upstream in lockstep.
        return nominal * random.uniform(0.5, 1.0)


def _is_retryable(status_code: int) -> bool:
    return status_code == 429 or status_code >= 500


def _parse_retry_after(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None  # HTTP-date form: fall back to normal backoff
