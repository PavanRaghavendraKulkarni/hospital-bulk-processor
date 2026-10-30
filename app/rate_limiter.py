"""Global cap on in-flight upstream requests, shared by every job and every worker process."""

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from redis.asyncio import Redis

UPSTREAM_INFLIGHT_KEY = "upstream:inflight"


class GlobalRateLimiter:
    def __init__(
        self,
        redis: Redis,
        *,
        max_in_flight: int,
        lease_seconds: float,
        key: str = UPSTREAM_INFLIGHT_KEY,
        poll_interval_seconds: float = 0.05,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._redis = redis
        self._max = max_in_flight
        self._lease = lease_seconds
        self._key = key
        self._poll = poll_interval_seconds
        self._clock = clock
        self._sleep = sleep

    async def try_acquire(self) -> str | None:
        raise NotImplementedError

    async def acquire(self) -> str:
        raise NotImplementedError

    async def release(self, token: str) -> None:
        raise NotImplementedError

    async def in_flight(self) -> int:
        raise NotImplementedError

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        raise NotImplementedError
        yield
