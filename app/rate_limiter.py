"""Global cap on in-flight upstream requests, shared by every job and every worker process."""

import asyncio
import math
import random
import time
import uuid
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
        self._acquire_script = redis.register_script(_ACQUIRE_SCRIPT)

    async def try_acquire(self) -> str | None:
        """Take a slot if one is free. Returns a lease token, or None if the cap is reached."""
        token = uuid.uuid4().hex
        now = self._clock()
        acquired = await self._acquire_script(
            keys=[self._key], args=[now, now + self._lease, self._max, token, math.ceil(self._lease)]
        )
        return token if acquired else None

    async def acquire(self) -> str:
        while (token := await self.try_acquire()) is None:
            # Jitter so waiting workers don't all poll Redis in the same instant.
            await self._sleep(self._poll * random.uniform(0.5, 1.5))
        return token

    async def release(self, token: str) -> None:
        await self._redis.zrem(self._key, token)

    async def in_flight(self) -> int:
        return await self._redis.zcount(self._key, f"({self._clock()}", "+inf")

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        token = await self.acquire()
        try:
            yield
        finally:
            await self.release(token)


# Atomic check-and-take: drop expired leases, then add ours only if under the cap.
# Members are lease tokens, scores are lease expiry timestamps.
_ACQUIRE_SCRIPT = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[3]) then
    redis.call('ZADD', KEYS[1], ARGV[2], ARGV[4])
    redis.call('EXPIRE', KEYS[1], ARGV[5])
    return 1
end
return 0
"""
