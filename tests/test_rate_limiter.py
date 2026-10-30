import asyncio

import fakeredis
import pytest

from app.rate_limiter import GlobalRateLimiter


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


def _limiter(redis, max_in_flight: int = 2, **kwargs) -> GlobalRateLimiter:
    return GlobalRateLimiter(
        redis, max_in_flight=max_in_flight, lease_seconds=30, poll_interval_seconds=0.001, **kwargs
    )


async def test_try_acquire_up_to_limit(redis) -> None:
    limiter = _limiter(redis, max_in_flight=2)

    assert await limiter.try_acquire() is not None
    assert await limiter.try_acquire() is not None
    assert await limiter.try_acquire() is None
    assert await limiter.in_flight() == 2


async def test_release_frees_a_slot(redis) -> None:
    limiter = _limiter(redis, max_in_flight=1)
    token = await limiter.try_acquire()

    await limiter.release(token)

    assert await limiter.in_flight() == 0
    assert await limiter.try_acquire() is not None


async def test_expired_leases_are_reclaimed(redis) -> None:
    """A worker that crashes while holding a slot must not leak it forever."""
    clock = FakeClock()
    limiter = _limiter(redis, max_in_flight=1, clock=clock)
    assert await limiter.try_acquire() is not None
    assert await limiter.try_acquire() is None

    clock.now += 31

    assert await limiter.try_acquire() is not None


async def test_slot_releases_on_exception(redis) -> None:
    limiter = _limiter(redis, max_in_flight=1)

    with pytest.raises(RuntimeError):
        async with limiter.slot():
            raise RuntimeError("upstream exploded")

    assert await limiter.in_flight() == 0


async def test_acquire_waits_for_a_free_slot(redis) -> None:
    limiter = _limiter(redis, max_in_flight=1)
    token = await limiter.acquire()

    waiter = asyncio.create_task(limiter.acquire())
    await asyncio.sleep(0.02)
    assert not waiter.done()

    await limiter.release(token)
    await asyncio.wait_for(waiter, timeout=1)


async def test_limit_is_global_across_limiter_instances(fake_server) -> None:
    """Three 'workers' (separate clients, same Redis) running 30 calls must never exceed the cap together."""
    cap = 4
    clients = [fakeredis.FakeAsyncRedis(server=fake_server, decode_responses=True) for _ in range(3)]
    limiters = [_limiter(c, max_in_flight=cap) for c in clients]
    current = peak = 0

    async def call(limiter: GlobalRateLimiter) -> None:
        nonlocal current, peak
        async with limiter.slot():
            current += 1
            peak = max(peak, current)
            await asyncio.sleep(0.005)
            current -= 1

    await asyncio.gather(*(call(limiters[i % 3]) for i in range(30)))

    assert peak == cap
    assert await limiters[0].in_flight() == 0
    for c in clients:
        await c.aclose()
