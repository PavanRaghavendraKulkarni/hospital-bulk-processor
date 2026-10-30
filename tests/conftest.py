from collections.abc import AsyncIterator

import fakeredis
import pytest

from app.progress_store import ProgressStore

TTL = 3600
IDEMPOTENCY_TTL = 600


@pytest.fixture
def fake_server() -> fakeredis.FakeServer:
    return fakeredis.FakeServer()


@pytest.fixture
async def redis(fake_server: fakeredis.FakeServer) -> AsyncIterator[fakeredis.FakeAsyncRedis]:
    client = fakeredis.FakeAsyncRedis(server=fake_server, decode_responses=True)
    yield client
    await client.aclose()


@pytest.fixture
def store(redis: fakeredis.FakeAsyncRedis) -> ProgressStore:
    return ProgressStore(redis, ttl_seconds=TTL, idempotency_ttl_seconds=IDEMPOTENCY_TTL)
