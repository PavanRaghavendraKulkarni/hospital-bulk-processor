from collections.abc import AsyncIterator

import fakeredis
import pytest
from httpx import ASGITransport, AsyncClient
from redis.exceptions import ConnectionError as RedisConnectionError

from app.dependencies import get_redis
from app.main import create_app


@pytest.fixture
async def redis() -> AsyncIterator[fakeredis.FakeAsyncRedis]:
    client = fakeredis.FakeAsyncRedis(decode_responses=True)
    yield client
    await client.aclose()


async def _get_health(redis_client: object) -> tuple[int, dict]:
    app = create_app()
    app.dependency_overrides[get_redis] = lambda: redis_client
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/health")
    return resp.status_code, resp.json()


async def test_health_ok(redis: fakeredis.FakeAsyncRedis) -> None:
    status_code, body = await _get_health(redis)
    assert status_code == 200
    assert body == {"status": "ok", "redis": "ok"}


async def test_health_degraded_when_redis_down() -> None:
    class DownRedis:
        async def ping(self) -> None:
            raise RedisConnectionError("down")

    status_code, body = await _get_health(DownRedis())
    assert status_code == 503
    assert body == {"status": "degraded", "redis": "unavailable"}


def test_openapi_documents_503_when_redis_is_down() -> None:
    responses = create_app().openapi()["paths"]["/health"]["get"]["responses"]

    assert "503" in responses
    schema = responses["503"]["content"]["application/json"]["schema"]
    assert schema == {"$ref": "#/components/schemas/HealthResponse"}
