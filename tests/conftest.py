from collections.abc import AsyncIterator

import fakeredis
import pytest
from httpx import ASGITransport, AsyncClient

from app.config import Settings, get_settings
from app.dependencies import get_bulk_service
from app.main import create_app
from app.progress_store import ProgressStore
from app.services.bulk_service import BulkService

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


class FakeQueue:
    """Records enqueued jobs. Asserts the job's metadata is already in Redis when it is enqueued."""

    def __init__(self, store: ProgressStore) -> None:
        self._store = store
        self.jobs: list[tuple[str, str]] = []
        self.fail_with: Exception | None = None

    async def enqueue(self, batch_id: str, job_id: str) -> None:
        if self.fail_with:
            raise self.fail_with
        assert await self._store.get_meta(batch_id) is not None, "job must be stored before it is enqueued"
        self.jobs.append((batch_id, job_id))


@pytest.fixture
def settings() -> Settings:
    return Settings(
        _env_file=None, max_csv_rows=20, max_upload_bytes=10_000, per_job_concurrency=5, sse_poll_interval_seconds=0.01
    )


@pytest.fixture
def queue(store: ProgressStore) -> FakeQueue:
    return FakeQueue(store)


@pytest.fixture
def bulk_service(store: ProgressStore, queue: FakeQueue, settings: Settings) -> BulkService:
    return BulkService(store, queue, settings)


@pytest.fixture
async def api(bulk_service: BulkService, settings: Settings) -> AsyncIterator[AsyncClient]:
    app = create_app()
    app.dependency_overrides[get_bulk_service] = lambda: bulk_service
    app.dependency_overrides[get_settings] = lambda: settings
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client
