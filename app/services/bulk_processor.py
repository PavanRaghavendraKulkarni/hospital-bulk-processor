"""Processes one bulk job: creates each row upstream, records progress, then activates the batch."""

from collections.abc import Callable
from datetime import UTC, datetime

from app.clients.hospital_api import HospitalApiClient
from app.config import Settings
from app.progress_store import ProgressStore


def _utcnow() -> datetime:
    return datetime.now(UTC)


class BulkJobProcessor:
    def __init__(
        self,
        store: ProgressStore,
        client: HospitalApiClient,
        settings: Settings,
        now: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._store = store
        self._client = client
        self._settings = settings
        self._now = now

    async def run(self, batch_id: str) -> None:
        raise NotImplementedError

    async def mark_failed(self, batch_id: str, error: str) -> None:
        raise NotImplementedError
