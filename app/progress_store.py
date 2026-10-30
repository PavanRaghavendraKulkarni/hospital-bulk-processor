"""All Redis access for bulk jobs lives here.

Key scheme (every bulk key expires after job_ttl_seconds):
    bulk:{batch_id}:meta     hash   JobMeta fields + processed/succeeded/failed counters
    bulk:{batch_id}:rows     hash   row_number -> HospitalRow JSON (the parsed upload)
    bulk:{batch_id}:results  hash   row_number -> RowResult JSON
    bulk:hash:{sha256}       string batch_id of the latest upload with that content (idempotency)
"""

from typing import Any

from redis.asyncio import Redis

from app.models import HospitalRow, JobMeta, RowResult


def meta_key(batch_id: str) -> str:
    return f"bulk:{batch_id}:meta"


def rows_key(batch_id: str) -> str:
    return f"bulk:{batch_id}:rows"


def results_key(batch_id: str) -> str:
    return f"bulk:{batch_id}:results"


def file_hash_key(file_hash: str) -> str:
    return f"bulk:hash:{file_hash}"


class ProgressStore:
    def __init__(self, redis: Redis, *, ttl_seconds: int, idempotency_ttl_seconds: int) -> None:
        self._redis = redis
        self._ttl = ttl_seconds
        self._idempotency_ttl = idempotency_ttl_seconds

    async def create_job(self, meta: JobMeta, rows: list[HospitalRow]) -> None:
        raise NotImplementedError

    async def get_meta(self, batch_id: str) -> JobMeta | None:
        raise NotImplementedError

    async def get_rows(self, batch_id: str) -> list[HospitalRow]:
        raise NotImplementedError

    async def get_results(self, batch_id: str) -> dict[int, RowResult]:
        raise NotImplementedError

    async def record_result(self, batch_id: str, result: RowResult) -> None:
        raise NotImplementedError

    async def update_meta(self, batch_id: str, **fields: Any) -> None:
        raise NotImplementedError

    async def reset_failed_rows(self, batch_id: str) -> list[int]:
        raise NotImplementedError

    async def refresh_ttl(self, batch_id: str) -> None:
        raise NotImplementedError

    async def claim_file_hash(self, file_hash: str, batch_id: str) -> str | None:
        raise NotImplementedError

    async def replace_file_hash(self, file_hash: str, batch_id: str) -> None:
        raise NotImplementedError
