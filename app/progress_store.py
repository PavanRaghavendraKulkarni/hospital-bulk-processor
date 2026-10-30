"""All Redis access for bulk jobs lives here.

Key scheme (every bulk key expires after job_ttl_seconds):
    bulk:{batch_id}:meta     hash   JobMeta fields + processed/succeeded/failed counters
    bulk:{batch_id}:rows     hash   row_number -> HospitalRow JSON (the parsed upload)
    bulk:{batch_id}:results  hash   row_number -> RowResult JSON
    bulk:hash:{sha256}       string batch_id of the latest upload with that content (idempotency)
"""

from datetime import datetime
from typing import Any

from redis.asyncio import Redis

from app.models import HospitalRow, JobMeta, JobStatus, RowResult, RowStatus


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
        self._transition_script = redis.register_script(_TRANSITION_SCRIPT)

    async def create_job(self, meta: JobMeta, rows: list[HospitalRow]) -> None:
        """Persist metadata and parsed rows atomically, before the job is enqueued."""
        batch_id = meta.batch_id
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hset(meta_key(batch_id), mapping=_to_hash(meta.model_dump()))
            pipe.hset(rows_key(batch_id), mapping={str(r.row_number): r.model_dump_json() for r in rows})
            pipe.expire(meta_key(batch_id), self._ttl)
            pipe.expire(rows_key(batch_id), self._ttl)
            await pipe.execute()

    async def get_meta(self, batch_id: str) -> JobMeta | None:
        data = await self._redis.hgetall(meta_key(batch_id))
        return JobMeta.model_validate(data) if data else None

    async def get_rows(self, batch_id: str) -> list[HospitalRow]:
        data = await self._redis.hgetall(rows_key(batch_id))
        rows = [HospitalRow.model_validate_json(v) for v in data.values()]
        return sorted(rows, key=lambda r: r.row_number)

    async def get_results(self, batch_id: str) -> dict[int, RowResult]:
        data = await self._redis.hgetall(results_key(batch_id))
        return {int(k): RowResult.model_validate_json(v) for k, v in sorted(data.items(), key=lambda kv: int(kv[0]))}

    async def record_result(self, batch_id: str, result: RowResult) -> None:
        """Store one row's outcome and bump counters in a single MULTI so they never drift."""
        counter = "succeeded" if result.status is RowStatus.CREATED else "failed"
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hset(results_key(batch_id), str(result.row), result.model_dump_json())
            pipe.hincrby(meta_key(batch_id), "processed", 1)
            pipe.hincrby(meta_key(batch_id), counter, 1)
            pipe.expire(results_key(batch_id), self._ttl)
            await pipe.execute()

    async def update_meta(self, batch_id: str, **fields: Any) -> None:
        """Set metadata fields; a value of None removes the field."""
        to_set = _to_hash({k: v for k, v in fields.items() if v is not None})
        to_clear = [k for k, v in fields.items() if v is None]
        async with self._redis.pipeline(transaction=True) as pipe:
            if to_set:
                pipe.hset(meta_key(batch_id), mapping=to_set)
            if to_clear:
                pipe.hdel(meta_key(batch_id), *to_clear)
            await pipe.execute()

    async def reset_failed_rows(self, batch_id: str) -> list[int]:
        """Forget failed results so they are reprocessed. Only called when no worker owns the job."""
        results = await self.get_results(batch_id)
        failed = [row for row, r in results.items() if r.status is RowStatus.FAILED]
        if failed:
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.hdel(results_key(batch_id), *map(str, failed))
                pipe.hincrby(meta_key(batch_id), "processed", -len(failed))
                pipe.hincrby(meta_key(batch_id), "failed", -len(failed))
                await pipe.execute()
        return failed

    async def refresh_ttl(self, batch_id: str) -> None:
        async with self._redis.pipeline(transaction=False) as pipe:
            for key in (meta_key(batch_id), rows_key(batch_id), results_key(batch_id)):
                pipe.expire(key, self._ttl)
            await pipe.execute()

    async def transition_status(self, batch_id: str, allowed_from: set[JobStatus], to: JobStatus) -> bool:
        """Atomic compare-and-set on status. Returns False if the job is missing or in another state."""
        changed = await self._transition_script(keys=[meta_key(batch_id)], args=[to, *allowed_from])
        return bool(changed)

    async def delete_job(self, batch_id: str) -> None:
        await self._redis.delete(meta_key(batch_id), rows_key(batch_id), results_key(batch_id))

    async def claim_file_hash(self, file_hash: str, batch_id: str) -> str | None:
        """Atomically map a file hash to batch_id. Returns the existing batch_id if already claimed."""
        key = file_hash_key(file_hash)
        if await self._redis.set(key, batch_id, nx=True, ex=self._idempotency_ttl):
            return None
        return await self._redis.get(key)

    async def replace_file_hash(self, file_hash: str, batch_id: str) -> None:
        await self._redis.set(file_hash_key(file_hash), batch_id, ex=self._idempotency_ttl)


# KEYS[1] = meta hash, ARGV[1] = new status, ARGV[2..] = statuses it may move from.
_TRANSITION_SCRIPT = """
local current = redis.call('HGET', KEYS[1], 'status')
if not current then return 0 end
for i = 2, #ARGV do
    if ARGV[i] == current then
        redis.call('HSET', KEYS[1], 'status', ARGV[1])
        return 1
    end
end
return 0
"""


def _to_hash(values: dict[str, Any]) -> dict[str, str]:
    """Encode values for a Redis hash. Pydantic parses these strings back when loading JobMeta."""
    encoded: dict[str, str] = {}
    for key, value in values.items():
        if value is None:
            continue
        if isinstance(value, bool):
            encoded[key] = "1" if value else "0"
        elif isinstance(value, datetime):
            encoded[key] = value.isoformat()
        else:
            encoded[key] = str(value)
    return encoded
