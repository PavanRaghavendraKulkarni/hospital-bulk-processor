"""Thin abstraction over arq so services don't depend on the queue library directly."""

from typing import Protocol

from arq.connections import ArqRedis

PROCESS_BULK_JOB = "process_bulk_job"


class JobQueue(Protocol):
    async def enqueue(self, batch_id: str, job_id: str) -> None: ...


class ArqJobQueue:
    def __init__(self, pool: ArqRedis) -> None:
        self._pool = pool

    async def enqueue(self, batch_id: str, job_id: str) -> None:
        # A fixed job_id makes arq drop duplicate enqueues of the same job.
        await self._pool.enqueue_job(PROCESS_BULK_JOB, batch_id, _job_id=job_id)
