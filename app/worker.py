"""arq worker entrypoint: ``arq app.worker.WorkerSettings``."""

from typing import Any, ClassVar

import httpx
from arq.connections import RedisSettings
from arq.worker import Retry
from redis.asyncio import Redis

from app.clients.hospital_api import HospitalApiClient
from app.config import get_settings
from app.logging_config import configure_logging, get_logger
from app.progress_store import ProgressStore
from app.rate_limiter import GlobalRateLimiter
from app.services.bulk_processor import BulkJobProcessor

settings = get_settings()
logger = get_logger(__name__)


async def process_bulk_job(ctx: dict[str, Any], batch_id: str) -> None:
    processor: BulkJobProcessor = ctx["processor"]
    job_try: int = ctx.get("job_try", 1)
    log = logger.bind(batch_id=batch_id, job_try=job_try)
    try:
        await processor.run(batch_id)
    except Exception as exc:
        # Row-level upstream errors never reach here; this is infrastructure (e.g. Redis) failing.
        if job_try < settings.worker_max_tries:
            log.warning("bulk_job_retrying", error=str(exc))
            raise Retry(defer=job_try * 5) from exc
        log.exception("bulk_job_gave_up")
        await processor.mark_failed(batch_id, f"Job failed after {job_try} attempts: {exc}")
        raise


async def startup(ctx: dict[str, Any]) -> None:
    configure_logging(settings.log_level, settings.log_json)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    http = httpx.AsyncClient(
        base_url=str(settings.hospital_api_base_url),
        timeout=httpx.Timeout(settings.hospital_api_timeout_seconds, connect=10.0),
    )
    limiter = GlobalRateLimiter(
        redis, max_in_flight=settings.upstream_max_in_flight, lease_seconds=settings.upstream_slot_ttl_seconds
    )
    client = HospitalApiClient(
        http,
        max_retries=settings.hospital_api_max_retries,
        backoff_base_seconds=settings.hospital_api_backoff_base_seconds,
        backoff_max_seconds=settings.hospital_api_backoff_max_seconds,
        limiter=limiter,
    )
    store = ProgressStore(
        redis, ttl_seconds=settings.job_ttl_seconds, idempotency_ttl_seconds=settings.idempotency_ttl_seconds
    )
    ctx.update(redis=redis, http=http, processor=BulkJobProcessor(store, client, settings))
    logger.info("worker_started", upstream=str(settings.hospital_api_base_url))


async def shutdown(ctx: dict[str, Any]) -> None:
    await ctx["http"].aclose()
    await ctx["redis"].aclose()
    logger.info("worker_stopped")


class WorkerSettings:
    functions: ClassVar[list[Any]] = [process_bulk_job]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    max_jobs = settings.worker_max_jobs
    job_timeout = settings.worker_job_timeout_seconds
    max_tries = settings.worker_max_tries
