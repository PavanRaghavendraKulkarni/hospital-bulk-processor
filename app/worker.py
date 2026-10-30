"""arq worker entrypoint: ``arq app.worker.WorkerSettings``.

Job logic is added in step 7; for now this only proves the worker boots and connects to Redis.
"""

from typing import Any, ClassVar

from arq.connections import RedisSettings

from app.config import get_settings
from app.logging_config import configure_logging, get_logger

settings = get_settings()
logger = get_logger(__name__)


async def process_bulk_job(ctx: dict[str, Any], batch_id: str) -> None:
    logger.warning("process_bulk_job_not_implemented", batch_id=batch_id)


async def startup(ctx: dict[str, Any]) -> None:
    configure_logging(settings.log_level, settings.log_json)
    logger.info("worker_started")


async def shutdown(ctx: dict[str, Any]) -> None:
    logger.info("worker_stopped")


class WorkerSettings:
    functions: ClassVar[list[Any]] = [process_bulk_job]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    max_jobs = settings.worker_max_jobs
    job_timeout = settings.worker_job_timeout_seconds
    max_tries = settings.worker_max_tries
