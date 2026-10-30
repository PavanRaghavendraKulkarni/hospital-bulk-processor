from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from arq import create_pool
from arq.connections import RedisSettings
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

from app.config import get_settings
from app.csv_validation import CsvValidationError
from app.job_queue import ArqJobQueue
from app.logging_config import configure_logging, get_logger
from app.progress_store import ProgressStore
from app.routes import bulk, health
from app.schemas import ErrorResponse
from app.services.bulk_service import BulkService, QueueUnavailableError, RetryNotAllowedError

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    arq_pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
    store = ProgressStore(
        redis, ttl_seconds=settings.job_ttl_seconds, idempotency_ttl_seconds=settings.idempotency_ttl_seconds
    )
    app.state.redis = redis
    app.state.bulk_service = BulkService(store, ArqJobQueue(arq_pool), settings)
    logger.info("api_started", redis_url=settings.redis_url)
    try:
        yield
    finally:
        await arq_pool.aclose()
        await redis.aclose()
        logger.info("api_stopped")


async def _csv_validation_handler(request: Request, exc: CsvValidationError) -> JSONResponse:
    body = ErrorResponse(detail=exc.message, errors=exc.errors)
    return JSONResponse(status_code=exc.status_code, content=body.model_dump())


async def _queue_unavailable_handler(request: Request, exc: QueueUnavailableError) -> JSONResponse:
    body = ErrorResponse(detail="Job queue is unavailable, please retry later")
    return JSONResponse(status_code=503, content=body.model_dump())


async def _retry_not_allowed_handler(request: Request, exc: RetryNotAllowedError) -> JSONResponse:
    return JSONResponse(status_code=409, content=ErrorResponse(detail=str(exc)).model_dump())


def create_app() -> FastAPI:
    app = FastAPI(title="Hospital Bulk Processor", version="0.1.0", lifespan=lifespan)
    app.add_exception_handler(CsvValidationError, _csv_validation_handler)
    app.add_exception_handler(QueueUnavailableError, _queue_unavailable_handler)
    app.add_exception_handler(RetryNotAllowedError, _retry_not_allowed_handler)
    app.include_router(health.router)
    app.include_router(bulk.router)
    return app


app = create_app()
