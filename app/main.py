from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from app.config import get_settings
from app.logging_config import configure_logging, get_logger
from app.routes import health

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    app.state.redis = Redis.from_url(settings.redis_url, decode_responses=True)
    logger.info("api_started", redis_url=settings.redis_url)
    try:
        yield
    finally:
        await app.state.redis.aclose()
        logger.info("api_stopped")


def create_app() -> FastAPI:
    app = FastAPI(title="Hospital Bulk Processor", version="0.1.0", lifespan=lifespan)
    app.include_router(health.router)
    return app


app = create_app()
