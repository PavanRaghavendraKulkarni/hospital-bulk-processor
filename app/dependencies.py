"""FastAPI dependencies. Shared resources live on app.state and are created in the lifespan."""

from fastapi import Request
from redis.asyncio import Redis

from app.services.bulk_service import BulkService


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def get_bulk_service(request: Request) -> BulkService:
    return request.app.state.bulk_service
