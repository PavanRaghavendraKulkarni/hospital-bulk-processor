"""FastAPI dependencies. Shared resources live on app.state and are created in the lifespan."""

from fastapi import Request
from redis.asyncio import Redis


def get_redis(request: Request) -> Redis:
    return request.app.state.redis
