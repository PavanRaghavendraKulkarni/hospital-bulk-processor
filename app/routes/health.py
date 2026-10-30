from fastapi import APIRouter, Depends, Response, status
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.dependencies import get_redis
from app.schemas import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health(response: Response, redis: Redis = Depends(get_redis)) -> HealthResponse:
    try:
        await redis.ping()
    except (RedisError, OSError):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return HealthResponse(status="degraded", redis="unavailable")
    return HealthResponse(status="ok", redis="ok")
