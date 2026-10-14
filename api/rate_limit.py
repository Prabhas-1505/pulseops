"""Fixed-window rate limiting backed by Redis.

Key = f"ratelimit:{route_path}:{client_ip}". First request in a window sets
the TTL; every request increments. Once the count exceeds `times` within
`seconds`, further requests get 429 until the window expires. This is a
real shared-state limiter (works across multiple API replicas, unlike an
in-process counter), which is the point of specifying Redis for it.
"""

from __future__ import annotations

import os

from fastapi import HTTPException, Request, status
from redis.asyncio import Redis

_redis: Redis | None = None


def get_redis() -> Redis:
    global _redis
    if _redis is None:
        host = os.environ.get("REDIS_HOST", "localhost")
        port = int(os.environ.get("REDIS_PORT", "6379"))
        _redis = Redis(host=host, port=port, decode_responses=True)
    return _redis


class RateLimiter:
    def __init__(self, times: int, seconds: int):
        self.times = times
        self.seconds = seconds

    async def __call__(self, request: Request) -> None:
        redis = get_redis()
        identity = request.client.host if request.client else "unknown"
        key = f"ratelimit:{request.url.path}:{identity}"
        count = await redis.incr(key)
        if count == 1:
            await redis.expire(key, self.seconds)
        if count > self.times:
            ttl = await redis.ttl(key)
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"rate limit exceeded ({self.times} requests per {self.seconds}s); retry after {max(ttl, 1)}s",
            )
