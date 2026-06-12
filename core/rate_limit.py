import hashlib
import logging
from functools import lru_cache
from typing import Optional

from fastapi import HTTPException, Request, status
from redis.asyncio import Redis

from core.config import settings

logger = logging.getLogger(__name__)


@lru_cache
def get_rate_limit_redis() -> Redis:
    return Redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)


def _request_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",", 1)[0].strip()
    return request.client.host if request.client else "unknown"


def _hash_identifier(value: str) -> str:
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest()


async def _consume_limit(
    redis: Redis,
    key: str,
    limit: int,
    window_seconds: int,
) -> None:
    count = await redis.incr(key)
    if count == 1:
        await redis.expire(key, window_seconds)
    if count <= limit:
        return

    ttl = await redis.ttl(key)
    retry_after = ttl if ttl and ttl > 0 else window_seconds
    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail="Too many requests",
        headers={"Retry-After": str(retry_after)},
    )


async def enforce_rate_limit(
    request: Request,
    scope: str,
    limit: int,
    window_seconds: int,
    account: Optional[str] = None,
) -> None:
    if not settings.RATE_LIMIT_ENABLED:
        return

    redis = get_rate_limit_redis()
    identifiers = [f"ip:{_hash_identifier(_request_ip(request))}"]
    if account:
        identifiers.append(f"account:{_hash_identifier(account)}")

    try:
        for identifier in identifiers:
            key = f"{settings.RATE_LIMIT_PREFIX}:{scope}:{identifier}"
            await _consume_limit(redis, key, limit, window_seconds)
    except HTTPException:
        raise
    except Exception:
        logger.exception("Rate limit backend unavailable for scope %s", scope)
