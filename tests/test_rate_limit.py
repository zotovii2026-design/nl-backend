from collections import defaultdict

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from core.rate_limit import enforce_rate_limit


class FakeRedis:
    def __init__(self):
        self.counts = defaultdict(int)
        self.expirations = {}

    async def incr(self, key):
        self.counts[key] += 1
        return self.counts[key]

    async def expire(self, key, seconds):
        self.expirations[key] = seconds

    async def ttl(self, key):
        return self.expirations.get(key, -1)


def _request(ip="203.0.113.10"):
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/api/v1/nl/login",
            "query_string": b"",
            "headers": [],
            "client": (ip, 12345),
        }
    )


@pytest.mark.asyncio
async def test_rate_limit_allows_requests_within_limit(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr("core.rate_limit.get_rate_limit_redis", lambda: redis)

    for _ in range(2):
        await enforce_rate_limit(
            _request(), "login", 2, 60, "user@example.test"
        )


@pytest.mark.asyncio
async def test_rate_limit_returns_429_with_retry_after(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr("core.rate_limit.get_rate_limit_redis", lambda: redis)

    await enforce_rate_limit(_request(), "login", 1, 60, "user@example.test")

    with pytest.raises(HTTPException) as exc:
        await enforce_rate_limit(
            _request(), "login", 1, 60, "user@example.test"
        )

    assert exc.value.status_code == 429
    assert exc.value.headers == {"Retry-After": "60"}


@pytest.mark.asyncio
async def test_rate_limit_separates_client_ips(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr("core.rate_limit.get_rate_limit_redis", lambda: redis)

    await enforce_rate_limit(_request("203.0.113.10"), "login", 1, 60)
    await enforce_rate_limit(_request("203.0.113.11"), "login", 1, 60)
