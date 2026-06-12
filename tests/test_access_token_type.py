import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from core.dependencies import get_current_superuser, get_current_user
from core.security import create_access_token, create_refresh_token


@pytest.mark.asyncio
async def test_refresh_token_is_rejected_by_access_dependency():
    credentials = HTTPAuthorizationCredentials(
        scheme="Bearer",
        credentials=create_refresh_token({"sub": str(uuid.uuid4())}),
    )

    with pytest.raises(HTTPException) as exc:
        await get_current_user(credentials=credentials, db=AsyncMock())

    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_access_token_authenticates_active_user():
    user = SimpleNamespace(id=uuid.uuid4(), is_active=True)
    db = AsyncMock()
    db.execute.return_value = SimpleNamespace(
        scalar_one_or_none=lambda: user
    )
    credentials = HTTPAuthorizationCredentials(
        scheme="Bearer",
        credentials=create_access_token({"sub": str(user.id)}),
    )

    assert await get_current_user(credentials=credentials, db=db) is user


@pytest.mark.asyncio
async def test_superuser_dependency_returns_403_or_200():
    regular_user = SimpleNamespace(is_superuser=False)
    with pytest.raises(HTTPException) as exc:
        await get_current_superuser(regular_user)
    assert exc.value.status_code == 403

    superuser = SimpleNamespace(is_superuser=True)
    assert await get_current_superuser(superuser) is superuser
