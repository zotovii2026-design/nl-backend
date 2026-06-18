import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from starlette.requests import Request

from core.security import create_access_token
from core.tenant_auth import require_query_organization_access
from core.role_deps import require_organization_role
from models.organization import Role


def _request(method: str, organization_id: uuid.UUID) -> Request:
    return Request(
        {
            "type": "http",
            "method": method,
            "path": "/api/v1/nl/reference",
            "query_string": f"org_id={organization_id}".encode(),
            "headers": [],
        }
    )


def _result(value):
    return SimpleNamespace(scalar_one_or_none=lambda: value)


@pytest.mark.asyncio
async def test_tenant_access_requires_bearer_token():
    with pytest.raises(HTTPException) as exc:
        await require_query_organization_access(
            _request("GET", uuid.uuid4()),
            credentials=None,
            db=AsyncMock(),
        )

    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_tenant_access_rejects_foreign_organization():
    user = SimpleNamespace(id=uuid.uuid4(), is_active=True)
    db = AsyncMock()
    db.execute.side_effect = [_result(user), _result(None)]
    credentials = HTTPAuthorizationCredentials(
        scheme="Bearer",
        credentials=create_access_token({"sub": str(user.id)}),
    )

    with pytest.raises(HTTPException) as exc:
        await require_query_organization_access(
            _request("GET", uuid.uuid4()),
            credentials=credentials,
            db=db,
        )

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_tenant_access_allows_viewer_read():
    user = SimpleNamespace(id=uuid.uuid4(), is_active=True)
    membership = SimpleNamespace(role=Role.VIEWER)
    db = AsyncMock()
    db.execute.side_effect = [_result(user), _result(membership)]
    credentials = HTTPAuthorizationCredentials(
        scheme="Bearer",
        credentials=create_access_token({"sub": str(user.id)}),
    )

    result = await require_query_organization_access(
        _request("GET", uuid.uuid4()),
        credentials=credentials,
        db=db,
    )

    assert result is membership


@pytest.mark.asyncio
async def test_tenant_access_rejects_viewer_write():
    user = SimpleNamespace(id=uuid.uuid4(), is_active=True)
    membership = SimpleNamespace(role=Role.VIEWER)
    db = AsyncMock()
    db.execute.side_effect = [_result(user), _result(membership)]
    credentials = HTTPAuthorizationCredentials(
        scheme="Bearer",
        credentials=create_access_token({"sub": str(user.id)}),
    )

    with pytest.raises(HTTPException) as exc:
        await require_query_organization_access(
            _request("POST", uuid.uuid4()),
            credentials=credentials,
            db=db,
        )

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_role_check_rejects_malformed_organization_id():
    with pytest.raises(HTTPException) as exc:
        await require_organization_role(
            "not-a-uuid",
            Role.VIEWER,
            current_user=SimpleNamespace(id=uuid.uuid4()),
            db=AsyncMock(),
        )

    assert exc.value.status_code == 400
