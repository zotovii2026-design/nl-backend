import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from api.v1.nl import get_unit_economics, save_unit_economics, UnitEconSave
from main import app
from models.organization import Role


def test_unit_economics_endpoints_require_bearer_token():
    client = TestClient(app)
    org_id = str(uuid.uuid4())

    get_response = client.get(
        "/api/v1/nl/unit-economics",
        params={"org_id": org_id},
    )
    post_response = client.post(
        "/api/v1/nl/unit-economics",
        params={"org_id": org_id},
        json={"nm_id": 1},
    )

    assert get_response.status_code == 401
    assert post_response.status_code == 401


@pytest.mark.asyncio
async def test_unit_economics_get_rejects_foreign_organization(monkeypatch):
    org_id = str(uuid.uuid4())
    user = SimpleNamespace(id=uuid.uuid4())
    db = AsyncMock()

    async def deny_access(*args, **kwargs):
        raise HTTPException(status_code=403, detail="Not a member")

    monkeypatch.setattr("api.v1.nl.resolve_org_id", AsyncMock(return_value=org_id))
    monkeypatch.setattr("api.v1.nl.require_organization_role", deny_access)

    with pytest.raises(HTTPException) as exc:
        await get_unit_economics(org_id, current_user=user, db=db)

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_unit_economics_get_allows_viewer(monkeypatch):
    org_id = str(uuid.uuid4())
    user = SimpleNamespace(id=uuid.uuid4())
    db = AsyncMock()
    require_role = AsyncMock()
    build = AsyncMock(return_value={"items": [], "total": 0})

    monkeypatch.setattr("api.v1.nl.resolve_org_id", AsyncMock(return_value=org_id))
    monkeypatch.setattr("api.v1.nl.require_organization_role", require_role)
    monkeypatch.setattr("api.v1.nl.build_unit_economics", build)

    result = await get_unit_economics(org_id, current_user=user, db=db)

    assert result == {"items": [], "total": 0}
    require_role.assert_awaited_once_with(uuid.UUID(org_id), Role.VIEWER, user, db)


@pytest.mark.asyncio
async def test_unit_economics_save_requires_admin(monkeypatch):
    org_id = str(uuid.uuid4())
    user = SimpleNamespace(id=uuid.uuid4())
    db = AsyncMock()

    async def deny_access(*args, **kwargs):
        raise HTTPException(status_code=403, detail="Role is not allowed")

    monkeypatch.setattr("api.v1.nl.resolve_org_id", AsyncMock(return_value=org_id))
    monkeypatch.setattr("api.v1.nl.require_organization_role", deny_access)

    with pytest.raises(HTTPException) as exc:
        await save_unit_economics(
            UnitEconSave(nm_id=1),
            org_id,
            current_user=user,
            db=db,
        )

    assert exc.value.status_code == 403
