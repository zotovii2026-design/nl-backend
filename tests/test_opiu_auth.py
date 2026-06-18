import uuid
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from api.v1.routers.opiu import get_opiu_report, trigger_opiu_sync
from main import app
from models.organization import Role


def test_legacy_duplicate_opiu_routes_are_removed():
    paths = [
        route.path
        for route in app.routes
        if route.path.startswith("/api/v1/nl/opiu")
    ]

    assert paths.count("/api/v1/nl/opiu") == 0
    assert paths.count("/api/v1/nl/opiu/report") == 1
    assert paths.count("/api/v1/nl/opiu/sync") == 1
    assert paths.count("/api/v1/nl/opiu/export") == 1


def test_opiu_endpoints_require_bearer_token():
    client = TestClient(app)
    org_id = str(uuid.uuid4())
    params = {
        "org_id": org_id,
        "date_from": "2026-06-01",
        "date_to": "2026-06-12",
    }

    assert client.get("/api/v1/nl/opiu/report", params=params).status_code == 401
    assert client.post("/api/v1/nl/opiu/sync", params=params).status_code == 401
    assert client.get("/api/v1/nl/opiu/export", params=params).status_code == 401


@pytest.mark.asyncio
async def test_opiu_report_allows_viewer(monkeypatch):
    org_id = str(uuid.uuid4())
    user = SimpleNamespace(id=uuid.uuid4())
    db = AsyncMock()
    require_role = AsyncMock()

    monkeypatch.setattr(
        "api.v1.routers.opiu.require_organization_role", require_role
    )
    monkeypatch.setattr(
        "api.v1.routers.opiu._load_report_rows",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        "api.v1.routers.opiu._sync_info",
        AsyncMock(return_value=None),
    )

    result = await get_opiu_report(
        org_id,
        date.fromisoformat("2026-06-01"),
        date.fromisoformat("2026-06-12"),
        current_user=user,
        db=db,
    )

    assert result["items"] == []
    require_role.assert_awaited_once_with(org_id, Role.VIEWER, user, db)


@pytest.mark.asyncio
async def test_opiu_sync_requires_admin(monkeypatch):
    org_id = str(uuid.uuid4())
    user = SimpleNamespace(id=uuid.uuid4())
    db = AsyncMock()

    async def deny_access(*args, **kwargs):
        raise HTTPException(status_code=403, detail="Admin required")

    monkeypatch.setattr(
        "api.v1.routers.opiu.require_organization_role", deny_access
    )

    with pytest.raises(HTTPException) as exc:
        await trigger_opiu_sync(
            org_id,
            date.fromisoformat("2026-06-01"),
            date.fromisoformat("2026-06-12"),
            current_user=user,
            db=db,
        )

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_opiu_sync_uses_canonical_celery_app(monkeypatch):
    org_id = str(uuid.uuid4())
    user = SimpleNamespace(id=uuid.uuid4())
    db = AsyncMock()
    send_task = MagicMock(return_value=SimpleNamespace(id="task-1"))

    monkeypatch.setattr(
        "api.v1.routers.opiu.require_organization_role", AsyncMock()
    )
    monkeypatch.setattr(
        "api.v1.routers.opiu.celery_app.send_task", send_task
    )

    result = await trigger_opiu_sync(
        org_id,
        date.fromisoformat("2026-06-01"),
        date.fromisoformat("2026-06-12"),
        current_user=user,
        db=db,
    )

    assert result == {"status": "queued", "task_id": "task-1"}
    send_task.assert_called_once_with(
        "wb.opiu.sync_org",
        args=[org_id, "2026-06-01", "2026-06-12"],
    )
