import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.v1.sync import _get_authorized_api_key, get_sync_logs
from models.organization import Role


@pytest.mark.asyncio
async def test_sync_key_requires_admin_membership(monkeypatch):
    api_key = SimpleNamespace(
        id=uuid.uuid4(),
        organization_id=uuid.uuid4(),
    )
    user = SimpleNamespace(id=uuid.uuid4())
    db = AsyncMock()
    db.execute.return_value = SimpleNamespace(
        scalar_one_or_none=lambda: api_key
    )
    require_role = AsyncMock()
    monkeypatch.setattr("api.v1.sync.require_organization_role", require_role)

    result = await _get_authorized_api_key(api_key.id, user, db)

    assert result is api_key
    require_role.assert_awaited_once_with(
        api_key.organization_id,
        Role.ADMIN,
        user,
        db,
    )


@pytest.mark.asyncio
async def test_sync_logs_are_filtered_by_current_user_memberships():
    user = SimpleNamespace(id=uuid.uuid4())
    scalars = SimpleNamespace(all=lambda: [])
    db = AsyncMock()
    db.execute.return_value = SimpleNamespace(scalars=lambda: scalars)

    result = await get_sync_logs(current_user=user, db=db)

    assert result == {"logs": []}
    statement = str(db.execute.await_args.args[0])
    assert "memberships" in statement
    assert "memberships.user_id" in statement
