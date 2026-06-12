import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.v1.organizations import get_organization
from models.organization import Role


@pytest.mark.asyncio
async def test_get_organization_checks_viewer_membership(monkeypatch):
    organization_id = uuid.uuid4()
    user = SimpleNamespace(id=uuid.uuid4())
    organization = SimpleNamespace(id=organization_id)
    db = AsyncMock()
    db.execute.return_value = SimpleNamespace(
        scalar_one_or_none=lambda: organization
    )
    require_role = AsyncMock()
    monkeypatch.setattr(
        "api.v1.organizations.require_organization_role",
        require_role,
    )

    result = await get_organization(organization_id, user, db)

    assert result is organization
    require_role.assert_awaited_once_with(
        organization_id,
        Role.VIEWER,
        user,
        db,
    )
