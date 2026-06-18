from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from api.v1.routers.sales_plans import router
from core.tenant_auth import require_query_organization_access
from services.sales_plans import resolve_sales_plan_organization_id


@pytest.mark.asyncio
async def test_sales_plans_reject_malformed_organization_id():
    db = AsyncMock()

    with pytest.raises(HTTPException) as exc:
        await resolve_sales_plan_organization_id("not-a-uuid", db)

    assert exc.value.status_code == 400
    db.execute.assert_not_awaited()


def test_sales_plans_router_uses_legacy_tenant_dependency():
    dependencies = [dependency.dependency for dependency in router.dependencies]

    assert require_query_organization_access in dependencies
