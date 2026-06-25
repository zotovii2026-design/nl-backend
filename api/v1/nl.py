"""NL Table — legacy router shim.

All routes have been extracted to dedicated routers:
- api/v1/routers/dashboard.py — products, dates, control
- api/v1/routers/sellers.py — sellers, seo-keywords
- api/v1/routers/marketer.py — marketer products/detail
- api/v1/routers/unit_economics.py — unit economics
- api/v1/routers/prices.py — WB prices refresh
- api/v1/routers/pages.py — register/login/nl-v2 HTML pages

This file is kept for backward compatibility but contains no routes.
"""
from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional

from core.database import get_db
from core.dependencies import get_current_user
from core.role_deps import require_organization_role
from models.organization import Role
from models.user import User
from services.reference import resolve_org_id
from api.v1.routers.unit_economics import (
    UnitEconSave,
    build_unit_economics,
    save_unit_economics as _save_unit_economics,
)

router = APIRouter(tags=["nl"])


async def get_unit_economics(
    org_id: str,
    search: Optional[str] = None,
    limit: Optional[int] = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Backward-compatible import shim for legacy unit economics tests."""
    org_id = await resolve_org_id(org_id, db)
    await require_organization_role(org_id, Role.VIEWER, current_user, db)
    return await build_unit_economics(org_id, db, search=search, limit=limit)


async def save_unit_economics(
    data: UnitEconSave,
    org_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Backward-compatible import shim for legacy unit economics tests."""
    org_id = await resolve_org_id(org_id, db)
    await require_organization_role(org_id, Role.ADMIN, current_user, db)
    return await _save_unit_economics(data, org_id, current_user=current_user, db=db)
