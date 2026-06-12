from typing import Optional
from uuid import UUID

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.dependencies import security
from core.role_deps import has_role_permission
from core.security import decode_token
from models.organization import Membership, Organization, Role
from models.user import User


async def _resolve_organization_id(raw_org_id: str, db: AsyncSession) -> UUID:
    try:
        return UUID(raw_org_id)
    except ValueError:
        try:
            seller_id = int(raw_org_id)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid organization ID",
            ) from exc

    result = await db.execute(
        select(Organization.id).where(Organization.wb_seller_id == seller_id)
    )
    organization_id = result.scalar_one_or_none()
    if not organization_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )
    return organization_id


async def require_query_organization_access(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: AsyncSession = Depends(get_db),
) -> Optional[Membership]:
    """Authorize legacy routes that carry their tenant in the org_id query."""
    raw_org_id = request.query_params.get("org_id")
    if not raw_org_id:
        return None

    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    payload = decode_token(credentials.credentials)
    if not payload or payload.get("type") != "access" or not payload.get("sub"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token",
        )

    user_result = await db.execute(select(User).where(User.id == payload["sub"]))
    user = user_result.scalar_one_or_none()
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user",
        )

    organization_id = await _resolve_organization_id(raw_org_id, db)
    membership_result = await db.execute(
        select(Membership).where(
            Membership.user_id == user.id,
            Membership.organization_id == organization_id,
        )
    )
    membership = membership_result.scalar_one_or_none()
    if not membership:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You are not a member of this organization",
        )

    required_role = (
        Role.VIEWER
        if request.method in {"GET", "HEAD", "OPTIONS"}
        else Role.ADMIN
    )
    if not has_role_permission(membership.role, required_role):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Role {membership.role.value} is not allowed for this action",
        )
    return membership
