from fastapi import Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from models.organization import Membership, Role
from core.database import get_db
from core.dependencies import get_current_user
from models.user import User
from uuid import UUID


async def require_organization_role(
    org_id: UUID,
    min_role: Role,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
) -> Membership:
    """Проверка доступа к организации с определённой ролью"""

    # Поиск membership пользователя
    result = await db.execute(
        select(Membership).where(
            Membership.user_id == current_user.id,
            Membership.organization_id == org_id
        )
    )
    membership = result.scalar_one_or_none()

    if not membership:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You are not a member of this organization"
        )

    # Проверка прав доступа
    if not has_role_permission(membership.role, min_role):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Role {membership.role.value} is not allowed for this action"
        )

    return membership


def has_role_permission(user_role: Role, required_role: Role) -> bool:
    """
    Проверка прав доступа на основе ролевой модели:

    owner   → полный доступ
    admin   → данные + настройки + приглашения (кроме назначения owner)
    viewer  → только чтение
    """
    role_hierarchy = {
        Role.VIEWER: 1,
        Role.ADMIN: 2,
        Role.OWNER: 3
    }

    return role_hierarchy.get(user_role, 0) >= role_hierarchy.get(required_role, 0)
