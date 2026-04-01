from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from datetime import datetime, timedelta
from uuid import UUID
import secrets
from core.database import get_db
from core.dependencies import get_current_user
from core.role_deps import require_organization_role
from core.security import encrypt_data
from models.organization import Organization, Membership, Invitation, WbApiKey, SubscriptionTier, Role, InvitationStatus
from schemas.organization import (
    OrganizationCreate,
    OrganizationResponse,
    OrganizationUpdate,
    MembershipResponse,
    InvitationCreate,
    InvitationResponse
)
from models.user import User

router = APIRouter(prefix="/organizations", tags=["Organizations"])


# === Organizations endpoints ===

@router.post("", response_model=OrganizationResponse)
async def create_organization(
    org_data: OrganizationCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Создание организации (создаёт org + membership owner)"""
    org = Organization(
        name=org_data.name,
        description=org_data.description,
        subscription_tier=SubscriptionTier.TRIAL,
        subscription_status="active"
    )
    db.add(org)
    await db.commit()
    await db.refresh(org)

    # Создание membership с ролью owner
    membership = Membership(
        user_id=current_user.id,
        organization_id=org.id,
        role=Role.OWNER
    )
    db.add(membership)
    await db.commit()

    return org


@router.get("", response_model=list[OrganizationResponse])
async def list_organizations(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Список организаций пользователя"""
    result = await db.execute(
        select(Organization)
        .join(Membership)
        .where(Membership.user_id == current_user.id)
    )
    organizations = result.scalars().all()
    return organizations


@router.get("/{org_id}", response_model=OrganizationResponse)
async def get_organization(
    org_id: UUID,
    membership: Membership = Depends(lambda: require_organization_role(UUID(int=0), Role.VIEWER)),
    db: AsyncSession = Depends(get_db)
):
    """Получение организации (viewer+)"""
    result = await db.execute(select(Organization).where(Organization.id == org_id))
    org = result.scalar_one_or_none()

    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )

    return org


@router.put("/{org_id}", response_model=OrganizationResponse)
async def update_organization(
    org_id: UUID,
    org_data: OrganizationUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Обновление организации (admin+)"""
    # Проверка прав через dependency (в реальном коде нужно использовать)
    membership = await require_organization_role(org_id, Role.ADMIN, current_user, db)

    result = await db.execute(select(Organization).where(Organization.id == org_id))
    org = result.scalar_one_or_none()

    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )

    if org_data.name is not None:
        org.name = org_data.name
    if org_data.description is not None:
        org.description = org_data.description

    await db.commit()
    await db.refresh(org)

    return org


@router.delete("/{org_id}")
async def delete_organization(
    org_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Удаление организации (только owner)"""
    membership = await require_organization_role(org_id, Role.OWNER, current_user, db)

    result = await db.execute(select(Organization).where(Organization.id == org_id))
    org = result.scalar_one_or_none()

    if not org:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found"
        )

    await db.delete(org)
    await db.commit()

    return {"message": "Organization deleted"}


# === Members endpoints ===

@router.get("/{org_id}/members", response_model=list[MembershipResponse])
async def list_members(
    org_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Список участников организации (viewer+)"""
    await require_organization_role(org_id, Role.VIEWER, current_user, db)

    result = await db.execute(
        select(Membership).where(Membership.organization_id == org_id)
    )
    members = result.scalars().all()
    return members


@router.post("/{org_id}/members/invite", response_model=InvitationResponse)
async def invite_member(
    org_id: UUID,
    invite_data: InvitationCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Приглашение участника (admin+)"""
    await require_organization_role(org_id, Role.ADMIN, current_user, db)

    # Генерация токена
    token = secrets.token_urlsafe(32)

    # Проверка на существование приглашения
    existing = await db.execute(
        select(Invitation).where(
            Invitation.organization_id == org_id,
            Invitation.email == invite_data.email,
            Invitation.status == InvitationStatus.PENDING
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invitation already sent"
        )

    invitation = Invitation(
        email=invite_data.email,
        organization_id=org_id,
        role=invite_data.role,
        token=token,
        status=InvitationStatus.PENDING,
        expires_at=datetime.utcnow() + timedelta(days=7)
    )
    db.add(invitation)
    await db.commit()
    await db.refresh(invitation)

    return invitation


@router.put("/{org_id}/members/{user_id}")
async def update_member_role(
    org_id: UUID,
    user_id: UUID,
    role: Role,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Изменение роли участника (только owner)"""
    await require_organization_role(org_id, Role.OWNER, current_user, db)

    # Поиск membership
    result = await db.execute(
        select(Membership).where(
            Membership.organization_id == org_id,
            Membership.user_id == user_id
        )
    )
    membership = result.scalar_one_or_none()

    if not membership:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Member not found"
        )

    # Owner не может дать другому пользователю роль owner
    if role == Role.OWNER:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Cannot assign owner role"
        )

    membership.role = role
    await db.commit()

    return {"message": "Role updated"}


@router.delete("/{org_id}/members/{user_id}")
async def remove_member(
    org_id: UUID,
    user_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Удаление участника (только owner)"""
    await require_organization_role(org_id, Role.OWNER, current_user, db)

    result = await db.execute(
        select(Membership).where(
            Membership.organization_id == org_id,
            Membership.user_id == user_id
        )
    )
    membership = result.scalar_one_or_none()

    if not membership:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Member not found"
        )

    await db.delete(membership)
    await db.commit()

    return {"message": "Member removed"}
