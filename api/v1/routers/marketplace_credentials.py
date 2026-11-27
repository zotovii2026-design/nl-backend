from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.dependencies import get_current_user
from core.role_deps import require_organization_role
from core.security import encrypt_data
from models.marketplace_credential import MarketplaceCredential
from models.organization import Role
from models.user import User
from schemas.marketplace_credentials import (
    MarketplaceCredentialCreate,
    MarketplaceCredentialRead,
    MarketplaceCredentialUpdate,
)
from services.marketplace_credentials import validate_capabilities, verify_credential_auth
from core.security import decrypt_data
from datetime import datetime, timezone


router = APIRouter(prefix="/api/v1/nl/marketplace-credentials", tags=["marketplace credentials"])


def _read(item: MarketplaceCredential) -> MarketplaceCredentialRead:
    return MarketplaceCredentialRead(
        id=item.id,
        marketplace=item.marketplace,
        name=item.name,
        purpose=item.purpose,
        capabilities=list(item.capabilities or []),
        status=item.status,
        is_enabled=bool(item.is_enabled),
        has_client_id=bool(item.encrypted_client_id),
        last_validated_at=item.last_validated_at,
        created_at=item.created_at,
    )


@router.get("", response_model=list[MarketplaceCredentialRead])
async def list_credentials(
    org_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    await require_organization_role(str(org_id), Role.VIEWER, current_user, db)
    result = await db.execute(
        select(MarketplaceCredential)
        .where(MarketplaceCredential.organization_id == org_id)
        .order_by(MarketplaceCredential.marketplace, MarketplaceCredential.created_at.desc())
    )
    return [_read(item) for item in result.scalars().all()]


@router.post("", response_model=MarketplaceCredentialRead, status_code=status.HTTP_201_CREATED)
async def create_credential(
    data: MarketplaceCredentialCreate,
    org_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    await require_organization_role(str(org_id), Role.ADMIN, current_user, db)
    try:
        capabilities = validate_capabilities(data.marketplace, data.capabilities)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    item = MarketplaceCredential(
        organization_id=org_id,
        marketplace=data.marketplace,
        name=data.name.strip(),
        purpose=data.purpose.strip(),
        capabilities=capabilities,
        encrypted_secret=encrypt_data(data.secret.strip()),
        encrypted_client_id=encrypt_data(data.client_id.strip()) if data.client_id else None,
        status="unverified",
        is_enabled=False,
        created_by_user_id=current_user.id,
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)
    return _read(item)


@router.patch("/{credential_id}", response_model=MarketplaceCredentialRead)
async def update_credential(
    credential_id: UUID,
    data: MarketplaceCredentialUpdate,
    org_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    await require_organization_role(str(org_id), Role.ADMIN, current_user, db)
    result = await db.execute(select(MarketplaceCredential).where(
        MarketplaceCredential.id == credential_id,
        MarketplaceCredential.organization_id == org_id,
    ))
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(404, "Подключение не найдено")
    changes = data.model_dump(exclude_unset=True)
    if "capabilities" in changes:
        try:
            item.capabilities = validate_capabilities(item.marketplace, changes["capabilities"])
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
    if "name" in changes:
        item.name = changes["name"].strip()
    if "purpose" in changes:
        item.purpose = changes["purpose"].strip()
    if changes.get("secret"):
        item.encrypted_secret = encrypt_data(changes["secret"].strip())
        item.status = "unverified"
        item.is_enabled = False
        item.last_validated_at = None
    if "client_id" in changes:
        if item.marketplace == "ozon" and not (changes["client_id"] or "").strip():
            raise HTTPException(400, "Для Ozon обязателен Client-Id")
        item.encrypted_client_id = encrypt_data(changes["client_id"].strip()) if changes["client_id"] else None
        item.status = "unverified"
        item.is_enabled = False
        item.last_validated_at = None
    if changes.get("is_enabled") is True and item.status != "valid":
        raise HTTPException(400, "Нельзя включить непроверенный ключ")
    if "is_enabled" in changes:
        item.is_enabled = bool(changes["is_enabled"])
    await db.commit()
    await db.refresh(item)
    return _read(item)


@router.post("/{credential_id}/verify", response_model=MarketplaceCredentialRead)
async def verify_credential(
    credential_id: UUID,
    org_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    await require_organization_role(str(org_id), Role.ADMIN, current_user, db)
    result = await db.execute(select(MarketplaceCredential).where(
        MarketplaceCredential.id == credential_id,
        MarketplaceCredential.organization_id == org_id,
    ))
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(404, "Подключение не найдено")
    try:
        check = await verify_credential_auth(
            item.marketplace,
            decrypt_data(item.encrypted_secret),
            decrypt_data(item.encrypted_client_id) if item.encrypted_client_id else None,
            list(item.capabilities or []),
        )
    except Exception as exc:
        item.status = "error"
        item.is_enabled = False
        await db.commit()
        raise HTTPException(502, "Не удалось выполнить безопасную проверку API") from exc
    item.last_validated_at = datetime.now(timezone.utc)
    item.status = "valid" if check["valid"] else "invalid"
    if not check["valid"]:
        item.is_enabled = False
    await db.commit()
    await db.refresh(item)
    if not check["valid"]:
        raise HTTPException(400, check.get("message", "Ключ не прошёл проверку"))
    return _read(item)


@router.delete("/{credential_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_credential(
    credential_id: UUID,
    org_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    await require_organization_role(str(org_id), Role.ADMIN, current_user, db)
    result = await db.execute(select(MarketplaceCredential).where(
        MarketplaceCredential.id == credential_id,
        MarketplaceCredential.organization_id == org_id,
    ))
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(404, "Подключение не найдено")
    await db.delete(item)
    await db.commit()
