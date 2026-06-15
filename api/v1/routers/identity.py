"""
Identity, organizations and WB API keys — маршруты вынесены из api/v1/nl.py
Контракты (URL, параметры, JSON) сохранены без изменений.
"""
import base64
import json as _json
import secrets
import uuid
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.dependencies import get_current_user
from core.rate_limit import enforce_rate_limit
from core.role_deps import require_organization_role
from core.security import (
    verify_password,
    get_password_hash,
    create_access_token,
    encrypt_data,
    decrypt_data,
)
from core.tenant_auth import require_query_organization_access
from models.organization import Role
from models.user import User


router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)


# ─── Pydantic модели ───────────────────────────────────────

class RegisterData(BaseModel):
    email: str
    password: str
    org_name: str = "Моя организация"


class LoginData(BaseModel):
    email: str
    password: str


# ─── Auth endpoints ────────────────────────────────────────

@router.post("/api/v1/nl/register")
async def nl_register(
    request: Request,
    data: RegisterData,
    db: AsyncSession = Depends(get_db),
):
    """Регистрация нового пользователя + создание организации"""
    await enforce_rate_limit(request, "nl-register", 3, 3600, data.email)
    from models.organization import Organization, Membership, WbApiKey, Role
    from models.organization import SubscriptionTier, SubscriptionStatus

    # Проверка email
    result = await db.execute(select(User).where(User.email == data.email))
    if result.scalar_one_or_none():
        raise HTTPException(400, "Email уже зарегистрирован")

    # Создаём пользователя
    user = User(email=data.email, password_hash=get_password_hash(data.password))
    db.add(user)
    await db.flush()

    # Создаём организацию
    org = Organization(name=data.org_name, subscription_tier=SubscriptionTier.TRIAL, subscription_status=SubscriptionStatus.ACTIVE)
    db.add(org)
    await db.flush()

    # Привязываем пользователя к организации
    membership = Membership(user_id=user.id, organization_id=org.id, role=Role.OWNER)
    db.add(membership)
    await db.commit()

    # Токен с org_id
    token = create_access_token(data={"sub": str(user.id), "org_id": str(org.id)})
    return {"access_token": token, "org_id": str(org.id)}


@router.post("/api/v1/nl/login")
async def nl_login(
    request: Request,
    data: LoginData,
    db: AsyncSession = Depends(get_db),
):
    """Логин"""
    await enforce_rate_limit(request, "nl-login", 5, 60, data.email)
    from models.organization import Membership

    result = await db.execute(select(User).where(User.email == data.email))
    user = result.scalar_one_or_none()
    if not user or not verify_password(data.password, user.password_hash):
        raise HTTPException(401, "Неверный email или пароль")

    # Получаем организацию
    result = await db.execute(
        select(Membership).where(Membership.user_id == user.id)
    )
    membership = result.scalars().first()
    org_id = str(membership.organization_id) if membership else None

    token = create_access_token(data={"sub": str(user.id), "org_id": org_id})
    return {"access_token": token, "org_id": org_id}


@router.get("/api/v1/nl/me")
async def nl_me(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Текущий пользователь"""
    from models.organization import Membership

    result = await db.execute(
        select(Membership).where(Membership.user_id == current_user.id)
    )
    membership = result.scalars().first()
    org_id = str(membership.organization_id) if membership else None
    return {"email": current_user.email, "org_id": org_id}


# ─── Organizations ─────────────────────────────────────────

@router.get("/api/v1/nl/organizations")
async def nl_organizations(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Список организаций пользователя"""
    from models.organization import Membership, Organization, WbApiKey
    result = await db.execute(
        select(Membership, Organization)
        .join(Organization, Membership.organization_id == Organization.id)
        .where(Membership.user_id == current_user.id)
    )
    rows = result.all()
    orgs = []
    for m, o in rows:
        # Count WB keys
        keys_result = await db.execute(
            select(WbApiKey).where(WbApiKey.organization_id == o.id)
        )
        keys = keys_result.scalars().all()
        orgs.append({
            "id": str(o.id),
            "name": o.name,
            "wb_seller_id": o.wb_seller_id,
            "wb_keys_count": len(keys),
            "role": m.role.value if hasattr(m.role, "value") else str(m.role),
        })
    return orgs


@router.post("/api/v1/nl/organizations")
async def nl_create_org(
    data: dict,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Создать новую организацию"""
    from models.organization import Organization, Membership, Role, SubscriptionTier, SubscriptionStatus
    org = Organization(name=data.get("name", "Новый магазин"), subscription_tier=SubscriptionTier.TRIAL, subscription_status=SubscriptionStatus.ACTIVE)
    db.add(org)
    await db.flush()
    membership = Membership(user_id=current_user.id, organization_id=org.id, role=Role.OWNER)
    db.add(membership)
    await db.commit()
    return {"id": str(org.id), "name": org.name}


@router.post("/api/v1/nl/connect-wb")
async def nl_connect_wb(
    data: dict,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Подключить новый магазин WB: создать организацию + ключ + membership"""
    from models.organization import Organization, Membership, WbApiKey, Role, SubscriptionTier, SubscriptionStatus
    from core.security import encrypt_data

    name = data.get("name", "").strip()
    api_key = data.get("api_key", "").strip()

    if not api_key:
        raise HTTPException(400, "API ключ обязателен")
    if not name:
        name = "Новый магазин"

    # Parse JWT to extract seller_id (oid) as integer
    oid = None
    try:
        parts = api_key.split(".")
        if len(parts) >= 2:
            payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
            payload_json = base64.urlsafe_b64decode(payload_b64)
            payload_data = _json.loads(payload_json)
            oid_val = payload_data.get("oid")
            if oid_val is not None:
                oid = int(oid_val)
    except Exception:
        pass

    # Check if org with this seller_id already exists for this user
    if oid:
        result = await db.execute(
            select(Organization, Membership)
            .join(Membership, Membership.organization_id == Organization.id)
            .where(
                Organization.wb_seller_id == oid,
                Membership.user_id == current_user.id,
            )
        )
        existing = result.first()
        if existing:
            raise HTTPException(400, f"Магазин с seller_id {oid} уже подключен ({existing[0].name})")

    # Create organization
    org = Organization(
        name=name,
        wb_seller_id=oid,
        subscription_tier=SubscriptionTier.TRIAL,
        subscription_status=SubscriptionStatus.ACTIVE
    )
    db.add(org)
    await db.flush()

    # Create membership (OWNER)
    membership = Membership(user_id=current_user.id, organization_id=org.id, role=Role.OWNER)
    db.add(membership)

    # Create WB API key (encrypted)
    encrypted = encrypt_data(api_key)
    wb_key = WbApiKey(organization_id=org.id, name=name, personal_token=encrypted, api_key="unused")
    db.add(wb_key)

    await db.commit()

    return {
        "status": "ok",
        "org_id": str(org.id),
        "name": org.name,
        "wb_seller_id": oid,
        "key_id": str(wb_key.id)
    }


# ─── Tax settings ──────────────────────────────────────────

@router.get("/api/v1/nl/tax-settings")
async def get_tax_settings(org_id: str, db: AsyncSession = Depends(get_db)):
    """Налоговые настройки кабинета"""
    from models.organization import Organization
    result = await db.execute(
        select(Organization).where(Organization.id == org_id)
    )
    org = result.scalar_one_or_none()
    if not org:
        raise HTTPException(404, "Организация не найдена")
    return {
        "tax_system": org.tax_system or "",
        "tax_rate": float(org.tax_rate) if org.tax_rate else None,
        "vat_type": org.vat_type or "нет"
    }


@router.post("/api/v1/nl/tax-settings")
async def save_tax_settings(data: dict, org_id: str, db: AsyncSession = Depends(get_db)):
    """Сохранить налоговые настройки кабинета"""
    from models.organization import Organization
    result = await db.execute(
        select(Organization).where(Organization.id == org_id)
    )
    org = result.scalar_one_or_none()
    if not org:
        raise HTTPException(404, "Организация не найдена")

    allowed_systems = ["УСН Доходы", "УСН Доходы-Расходы", "ОСНО", "АУСН Доходы", "АУСН Доходы-Расходы"]
    ts = data.get("tax_system", "")
    if ts and ts not in allowed_systems:
        raise HTTPException(400, f"Недопустимый вид налогообложения. Допустимые: {', '.join(allowed_systems)}")

    allowed_vat = ["нет", "5%", "7%"]
    vt = data.get("vat_type", "нет")
    if vt and vt not in allowed_vat:
        raise HTTPException(400, f"Недопустимый НДС. Допустимые: {', '.join(allowed_vat)}")

    org.tax_system = ts if ts else None
    org.tax_rate = float(data["tax_rate"]) if data.get("tax_rate") else None
    org.vat_type = vt if vt else "нет"
    await db.commit()

    return {
        "tax_system": org.tax_system or "",
        "tax_rate": float(org.tax_rate) if org.tax_rate else None,
        "vat_type": org.vat_type or "нет"
    }


# ─── WB API keys ───────────────────────────────────────────

@router.get("/api/v1/nl/wb-keys")
async def nl_wb_keys(org_id: str, db: AsyncSession = Depends(get_db)):
    """Список WB API ключей организации"""
    from models.organization import WbApiKey
    result = await db.execute(
        select(WbApiKey).where(WbApiKey.organization_id == org_id)
    )
    keys = result.scalars().all()
    return [{"id": str(k.id), "name": k.name, "created_at": str(k.created_at)} for k in keys]


@router.post("/api/v1/nl/wb-keys")
async def nl_add_wb_key(data: dict, org_id: str, db: AsyncSession = Depends(get_db)):
    """Добавить WB API ключ"""
    from models.organization import WbApiKey, Organization
    name = data.get("name", "WB Key")
    api_key = data.get("api_key", "")
    if not api_key:
        raise HTTPException(400, "API ключ обязателен")
    encrypted = encrypt_data(api_key)
    key = WbApiKey(organization_id=org_id, name=name, personal_token=encrypted, api_key="unused")
    db.add(key)
    # Извлекаем wb_seller_id (oid) из JWT payload
    try:
        parts = api_key.split(".")
        if len(parts) >= 2:
            payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
            payload_json = base64.urlsafe_b64decode(payload_b64)
            payload_data = _json.loads(payload_json)
            oid = payload_data.get("oid")
            if oid:
                result = await db.execute(select(Organization).where(Organization.id == org_id))
                org = result.scalar_one_or_none()
                if org:
                    org.wb_seller_id = oid
    except Exception:
        pass  # Не критично если не удалось распарсить
    await db.commit()
    return {"status": "ok", "id": str(key.id)}


@router.delete("/api/v1/nl/wb-keys/{key_id}")
async def nl_delete_wb_key(key_id: str, org_id: str, db: AsyncSession = Depends(get_db)):
    """Удалить WB API ключ"""
    from models.organization import WbApiKey
    result = await db.execute(
        select(WbApiKey).where(WbApiKey.id == key_id, WbApiKey.organization_id == org_id)
    )
    key = result.scalar_one_or_none()
    if not key:
        raise HTTPException(404, "Ключ не найден")
    await db.delete(key)
    await db.commit()
    return {"status": "ok"}


# ─── Profile ───────────────────────────────────────────────

@router.get("/api/v1/nl/profile")
async def nl_profile(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Профиль текущего пользователя + список магазинов (быстро, без подсчётов)"""
    from models.organization import Membership, Organization, WbApiKey
    result = await db.execute(
        select(Membership, Organization)
        .join(Organization, Membership.organization_id == Organization.id)
        .where(Membership.user_id == current_user.id)
    )
    rows = result.all()

    shops = []
    for m, o in rows:
        keys_result = await db.execute(
            select(WbApiKey).where(WbApiKey.organization_id == o.id)
        )
        keys = keys_result.scalars().all()

        keys_info = [{"id": str(k.id), "name": k.name, "created_at": str(k.created_at)[:10] if k.created_at else ""} for k in keys]

        shops.append({
            "id": str(o.id),
            "name": o.name,
            "wb_seller_id": o.wb_seller_id,
            "role": m.role.value if hasattr(m.role, "value") else str(m.role),
            "keys": keys_info,
            "keys_count": len(keys),
        })

    return {"email": current_user.email, "is_superuser": current_user.is_superuser or False, "shops": shops, "shops_count": len(shops)}


# ─── Verify WB key ─────────────────────────────────────────

@router.post("/api/v1/nl/verify-wb-key")
async def nl_verify_wb_key(
    request: Request,
    data: dict,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Проверить работает ли WB API ключ магазина — реальный запрос к WB"""
    import httpx
    from core.security import decrypt_data

    org_id = data.get("org_id", "").strip()
    if not org_id:
        raise HTTPException(400, "org_id обязателен")
    await enforce_rate_limit(request, "wb-key-verify", 10, 300, org_id)
    await require_organization_role(
        uuid.UUID(org_id),
        Role.ADMIN,
        current_user,
        db,
    )

    # Get first active key for this org
    from models.organization import WbApiKey
    result = await db.execute(
        select(WbApiKey).where(WbApiKey.organization_id == org_id)
    )
    keys = result.scalars().all()

    if not keys:
        return {"status": "error", "message": "Нет API ключей", "products_count": 0}

    # Decrypt and try WB API
    for key in keys:
        try:
            api_token = decrypt_data(key.personal_token)
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.post(
                    "https://content-api.wildberries.ru/content/v2/get/cards/list",
                    headers={"Authorization": api_token},
                    json={"settings": {"cursor": {"limit": 1}, "filter": {"withPhoto": -1}}}
                )
                if r.status_code == 200:
                    body = r.json()
                    cards = body.get("cards", [])
                    total = 0
                    try:
                        from models.product_entity import ProductEntity
                        from sqlalchemy import func as sqlfunc
                        result2 = await db.execute(
                            select(sqlfunc.count(ProductEntity.id)).where(ProductEntity.organization_id == org_id)
                        )
                        row = result2.first()
                        if row:
                            total = row[0]
                    except Exception:
                        pass

                    return {
                        "status": "ok",
                        "message": "Ключ работает",
                        "key_name": key.name,
                        "products_count": total,
                        "wb_response": "200 OK"
                    }
                elif r.status_code == 401:
                    return {"status": "error", "message": "Ключ не авторизован (401)", "key_name": key.name, "products_count": 0}
                elif r.status_code == 429:
                    return {"status": "warn", "message": "Слишком много запросов (429), попробуйте позже", "key_name": key.name, "products_count": 0}
                else:
                    return {"status": "error", "message": f"Ошибка WB: {r.status_code}", "key_name": key.name, "products_count": 0}
        except Exception as e:
            return {"status": "error", "message": f"Ошибка: {str(e)[:100]}", "products_count": 0}

    return {"status": "error", "message": "Не удалось проверить", "products_count": 0}


# ─── Rename org ────────────────────────────────────────────

@router.post("/api/v1/nl/rename-org")
async def nl_rename_org(
    data: dict,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Переименовать магазин (организацию)"""
    from models.organization import Membership, Role
    org_id = data.get("org_id", "").strip()
    new_name = data.get("name", "").strip()

    if not org_id or not new_name:
        raise HTTPException(400, "org_id и name обязательны")
    if len(new_name) > 100:
        raise HTTPException(400, "Название слишком длинное (макс 100 символов)")

    # Check user is OWNER or ADMIN in this org
    result = await db.execute(
        select(Membership).where(
            Membership.user_id == current_user.id,
            Membership.organization_id == org_id
        )
    )
    membership = result.scalar_one_or_none()
    if not membership or membership.role not in (Role.OWNER, Role.ADMIN):
        raise HTTPException(403, "Только OWNER или ADMIN может переименовывать")

    from models.organization import Organization
    result = await db.execute(select(Organization).where(Organization.id == org_id))
    org = result.scalar_one_or_none()
    if not org:
        raise HTTPException(404, "Организация не найдена")

    org.name = new_name
    await db.commit()

    return {"status": "ok", "name": new_name}


# ─── Invite ────────────────────────────────────────────────

@router.post("/api/v1/nl/invite")
async def nl_invite(
    request: Request,
    data: dict,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Пригласить коллегу в организацию"""
    from models.organization import Membership, Role
    import secrets

    org_id = data.get("org_id", "").strip()
    email = data.get("email", "").strip().lower()
    role_str = data.get("role", "VIEWER").upper()

    if not org_id or not email:
        raise HTTPException(400, "org_id и email обязательны")
    await enforce_rate_limit(
        request,
        "organization-invite",
        10,
        3600,
        f"{org_id}:{email}",
    )

    # Check inviter is ADMIN+ in this org
    result = await db.execute(
        select(Membership).where(
            Membership.user_id == current_user.id,
            Membership.organization_id == org_id
        )
    )
    membership = result.scalar_one_or_none()
    if not membership or membership.role not in (Role.OWNER, Role.ADMIN):
        raise HTTPException(403, "Только OWNER или ADMIN может приглашать")

    # Check role is valid
    if role_str not in ("ADMIN", "VIEWER"):
        role_str = "VIEWER"

    from models.organization import Invitation, InvitationStatus
    invite_token = secrets.token_urlsafe(32)
    invitation = Invitation(
        email=email,
        organization_id=org_id,
        role=Role[role_str],
        token=invite_token,
        status=InvitationStatus.PENDING,
        expires_at=datetime.utcnow() + timedelta(days=7)
    )
    db.add(invitation)
    await db.commit()

    return {
        "status": "ok",
        "email": email,
        "role": role_str,
        "invite_token": invite_token,
        "expires_at": str(invitation.expires_at)[:19]
    }
