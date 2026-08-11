"""
Identity, organizations and WB API keys — маршруты вынесены из api/v1/nl.py
Контракты (URL, параметры, JSON) сохранены без изменений.
"""
import base64
import json as _json
import logging
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
from domain.unit_economics import DEFAULT_FBO_AVERAGE_WAREHOUSES, normalize_fbo_average_warehouses
from models.organization import Role
from models.user import User


router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)
logger = logging.getLogger(__name__)
ADS_INITIAL_BACKFILL_DAYS = 9


def _enqueue_wb_initial_tasks(org_id: str):
    """Queue initial WB syncs after a valid token is saved."""
    task_ids = {}
    warnings = {}
    try:
        from tasks.celery_app import celery_app

        initial_task = celery_app.send_task("wb.initial_sync", kwargs={"org_id": org_id})
        task_ids["initial_sync_task_id"] = initial_task.id

        ads_task = celery_app.send_task(
            "wb.sched.ad_stats",
            kwargs={
                "days_back": ADS_INITIAL_BACKFILL_DAYS,
                "org_id": org_id,
                "include_current_day": True,
            },
        )
        task_ids["ads_backfill_task_id"] = ads_task.id
    except Exception as exc:
        logger.exception("[connect-wb] failed to enqueue initial tasks org=%s", org_id)
        warnings["initial_sync_warning"] = str(exc)
    return task_ids, warnings


def _extract_wb_seller_id(api_key: str) -> Optional[int]:
    try:
        parts = api_key.split(".")
        if len(parts) < 2:
            return None
        payload_b64 = parts[1] + "=" * (4 - len(parts[1]) % 4)
        payload_json = base64.urlsafe_b64decode(payload_b64)
        payload_data = _json.loads(payload_json)
        oid_val = payload_data.get("oid")
        return int(oid_val) if oid_val is not None else None
    except Exception:
        return None


async def _activate_wb_key(db: AsyncSession, key) -> None:
    from models.organization import WbApiKey

    result = await db.execute(
        select(WbApiKey).where(WbApiKey.organization_id == key.organization_id)
    )
    for other in result.scalars().all():
        other.is_active = False
    await db.flush()
    key.is_active = True


async def _find_user_org_by_seller_id(
    db: AsyncSession,
    user_id,
    seller_id: int,
):
    from models.organization import Membership, Organization

    result = await db.execute(
        select(Organization, Membership)
        .join(Membership, Membership.organization_id == Organization.id)
        .where(
            Organization.wb_seller_id == seller_id,
            Membership.user_id == user_id,
        )
    )
    return result.first()


def _duplicate_shop_detail(org, seller_id: int) -> dict:
    return {
        "code": "shop_already_connected",
        "message": f"Магазин уже подключён: {org.name}, ID {seller_id}. Добавьте ключ в существующий магазин.",
        "org_id": str(org.id),
        "org_name": org.name,
        "wb_seller_id": seller_id,
    }


# ─── Pydantic модели ───────────────────────────────────────

class RegisterData(BaseModel):
    email: str
    password: str
    # Backward-compatible; registration no longer creates an empty organization.
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
    """Регистрация нового пользователя без пустой организации."""
    await enforce_rate_limit(request, "nl-register", 3, 3600, data.email)

    # Проверка email
    result = await db.execute(select(User).where(User.email == data.email))
    if result.scalar_one_or_none():
        raise HTTPException(400, "Email уже зарегистрирован")

    # Создаём пользователя
    user = User(email=data.email, password_hash=get_password_hash(data.password))
    db.add(user)
    await db.commit()

    # Токен без org_id: первый кабинет подключается в onboarding через /connect-wb.
    token = create_access_token(data={"sub": str(user.id), "org_id": None})
    return {"access_token": token, "org_id": None, "needs_onboarding": True}


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
            select(WbApiKey)
            .where(WbApiKey.organization_id == o.id)
            .order_by(WbApiKey.is_active.desc(), WbApiKey.created_at.desc().nullslast())
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
    from services.wb_api.token_validator import validate_wb_token

    name = data.get("name", "").strip()
    api_key = data.get("api_key", "").strip()

    if not api_key:
        raise HTTPException(400, "API ключ обязателен")
    if not name:
        name = "Новый магазин"

    validation = await validate_wb_token(api_key)
    if validation["token_status"] in ("invalid", "limited"):
        raise HTTPException(400, validation["message"])

    oid = _extract_wb_seller_id(api_key)

    if oid:
        existing = await _find_user_org_by_seller_id(db, current_user.id, oid)
        if existing:
            raise HTTPException(409, _duplicate_shop_detail(existing[0], oid))

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
    wb_key = WbApiKey(
        organization_id=org.id,
        name=name,
        personal_token=encrypted,
        api_key="unused",
        token_status=validation["token_status"],
        validated_at=datetime.utcnow(),
        is_active=True,
    )
    db.add(wb_key)

    await db.commit()

    task_ids, task_warnings = _enqueue_wb_initial_tasks(str(org.id))

    return {
        "status": "ok",
        "org_id": str(org.id),
        "name": org.name,
        "wb_seller_id": oid,
        "key_id": str(wb_key.id),
        "token_status": validation["token_status"],
        "message": validation["message"],
        **task_ids,
        **task_warnings,
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


# ─── FBO average warehouse settings ─────────────────────────

async def _latest_fbo_tariff_warehouses(db: AsyncSession, org_id: str) -> list[str]:
    from sqlalchemy import text

    result = await db.execute(text("""
        SELECT warehouse_name
        FROM wb_box_tariffs
        WHERE organization_id = :org_id
          AND box_delivery_base IS NOT NULL
          AND box_delivery_liter IS NOT NULL
          AND snapshot_date = (
              SELECT MAX(snapshot_date)
              FROM wb_box_tariffs
              WHERE organization_id = :org_id
          )
        ORDER BY warehouse_name
    """), {"org_id": org_id})
    return [row[0] for row in result.all() if row[0]]


def _clean_selected_fbo_warehouses(raw) -> list[str]:
    if not isinstance(raw, list):
        return []
    selected = []
    seen = set()
    for item in raw:
        clean = str(item or "").strip()
        key = clean.lower()
        if clean and key not in seen:
            selected.append(clean)
            seen.add(key)
    return selected


def _fbo_settings_payload(org, available: list[str]) -> dict:
    selected = _clean_selected_fbo_warehouses(org.fbo_avg_warehouses)
    available_by_key = {name.lower(): name for name in available}
    matched_selected = [
        available_by_key[name.lower()]
        for name in selected
        if name.lower() in available_by_key
    ]
    effective_source = matched_selected if available else selected
    effective = list(normalize_fbo_average_warehouses(effective_source))
    return {
        "available": available,
        "selected": selected,
        "effective": effective,
        "default": list(DEFAULT_FBO_AVERAGE_WAREHOUSES),
        "is_default": not selected,
    }


@router.get("/api/v1/nl/fbo-average-warehouses")
async def get_fbo_average_warehouses(org_id: str, db: AsyncSession = Depends(get_db)):
    """Склады FBO, по которым усредняется логистика в Юнит Экономике."""
    from models.organization import Organization

    result = await db.execute(
        select(Organization).where(Organization.id == org_id)
    )
    org = result.scalar_one_or_none()
    if not org:
        raise HTTPException(404, "Организация не найдена")
    return _fbo_settings_payload(
        org,
        await _latest_fbo_tariff_warehouses(db, org_id),
    )


@router.post("/api/v1/nl/fbo-average-warehouses")
async def save_fbo_average_warehouses(
    data: dict,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Сохранить склады FBO для усреднения логистики."""
    from models.organization import Organization

    result = await db.execute(
        select(Organization).where(Organization.id == org_id)
    )
    org = result.scalar_one_or_none()
    if not org:
        raise HTTPException(404, "Организация не найдена")

    selected = _clean_selected_fbo_warehouses(data.get("warehouses"))
    if len(selected) > 5:
        raise HTTPException(400, "Можно выбрать не больше 5 складов FBO")

    available = await _latest_fbo_tariff_warehouses(db, org_id)
    available_by_key = {name.lower(): name for name in available}
    invalid = [name for name in selected if name.lower() not in available_by_key]
    if invalid:
        raise HTTPException(400, "Склад не найден в тарифах WB: " + ", ".join(invalid))

    org.fbo_avg_warehouses = [available_by_key[name.lower()] for name in selected] or None
    await db.commit()

    try:
        import redis as _redis_lib

        _redis_lib.from_url("redis://redis:6379/0").delete(f"ue_cache:{org_id}")
    except Exception as exc:
        logger.warning("Failed to clear UE cache after FBO settings save org=%s: %s", org_id, exc)

    return _fbo_settings_payload(org, available)


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
        select(WbApiKey)
        .where(WbApiKey.organization_id == org_id)
        .order_by(WbApiKey.is_active.desc(), WbApiKey.created_at.desc().nullslast())
    )
    keys = result.scalars().all()
    return [
        {
            "id": str(k.id),
            "name": k.name,
            "created_at": str(k.created_at),
            "token_status": k.token_status or "unknown",
            "validated_at": str(k.validated_at) if k.validated_at else None,
            "is_active": bool(k.is_active),
        }
        for k in keys
    ]


@router.post("/api/v1/nl/wb-keys")
async def nl_add_wb_key(
    data: dict,
    org_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Добавить WB API ключ с обязательной валидацией"""
    from models.organization import WbApiKey, Organization
    from services.wb_api.token_validator import validate_wb_token
    await require_organization_role(org_id, Role.ADMIN, current_user, db)

    name = data.get("name", "WB Key")
    api_key = data.get("api_key", "")
    use_this = bool(data.get("is_active", data.get("use_this", False)))
    if not api_key:
        raise HTTPException(400, "API ключ обязателен")

    # Валидация токена перед сохранением
    validation = await validate_wb_token(api_key)
    if validation["token_status"] == "invalid":
        raise HTTPException(400, validation["message"])
    if validation["token_status"] == "limited":
        raise HTTPException(400, validation["message"])

    result = await db.execute(select(Organization).where(Organization.id == org_id))
    org = result.scalar_one_or_none()
    if not org:
        raise HTTPException(404, "Организация не найдена")

    oid = _extract_wb_seller_id(api_key)
    if not oid:
        raise HTTPException(400, "Не удалось определить WB ID из API ключа")

    existing = await _find_user_org_by_seller_id(db, current_user.id, oid)
    if existing and str(existing[0].id) != str(org_id):
        raise HTTPException(409, _duplicate_shop_detail(existing[0], oid))

    if org.wb_seller_id and int(org.wb_seller_id) != int(oid):
        raise HTTPException(
            400,
            f"Этот ключ относится к WB ID {oid}, а выбранный магазин — {org.wb_seller_id}. Добавьте ключ в правильный магазин.",
        )

    encrypted = encrypt_data(api_key)
    key = WbApiKey(
        organization_id=org_id,
        name=name,
        personal_token=encrypted,
        api_key="unused",
        token_status=validation["token_status"],
        validated_at=datetime.utcnow(),
    )
    db.add(key)
    await db.flush()
    if not org.wb_seller_id:
        org.wb_seller_id = oid

    active_result = await db.execute(
        select(WbApiKey).where(WbApiKey.organization_id == org_id, WbApiKey.is_active.is_(True))
    )
    if use_this or active_result.scalar_one_or_none() is None:
        await _activate_wb_key(db, key)

    await db.commit()
    task_ids, task_warnings = _enqueue_wb_initial_tasks(str(org_id))
    return {
        "status": "ok",
        "id": str(key.id),
        "token_status": validation["token_status"],
        "is_active": bool(key.is_active),
        "message": validation["message"],
        **task_ids,
        **task_warnings,
    }


@router.patch("/api/v1/nl/wb-keys/{key_id}")
async def nl_update_wb_key(
    key_id: str,
    data: dict,
    org_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Редактировать имя/токен WB API ключа и при необходимости сделать его активным."""
    from models.organization import Organization, WbApiKey
    from services.wb_api.token_validator import validate_wb_token

    await require_organization_role(org_id, Role.ADMIN, current_user, db)

    result = await db.execute(
        select(WbApiKey).where(WbApiKey.id == key_id, WbApiKey.organization_id == org_id)
    )
    key = result.scalar_one_or_none()
    if not key:
        raise HTTPException(404, "Ключ не найден")

    name = data.get("name")
    if name is not None:
        name = str(name).strip()
        if not name:
            raise HTTPException(400, "Название ключа не может быть пустым")
        if len(name) > 255:
            raise HTTPException(400, "Название ключа слишком длинное")
        key.name = name

    validation = None
    api_key = str(data.get("api_key") or "").strip()
    if api_key:
        validation = await validate_wb_token(api_key)
        if validation["token_status"] in ("invalid", "limited"):
            raise HTTPException(400, validation["message"])

        oid = _extract_wb_seller_id(api_key)
        if not oid:
            raise HTTPException(400, "Не удалось определить WB ID из API ключа")

        org_result = await db.execute(select(Organization).where(Organization.id == org_id))
        org = org_result.scalar_one_or_none()
        if not org:
            raise HTTPException(404, "Организация не найдена")

        existing = await _find_user_org_by_seller_id(db, current_user.id, oid)
        if existing and str(existing[0].id) != str(org_id):
            raise HTTPException(409, _duplicate_shop_detail(existing[0], oid))

        if org.wb_seller_id and int(org.wb_seller_id) != int(oid):
            raise HTTPException(
                400,
                f"Этот ключ относится к WB ID {oid}, а выбранный магазин — {org.wb_seller_id}. Добавьте ключ в правильный магазин.",
            )
        if not org.wb_seller_id:
            org.wb_seller_id = oid

        key.personal_token = encrypt_data(api_key)
        key.api_key = "unused"
        key.token_status = validation["token_status"]
        key.validated_at = datetime.utcnow()

    if "is_active" in data or "use_this" in data:
        use_this = bool(data.get("is_active", data.get("use_this")))
        if use_this:
            if key.token_status != "valid":
                raise HTTPException(400, "Сначала проверьте ключ: активным можно сделать только валидный ключ")
            await _activate_wb_key(db, key)
        else:
            key.is_active = False

    await db.commit()
    await db.refresh(key)
    return {
        "status": "ok",
        "id": str(key.id),
        "name": key.name,
        "token_status": key.token_status or "unknown",
        "validated_at": str(key.validated_at) if key.validated_at else None,
        "is_active": bool(key.is_active),
        "message": validation["message"] if validation else "Сохранено",
    }


@router.delete("/api/v1/nl/wb-keys/{key_id}")
async def nl_delete_wb_key(
    key_id: str,
    org_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Удалить WB API ключ"""
    from models.organization import WbApiKey
    await require_organization_role(org_id, Role.ADMIN, current_user, db)

    result = await db.execute(
        select(WbApiKey).where(WbApiKey.id == key_id, WbApiKey.organization_id == org_id)
    )
    key = result.scalar_one_or_none()
    if not key:
        raise HTTPException(404, "Ключ не найден")
    was_active = bool(key.is_active)
    await db.delete(key)
    if was_active:
        fallback_result = await db.execute(
            select(WbApiKey)
            .where(
                WbApiKey.organization_id == org_id,
                WbApiKey.id != key.id,
                WbApiKey.token_status == "valid",
            )
            .order_by(WbApiKey.validated_at.desc().nullslast(), WbApiKey.created_at.desc().nullslast())
        )
        fallback = fallback_result.scalars().first()
        if fallback:
            fallback.is_active = True
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

        keys_info = [{
            "id": str(k.id),
            "name": k.name,
            "created_at": str(k.created_at)[:10] if k.created_at else "",
            "token_status": k.token_status or "unknown",
            "validated_at": str(k.validated_at) if k.validated_at else None,
            "is_active": bool(k.is_active),
        } for k in keys]

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
    """Полная проверка WB API ключа — все endpoints"""
    from core.security import decrypt_data
    from services.wb_api.token_validator import validate_wb_token
    from datetime import datetime as _dt, timezone as _tz

    org_id = data.get("org_id", "").strip()
    if not org_id:
        raise HTTPException(400, "org_id обязателен")
    await enforce_rate_limit(request, "wb-key-verify", 10, 300, org_id)
    await require_organization_role(
        org_id,
        Role.ADMIN,
        current_user,
        db,
    )

    from models.organization import WbApiKey
    result = await db.execute(
        select(WbApiKey).where(WbApiKey.organization_id == org_id)
    )
    keys = result.scalars().all()

    if not keys:
        return {"status": "error", "message": "Нет API ключей", "products_count": 0}

    key_id = str(data.get("key_id") or "").strip()
    if key_id:
        key = next((k for k in keys if str(k.id) == key_id), None)
        if not key:
            raise HTTPException(404, "Ключ не найден")
    else:
        key = next((k for k in keys if k.is_active), None) or keys[0]
    api_token = decrypt_data(key.personal_token or key.api_key)
    if not api_token:
        raise HTTPException(400, "Не удалось расшифровать ключ")
    validation = await validate_wb_token(api_token)

    # Обновляем статус в БД
    key.token_status = validation["token_status"]
    key.validated_at = _dt.now(_tz.utc)
    await db.commit()

    # Считаем товары
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
        "status": validation["token_status"],
        "message": validation["message"],
        "key_id": str(key.id),
        "key_name": key.name,
        "is_active": bool(key.is_active),
        "products_count": total,
        "token_status": validation["token_status"],
        "acc_level": validation["acc_level"],
        "details": validation["details"],
        "ok_count": validation["ok_count"],
        "total_endpoints": validation["total"],
    }


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
