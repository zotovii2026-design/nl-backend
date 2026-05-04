"""API для справочного листа, авторизации и фронтенд НЛ"""
from fastapi import APIRouter, Depends, Query, Request, HTTPException, status
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date, timedelta

from core.database import get_db
from core.security import verify_password, get_password_hash, create_access_token, decode_token, encrypt_data, decrypt_data
from core.dependencies import get_current_user
from models.user import User
from models.reference_book import ReferenceBook
from models.raw_data import TechStatus

router = APIRouter(tags=["nl"])


# ─── AUTH HELPERS ──────────────────────────────────────────

def get_org_from_token(token: str) -> str:
    """Извлечь org_id из JWT"""
    payload = decode_token(token)
    if not payload:
        return None
    return payload.get("org_id")


# ─── API ENDPOINTS ─────────────────────────────────────────

class RegisterData(BaseModel):
    email: str
    password: str
    org_name: str = "Моя организация"


class LoginData(BaseModel):
    email: str
    password: str


class RefItem(BaseModel):
    nm_id: int
    vendor_code: Optional[str] = None
    product_name: Optional[str] = None
    target_date: Optional[str] = None  # YYYY-MM-DD
    cost_price: Optional[float] = None
    purchase_price: Optional[float] = None
    packaging_cost: Optional[float] = None
    logistics_cost: Optional[float] = None
    other_costs: Optional[float] = None
    notes: Optional[str] = None
    product_class: Optional[str] = None
    brand: Optional[str] = None
    tax_system: Optional[str] = None  # usn / osn / usn_dr
    tax_rate: Optional[float] = None
    vat_rate: Optional[float] = None


@router.post("/api/v1/nl/register")
async def nl_register(data: RegisterData, db: AsyncSession = Depends(get_db)):
    """Регистрация нового пользователя + создание организации"""
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
async def nl_login(data: LoginData, db: AsyncSession = Depends(get_db)):
    """Логин"""
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
async def nl_me(token: str = Query(""), db: AsyncSession = Depends(get_db)):
    """Текущий пользователь"""
    payload = decode_token(token)
    if not payload:
        raise HTTPException(401, "Не авторизован")
    user_id = payload.get("sub")
    org_id = payload.get("org_id")
    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(401, "Пользователь не найден")
    return {"email": user.email, "org_id": org_id}


@router.get("/api/v1/nl/reference")
async def get_reference(org_id: str, target_date: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    """Справочник — все актуальные записи"""
    from datetime import datetime as dt
    from sqlalchemy import text
    sql = (
        "SELECT nm_id, vendor_code, cost_price, purchase_cost as purchase_price, packaging_cost, "
        "logistics_cost, other_costs, notes, product_class, brand, tax_system, tax_rate, vat_rate, "
        "valid_from FROM reference_book "
        "WHERE organization_id = :org AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)"
    )
    params = {"org": org_id}
    if target_date:
        sql += " AND valid_from <= :td"
        params["td"] = dt.strptime(target_date, "%Y-%m-%d").date()
    sql += " ORDER BY nm_id, valid_from DESC"
    result = await db.execute(text(sql), params)
    return [{
        "nm_id": r[0],
        "vendor_code": r[1],
        "target_date": str(r[13]),
        "cost_price": float(r[2]) if r[2] else None,
        "purchase_price": float(r[3]) if r[3] else None,
        "packaging_cost": float(r[4]) if r[4] else None,
        "logistics_cost": float(r[5]) if r[5] else None,
        "other_costs": float(r[6]) if r[6] else None,
        "notes": r[7],
        "product_class": r[8],
        "brand": r[9],
        "tax_system": r[10],
        "tax_rate": float(r[11]) if r[11] else None,
        "vat_rate": float(r[12]) if r[12] else None,
    } for r in result.all()]


@router.post("/api/v1/nl/reference")
async def save_reference(item: RefItem, org_id: str, db: AsyncSession = Depends(get_db)):
    """Сохранить строку справочного листа"""
    from datetime import datetime as dt_mod
    t_date = dt_mod.strptime(item.target_date, "%Y-%m-%d").date() if item.target_date else date.today()
    # entity_id lookup
    from sqlalchemy import text as _sql_text
    ent_q = await db.execute(_sql_text(
        "SELECT id FROM product_entities WHERE organization_id = :org AND nm_id = :nm LIMIT 1"
    ), {"org": org_id, "nm": item.nm_id})
    ent_row = ent_q.first()
    eid = ent_row[0] if ent_row else None
    ins = pg_insert(ReferenceBook).values(
        organization_id=org_id, nm_id=item.nm_id, vendor_code=item.vendor_code,
        valid_from=t_date, entity_id=eid,
        cost_price=item.cost_price,
        purchase_cost=item.purchase_price, packaging_cost=item.packaging_cost,
        logistics_cost=item.logistics_cost, other_costs=item.other_costs, notes=item.notes,
        product_class=item.product_class, brand=item.brand,
        tax_system=item.tax_system, tax_rate=item.tax_rate, vat_rate=item.vat_rate,
    )
    stmt = ins.on_conflict_do_update(
        constraint="reference_book_org_entity_vf_key",
        set_={
            "vendor_code": ins.excluded.vendor_code,
            "cost_price": ins.excluded.cost_price, "purchase_cost": ins.excluded.purchase_cost,
            "packaging_cost": ins.excluded.packaging_cost, "logistics_cost": ins.excluded.logistics_cost,
            "other_costs": ins.excluded.other_costs, "notes": ins.excluded.notes,
            "product_class": ins.excluded.product_class, "brand": ins.excluded.brand,
            "tax_system": ins.excluded.tax_system, "tax_rate": ins.excluded.tax_rate, "vat_rate": ins.excluded.vat_rate,
            "updated_at": func.now(),
        }
    )
    await db.execute(stmt)
    await db.commit()
    return {"status": "ok"}


@router.get("/api/v1/nl/products")
async def get_products(org_id: str, target_date: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    """Список уникальных карточек из ТС на дату с entity_id и size_name"""
    from datetime import datetime as dt_mod
    from models.product_entity import ProductEntity, EntityBarcode
    q = select(
        TechStatus.entity_id, TechStatus.nm_id, TechStatus.vendor_code, TechStatus.product_name,
        TechStatus.photo_main, TechStatus.barcode, TechStatus.sku
    ).where(TechStatus.organization_id == org_id, TechStatus.nm_id.isnot(None))
    if target_date:
        q = q.where(TechStatus.target_date == dt_mod.strptime(target_date, "%Y-%m-%d").date())
    q = q.distinct()
    result = await db.execute(q)

    # Получаем маппинг entity_id → size_name
    ent_result = await db.execute(
        select(ProductEntity.id, ProductEntity.size_name).where(
            ProductEntity.organization_id == org_id
        )
    )
    size_map = {str(r[0]): r[1] for r in ent_result.all()}

    # Получаем все активные ШК по сущностям
    bc_result = await db.execute(
        select(EntityBarcode.entity_id, EntityBarcode.barcode).where(
            EntityBarcode.organization_id == org_id,
            EntityBarcode.is_active == True,
        )
    )
    barcode_map = {}
    for r in bc_result.all():
        eid = str(r[0])
        if eid not in barcode_map:
            barcode_map[eid] = []
        barcode_map[eid].append(r[1])

    items = []
    seen = set()
    for r in result.all():
        eid = str(r[0]) if r[0] else None
        key = (r[1], eid)  # уникальность по nm_id + entity_id
        if key in seen:
            continue
        seen.add(key)
        items.append({
            "entity_id": eid,
            "nm_id": r[1],
            "vendor_code": r[2],
            "product_name": r[3],
            "photo_main": r[4],
            "barcode": r[5],
            "sku": r[6],
            "size_name": size_map.get(eid, "") if eid else "",
            "barcodes": barcode_map.get(eid, []) if eid else ([r[5]] if r[5] else []),
        })
    return items




@router.get("/api/v1/nl/organizations")
async def nl_organizations(token: str = Query(""), db: AsyncSession = Depends(get_db)):
    """Список организаций пользователя"""
    from models.organization import Membership, Organization, WbApiKey
    payload = decode_token(token)
    if not payload:
        raise HTTPException(401, "Не авторизован")
    user_id = payload.get("sub")
    result = await db.execute(
        select(Membership, Organization)
        .join(Organization, Membership.organization_id == Organization.id)
        .where(Membership.user_id == user_id)
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
async def nl_create_org(data: dict, token: str = Query(""), db: AsyncSession = Depends(get_db)):
    """Создать новую организацию"""
    from models.organization import Organization, Membership, Role, SubscriptionTier, SubscriptionStatus
    payload = decode_token(token)
    if not payload:
        raise HTTPException(401, "Не авторизован")
    user_id = payload.get("sub")
    org = Organization(name=data.get("name", "Новый магазин"), subscription_tier=SubscriptionTier.TRIAL, subscription_status=SubscriptionStatus.ACTIVE)
    db.add(org)
    await db.flush()
    membership = Membership(user_id=user_id, organization_id=org.id, role=Role.OWNER)
    db.add(membership)
    await db.commit()
    return {"id": str(org.id), "name": org.name}


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
    import base64, json as _json
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



@router.get("/api/v1/nl/dates")
async def get_available_dates(org_id: str, db: AsyncSession = Depends(get_db)):
    """Доступные даты в ТС"""
    result = await db.execute(
        select(TechStatus.target_date)
        .where(TechStatus.organization_id == org_id)
        .distinct()
        .order_by(TechStatus.target_date.desc())
        .limit(30)
    )
    return [str(r[0]) for r in result.all()]


@router.get("/api/v1/nl/control")
async def get_control_metrics(org_id: str, target_date: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    """Оперативный контроль — метрики на дату"""
    from sqlalchemy import func, case, and_
    from datetime import datetime as dt_mod
    import decimal

    d = dt_mod.strptime(target_date, "%Y-%m-%d").date() if target_date else date.today()

    # Основные метрики за день
    result = await db.execute(
        select(
            func.count(TechStatus.id).label("total_products"),
            func.sum(TechStatus.stock_qty).label("total_stock"),
            func.sum(TechStatus.orders_count).label("total_orders"),
            func.sum(TechStatus.buyouts_count).label("total_buyouts"),
            func.sum(TechStatus.returns_count).label("total_returns"),
            func.sum(TechStatus.impressions).label("total_impressions"),
            func.sum(TechStatus.clicks).label("total_clicks"),
            func.sum(TechStatus.ad_cost).label("total_ad_cost"),
            func.sum(TechStatus.price_discount * TechStatus.buyouts_count).label("total_revenue"),
            func.sum(TechStatus.price_discount * TechStatus.buyouts_count).label("total_revenue_gross"),
            func.avg(TechStatus.rating).label("avg_rating"),
        ).where(TechStatus.organization_id == org_id, TechStatus.target_date == d)
    )
    row = result.one()

    # Товары с нулевым остатком
    zero_stock = await db.execute(
        select(func.count(TechStatus.id)).where(
            TechStatus.organization_id == org_id, TechStatus.target_date == d,
            TechStatus.stock_qty <= 0
        )
    )

    # Товары с низким остатком (<=5)
    low_stock = await db.execute(
        select(func.count(TechStatus.id)).where(
            TechStatus.organization_id == org_id, TechStatus.target_date == d,
            TechStatus.stock_qty > 0, TechStatus.stock_qty <= 5
        )
    )

    # Товары по рейтингу (< 4)
    low_rating = await db.execute(
        select(func.count(TechStatus.id)).where(
            TechStatus.organization_id == org_id, TechStatus.target_date == d,
            TechStatus.rating < 4.0
        )
    )

    # Детализация по товарам (с entity_id)
    products_detail = await db.execute(
        select(
            TechStatus.entity_id, TechStatus.nm_id, TechStatus.vendor_code, TechStatus.product_name,
            TechStatus.photo_main, TechStatus.stock_qty, TechStatus.orders_count,
            TechStatus.buyouts_count, TechStatus.returns_count, TechStatus.rating,
            TechStatus.impressions, TechStatus.clicks, TechStatus.ad_cost,
            TechStatus.price, TechStatus.price_discount, TechStatus.tariff,
            TechStatus.barcode,
        ).where(TechStatus.organization_id == org_id, TechStatus.target_date == d)
        .order_by(TechStatus.orders_count.desc().nullslast())
    )

    # Маппинг entity_id -> size_name
    from models.product_entity import ProductEntity
    ent_result = await db.execute(
        select(ProductEntity.id, ProductEntity.size_name).where(
            ProductEntity.organization_id == org_id
        )
    )
    size_map = {str(r[0]): r[1] for r in ent_result.all()}

    # --- Юнит Экономика для ТС ---
    from sqlalchemy import text

    # Себестоимость и справочник
    ref_result = await db.execute(text(
        "SELECT entity_id, nm_id, cost_price, product_class, brand, tax_system, tax_rate, vat_rate "
        "FROM reference_book WHERE organization_id = :org AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)"
    ), {"org": org_id})
    ref_by_entity = {}
    ref_by_nm = {}
    for r in ref_result.all():
        d_item = {
            "cost_price": float(r[2]) if r[2] else 0,
            "product_class": r[3] or "",
            "brand": r[4] or "",
            "tax_system": r[5] or "",
            "tax_rate": float(r[6]) if r[6] else 0,
            "vat_rate": float(r[7]) if r[7] else 0,
        }
        if r[0]:
            ref_by_entity[str(r[0])] = d_item
        if r[1]:
            ref_by_nm[r[1]] = d_item

    # WB тарифы (из wb_tariff_snapshot)
    snap_result = await db.execute(text(
        "SELECT nm_id, logistics_tariff, storage_tariff, commission_pct, buyout_pct_fact "
        "FROM wb_tariff_snapshot WHERE organization_id = :org ORDER BY target_date DESC"
    ), {"org": org_id})
    snap_by_nm_ts = {}
    for r in snap_result.all():
        if r[0] not in snap_by_nm_ts:
            snap_by_nm_ts[r[0]] = {
                "logistics_tariff": float(r[1]) if r[1] else 0,
                "storage_tariff": float(r[2]) if r[2] else 0,
                "commission_pct": float(r[3]) if r[3] else 0,
                "buyout_pct_fact": float(r[4]) if r[4] else 0,
            }

    def _get_ref(eid, nm):
        return ref_by_entity.get(eid, ref_by_nm.get(nm, {"cost_price":0,"product_class":"","brand":"","tax_system":"","tax_rate":0,"vat_rate":0}))

    def _get_snap(nm):
        return snap_by_nm_ts.get(nm, {"logistics_tariff":0,"storage_tariff":0,"commission_pct":0,"buyout_pct_fact":0})

    def _calc_unit(price_disc, ad_cost, ref, snap):
        """Упрощённый расчёт юнитки для ТС"""
        p = float(price_disc or 0)
        a = float(ad_cost or 0)
        cp = ref["cost_price"]
        comm = p * snap["commission_pct"] / 100 if p and snap["commission_pct"] else 0
        logist = snap["logistics_tariff"]
        expenses = round(cp + comm + logist + a, 2)
        profit = round(p - expenses, 2)
        margin = round(profit / p * 100, 1) if p else 0
        roi = round(profit / cp * 100, 1) if cp else 0
        return {"unit_expenses": expenses, "unit_profit": profit, "unit_margin": margin, "unit_roi": roi}

    def safe_float(v):
        return float(v) if v is not None and not isinstance(v, decimal.Decimal) else (float(v) if isinstance(v, decimal.Decimal) else None)
    def safe_int(v):
        return int(v) if v is not None else None

    total_clicks = safe_int(row.total_clicks) or 0
    total_impressions = safe_int(row.total_impressions) or 0

    return {
        "date": str(d),
        "summary": {
            "total_products": safe_int(row.total_products) or 0,
            "total_stock": safe_int(row.total_stock) or 0,
            "total_orders": safe_int(row.total_orders) or 0,
            "total_buyouts": safe_int(row.total_buyouts) or 0,
            "total_returns": safe_int(row.total_returns) or 0,
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "ctr": round(total_clicks / total_impressions * 100, 2) if total_impressions > 0 else 0,
            "total_ad_cost": safe_float(row.total_ad_cost) or 0,
            "total_revenue": safe_float(row.total_revenue) or 0,
            "avg_rating": round(float(row.avg_rating), 2) if row.avg_rating else None,
            "zero_stock_count": safe_int(zero_stock.scalar()) or 0,
            "low_stock_count": safe_int(low_stock.scalar()) or 0,
            "low_rating_count": safe_int(low_rating.scalar()) or 0,
        },
        "products": (lambda rows: [
            {**{
                "entity_id": str(r[0]) if r[0] else None,
                "nm_id": r[1],
                "vendor_code": r[2],
                "product_name": r[3],
                "photo_main": r[4],
                "stock_qty": safe_int(r[5]),
                "orders_count": safe_int(r[6]),
                "buyouts_count": safe_int(r[7]),
                "returns_count": safe_int(r[8]),
                "rating": safe_float(r[9]),
                "impressions": safe_int(r[10]),
                "clicks": safe_int(r[11]),
                "ad_cost": safe_float(r[12]),
                "price": safe_float(r[13]),
                "price_discount": safe_float(r[14]),
                "tariff": safe_float(r[15]),
                "barcode": r[16] or "",
                "size_name": size_map.get(str(r[0]), "") if r[0] else "",
            },
            **{k: v for k, v in _get_ref(str(r[0]) if r[0] else "", r[1]).items()},
            **{f"snap_{k}": v for k, v in _get_snap(r[1]).items()},
            **_calc_unit(safe_float(r[14]), safe_float(r[12]), _get_ref(str(r[0]) if r[0] else "", r[1]), _get_snap(r[1])),
            }
            for r in rows
        ])(products_detail.all())
    }


@router.post("/api/v1/nl/reference/import")
async def import_reference_excel(org_id: str, request: Request, db: AsyncSession = Depends(get_db)):
    """Импорт себестоимости из Excel/CSV файла"""
    from fastapi import UploadFile, File
    import io
    import csv

    content_type = request.headers.get("content-type", "")

    # Читаем тело
    body = await request.body()

    try:
        # Попробуем openpyxl для .xlsx
        try:
            from openpyxl import load_workbook
            wb = load_workbook(io.BytesIO(body), read_only=True)
            ws = wb.active
            rows = list(ws.iter_rows(min_row=2, values_only=True))  # skip header
        except Exception:
            # Fallback: CSV
            text = body.decode("utf-8-sig")
            reader = csv.reader(io.StringIO(text), delimiter=";", quotechar='"')
            rows = list(reader)[1:]  # skip header

        imported = 0
        for row in rows:
            if not row or not row[0]:
                continue
            try:
                nm_id = int(row[0])
            except (ValueError, TypeError):
                continue

            def parse_float(v, idx):
                try:
                    val = row[idx] if idx < len(row) else None
                    if val is None or val == '' or val == '-':
                        return None
                    return float(str(val).replace(",", ".").replace(" ", "").replace("₽", ""))
                except (ValueError, TypeError, IndexError):
                    return None

            def parse_str(v, idx):
                try:
                    return str(row[idx]).strip() if idx < len(row) and row[idx] else None
                except (IndexError, TypeError):
                    return None

            vendor_code = parse_str(row, 1)
            product_name = parse_str(row, 2)
            target_date_str = parse_str(row, 3)
            cost_price = parse_float(row, 4)
            purchase_price = parse_float(row, 5)
            packaging_cost = parse_float(row, 6)
            logistics_cost = parse_float(row, 7)
            other_costs = parse_float(row, 8)
            notes = parse_str(row, 9)

            from datetime import datetime as dt_mod
            t_date = dt_mod.strptime(target_date_str, "%Y-%m-%d").date() if target_date_str else date.today()

            ins = pg_insert(ReferenceBook).values(
                organization_id=org_id, nm_id=nm_id, vendor_code=vendor_code,
                valid_from=t_date,
                cost_price=cost_price, purchase_cost=purchase_price,
                packaging_cost=packaging_cost, logistics_cost=logistics_cost,
                other_costs=other_costs, notes=notes,
            )
            stmt = ins.on_conflict_do_update(
                constraint="reference_book_org_entity_vf_key",
                set_={
                    "vendor_code": ins.excluded.vendor_code,
                    "cost_price": ins.excluded.cost_price,
                    "purchase_cost": ins.excluded.purchase_cost,
                    "packaging_cost": ins.excluded.packaging_cost,
                    "logistics_cost": ins.excluded.logistics_cost,
                    "other_costs": ins.excluded.other_costs,
                    "notes": ins.excluded.notes,
                    "updated_at": date.today(),
                }
            )
            await db.execute(stmt)
            imported += 1

        await db.commit()
        return {"status": "ok", "imported": imported}
    except Exception as e:
        raise HTTPException(400, f"Ошибка импорта: {str(e)}")

# ─── FRONTEND ──────────────────────────────────────────────



@router.get("/nl/register", response_class=HTMLResponse)
async def nl_register_page():
    html = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>НЛ — Регистрация</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f7fa;color:#1a1a2e}
.auth-container{max-width:400px;margin:80px auto;background:#fff;padding:32px;border-radius:12px;box-shadow:0 2px 12px rgba(0,0,0,.08)}
h2{color:#6c5ce7;margin-bottom:8px;font-size:1.3em}
p{color:#999;font-size:.85em;margin-bottom:20px}
.field{margin-bottom:14px}
label{display:block;font-size:.8em;color:#666;margin-bottom:4px}
input{width:100%;border:1px solid #e0e0e0;border-radius:4px;padding:8px;font-size:.9em}
input:focus{outline:none;border-color:#6c5ce7;box-shadow:0 0 0 2px rgba(108,92,231,.15)}
.btn{background:#6c5ce7;color:#fff;border:none;padding:10px;width:100%;border-radius:6px;cursor:pointer;font-size:.95em;font-weight:500}
.btn:hover{background:#5a4bd1}
.link{text-align:center;margin-top:16px;font-size:.85em}
.link a{color:#6c5ce7;text-decoration:none}
.link a:hover{text-decoration:underline}
.error{color:#e74c3c;font-size:.85em;margin-bottom:10px;display:none}
</style>
</head>
<body>
<div class="auth-container">
<h2>📝 Регистрация</h2>
<p>Создайте аккаунт для доступа к аналитике</p>
<div id="err" class="error"></div>
<div class="field"><label>Email</label><input type="email" id="email"></div>
<div class="field"><label>Пароль</label><input type="password" id="password"></div>
<div class="field"><label>Название магазина</label><input type="text" id="org" value="Мой магазин"></div>
<button class="btn" id="submitBtn">Зарегистрироваться</button>
<div class="link"><a href="/nl/v2">Уже есть аккаунт? Войти</a></div>
</div>
<script>
document.getElementById('submitBtn').addEventListener('click', async function() {
    var email = document.getElementById('email').value;
    var password = document.getElementById('password').value;
    var org_name = document.getElementById('org').value;
    var err = document.getElementById('err');
    err.style.display = 'none';
    try {
        var res = await fetch('/api/v1/nl/register', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({email: email, password: password, org_name: org_name})
        });
        if (!res.ok) {
            var d = await res.json();
            throw new Error(d.detail || 'Ошибка');
        }
        var data = await res.json();
        localStorage.setItem('nl_token', data.access_token);
        localStorage.setItem('nl_org_id', data.org_id);
        window.location.href = '/nl/v2';
    } catch(e) {
        err.textContent = e.message;
        err.style.display = 'block';
    }
});
</script>
</body>
</html>"""
    from fastapi.responses import HTMLResponse
    resp = HTMLResponse(html)
    resp.headers["Cache-Control"] = "no-cache, no-store"
    return resp



@router.get("/nl/login", response_class=HTMLResponse)
async def nl_login_page():
    html = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>НЛ — Вход</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f7fa;color:#1a1a2e}
.auth-container{max-width:400px;margin:80px auto;background:#fff;padding:32px;border-radius:12px;box-shadow:0 2px 12px rgba(0,0,0,.08)}
h2{color:#6c5ce7;margin-bottom:8px;font-size:1.3em}
p{color:#999;font-size:.85em;margin-bottom:20px}
.field{margin-bottom:14px}
label{display:block;font-size:.8em;color:#666;margin-bottom:4px}
input{width:100%;border:1px solid #e0e0e0;border-radius:4px;padding:8px;font-size:.9em}
input:focus{outline:none;border-color:#6c5ce7;box-shadow:0 0 0 2px rgba(108,92,231,.15)}
.btn{background:#6c5ce7;color:#fff;border:none;padding:10px;width:100%;border-radius:6px;cursor:pointer;font-size:.95em;font-weight:500}
.btn:hover{background:#5a4bd1}
.link{text-align:center;margin-top:16px;font-size:.85em}
.link a{color:#6c5ce7;text-decoration:none}
.link a:hover{text-decoration:underline}
.error{color:#e74c3c;font-size:.85em;margin-bottom:10px;display:none}
</style>
</head>
<body>
<div class="auth-container">
<h2>🔑 Вход</h2>
<p>Войдите в свой аккаунт НЛ</p>
<div id="err" class="error"></div>
<div class="field"><label>Email</label><input type="email" id="email"></div>
<div class="field"><label>Пароль</label><input type="password" id="password"></div>
<button class="btn" id="submitBtn">Войти</button>
<div class="link"><a href="/nl/register">Нет аккаунта? Зарегистрироваться</a></div>
</div>
<script>
document.getElementById('submitBtn').addEventListener('click', async function() {
    var email = document.getElementById('email').value;
    var password = document.getElementById('password').value;
    var err = document.getElementById('err');
    err.style.display = 'none';
    try {
        var res = await fetch('/api/v1/nl/login', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({email: email, password: password})
        });
        if (!res.ok) {
            var d = await res.json();
            throw new Error(d.detail || 'Неверный email или пароль');
        }
        var data = await res.json();
        localStorage.setItem('nl_token', data.access_token);
        localStorage.setItem('nl_org_id', data.org_id);
        window.location.href = '/nl/v2';
    } catch(e) {
        err.textContent = e.message;
        err.style.display = 'block';
    }
});
</script>
</body>
</html>"""
    from fastapi.responses import HTMLResponse
    resp = HTMLResponse(html)
    resp.headers["Cache-Control"] = "no-cache, no-store"
    return resp

@router.get("/api/v1/nl/analytics")
async def get_analytics(org_id: str, target_date: Optional[str] = None, search: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    """Аналитика по товарам — детальная таблица"""
    from datetime import datetime as dt_mod
    import decimal

    d = dt_mod.strptime(target_date, "%Y-%m-%d").date() if target_date else date.today()

    query = select(
        TechStatus.nm_id, TechStatus.vendor_code, TechStatus.product_name,
        TechStatus.photo_main, TechStatus.stock_qty, TechStatus.orders_count,
        TechStatus.buyouts_count, TechStatus.returns_count,
        TechStatus.price, TechStatus.price_discount, TechStatus.tariff,
        TechStatus.ad_cost, TechStatus.rating,
        TechStatus.impressions, TechStatus.clicks,
        TechStatus.warehouse_name, TechStatus.barcode,
    ).where(TechStatus.organization_id == org_id, TechStatus.target_date == d)

    if search:
        query = query.where(
            (TechStatus.vendor_code.ilike(f"%{search}%")) |
            (TechStatus.product_name.ilike(f"%{search}%")) |
            (TechStatus.nm_id == int(search) if search.isdigit() else False)
        )

    query = query.order_by(TechStatus.orders_count.desc().nullslast())
    result = await db.execute(query)
    rows = result.all()

    def sf(v): return float(v) if v and not isinstance(v, decimal.Decimal) else (float(v) if isinstance(v, decimal.Decimal) else None)
    def si(v): return int(v) if v else None

    products = []
    for r in rows:
        price = sf(r[8])
        price_disc = sf(r[9])
        tariff = sf(r[10])
        ad_cost = sf(r[11]) or 0
        orders = si(r[5]) or 0
        buyouts = si(r[6]) or 0
        revenue = price_disc * buyouts if price_disc and buyouts else 0
        commission = revenue * (tariff / 100) if revenue and tariff else 0
        payout = revenue - commission - ad_cost

        products.append({
            "nm_id": r[0], "vendor_code": r[1], "product_name": r[2],
            "photo_main": r[3], "stock_qty": si(r[4]),
            "orders_count": orders, "buyouts_count": buyouts,
            "returns_count": si(r[7]),
            "buyout_percent": round(buyouts / orders * 100, 1) if orders else 0,
            "price": price, "price_discount": price_disc,
            "tariff_percent": tariff,
            "commission": round(commission, 2),
            "logistics": 0, "ad_cost": round(ad_cost, 2),
            "drr": round(ad_cost / revenue * 100, 1) if revenue else 0,
            "fines": 0, "storage": 0, "reception": 0, "other_deductions": 0,
            "avg_check": round(revenue / buyouts, 2) if buyouts else 0,
            "revenue": round(revenue, 2), "payout": round(payout, 2),
            "cost_price": 0, "margin": round(payout, 2),
            "margin_per_unit": round(payout / buyouts, 2) if buyouts else 0,
            "profitability": round(payout / revenue * 100, 1) if revenue else 0,
            "roi": 0, "rating": sf(r[12]),
            "impressions": si(r[13]), "clicks": si(r[14]),
            "ctr": round((r[14] or 0) / (r[13] or 1) * 100, 2) if r[13] else 0,
            "turnover": 0, "in_transit": 0,
        })

    return {"date": str(d), "count": len(products), "products": products}


@router.get("/api/v1/nl/warehouses")
async def get_warehouse_stock(org_id: str, target_date: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    """Остатки на складах WB"""
    from datetime import datetime as dt_mod
    d = dt_mod.strptime(target_date, "%Y-%m-%d").date() if target_date else date.today()
    result = await db.execute(
        select(TechStatus.nm_id, TechStatus.vendor_code, TechStatus.product_name,
               TechStatus.warehouse_name, TechStatus.stock_qty, TechStatus.barcode)
        .where(TechStatus.organization_id == org_id, TechStatus.target_date == d)
        .order_by(TechStatus.stock_qty.desc().nullslast())
    )
    return [{"nm_id": r[0], "vendor_code": r[1], "product_name": r[2],
             "warehouse": r[3], "qty": int(r[4]) if r[4] else 0, "barcode": r[5]} for r in result.all()]


@router.get("/api/v1/nl/operating-expenses")
async def get_operating_expenses(org_id: str, db: AsyncSession = Depends(get_db)):
    """Операционные расходы"""
    # TODO: добавить модель OperatingExpense
    return []


@router.post("/api/v1/nl/operating-expenses")
async def add_operating_expense(data: dict, org_id: str, db: AsyncSession = Depends(get_db)):
    """Добавить операционный расход"""
    # TODO: сохранить в БД
    return {"ok": True}


@router.get("/api/v1/nl/rnp")
async def get_rnp(org_id: str, days: str = "10", search: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    """РНП — Рука на пульсе: агрегация за N дней"""
    from sqlalchemy import func
    import decimal
    n_days = int(days) if days.isdigit() else 10
    start = date.today() - timedelta(days=n_days)

    query = select(
        TechStatus.nm_id, TechStatus.vendor_code, TechStatus.product_name,
        func.sum(TechStatus.orders_count).label("orders"),
        func.sum(TechStatus.buyouts_count).label("buyouts"),
        func.sum(TechStatus.returns_count).label("returns"),
        func.sum(TechStatus.stock_qty).label("stock"),
        func.sum(TechStatus.ad_cost).label("ad_cost"),
        func.avg(TechStatus.price_discount).label("avg_price"),
        func.avg(TechStatus.tariff).label("avg_tariff"),
    ).where(
        TechStatus.organization_id == org_id,
        TechStatus.target_date >= start
    ).group_by(TechStatus.nm_id, TechStatus.vendor_code, TechStatus.product_name)

    if search:
        query = query.where(
            (TechStatus.vendor_code.ilike(f"%{search}%")) |
            (TechStatus.product_name.ilike(f"%{search}%"))
        )

    result = await db.execute(query.order_by(func.sum(TechStatus.orders_count).desc().nullslast()))

    def sf(v): return float(v) if v and not isinstance(v, decimal.Decimal) else (float(v) if isinstance(v, decimal.Decimal) else 0)
    def si(v): return int(v) if v else 0

    products = []
    for r in result.all():
        orders = si(r[3])
        buyouts = si(r[4])
        returns = si(r[5])
        stock = si(r[6])
        ad_cost = sf(r[7])
        avg_price = sf(r[8])
        avg_tariff = sf(r[9])
        revenue = avg_price * buyouts
        commission = revenue * (avg_tariff / 100) if revenue and avg_tariff else 0
        expenses = commission + ad_cost
        margin = revenue - expenses
        products.append({
            "nm_id": r[0], "vendor_code": r[1], "product_name": r[2],
            "orders": orders, "buyouts": buyouts, "returns": returns,
            "buyout_pct": round(buyouts/orders*100,1) if orders else 0,
            "revenue": round(revenue,2), "expenses": round(expenses,2),
            "margin": round(margin,2),
            "margin_per_unit": round(margin/buyouts,2) if buyouts else 0,
            "profitability": round(margin/revenue*100,1) if revenue else 0,
            "stock": stock,
            "turnover": round(buyouts / stock * n_days, 1) if stock else 0,
        })

    return {"days": n_days, "count": len(products), "products": products}


@router.get("/api/v1/nl/opiu")
async def get_opiu(org_id: str, period: str = "4", db: AsyncSession = Depends(get_db)):
    """ОПиУ — отчёт о прибылях и убытках по неделям"""
    from sqlalchemy import func
    from datetime import timedelta
    import decimal

    weeks = int(period) if period.isdigit() else 4
    today = date.today()
    
    # Получаем данные за N недель
    start_date = today - timedelta(weeks=weeks)
    
    result = await db.execute(
        select(
            TechStatus.target_date,
            func.sum(TechStatus.orders_count).label("orders"),
            func.sum(TechStatus.buyouts_count).label("buyouts"),
            func.sum(TechStatus.returns_count).label("returns"),
            func.sum(TechStatus.stock_qty).label("stock"),
            func.sum(TechStatus.ad_cost).label("ad_cost"),
            func.sum(TechStatus.impressions).label("impressions"),
            func.sum(TechStatus.clicks).label("clicks"),
            func.avg(TechStatus.tariff).label("avg_tariff"),
            func.sum(TechStatus.price_discount).label("revenue"),
        ).where(
            TechStatus.organization_id == org_id,
            TechStatus.target_date >= start_date
        ).group_by(TechStatus.target_date)
        .order_by(TechStatus.target_date.desc())
    )
    rows = result.all()
    
    # Группируем по неделям
    from collections import OrderedDict
    weeks_data = OrderedDict()
    for r in rows:
        d = r[0]
        # ISO week
        week_start = d - timedelta(days=d.weekday())
        week_key = week_start.isoformat()
        week_label = week_start.strftime("%d.%m") + " - " + (week_start + timedelta(days=6)).strftime("%d.%m.%Y")
        
        if week_key not in weeks_data:
            weeks_data[week_key] = {"label": week_label, "orders": 0, "buyouts": 0, "returns": 0, "revenue": 0, "ad_cost": 0, "stock": 0}
        
        def safe(v): return float(v) if v and not isinstance(v, decimal.Decimal) else (float(v) if isinstance(v, decimal.Decimal) else 0)
        
        w = weeks_data[week_key]
        w["orders"] += safe(r[1])
        w["buyouts"] += safe(r[2])
        w["returns"] += safe(r[3])
        w["stock"] += safe(r[4]) or 0
        w["ad_cost"] += safe(r[5])
        w["revenue"] += safe(r[9])
    
    # Считаем итоги
    total = {"orders": 0, "buyouts": 0, "returns": 0, "revenue": 0, "ad_cost": 0}
    for w in weeks_data.values():
        for k in total:
            total[k] += w.get(k, 0)
    
    return {
        "total": total,
        "weeks": [{"key": k, **v} for k, v in weeks_data.items()]
    }


@router.get("/api/v1/nl/opiu")
async def get_opiu(org_id: str, period: str = "4", db: AsyncSession = Depends(get_db)):
    """ОПиУ по неделям"""
    from sqlalchemy import func
    from datetime import timedelta
    import decimal
    weeks = int(period) if period.isdigit() else 4
    start_date = date.today() - timedelta(weeks=weeks)
    result = await db.execute(
        select(TechStatus.target_date,
            func.sum(TechStatus.orders_count).label("orders"),
            func.sum(TechStatus.buyouts_count).label("buyouts"),
            func.sum(TechStatus.returns_count).label("returns"),
            func.sum(TechStatus.ad_cost).label("ad_cost"),
            func.sum(TechStatus.price_discount).label("revenue"),
        ).where(TechStatus.organization_id == org_id, TechStatus.target_date >= start_date)
        .group_by(TechStatus.target_date).order_by(TechStatus.target_date.desc())
    )
    from collections import OrderedDict
    weeks_data = OrderedDict()
    total = {"orders": 0, "buyouts": 0, "returns": 0, "revenue": 0, "ad_cost": 0}
    for r in result.all():
        d = r[0]
        ws = d - timedelta(days=d.weekday())
        wk = ws.isoformat()
        if wk not in weeks_data:
            weeks_data[wk] = {"label": ws.strftime("%d.%m") + " - " + (ws + timedelta(days=6)).strftime("%d.%m"), "orders": 0, "buyouts": 0, "returns": 0, "revenue": 0, "ad_cost": 0}
        def sf(v): return float(v) if v else 0
        w = weeks_data[wk]
        w["orders"] += sf(r[1]); w["buyouts"] += sf(r[2]); w["returns"] += sf(r[3]); w["ad_cost"] += sf(r[4]); w["revenue"] += sf(r[5])
        total["orders"] += sf(r[1]); total["buyouts"] += sf(r[2]); total["returns"] += sf(r[3]); total["ad_cost"] += sf(r[4]); total["revenue"] += sf(r[5])
    return {"total": total, "weeks": [{"key": k, **v} for k, v in weeks_data.items()]}



# ==================== USER DATA APIs ====================

@router.get("/api/v1/nl/cost-prices")
async def get_cost_prices(org_id: str, db: AsyncSession = Depends(get_db)):
    """Себестоимость товаров"""
    from sqlalchemy import text
    result = await db.execute(text(
        "SELECT cp.id, cp.entity_id, cp.nm_id, cp.barcode, cp.vendor_code, cp.size_name, "
        "cp.cost_price, cp.purchase_cost, cp.logistics_cost, cp.packaging_cost, "
        "cp.other_costs, cp.vat, cp.valid_from, cp.valid_to, cp.source, cp.notes, "
        "cp.product_class, cp.brand, cp.tax_system, cp.tax_rate, cp.vat_rate, "
        "ts.product_name FROM reference_book cp "
        "LEFT JOIN (SELECT DISTINCT nm_id, product_name FROM tech_status WHERE organization_id = :org) ts ON cp.nm_id = ts.nm_id "
        "WHERE cp.organization_id = :org AND (cp.valid_to IS NULL OR cp.valid_to >= CURRENT_DATE) "
        "ORDER BY cp.nm_id, cp.valid_from DESC"
    ), {"org": org_id})
    return [{"id": str(r[0]), "entity_id": str(r[1]) if r[1] else None, "nm_id": r[2], "barcode": r[3], "vendor_code": r[4],
             "size_name": r[5], "cost_price": float(r[6]) if r[6] else 0,
             "purchase_cost": float(r[7]) if r[7] else None,
             "logistics_cost": float(r[8]) if r[8] else None,
             "packaging_cost": float(r[9]) if r[9] else None,
             "other_costs": float(r[10]) if r[10] else None,
             "vat": float(r[11]) if r[11] else 0,
             "valid_from": str(r[12]), "valid_to": str(r[13]) if r[13] else None,
             "source": r[14], "notes": r[15],
             "product_class": r[16], "brand": r[17],
             "tax_system": r[18], "tax_rate": float(r[19]) if r[19] else None,
             "vat_rate": float(r[20]) if r[20] else None,
             "product_name": r[21]} for r in result.all()]


@router.post("/api/v1/nl/cost-prices")
async def save_cost_price(data: dict, org_id: str, db: AsyncSession = Depends(get_db)):
    """Сохранить себестоимость (создать/обновить)"""
    from sqlalchemy import text
    nm_id = data.get("nm_id")
    cost = data.get("cost_price", 0)
    valid_from = data.get("valid_from", date.today().isoformat())
    if isinstance(valid_from, str):
        from datetime import datetime as _dt
        valid_from = _dt.strptime(valid_from, "%Y-%m-%d").date()
    if not nm_id:
        raise HTTPException(400, "nm_id обязателен")
    # Определяем entity_id по nm_id (+ size_name если есть)
    entity_id = data.get("entity_id")
    if not entity_id:
        ent_q = await db.execute(text(
            "SELECT pe.id FROM product_entities pe " +
            "WHERE pe.organization_id = :org AND pe.nm_id = :nm " +
            "ORDER BY CASE WHEN pe.size_name = :sz THEN 0 ELSE 1 END LIMIT 1"
        ), {"org": org_id, "nm": nm_id, "sz": data.get("size_name", "")})
        ent_row = ent_q.first()
        entity_id = str(ent_row[0]) if ent_row else None
    await db.execute(text(
        "INSERT INTO reference_book (organization_id, nm_id, barcode, vendor_code, size_name, entity_id, "
        "cost_price, purchase_cost, logistics_cost, packaging_cost, other_costs, vat, valid_from, source, "
        "product_class, brand, tax_system, tax_rate, vat_rate) "
        "VALUES (:org, :nm, :bc, :vc, :sz, :eid, :cp, :pc, :lc, :pk, :oc, :vat, :vf, :src, "
        ":pcls, :brand, :tsys, :trate, :vrate) "
        "ON CONFLICT (organization_id, entity_id, valid_from) DO UPDATE SET "
        "barcode = EXCLUDED.barcode, vendor_code = EXCLUDED.vendor_code, "
        "cost_price = EXCLUDED.cost_price, purchase_cost = EXCLUDED.purchase_cost, "
        "product_class = EXCLUDED.product_class, brand = EXCLUDED.brand, "
        "tax_system = EXCLUDED.tax_system, tax_rate = EXCLUDED.tax_rate, vat_rate = EXCLUDED.vat_rate, "
        "logistics_cost = EXCLUDED.logistics_cost, packaging_cost = EXCLUDED.packaging_cost, "
        "other_costs = EXCLUDED.other_costs, vat = EXCLUDED.vat, source = EXCLUDED.source"
    ), {"org": org_id, "nm": nm_id, "bc": data.get("barcode"), "vc": data.get("vendor_code"),
        "sz": data.get("size_name"), "eid": entity_id,
        "cp": cost, "pc": data.get("purchase_cost"),
        "lc": data.get("logistics_cost"), "pk": data.get("packaging_cost"),
        "oc": data.get("other_costs"), "vat": data.get("vat", 0),
        "vf": valid_from, "src": data.get("source", "manual"),
        "pcls": data.get("product_class"), "brand": data.get("brand"),
        "tsys": data.get("tax_system"), "trate": data.get("tax_rate"), "vrate": data.get("vat_rate")})
    await db.commit()
    return {"ok": True}


@router.post("/api/v1/nl/cost-prices/upload")
async def upload_cost_prices_excel(org_id: str, request: Request, db: AsyncSession = Depends(get_db)):
    """Загрузка себестоимости из Excel/CSV"""
    import io, csv
    from sqlalchemy import text
    body = await request.body()
    filename = request.headers.get("x-filename", "upload.csv")
    rows = []
    if filename.endswith(".csv"):
        reader = csv.DictReader(io.StringIO(body.decode("utf-8-sig")), delimiter=";")
        rows = list(reader)
    else:
        try:
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(body))
            ws = wb.active
            headers = [str(c.value).strip() if c.value else "" for c in ws[1]]
            for row in ws.iter_rows(min_row=2, values_only=True):
                rows.append(dict(zip(headers, row)))
        except ImportError:
            raise HTTPException(400, "xlsx не поддерживается")
    updated = 0
    for row in rows:
        nm = row.get("Арт WB") or row.get("nm_id")
        cost = row.get("Себестоимость") or row.get("cost_price")
        if nm and cost:
            await db.execute(text(
                "INSERT INTO reference_book (organization_id, nm_id, barcode, vendor_code, cost_price, vat, valid_from, source, entity_id) "
                "VALUES (:org, :nm, :bc, :vc, :cp, :vat, CURRENT_DATE, 'excel', :eid) "
                "ON CONFLICT (organization_id, entity_id, valid_from) DO UPDATE SET "
                "cost_price = EXCLUDED.cost_price, vat = EXCLUDED.vat, barcode = EXCLUDED.barcode, vendor_code = EXCLUDED.vendor_code"
            ), {"org": org_id, "nm": int(nm), "bc": str(row.get("Баркод","")),
                "vc": str(row.get("Арт продавца","")), "cp": float(cost),
                "vat": float(row.get("НДС",0)), "eid": None})
            updated += 1
    await db.commit()
    return {"updated": updated, "total": len(rows)}


@router.get("/api/v1/nl/sellers")
async def get_sellers(org_id: str, db: AsyncSession = Depends(get_db)):
    """Список продавцов"""
    from sqlalchemy import text
    result = await db.execute(text(
        "SELECT id, seller_id, seller_name, inn, seller_type, contact_name, "
        "contact_email, contact_phone, role, is_active, notes, created_at "
        "FROM sellers WHERE organization_id = :org ORDER BY created_at DESC"
    ), {"org": org_id})
    return [{"id": str(r[0]), "seller_id": r[1], "seller_name": r[2], "inn": r[3],
             "seller_type": r[4], "contact_name": r[5], "contact_email": r[6],
             "contact_phone": r[7], "role": r[8], "is_active": r[9],
             "notes": r[10], "created_at": str(r[11])} for r in result.all()]


@router.post("/api/v1/nl/sellers")
async def add_seller(data: dict, org_id: str, db: AsyncSession = Depends(get_db)):
    """Добавить продавца"""
    from sqlalchemy import text
    await db.execute(text(
        "INSERT INTO sellers (organization_id, seller_id, seller_name, inn, seller_type, "
        "contact_name, contact_email, contact_phone, role, notes) "
        "VALUES (:org, :sid, :name, :inn, :type, :cname, :email, :phone, :role, :notes)"
    ), {"org": org_id, "sid": data.get("seller_id"), "name": data.get("seller_name"),
        "inn": data.get("inn"), "type": data.get("seller_type", "fbo"),
        "cname": data.get("contact_name"), "email": data.get("contact_email"),
        "phone": data.get("contact_phone"), "role": data.get("role", "seller"),
        "notes": data.get("notes")})
    await db.commit()
    return {"ok": True}


@router.get("/api/v1/nl/seo-keywords")
async def get_seo_keywords(org_id: str, nm_id: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    """SEO ключевые запросы"""
    from sqlalchemy import text
    q = "SELECT id, nm_id, vendor_code, keyword, position, frequency_monthly, frequency_weekly, "         "season_start, season_end, season_multiplier, trend, trend_value, competition, "         "target_date, source, notes FROM seo_keywords WHERE organization_id = :org"
    params = {"org": org_id}
    if nm_id:
        q += " AND nm_id = :nm"
        params["nm"] = int(nm_id)
    q += " ORDER BY target_date DESC NULLS LAST, frequency_monthly DESC NULLS LAST"
    result = await db.execute(text(q), params)
    return [{"id": str(r[0]), "nm_id": r[1], "vendor_code": r[2], "keyword": r[3],
             "position": r[4], "frequency_monthly": r[5], "frequency_weekly": r[6],
             "season_start": r[7], "season_end": r[8],
             "season_multiplier": float(r[9]) if r[9] else 1.0,
             "trend": r[10], "trend_value": float(r[11]) if r[11] else None,
             "competition": r[12], "target_date": str(r[13]) if r[13] else None,
             "source": r[14], "notes": r[15]} for r in result.all()]


@router.post("/api/v1/nl/seo-keywords")
async def add_seo_keyword(data: dict, org_id: str, db: AsyncSession = Depends(get_db)):
    """Добавить SEO запрос"""
    from sqlalchemy import text
    await db.execute(text(
        "INSERT INTO seo_keywords (organization_id, nm_id, vendor_code, keyword, position, "
        "frequency_monthly, frequency_weekly, season_start, season_end, season_multiplier, "
        "trend, trend_value, competition, target_date, source, notes) "
        "VALUES (:org, :nm, :vc, :kw, :pos, :fm, :fw, :ss, :se, :sm, :trend, :tv, :comp, :td, :src, :notes)"
    ), {"org": org_id, "nm": data.get("nm_id"), "vc": data.get("vendor_code"),
        "kw": data.get("keyword"), "pos": data.get("position"),
        "fm": data.get("frequency_monthly"), "fw": data.get("frequency_weekly"),
        "ss": data.get("season_start"), "se": data.get("season_end"),
        "sm": data.get("season_multiplier", 1.0), "trend": data.get("trend"),
        "tv": data.get("trend_value"), "comp": data.get("competition"),
        "td": data.get("target_date"), "src": data.get("source", "manual"),
        "notes": data.get("notes")})
    await db.commit()
    return {"ok": True}


@router.post("/api/v1/nl/seo-keywords/upload")
async def upload_seo_keywords(org_id: str, request: Request, db: AsyncSession = Depends(get_db)):
    """Загрузка SEO запросов из Excel/CSV"""
    import io, csv
    from sqlalchemy import text
    body = await request.body()
    filename = request.headers.get("x-filename", "upload.csv")
    rows = []
    if filename.endswith(".csv"):
        rows = list(csv.DictReader(io.StringIO(body.decode("utf-8-sig")), delimiter=";"))
    updated = 0
    for row in rows:
        nm = row.get("Арт WB") or row.get("nm_id")
        kw = row.get("Запрос") or row.get("keyword")
        if nm and kw:
            await db.execute(text(
                "INSERT INTO seo_keywords (organization_id, nm_id, vendor_code, keyword, "
                "position, frequency_monthly, season_start, season_end, trend, competition, target_date, source) "
                "VALUES (:org, :nm, :vc, :kw, :pos, :fm, :ss, :se, :trend, :comp, CURRENT_DATE, 'excel')"
            ), {"org": org_id, "nm": int(nm), "vc": row.get("Арт продавца",""),
                "kw": kw, "pos": row.get("Позиция"), "fm": row.get("Частотность"),
                "ss": row.get("Сезон начало"), "se": row.get("Сезон конец"),
                "trend": row.get("Тренд"), "comp": row.get("Конкуренция")})
            updated += 1
    await db.commit()
    return {"updated": updated, "total": len(rows)}



# ─── РЕКЛАМА ────────────────────────────────────────────

@router.get("/api/v1/nl/ad-stats")
async def get_ad_stats(org_id: str, days: str = "7", db: AsyncSession = Depends(get_db)):
    """Рекламная статистика по дням"""
    import decimal as _dec
    try:
        days_int = int(days)
    except:
        days_int = 7

    rows = await db.execute(text("""
        SELECT s.stat_date,
               SUM(s.views) as views,
               SUM(s.clicks) as clicks,
               SUM(s.spent) as spent,
               AVG(s.ctr) as avg_ctr,
               AVG(s.cpc) as avg_cpc,
               SUM(s.orders) as orders,
               SUM(s.atbs) as atbs,
               AVG(s.cr) as avg_cr
        FROM ad_stats s
        WHERE s.organization_id = :org
          AND s.stat_date >= CURRENT_DATE - make_interval(days => :days)
        GROUP BY s.stat_date
        ORDER BY s.stat_date DESC
    """), {"org": org_id, "days": days_int})
    daily = []
    for r in rows:
        def sf(v): return float(v) if v and not isinstance(v, _dec.Decimal) else (float(v) if isinstance(v, _dec.Decimal) else 0)
        daily.append({
            "date": str(r[0]),
            "views": int(r[1] or 0),
            "clicks": int(r[2] or 0),
            "spent": round(sf(r[3]), 2),
            "ctr": round(sf(r[4]), 2),
            "cpc": round(sf(r[5]), 2),
            "orders": int(r[6] or 0),
            "atbs": int(r[7] or 0),
            "cr": round(sf(r[8]), 2),
        })

    # Топ кампаний за период
    top_rows = await db.execute(text("""
        SELECT c.name, s.wb_campaign_id,
               SUM(s.views) as views, SUM(s.clicks) as clicks,
               SUM(s.spent) as spent, AVG(s.ctr) as ctr,
               SUM(s.orders) as orders, SUM(s.atbs) as atbs
        FROM ad_stats s
        LEFT JOIN ad_campaigns c ON c.wb_campaign_id = s.wb_campaign_id AND c.organization_id = s.organization_id
        WHERE s.organization_id = :org
          AND s.stat_date >= CURRENT_DATE - make_interval(days => :days)
        GROUP BY c.name, s.wb_campaign_id
        ORDER BY SUM(s.spent) DESC
        LIMIT 20
    """), {"org": org_id, "days": days_int})
    top_campaigns = []
    for r in top_rows:
        def sf(v): return float(v) if v and not isinstance(v, _dec.Decimal) else (float(v) if isinstance(v, _dec.Decimal) else 0)
        top_campaigns.append({
            "name": r[0] or "Без названия",
            "campaign_id": r[1],
            "views": int(r[2] or 0),
            "clicks": int(r[3] or 0),
            "spent": round(sf(r[4]), 2),
            "ctr": round(sf(r[5]), 2),
            "orders": int(r[6] or 0),
            "atbs": int(r[7] or 0),
        })

    # Баланс
    balance = None
    bal_row = await db.execute(text("""
        SELECT raw_response FROM raw_api_data
        WHERE api_method = 'ad_balance' AND status = 'ok' AND organization_id = :org
        ORDER BY fetched_at DESC LIMIT 1
    """), {"org": org_id})
    br = bal_row.first()
    if br and br[0]:
        balance = br[0]

    # Итого
    totals = {"views": 0, "clicks": 0, "spent": 0, "orders": 0, "atbs": 0}
    for d in daily:
        for k in totals:
            totals[k] += d.get(k, 0)
    totals["ctr"] = round(totals["clicks"] / totals["views"] * 100, 2) if totals["views"] else 0
    totals["cpc"] = round(totals["spent"] / totals["clicks"], 2) if totals["clicks"] else 0
    totals["cr"] = round(totals["orders"] / totals["clicks"] * 100, 2) if totals["clicks"] else 0
    return {
        "daily": daily,
        "top_campaigns": top_campaigns,
        "totals": totals,
        "balance": balance,
    }



# ==================== UNIT ECONOMICS APIs ====================

@router.get("/api/v1/nl/unit-economics")
async def get_unit_economics(org_id: str, search: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    """Юнит Экономика — сборка всех данных по SKU"""
    from models.reference_book import ReferenceBook
    from sqlalchemy import text as sql_text

    # 1) Получаем список товаров из tech_status (последняя дата)
    dates_result = await db.execute(
        sql_text("SELECT DISTINCT target_date FROM tech_status WHERE organization_id = :org ORDER BY target_date DESC LIMIT 1"),
        {"org": org_id}
    )
    latest_date_row = dates_result.first()
    if not latest_date_row:
        return {"items": [], "total": 0}
    latest_date = latest_date_row[0]

    # 2) Продукты с базовыми данными
    prods_result = await db.execute(
        sql_text("""
            SELECT DISTINCT ON (entity_id, nm_id) entity_id, nm_id, vendor_code, product_name, photo_main, barcode, price, price_discount, tariff, ad_cost
            FROM tech_status 
            WHERE organization_id = :org AND target_date = :dt
            ORDER BY entity_id, nm_id, barcode
        """),
        {"org": org_id, "dt": latest_date}
    )
    products = prods_result.all()

    # 3) Ручные вводы Юнит Экономики — по entity_id, fallback nm_id+barcode
    ue_result = await db.execute(
        sql_text("SELECT entity_id, nm_id, mp_correction_pct, buyout_niche_pct, extra_costs, ad_plan_rub, price_before_spp_plan, price_before_spp_change, change_date, fulfillment_model, wb_club_discount_pct, storage_pct, product_status FROM reference_book WHERE organization_id = :org AND (valid_to IS NULL OR valid_to >= CURRENT_DATE) ORDER BY valid_from DESC"),
        {"org": org_id}
    )
    ue_rows = ue_result.all()
    ue_by_entity = {}
    ue_by_nm_bc = {}
    _ue_fields = lambda r: {
        "mp_correction_pct": r[2], "buyout_niche_pct": r[3],
        "extra_costs": r[4], "ad_plan_rub": r[5],
        "price_before_spp_plan": r[6], "price_before_spp_change": r[7],
        "change_date": r[8], "tariff_type": r[9],
        "wb_club_discount_pct": r[10],
    }
    for r in ue_rows:
        eid = str(r[0]) if r[0] else None
        fields = _ue_fields(r)
        if eid:
            if eid not in ue_by_entity:
                ue_by_entity[eid] = fields
            else:
                # Merge: newer non-null values override
                existing = ue_by_entity[eid]
                for k, v in fields.items():
                    if v is not None and v != 0 and v != "":
                        existing[k] = v
        else:
            key = (r[1], "")
            if key not in ue_by_nm_bc:
                ue_by_nm_bc[key] = fields
            else:
                existing = ue_by_nm_bc[key]
                for k, v in fields.items():
                    if v is not None and v != 0 and v != "":
                        existing[k] = v

    # 4) Себестоимость из reference_book — приоритет по entity_id, fallback по nm_id
    cost_result = await db.execute(
        sql_text("""
            SELECT entity_id, nm_id, cost_price, purchase_cost, logistics_cost, packaging_cost,
            other_costs, vat, product_class, brand, tax_system, tax_rate, vat_rate as cost_vat_rate
            FROM reference_book WHERE organization_id = :org 
            AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)
            ORDER BY entity_id NULLS LAST, valid_from DESC
        """),
        {"org": org_id}
    )
    cost_rows = cost_result.all()
    cost_by_entity = {}
    cost_by_nm = {}
    # cols: entity_id(0), nm_id(1), cost_price(2), purchase_cost(3), logistics_cost(4),
    # packaging_cost(5), other_costs(6), vat(7), product_class(8), brand(9), tax_system(10), tax_rate(11), cost_vat_rate(12)
    _cost_fields = lambda r: {
        "cost_price": r[2], "purchase_cost": r[3], "logistics_cost": r[4],
        "packaging_cost": r[5], "other_costs": r[6], "vat": r[7],
        "product_class": r[8], "brand": r[9], "tax_system": r[10],
        "tax_rate": r[11], "vat_rate": r[12],
    }
    for r in cost_rows:
        fields = _cost_fields(r)
        if r[0]:
            eid = str(r[0])
            if eid not in cost_by_entity:
                cost_by_entity[eid] = fields
            else:
                for k, v in fields.items():
                    if v is not None and v != 0:
                        cost_by_entity[eid][k] = v
        if r[1]:
            nm = r[1]
            if nm not in cost_by_nm:
                cost_by_nm[nm] = fields
            else:
                for k, v in fields.items():
                    if v is not None and v != 0:
                        cost_by_nm[nm][k] = v

    # 5) Автоматические WB-данные из wb_tariff_snapshot
    tsnap_result = await db.execute(
        sql_text("""
            SELECT nm_id, logistics_tariff, storage_tariff, ad_cost_fact,
                   buyout_pct_fact, commission_pct, price_retail, price_with_spp,
                   spp_pct
            FROM wb_tariff_snapshot
            WHERE organization_id = :org
            ORDER BY target_date DESC
        """),
        {"org": org_id}
    )
    snap_by_nm = {}
    for r in tsnap_result.all():
        if r[0] not in snap_by_nm:  # Берём только последнюю дату
            snap_by_nm[r[0]] = {
                "logistics_tariff": float(r[1]) if r[1] else 0,
                "storage_tariff": float(r[2]) if r[2] else 0,
                "ad_cost_fact": float(r[3]) if r[3] else 0,
                "buyout_pct_fact": float(r[4]) if r[4] else 0,
                "commission_pct": float(r[5]) if r[5] else 0,
                "price_retail": float(r[6]) if r[6] else 0,
                "price_with_spp": float(r[7]) if r[7] else 0,
                "spp_pct": float(r[8]) if r[8] else 0,
            }

    # 8) Собираем результат
    items = []
    search_q = search.lower() if search else ""
    
    # Маппинг entity_id → size_name
    from models.product_entity import ProductEntity
    ent_result = await db.execute(
        select(ProductEntity.id, ProductEntity.size_name).where(
            ProductEntity.organization_id == org_id
        )
    )
    size_map_ue = {str(r[0]): r[1] for r in ent_result.all()}

    for p in products:
        entity_id = str(p[0]) if p[0] else None
        nm_id = p[1]
        vendor_code = p[2] or ""
        product_name = p[3] or ""
        photo = p[4] or ""
        main_barcode = p[5] or ""
        price = float(p[6]) if p[6] else 0
        price_discount = float(p[7]) if p[7] else 0

        # Фильтр поиска
        if search_q and search_q not in str(nm_id) and search_q not in product_name.lower() and search_q not in vendor_code.lower():
            continue

        cost = cost_by_entity.get(entity_id, cost_by_nm.get(nm_id, {}))
        ue = ue_by_entity.get(entity_id, ue_by_nm_bc.get((nm_id, main_barcode), ue_by_nm_bc.get((nm_id, ""), {})))

        item = {
            "entity_id": entity_id,
            "nm_id": nm_id,
            "vendor_code": vendor_code,
            "product_name": product_name,
            "photo": photo.replace("/hq/", "/c246x328/") if photo else "",
            "barcode": main_barcode,
            "size_name": size_map_ue.get(entity_id, "") if entity_id else "",
            "sku": f"{vendor_code}_{main_barcode}" if vendor_code else str(nm_id),

            # Из справочника / себестоимости
            "cost_price": float(cost.get("cost_price") or 0),
            "purchase_cost": float(cost.get("purchase_cost") or 0),
            "logistics_cost": float(cost.get("logistics_cost") or 0),
            "packaging_cost": float(cost.get("packaging_cost") or 0),
            "other_costs": float(cost.get("other_costs") or 0),
            "product_class": cost.get("product_class"),
            "brand": cost.get("brand"),
            "tax_system": cost.get("tax_system"),
            "tax_rate": float(cost.get("tax_rate") or 0),
            "vat_rate": float(cost.get("vat_rate") or 0),

            # Из wb_tariff_snapshot (автоподтяжка)
            "mp_base_pct": (snap_by_nm.get(nm_id, {})).get("commission_pct") or float(p[8] or 0),  # комиссия из snapshot, fallback tech_status
            "buyout_fact_pct": snap_by_nm.get(nm_id, {}).get("buyout_pct_fact", 0),
            "logistics_tariff": snap_by_nm.get(nm_id, {}).get("logistics_tariff", 0),
            "logistics_actual": 0,  # Будет из финотчётов
            "storage_tariff": snap_by_nm.get(nm_id, {}).get("storage_tariff", 0),
            "storage_actual": 0,  # Будет из финотчётов
            "acceptance_avg": 0,  # Будет из API приёмки
            "price_before_spp": snap_by_nm.get(nm_id, {}).get("price_retail") or price,
            "spp_pct": snap_by_nm.get(nm_id, {}).get("spp_pct") or round((1 - price_discount / price) * 100, 1) if price and price_discount and price_discount < price else 0,
            "price_with_spp": snap_by_nm.get(nm_id, {}).get("price_with_spp") or price_discount or price,
            "ad_fact_rub": snap_by_nm.get(nm_id, {}).get("ad_cost_fact") or float(p[9] or 0),
            "wb_club_discount_pct_api": 0,

            # Ручные вводы
            "mp_correction_pct": float(ue.get("mp_correction_pct") or 0),
            "buyout_niche_pct": float(ue.get("buyout_niche_pct") or 0),
            "extra_costs": float(ue.get("extra_costs") or 0),
            "ad_plan_rub": float(ue.get("ad_plan_rub") or 0),
            "price_before_spp_plan": float(ue.get("price_before_spp_plan") or 0),
            "price_before_spp_change": float(ue.get("price_before_spp_change") or 0),
            "change_date": str(ue.get("change_date")) if ue.get("change_date") else None,
            "tariff_type": ue.get("tariff_type") or "box",
            "wb_club_discount_pct": float(ue.get("wb_club_discount_pct") or 0),
        }

        # Расчётные формулы
        mp_total_pct = item["mp_base_pct"] + item["mp_correction_pct"]
        item["mp_total_pct"] = mp_total_pct

        # Комиссия МП
        mp_commission = round(item["price_with_spp"] * mp_total_pct / 100, 2)

        # Эквайринг 1.5%
        acquiring = round(item["price_with_spp"] * 0.015, 2)

        # Налог
        tax = 0
        ts = item["tax_system"]
        if ts == "usn":
            tax = round(item["price_with_spp"] * item["tax_rate"] / 100, 2)
        elif ts == "usn_dr":
            income = item["price_with_spp"] - mp_commission - item["cost_price"] - item["extra_costs"]
            tax = round(max(income, 0) * item["tax_rate"] / 100, 2)
        elif ts == "osn":
            nds = round(item["price_with_spp"] * item["vat_rate"] / 100, 2)
            input_nds = round(item["purchase_cost"] / 120 * item["vat_rate"] if item["purchase_cost"] else 0, 2)
            tax = round(nds - input_nds, 2)

        item["tax_total"] = tax

        # === БЛОК 7: Расчёт по ФАКТУ ===
        expenses_fact = (
            item["cost_price"] + item["logistics_cost"] + item["packaging_cost"] +
            item["other_costs"] + item["extra_costs"] +
            mp_commission + item["logistics_actual"] + item["storage_actual"] +
            item["acceptance_avg"] + acquiring + tax +
            item["ad_fact_rub"]
        )
        profit_fact = round(item["price_with_spp"] - expenses_fact, 2)
        margin_fact = round(profit_fact / item["price_with_spp"] * 100, 2) if item["price_with_spp"] else 0
        roi_fact = round(profit_fact / item["cost_price"] * 100, 2) if item["cost_price"] else 0
        to_account_fact = round(item["price_with_spp"] - mp_commission - tax, 2)

        item["expenses_fact"] = round(expenses_fact, 2)
        item["profit_fact"] = profit_fact
        item["margin_fact"] = margin_fact
        item["roi_fact"] = roi_fact
        item["to_account_fact"] = to_account_fact

        # === БЛОК 8: Расчёт по ПЛАНУ ===
        plan_price = item["price_before_spp_plan"] or item["price_before_spp"]
        plan_price_spp = round(plan_price * (1 - item["spp_pct"] / 100), 2) if item["spp_pct"] else plan_price
        plan_mp = round(plan_price_spp * mp_total_pct / 100, 2)
        plan_acquiring = round(plan_price_spp * 0.015, 2)
        
        # Пересчёт налога для плановой цены
        plan_tax = 0
        if ts == "usn":
            plan_tax = round(plan_price_spp * item["tax_rate"] / 100, 2)
        elif ts == "usn_dr":
            plan_income = plan_price_spp - plan_mp - item["cost_price"] - item["extra_costs"]
            plan_tax = round(max(plan_income, 0) * item["tax_rate"] / 100, 2)
        elif ts == "osn":
            plan_nds = round(plan_price_spp * item["vat_rate"] / 100, 2)
            plan_input_nds = round(item["purchase_cost"] / 120 * item["vat_rate"] if item["purchase_cost"] else 0, 2)
            plan_tax = round(plan_nds - plan_input_nds, 2)

        expenses_plan = (
            item["cost_price"] + item["logistics_cost"] + item["packaging_cost"] +
            item["other_costs"] + item["extra_costs"] +
            plan_mp + item["logistics_actual"] + item["storage_actual"] +
            item["acceptance_avg"] + plan_acquiring + plan_tax +
            item["ad_plan_rub"]
        )
        profit_plan = round(plan_price_spp - expenses_plan, 2)
        margin_plan = round(profit_plan / plan_price_spp * 100, 2) if plan_price_spp else 0
        roi_plan = round(profit_plan / item["cost_price"] * 100, 2) if item["cost_price"] else 0
        to_account_plan = round(plan_price_spp - plan_mp - plan_tax, 2)

        item["plan_price_spp"] = plan_price_spp
        item["expenses_plan"] = round(expenses_plan, 2)
        item["profit_plan"] = profit_plan
        item["margin_plan"] = margin_plan
        item["roi_plan"] = roi_plan
        item["to_account_plan"] = to_account_plan

        # === БЛОК 9: После изменений ===
        change_price = item["price_before_spp_change"] or item["price_before_spp"]
        change_price_spp = round(change_price * (1 - item["spp_pct"] / 100), 2) if item["spp_pct"] else change_price
        change_mp = round(change_price_spp * mp_total_pct / 100, 2)
        
        change_tax = 0
        if ts == "usn":
            change_tax = round(change_price_spp * item["tax_rate"] / 100, 2)
        elif ts == "usn_dr":
            change_income = change_price_spp - change_mp - item["cost_price"] - item["extra_costs"]
            change_tax = round(max(change_income, 0) * item["tax_rate"] / 100, 2)
        elif ts == "osn":
            change_nds = round(change_price_spp * item["vat_rate"] / 100, 2)
            change_input_nds = round(item["purchase_cost"] / 120 * item["vat_rate"] if item["purchase_cost"] else 0, 2)
            change_tax = round(change_nds - change_input_nds, 2)

        expenses_change = (
            item["cost_price"] + item["logistics_cost"] + item["packaging_cost"] +
            item["other_costs"] + item["extra_costs"] +
            change_mp + item["logistics_actual"] + item["storage_actual"] +
            item["acceptance_avg"] + round(change_price_spp * 0.015, 2) + change_tax +
            item["ad_fact_rub"]
        )
        profit_change = round(change_price_spp - expenses_change, 2)
        roi_change = round(profit_change / item["cost_price"] * 100, 2) if item["cost_price"] else 0

        item["profit_change"] = profit_change
        item["roi_change"] = roi_change

        items.append(item)

    return {"items": items, "total": len(items)}


class UnitEconSave(BaseModel):
    nm_id: int
    barcode: Optional[str] = None
    entity_id: Optional[str] = None
    mp_correction_pct: Optional[float] = None
    buyout_niche_pct: Optional[float] = None
    extra_costs: Optional[float] = None
    ad_plan_rub: Optional[float] = None
    price_before_spp_plan: Optional[float] = None
    price_before_spp_change: Optional[float] = None
    change_date: Optional[str] = None
    tariff_type: Optional[str] = None
    wb_club_discount_pct: Optional[float] = None


@router.post("/api/v1/nl/unit-economics")
async def save_unit_economics(data: UnitEconSave, org_id: str, db: AsyncSession = Depends(get_db)):
    """Сохранить ручные вводы Юнит Экономики"""
    from models.reference_book import ReferenceBook
    from datetime import datetime as dt_mod

    change_date = None
    if data.change_date:
        change_date = dt_mod.strptime(data.change_date, "%Y-%m-%d").date()

    # Определяем entity_id
    entity_id_ue = data.entity_id if hasattr(data, "entity_id") and data.entity_id else None
    if not entity_id_ue:
        from sqlalchemy import text as sql_text_sync
        ent_q = await db.execute(sql_text_sync(
            "SELECT pe.id FROM product_entities pe "
            "WHERE pe.organization_id = :org AND pe.nm_id = :nm "
            "ORDER BY CASE WHEN pe.size_name = :sz THEN 0 ELSE 1 END LIMIT 1"
        ), {"org": org_id, "nm": data.nm_id, "sz": data.barcode or ""})
        ent_row = ent_q.first()
        entity_id_ue = ent_row[0] if ent_row else None
    ins = pg_insert(ReferenceBook).values(
        organization_id=org_id,
        nm_id=data.nm_id,
        barcode=data.barcode,
        entity_id=entity_id_ue,
        valid_from=date.today(),
        mp_correction_pct=data.mp_correction_pct,
        buyout_niche_pct=data.buyout_niche_pct,
        extra_costs=data.extra_costs,
        ad_plan_rub=data.ad_plan_rub,
        price_before_spp_plan=data.price_before_spp_plan,
        price_before_spp_change=data.price_before_spp_change,
        change_date=change_date,
        fulfillment_model=data.tariff_type or "fbo",
        wb_club_discount_pct=data.wb_club_discount_pct,
    )
    stmt = ins.on_conflict_do_update(
        constraint="reference_book_org_entity_vf_key",
        set_={
            "mp_correction_pct": ins.excluded.mp_correction_pct,
            "buyout_niche_pct": ins.excluded.buyout_niche_pct,
            "extra_costs": ins.excluded.extra_costs,
            "ad_plan_rub": ins.excluded.ad_plan_rub,
            "price_before_spp_plan": ins.excluded.price_before_spp_plan,
            "price_before_spp_change": ins.excluded.price_before_spp_change,
            "change_date": ins.excluded.change_date,
            "fulfillment_model": ins.excluded.fulfillment_model,
            "wb_club_discount_pct": ins.excluded.wb_club_discount_pct,
        }
    )
    await db.execute(stmt)
    await db.commit()
    return {"ok": True}


@router.get("/nl/v2", response_class=HTMLResponse)
async def nl_page():
    """НЛ — главная страница"""
    html = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>НЛ — Аналитика v3</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f7fa;color:#1a1a2e}

.header{background:#fff;border-bottom:1px solid #e0e0e0;padding:12px 20px;display:flex;align-items:center;gap:12px}
.header h1{font-size:1.2em;color:#6c5ce7}
.header .logo{font-size:1.4em}
.header .logout{margin-left:auto;color:#999;cursor:pointer;font-size:0.85em}
.header .logout:hover{color:#e74c3c}
.org-switcher{display:flex;align-items:center;gap:4px}
.org-switcher select{border:1px solid #e0e0e0;border-radius:4px;padding:4px 8px;font-size:.85em;background:#fff;cursor:pointer}
.btn-sm{background:#6c5ce7;color:#fff;border:none;width:24px;height:24px;border-radius:50%;cursor:pointer;font-size:1em;display:flex;align-items:center;justify-content:center}
.btn-sm:hover{background:#5a4bd1}
.wb-key-item{display:flex;align-items:center;gap:10px;padding:8px 12px;background:#fff;border-radius:6px;margin-bottom:6px;box-shadow:0 1px 2px rgba(0,0,0,.06)}
.wb-key-item .name{font-weight:500}
.wb-key-item .date{color:#999;font-size:.8em}
.wb-key-item .del{margin-left:auto;color:#e74c3c;cursor:pointer;font-size:.8em}
.user-info{color:#666;font-size:0.85em}

.tabs{display:flex;background:#fff;border-bottom:2px solid #e0e0e0;padding:0 20px}
.tab{padding:12px 24px;cursor:pointer;color:#666;font-weight:500;border-bottom:3px solid transparent;transition:all .2s}
.tab:hover{color:#6c5ce7}
.tab.active{color:#6c5ce7;border-bottom-color:#6c5ce7}

.content{padding:20px}
.tab-content{display:none}
.tab-content.active{display:block}

table{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.08)}
th{background:#f8f9fa;padding:10px 8px;text-align:left;font-size:.78em;color:#666;text-transform:uppercase;letter-spacing:.5px;border-bottom:2px solid #e0e0e0}
td{padding:8px;border-bottom:1px solid #f0f0f0;font-size:.85em}
tr:hover{background:#f8f9ff}
.r{text-align:right}
input[type="number"],input[type="text"],input[type="email"],input[type="password"]{width:100%;border:1px solid #e0e0e0;border-radius:4px;padding:6px 8px;font-size:.85em}
input:focus{outline:none;border-color:#6c5ce7;box-shadow:0 0 0 2px rgba(108,92,231,.15)}
.btn{background:#6c5ce7;color:#fff;border:none;padding:8px 16px;border-radius:6px;cursor:pointer;font-size:.9em;font-weight:500}
.btn:hover{background:#5a4bd1}
.btn-outline{background:#fff;color:#6c5ce7;border:1px solid #6c5ce7}
.btn-outline:hover{background:#f0edfc}
.save-btn{background:#6c5ce7;color:#fff;border:none;padding:4px 10px;border-radius:4px;cursor:pointer;font-size:.8em}
.save-btn:hover{background:#5a4bd1}
.save-btn.saved{background:#00b894}
.empty{text-align:center;padding:40px;color:#999}
.photo{width:36px;height:36px;border-radius:4px;object-fit:cover}

/* Auth */
.auth-container{max-width:400px;margin:80px auto;background:#fff;padding:32px;border-radius:12px;box-shadow:0 2px 12px rgba(0,0,0,.08)}
.auth-container h2{color:#6c5ce7;margin-bottom:8px;font-size:1.3em}
.auth-container p{color:#999;font-size:.85em;margin-bottom:20px}
.auth-container .field{margin-bottom:14px}
.auth-container label{display:block;font-size:.8em;color:#666;margin-bottom:4px}
.auth-container .toggle{color:#6c5ce7;cursor:pointer;font-size:.85em;text-align:center;margin-top:16px}
.auth-container .toggle:hover{text-decoration:underline}
//.auth-error{color:#e74c3c;font-size:.85em;margin-bottom:10px}
.metric-card{background:#fff;border-radius:8px;padding:12px 14px;box-shadow:0 1px 3px rgba(0,0,0,.06)}
.metric-card .mc-label{font-size:.75em;color:#999;text-transform:uppercase;letter-spacing:.3px;margin-bottom:4px}
.metric-card .mc-value{font-size:1.15em;font-weight:600;color:#1a1a2e}
.metric-card .mc-delta{font-size:.75em;margin-top:2px}
.metric-card .mc-delta.pos{color:#00b894}
.metric-card .mc-delta.neg{color:#e74c3c}
.metric-card .mc-sub{font-size:.75em;color:#999;margin-top:2px}
.metric-card.expense{border-left:3px solid #e17055}
.metric-card.expense .mc-value{color:#d63031}
.alert-card{background:#fff3cd;border:1px solid #ffc107;border-radius:8px;padding:10px 14px;margin-bottom:8px;font-size:.85em;display:flex;align-items:center;gap:8px}
.alert-card.red{background:#ffeaea;border-color:#e74c3c}
.alert-card.yellow{background:#fff8e1;border-color:#f39c12}

/* Sidebar layout */
.app-layout{display:flex;min-height:100vh}
.sidebar{width:220px;background:#1a1a2e;color:#fff;padding:0;flex-shrink:0;position:fixed;top:0;left:0;bottom:0;overflow-y:auto;z-index:50}
.sidebar .logo{padding:16px 20px;font-size:1.1em;font-weight:700;color:#fff;border-bottom:1px solid rgba(255,255,255,.1)}
.sidebar .nav-group{padding:8px 0}
.sidebar .nav-label{padding:6px 20px;font-size:.7em;text-transform:uppercase;color:rgba(255,255,255,.4);letter-spacing:1px}
.sidebar .nav-item{display:flex;align-items:center;gap:10px;padding:8px 20px;color:rgba(255,255,255,.7);cursor:pointer;font-size:.85em;transition:all .15s;text-decoration:none;border-left:3px solid transparent}
.sidebar .nav-item:hover{background:rgba(255,255,255,.08);color:#fff}
.sidebar .nav-item.active{background:rgba(108,92,231,.2);color:#fff;border-left-color:#6c5ce7}
.sidebar .nav-item .icon{width:20px;text-align:center}
.sidebar .user-block{position:absolute;bottom:0;left:0;right:0;padding:12px 20px;border-top:1px solid rgba(255,255,255,.1);font-size:.8em;color:rgba(255,255,255,.5)}
.sidebar .user-block .logout-btn{color:#e74c3c;cursor:pointer;display:block;margin-top:6px}
.sidebar .user-block .logout-btn:hover{color:#ff6b6b}
.main-area{margin-left:220px;flex:1;min-height:100vh;background:#f5f7fa}
.top-bar{background:#fff;border-bottom:1px solid #e0e0e0;padding:10px 24px;display:flex;align-items:center;gap:12px;position:sticky;top:0;z-index:10}
.top-bar .page-title{font-size:1.1em;font-weight:600;color:#1a1a2e}
.top-bar .filters{display:flex;align-items:center;gap:10px;margin-left:auto}
.top-bar select,.top-bar input{border:1px solid #e0e0e0;border-radius:4px;padding:4px 8px;font-size:.85em}
.page-content{padding:20px 24px}
.page-section{display:none}
.page-section.active{display:block}

th.sortable { cursor: pointer; user-select: none; white-space: nowrap; }
th.sortable:hover { background: rgba(255,255,255,0.15); }
th.sortable::after { content: ' ↕'; font-size: 0.7em; opacity: 0.5; }
th.sortable.asc::after { content: ' ↑'; opacity: 1; }
th.sortable.desc::after { content: ' ↓'; opacity: 1; }
</style>
</head>
<body>

<!-- Auth -->
<div id="auth-section">
<div class="auth-container">
<div id="auth-login">
<h2>🔑 Вход</h2>
<p>Войдите в свой аккаунт НЛ</p>
<div id="login-error" class="auth-error" style="display:none"></div>
<div class="field"><label>Email</label><input type="email" id="login-email"></div>
<div class="field"><label>Пароль</label><input type="password" id="login-password"></div>
<button class="btn" onclick="doLogin()" style="width:100%">Войти</button>
<div style="text-align:center;margin-top:16px;font-size:.85em"><a href="#" onclick="showRegister();return false" style="color:#6c5ce7;text-decoration:none">Нет аккаунта? Зарегистрироваться</a></div>
</div>

<div id="auth-register" style="display:none">
<h2>📝 Регистрация</h2>
<p>Создайте аккаунт для доступа к аналитике</p>
<div id="reg-error" class="auth-error" style="display:none"></div>
<div class="field"><label>Email</label><input type="email" id="reg-email"></div>
<div class="field"><label>Пароль</label><input type="password" id="reg-password"></div>
<div class="field"><label>Название организации</label><input type="text" id="reg-org" value="Моя организация"></div>
<button class="btn" onclick="doRegister()" style="width:100%">Зарегистрироваться</button>
<a href="#" class="toggle" onclick="showLogin();return false">Уже есть аккаунт? Войти</a>
</div>
</div>
</div>

<!-- Main app -->
<div id="app-section" style="display:none">
<div class="app-layout">
<aside class="sidebar">
<div class="logo">📊 НЛ Аналитика</div>
<div class="nav-group">
<div class="nav-label">Финансы</div>
<a class="nav-item active" onclick="navTo('stats',this)"><span class="icon">📊</span>Основные показатели</a>
<a class="nav-item" onclick="navTo('rnp',this)"><span class="icon">🎯</span>РНП</a>
<a class="nav-item" onclick="navTo('opiu',this)"><span class="icon">📑</span>ОПиУ</a>
<a class="nav-item" onclick="navTo('analytics',this)"><span class="icon">📈</span>Аналитика по товарам</a>
<a class="nav-item" onclick="navTo('costprice',this)"><span class="icon">📖</span>Справочник</a>
<a class="nav-item" onclick="navTo('warehouses',this)"><span class="icon">📦</span>Склады</a>
<a class="nav-item" onclick="navTo('opexpenses',this)"><span class="icon">📝</span>Опер. расходы</a>
<a class="nav-item" onclick="navTo('ads',this)"><span class="icon">📢</span>Реклама</a>
<a class="nav-item" onclick="navTo('unitecon',this)"><span class="icon">🧮</span>Юнит Экономика</a>
</div>
<div class="nav-group">
<div class="nav-label">Остальное</div>
<a class="nav-item" onclick="navTo('connectors',this)"><span class="icon">🔌</span>Подключения</a>
<a class="nav-item" onclick="navTo('subscription',this)"><span class="icon">💳</span>Подписка</a>
<a class="nav-item" onclick="navTo('settings',this)"><span class="icon">⚙️</span>Настройки</a>
<a class="nav-item" onclick="navTo('help',this)"><span class="icon">❓</span>Помощь</a>
</div>
<div class="user-block">
<div id="user-email"></div>
<div style="margin-top:4px">
<select id="org-select" onchange="switchOrg()" style="width:100%;background:rgba(255,255,255,.1);color:#fff;border:1px solid rgba(255,255,255,.2);border-radius:4px;padding:4px;font-size:.85em"></select>
</div>
<span class="logout-btn" onclick="doLogout()">Выйти</span>
</div>
</aside>
<div class="main-area">
<div class="top-bar">
<span class="page-title" id="page-title">Основные показатели</span>
<div class="filters" id="top-filters">
<select id="filter-store" style="min-width:120px"><option>Все магазины</option></select>
<select id="filter-period"><option value="yesterday">Вчера</option><option value="week">Неделя</option><option value="month" selected>Месяц</option></select>
<input type="text" id="filter-article" placeholder="Артикул" style="width:120px">
</div>
</div>
<div class="page-content">
<div id="page-stats" class="page-section active">
<!-- Фильтр по дате -->
<div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;flex-wrap:wrap">
<select id="stats-date" onchange="loadStats()" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em;cursor:pointer"></select>
<button class="btn" onclick="loadStats()" style="padding:6px 14px;font-size:.85em">🔄 Обновить</button>
</div>

<!-- Прибыль -->
<div style="margin-bottom:24px">
<div style="font-size:.85em;color:#999;margin-bottom:8px">ПРИБЫЛЬ</div>
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px">
<div class="metric-card"><div class="mc-label">Выручка</div><div class="mc-value" id="v-revenue">—</div></div>
<div class="metric-card"><div class="mc-label">Выкупов</div><div class="mc-value" id="v-buyouts-count">—</div></div>
<div class="metric-card" id="mc-profit"><div class="mc-label">Прибыль</div><div class="mc-value" id="v-profit">—</div><div class="mc-delta" id="d-profit"></div></div>
<div class="metric-card" id="mc-realization"><div class="mc-label">Реализация</div><div class="mc-value" id="v-realization">—</div><div class="mc-delta" id="d-realization"></div></div>
<div class="metric-card"><div class="mc-label">Продажи</div><div class="mc-value" id="v-sales">—</div><div class="mc-delta" id="d-sales"></div></div>
<div class="metric-card"><div class="mc-label">К выплате</div><div class="mc-value" id="v-topay">—</div><div class="mc-delta" id="d-topay"></div></div>
<div class="metric-card"><div class="mc-label">Продано штук</div><div class="mc-value" id="v-sold">—</div><div class="mc-delta" id="d-sold"></div></div>
<div class="metric-card"><div class="mc-label">Отмен штук</div><div class="mc-value" id="v-cancelled">—</div><div class="mc-delta" id="d-cancelled"></div></div>
<div class="metric-card"><div class="mc-label">Возвратов штук</div><div class="mc-value" id="v-returned">—</div><div class="mc-delta" id="d-returned"></div></div>
<div class="metric-card"><div class="mc-label">% возвратов</div><div class="mc-value" id="v-retpercent">—</div><div class="mc-delta" id="d-retpercent"></div></div>
<div class="metric-card"><div class="mc-label">ROI</div><div class="mc-value" id="v-roi">—</div><div class="mc-delta" id="d-roi"></div></div>
<div class="metric-card"><div class="mc-label">Рентабельность</div><div class="mc-value" id="v-rent">—</div><div class="mc-delta" id="d-rent"></div></div>
<div class="metric-card"><div class="mc-label">Процент выкупов</div><div class="mc-value" id="v-buyout">—</div><div class="mc-delta" id="d-buyout"></div></div>
<div class="metric-card"><div class="mc-label">Прибыль/ед.</div><div class="mc-value" id="v-profitunit">—</div><div class="mc-delta" id="d-profitunit"></div></div>
</div>
</div>

<!-- Расходы -->
<div style="margin-bottom:24px">
<div style="font-size:.85em;color:#999;margin-bottom:8px">РАСХОДЫ</div>
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px">
<div class="metric-card expense"><div class="mc-label">Налог</div><div class="mc-value" id="v-tax">—</div></div>
<div class="metric-card expense"><div class="mc-label">Себестоимость</div><div class="mc-value" id="v-costprice">—</div></div>
<div class="metric-card expense"><div class="mc-label">Комиссия ВБ</div><div class="mc-value" id="v-commission">—</div></div>
<div class="metric-card expense"><div class="mc-label">Реклама (ДРР)</div><div class="mc-value" id="v-ads">—</div></div>
<div class="metric-card expense"><div class="mc-label">Логистика</div><div class="mc-value" id="v-logistics">—</div></div>
<div class="metric-card expense"><div class="mc-label">Штрафы</div><div class="mc-value" id="v-fines">—</div></div>
<div class="metric-card expense"><div class="mc-label">Хранение</div><div class="mc-value" id="v-storage">—</div></div>
<div class="metric-card expense"><div class="mc-label">Платная приемка</div><div class="mc-value" id="v-reception">—</div></div>
<div class="metric-card expense"><div class="mc-label">Прочие удержания</div><div class="mc-value" id="v-other">—</div></div>
</div>
</div>

<!-- Остатки -->
<div style="margin-bottom:24px">
<div style="font-size:.85em;color:#999;margin-bottom:8px">ОСТАТКИ</div>
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px">
<div class="metric-card"><div class="mc-label">Остатки всего</div><div class="mc-value" id="v-stock-total">—</div><div class="mc-sub" id="v-stock-sub"></div></div>
<div class="metric-card"><div class="mc-label">На складах WB</div><div class="mc-value" id="v-stock-wb">—</div><div class="mc-sub" id="v-stock-wb-sub"></div></div>
<div class="metric-card"><div class="mc-label">На моих складах</div><div class="mc-value" id="v-stock-my">—</div><div class="mc-sub" id="v-stock-my-sub"></div></div>
</div>
</div>

<!-- Алерты -->
<div id="stats-alerts" style="margin-bottom:20px"></div>

<!-- Таблица товаров -->
<div style="font-size:.85em;color:#999;margin-bottom:8px">ТОВАРЫ</div>
<table id="stats-table">
<thead><tr><th>Фото</th><th>Арт WB</th><th>Название</th><th>Размер</th><th>ШК</th><th>Остаток</th><th>Заказы</th><th>Выкупы</th><th>Возвраты</th><th>Рейтинг</th><th>Показы</th><th>Клики</th><th>CTR</th><th>Реклама ₽</th><th>Цена</th></tr></thead>
<tbody id="stats-products"><tr><td colspan="15" class="empty">Выберите дату</td></tr></tbody>
</table>
</div>
<div id="page-analytics" class="page-section">
<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
<input type="text" id="analytics-search" placeholder="🔍 Поиск по артикулу/названию" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em;width:250px" oninput="loadAnalytics()">
<select id="analytics-date" onchange="loadAnalytics()" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em"></select>
<button class="btn" onclick="loadAnalytics()" style="padding:6px 14px;font-size:.85em">🔄 Обновить</button>
<button class="btn btn-outline" onclick="exportAnalytics()" style="padding:6px 14px;font-size:.85em">📥 Скачать</button>
</div>
<div style="overflow-x:auto">
<table id="analytics-table" style="font-size:.8em">
<thead><tr>
<th>Фото</th>
<th>Арт продавца</th>
<th>Арт WB</th>
<th>Товар</th>
<th>Остаток</th>
<th>Заказы</th>
<th>Выкупы</th>
<th>Отмены</th>
<th>Возвраты</th>
<th>% выкупа</th>
<th>Цена</th>
<th>Цена со скидкой</th>
<th>Комиссия ВБ %</th>
<th>Комиссия ВБ ₽</th>
<th>Логистика</th>
<th>Реклама ₽</th>
<th>ДРР %</th>
<th>Штрафы</th>
<th>Хранение</th>
<th>Приёмка</th>
<th>Прочие удерж.</th>
<th>Средний чек</th>
<th>Сумма реализации</th>
<th>К выплате</th>
<th>Себестоимость</th>
<th>Маржа</th>
<th>Маржа/ед.</th>
<th>Рентабельность</th>
<th>ROI</th>
<th>Рейтинг</th>
<th>Показы</th>
<th>Клики</th>
<th>CTR</th>
<th>Оборачив.</th>
<th>В пути к клиенту</th>
</tr></thead>
<tbody id="analytics-body"><tr><td colspan="35" class="empty">Загрузка...</td></tr></tbody>
</table>
</div>
<div style="margin-top:12px;display:flex;align-items:center;gap:12px;font-size:.85em;color:#999">
<span id="analytics-count"></span>
<select id="analytics-pagesize" onchange="loadAnalytics()" style="border:1px solid #e0e0e0;border-radius:4px;padding:4px 8px;font-size:.85em">
<option value="10">10</option><option value="25">25</option><option value="50">50</option><option value="100">100</option>
</select>
</div>
</div>


<div id="page-rnp" class="page-section">
<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
<select id="rnp-days" onchange="loadRnp()" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em">
<option value="7">7 дней</option><option value="10" selected>10 дней</option><option value="14">14 дней</option><option value="30">30 дней</option>
</select>
<button class="btn" onclick="loadRnp()" style="padding:6px 14px;font-size:.85em">🔄</button>
<input type="text" id="rnp-search" placeholder="🔍 Поиск" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em;width:200px" oninput="loadRnp()">
</div>
<div style="overflow-x:auto">
<table id="rnp-table" style="font-size:.82em">
<thead><tr>
<th>Арт продавца</th><th>Арт WB</th><th>Товар</th>
<th>Продажи шт</th><th>Выкупы шт</th><th>Возвраты шт</th><th>% выкупа</th>
<th>Выручка</th><th>Расходы</th><th>Маржа</th><th>Маржа/ед</th><th>Рентабельность</th>
<th>Остаток</th><th>Оборачиваемость</th>
</tr></thead>
<tbody id="rnp-body"><tr><td colspan="14" class="empty">Нажмите обновить</td></tr></tbody>
</table>
</div>
<div style="margin-top:12px;font-size:.85em;color:#999" id="rnp-count"></div>
</div>

<div id="page-opiu" class="page-section">
<div style="display:flex;align-items:center;gap:12px;margin-bottom:20px;flex-wrap:wrap">
<select id="opiu-period" onchange="loadOpiu()" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em">
<option value="4">Последние 4 недели</option><option value="8">8 недель</option><option value="12">12 недель</option>
</select>
<button class="btn" onclick="loadOpiu()" style="padding:6px 14px;font-size:.85em">🔄</button>
<button class="btn btn-outline" onclick="exportOpiu()" style="padding:6px 14px;font-size:.85em">📥 Excel</button>
</div>
<table id="opiu-table"><thead><tr><th>Статья</th><th>Итого</th><th>%</th></tr></thead>
<tbody id="opiu-body"><tr><td colspan="3" class="empty">Нажмите обновить</td></tr></tbody></table>
</div>

<div id="page-costprice" class="page-section">
<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid #e0e0e0;flex-wrap:wrap;background:#f8f9fb;padding:10px 16px;border-radius:8px">
<select id="cp-store" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em;min-width:130px"><option>Все магазины</option></select>
<select id="cp-period" onchange="loadCostPrices()" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em"><option value="yesterday">Вчера</option><option value="week">Неделя</option><option value="month" selected>Месяц</option></select>
<input type="text" id="cp-article" placeholder="Артикул" oninput="loadCostPrices()" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em;width:120px">
</div>

<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
<button class="btn" onclick="document.getElementById('cost-file-input').click()" style="padding:6px 14px;font-size:.85em">📤 Загрузить Excel</button>
<input type="file" id="cost-file-input" accept=".xlsx,.csv" style="display:none" onchange="uploadCostExcel(this)">
<button class="btn btn-outline" onclick="exportCostTemplate()" style="padding:6px 14px;font-size:.85em">📥 Скачать шаблон</button>
<input type="text" id="cost-search" placeholder="🔍 Поиск" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em;width:200px" oninput="loadCostPrices()">
<span style="font-size:.85em;color:#999;margin-left:auto" id="cost-count"></span>
<button class="btn" onclick="saveAllCostPrices()" style="padding:6px 14px;font-size:.85em;background:#00b894;color:#fff">💾 Сохранить всё</button>
</div>
<div style="overflow-x:auto">
<table id="cost-table" style="font-size:.82em"><thead><tr>
<th>Фото</th><th>Арт WB</th><th>Арт продавца</th><th>Размер</th><th>Товар</th><th>Баркод</th>
<th>Закупка ₽</th><th>Логистика ₽</th><th>Упаковка ₽</th><th>Прочее ₽</th>
<th>Себестоимость ₽</th><th>НДС %</th><th>Дата начала</th>
<th>Класс товара</th><th>Бренд</th><th>Налог. система</th><th>Ставка налога %</th><th>НДС ставка %</th>
</tr></thead>
<tbody id="cost-body"><tr><td colspan="18" class="empty">Загрузка...</td></tr></tbody></table>
</div>
<div style="margin-top:12px;display:flex;gap:16px;font-size:.85em" id="cost-summary"></div>
</div>

<div id="page-warehouses" class="page-section">
<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
<select id="wh-date" onchange="loadWarehouses()" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em"></select>
<button class="btn" onclick="loadWarehouses()" style="padding:6px 14px;font-size:.85em">🔄 Обновить</button>
<label style="font-size:.85em;display:flex;align-items:center;gap:6px"><input type="checkbox" id="wh-sizes" onchange="loadWarehouses()"> Размеры</label>
</div>
<div style="overflow-x:auto">
<table id="wh-table"><thead><tr><th>Арт WB</th><th>Арт продавца</th><th>Товар</th><th>Склад</th><th>Кол-во, шт</th><th>Общая себестоимость</th></tr></thead>
<tbody id="wh-body"><tr><td colspan="6" class="empty">Выберите дату</td></tr></tbody></table>
</div>
<div style="margin-top:12px;font-size:.85em;color:#999" id="wh-count"></div>
</div>

<div id="page-opexpenses" class="page-section">
<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
<button class="btn" onclick="showOpExDialog()" style="padding:6px 14px;font-size:.85em">➕ Добавить</button>
<div style="font-size:.85em;color:#999;margin-left:auto" id="opex-count"></div>
</div>
<table id="opex-table"><thead><tr><th>Дата</th><th>Статья расходов</th><th>Описание</th><th>Сумма ₽</th><th>НДС</th><th>Магазин</th><th></th></tr></thead>
<tbody id="opex-body"><tr><td colspan="7" class="empty">Нет записей</td></tr></tbody></table>

<div id="opex-dialog" style="display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.4);z-index:100;align-items:center;justify-content:center">
<div style="background:#fff;padding:24px;border-radius:12px;width:400px;box-shadow:0 4px 20px rgba(0,0,0,.15)">
<h3 style="color:#6c5ce7;margin-bottom:16px">➕ Операционный расход</h3>
<div style="margin-bottom:10px"><label style="font-size:.8em;color:#666">Дата</label><input type="date" id="opex-date" style="width:100%"></div>
<div style="margin-bottom:10px"><label style="font-size:.8em;color:#666">Статья</label><select id="opex-category" style="width:100%"><option>Аренда</option><option>Зарплата</option><option>Маркетинг</option><option>Логистика</option><option>Упаковка</option><option>Прочее</option></select></div>
<div style="margin-bottom:10px"><label style="font-size:.8em;color:#666">Описание</label><input type="text" id="opex-desc" style="width:100%"></div>
<div style="margin-bottom:10px"><label style="font-size:.8em;color:#666">Сумма ₽</label><input type="number" id="opex-amount" style="width:100%"></div>
<div style="margin-bottom:10px"><label style="font-size:.8em;color:#666">НДС %</label><input type="number" id="opex-vat" placeholder="0" style="width:100%"></div>
<div style="display:flex;gap:8px;margin-top:16px">
<button class="btn" onclick="saveOpEx()" style="flex:1">Сохранить</button>
<button class="btn btn-outline" onclick="hideOpExDialog()" style="flex:1">Отмена</button>
</div>
</div>
</div>
</div>


<!-- ─── РЕКЛАМА ──────────────────────────────────────────── -->
<div id="page-ads" class="page-section">
<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
<select id="ads-period" onchange="loadAds()" style="border:1px solid #e0e0e0;border-radius:4px;padding:4px 8px;font-size:.85em">
<option value="7">7 дней</option>
<option value="14">14 дней</option>
<option value="30" selected>30 дней</option>
<option value="60">60 дней</option>
</select>
<button class="btn" onclick="loadAds()" style="padding:6px 14px;font-size:.85em">Обновить</button>
<div style="margin-left:auto;font-size:.85em;color:#999" id="ads-updated"></div>
</div>

<!-- Метрики -->
<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px;margin-bottom:20px" id="ads-metrics">
<div class="metric-card"><div class="mc-label">Баланс</div><div class="mc-value" id="ad-balance">—</div></div>
<div class="metric-card"><div class="mc-label">Расход за период</div><div class="mc-value" id="ad-spent">—</div></div>
<div class="metric-card"><div class="mc-label">Показы</div><div class="mc-value" id="ad-views">—</div></div>
<div class="metric-card"><div class="mc-label">Клики</div><div class="mc-value" id="ad-clicks">—</div></div>
<div class="metric-card"><div class="mc-label">CTR</div><div class="mc-value" id="ad-ctr">—</div></div>
<div class="metric-card"><div class="mc-label">CPC</div><div class="mc-value" id="ad-cpc">—</div></div>
<div class="metric-card"><div class="mc-label">Заказы</div><div class="mc-value" id="ad-orders">—</div></div>
<div class="metric-card"><div class="mc-label">Конверсия (CR)</div><div class="mc-value" id="ad-cr">—</div></div>
<div class="metric-card"><div class="mc-label">В корзину</div><div class="mc-value" id="ad-atbs">—</div></div>
</div>

<!-- Таблица по дням -->
<h3 style="color:#6c5ce7;margin-bottom:10px;font-size:1em">📅 Статистика по дням</h3>
<table id="ads-daily-table" style="margin-bottom:24px">
<thead><tr><th>Дата</th><th class="r">Показы</th><th class="r">Клики</th><th class="r">CTR %</th><th class="r">CPC ₽</th><th class="r">Расход ₽</th><th class="r">Заказы</th><th class="r">CR %</th><th class="r">В корзину</th></tr></thead>
<tbody id="ads-daily-body"><tr><td colspan="9" class="empty">Загрузка...</td></tr></tbody>
</table>

<!-- Топ кампаний -->
<h3 style="color:#6c5ce7;margin-bottom:10px;font-size:1em">🏆 Топ кампаний по расходу</h3>
<table id="ads-campaigns-table">
<thead><tr><th>Кампания</th><th>ID</th><th class="r">Показы</th><th class="r">Клики</th><th class="r">CTR %</th><th class="r">Расход ₽</th><th class="r">Заказы</th><th class="r">В корзину</th></tr></thead>
<tbody id="ads-campaigns-body"><tr><td colspan="8" class="empty">Загрузка...</td></tr></tbody>
</table>
</div>
<div id="page-unitecon" class="page-section">
<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid #e0e0e0;flex-wrap:wrap;background:#f8f9fb;padding:10px 16px;border-radius:8px">
<select id="ue-store" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em;min-width:130px"><option>Все магазины</option></select>
<select id="ue-period" onchange="loadUnitEcon()" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em"><option value="yesterday">Вчера</option><option value="week">Неделя</option><option value="month" selected>Месяц</option></select>
<input type="text" id="ue-article" placeholder="Артикул" oninput="loadUnitEcon()" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em;width:120px">
</div>

<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
<button class="btn" onclick="loadUnitEcon()" style="padding:6px 14px;font-size:.85em">🔄 Обновить</button>
<input type="text" id="ue-search" placeholder="🔍 Поиск по артикулу/названию" style="border:1px solid #e0e0e0;border-radius:6px;padding:6px 12px;font-size:.9em;width:240px" oninput="loadUnitEcon()">
<button class="btn btn-outline" onclick="exportUnitEcon()" style="padding:6px 14px;font-size:.85em">📥 Excel</button>
<span style="font-size:.85em;color:#999;margin-left:auto" id="ue-count"></span>
<button class="btn" onclick="saveAllUnitEcon()" style="padding:6px 14px;font-size:.85em;background:#00b894;color:#fff">💾 Сохранить</button>
</div>
<div style="overflow-x:auto;max-height:70vh">
<table id="ue-table" style="font-size:.75em"><thead><tr>
<th style="position:sticky;left:0;z-index:2;background:#fff">Фото</th>
<th style="position:sticky;left:40px;z-index:2;background:#fff">Арт WB</th>
<th>Арт продавца</th>
<th style="min-width:140px">Название</th>
<th>Класс</th>
<th>Бренд</th>
<th>Себест. ₽</th>
<th>Доп. затраты</th>
<th style="background:#f0f0ff">Баз. % МП</th>
<th style="background:#f0f0ff">Корр. % МП</th>
<th style="background:#f0f0ff">Итог. % МП</th>
<th style="background:#f0f0ff">% выкупа ниши</th>
<th>% выкупа факт</th>
<th style="background:#fff3f0">Лог. тариф</th>
<th style="background:#fff3f0">Лог. факт</th>
<th style="background:#fff3f0">Хран. тариф</th>
<th style="background:#fff3f0">Хран. факт</th>
<th>Эквайр. 1.5%</th>
<th>Приёмка</th>
<th>Налог %</th>
<th>НДС %</th>
<th>Налог ₽</th>
<th style="background:#f0fff0">Рекл. факт</th>
<th style="background:#f0fff0">Рекл. план</th>
<th style="background:#e8f4fd">Цена до СПП</th>
<th style="background:#e8f4fd">СПП %</th>
<th style="background:#e8f4fd">Цена с СПП</th>
<th style="background:#fff8e0">Скидка ВБ Клуб %</th>
<th style="background:#ffe8e8">Расходы</th>
<th style="background:#ffe8e8">Прибыль</th>
<th style="background:#ffe8e8">Маржа %</th>
<th style="background:#ffe8e8">ROI %</th>
<th style="background:#ffe8e8">На Р/С</th>
<th style="background:#e8f0ff">Цена план</th>
<th style="background:#e8f0ff">Расходы план</th>
<th style="background:#e8f0ff">Прибыль план</th>
<th style="background:#e8f0ff">Маржа план %</th>
<th style="background:#e8f0ff">ROI план %</th>
<th style="background:#e8f0ff">На Р/С план</th>
<th style="background:#f0e8ff">Дата правок</th>
<th style="background:#f0e8ff">Цена к изм.</th>
<th style="background:#f0e8ff">Прибыль изм.</th>
<th style="background:#f0e8ff">ROI изм.</th>
<th>Тариф тип</th>
</tr></thead>
<tbody id="ue-body"><tr><td colspan="44" class="empty">Нажмите обновить</td></tr></tbody></table>
</div>
<div style="margin-top:12px;display:flex;gap:16px;font-size:.85em;flex-wrap:wrap" id="ue-summary"></div>
</div>

<div id="page-connectors" class="page-section">
<div style="max-width:600px;margin:0 auto;padding:20px">
<h3 style="color:#6c5ce7;margin-bottom:16px">🔌 Подключения</h3>
<div style="background:#fff;border-radius:8px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,.08);margin-bottom:16px">
<div style="font-weight:600;margin-bottom:8px">Wildberries</div>
<div style="display:flex;gap:8px;flex-wrap:wrap">
<input type="text" id="wb-key-name" placeholder="Название" style="width:140px">
<input type="text" id="wb-key-value" placeholder="API ключ WB" style="flex:1;min-width:200px">
<button class="btn" onclick="addWbKey()">Подключить</button>
</div>
<p style="color:#999;font-size:.8em;margin-top:8px">Кабинет WB → Настройки → Доступ к API</p>
</div>
<div id="wb-keys-list"></div>
</div>
</div>

<div id="page-subscription" class="page-section">
<div style="max-width:900px;margin:0 auto;padding:20px;text-align:center">
<h3 style="color:#6c5ce7;margin-bottom:24px">Тарифные планы</h3>
<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px">
<div style="background:#fff;border-radius:12px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.08)">
<h4>Новичок</h4><div style="font-size:1.5em;font-weight:700;color:#6c5ce7;margin:12px 0">990 ₽/мес</div>
<ul style="text-align:left;font-size:.85em;color:#666;list-style:none;padding:0"><li>✅ 1 магазин</li><li>✅ Дашборд + ОПиУ</li><li>✅ Аналитика по товарам</li></ul>
<button class="btn" style="width:100%;margin-top:12px">Выбрать</button>
</div>
<div style="background:#fff;border-radius:12px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.08);border:2px solid #6c5ce7">
<h4>Старт ⭐</h4><div style="font-size:1.5em;font-weight:700;color:#6c5ce7;margin:12px 0">1490 ₽/мес</div>
<ul style="text-align:left;font-size:.85em;color:#666;list-style:none;padding:0"><li>✅ 2 магазина</li><li>✅ Всё из Новичок</li><li>✅ РНП + Склады</li></ul>
<button class="btn" style="width:100%;margin-top:12px">Выбрать</button>
</div>
<div style="background:#fff;border-radius:12px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,.08)">
<h4>Бизнес</h4><div style="font-size:1.5em;font-weight:700;color:#6c5ce7;margin:12px 0">3990 ₽/мес</div>
<ul style="text-align:left;font-size:.85em;color:#666;list-style:none;padding:0"><li>✅ 5 магазинов</li><li>✅ Всё из Старт</li><li>✅ Приоритетная поддержка</li></ul>
<button class="btn" style="width:100%;margin-top:12px">Выбрать</button>
</div>
</div>
</div>
</div>

<div id="page-help" class="page-section">
<div style="max-width:600px;margin:0 auto;padding:40px;text-align:center;color:#999"><div style="font-size:3em;margin-bottom:16px">❓</div><h3>Помощь</h3><p>support@nl-table.ru</p></div>
</div>

<div id="page-settings" class="page-section">
<h3 style="margin-bottom:12px;color:#6c5ce7">🔑 WB API ключи</h3>
<div id="seller-id-display" style="margin-bottom:12px;font-size:.9em;color:#666"></div>
<div id="wb-keys-list"></div>
<div style="margin-top:16px;padding:16px;background:#fff;border-radius:8px;box-shadow:0 1px 3px rgba(0,0,0,.08)">
<h4 style="margin-bottom:10px">Добавить ключ</h4>
<div style="display:flex;gap:8px;flex-wrap:wrap">
<input type="text" id="wb-key-name" placeholder="Название" style="width:150px">
<input type="text" id="wb-key-value" placeholder="API ключ WB" style="flex:1;min-width:200px">
<button class="btn" onclick="addWbKey()">Добавить</button>
</div>
<p style="color:#999;font-size:.8em;margin-top:8px">Получить ключ: Кабинет WB → Настройки → Доступ к API</p>
</div>
</div>
</div>
</div>

<!-- New org dialog -->
<div id="new-org-dialog" style="display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,.4);z-index:100;display:none;align-items:center;justify-content:center">
<div style="background:#fff;padding:24px;border-radius:12px;width:360px;box-shadow:0 4px 20px rgba(0,0,0,.15)">
<h3 style="color:#6c5ce7;margin-bottom:12px">Новый магазин</h3>
<input type="text" id="new-org-name" placeholder="Название магазина" style="width:100%;margin-bottom:12px">
<div style="display:flex;gap:8px">
<button class="btn" onclick="createNewOrg()" style="flex:1">Создать</button>
<button class="btn btn-outline" onclick="hideNewOrgDialog()" style="flex:1">Отмена</button>
</div>
</div>
</div>

</div>
</div>

<script>
function esc(s) { return (s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;"); }

function toggleGroup(tr) {
    let el = tr.nextElementSibling;
    let show = el && el.style.display === 'none';
    while (el && el.classList.contains('group-child')) {
        el.style.display = show ? '' : 'none';
        el = el.nextElementSibling;
    }
    const badge = tr.querySelector('span');
    if (badge) badge.textContent = show ? badge.textContent.replace('▸', '▾') : badge.textContent.replace('▾', '▸');
}

let TOKEN = localStorage.getItem('nl_token');
let ORG_ID = localStorage.getItem('nl_org_id');

function showRegister() {
    document.getElementById('auth-login').style.display = 'none';
    document.getElementById('auth-register').style.display = '';
}
function showLogin() {
    document.getElementById('auth-register').style.display = 'none';
    document.getElementById('auth-login').style.display = '';
}

async function showApp() {
    document.getElementById('auth-section').style.display = 'none';
    document.getElementById('app-section').style.display = '';
    try {
        if (ORG_ID) {
            await loadDates();
            loadStats();
            loadOpiu();
            loadAnalytics();
            loadRnp();
            
            loadWarehouses();
            loadOpEx();
        }
    } catch(e) { console.error('init error:', e); }
}

async function loadStats() {
    if (!ORG_ID) return;
    const sel = document.getElementById('stats-date') || document.getElementById('ref-date');
    let dateVal = sel ? sel.value : '';
    if (!dateVal || dateVal === 'Нет данных') dateVal = new Date().toISOString().split('T')[0];
    try {
        const res = await fetch('/api/v1/nl/control?org_id=' + ORG_ID + '&target_date=' + dateVal);
        if (!res.ok) return;
        const data = await res.json();
        const s = data.summary || {};
        // Заполняем карточки
        const fmt = (v, suffix) => { if (v == null) return '—'; return Number(v).toLocaleString('ru-RU', {maximumFractionDigits:2}) + (suffix || ''); };
        // Карточки прибыли
        const revenue = s.total_revenue || 0;
        const adCost = s.total_ad_cost || 0;
        const profit = revenue - adCost;
        document.getElementById('v-profit').textContent = fmt(profit, ' ₽');
        document.getElementById('v-profit').style.color = profit >= 0 ? '#00b894' : '#e74c3c';
        document.getElementById('v-sold').textContent = fmt(s.total_orders);
        document.getElementById('v-returned').textContent = fmt(s.total_returns);
        document.getElementById('v-stock-total').textContent = fmt(s.total_stock, ' шт');
        document.getElementById('v-ads').textContent = fmt(adCost, ' ₽');
        document.getElementById('v-buyout').textContent = s.total_orders ? (s.total_buyouts / s.total_orders * 100).toFixed(1) + '%' : '—';
        // Доп. карточки
        const el1 = document.getElementById('v-revenue');
        if (el1) el1.textContent = fmt(revenue, ' ₽');
        const el2 = document.getElementById('v-buyouts-count');
        if (el2) el2.textContent = s.total_buyouts || 0;
        const el3 = document.getElementById('v-profitunit');
        if (el3) el3.textContent = s.total_buyouts ? fmt(profit / s.total_buyouts, ' ₽') : '—';
        // Алерты
        let alerts = '';
        if (s.zero_stock_count > 0) alerts += '<div class="alert-card red">🔴 Нет в наличии: ' + s.zero_stock_count + ' товаров</div>';
        if (s.low_stock_count > 0) alerts += '<div class="alert-card yellow">🟡 Низкий остаток (≤5): ' + s.low_stock_count + ' товаров</div>';
        document.getElementById('stats-alerts').innerHTML = alerts;
        // Таблица товаров — группировка по nm_id
        const tbody = document.getElementById('stats-products');
        const prods = data.products || [];
        if (!prods.length) { tbody.innerHTML = '<tr><td colspan="15" class="empty">Нет данных</td></tr>'; return; }
        
        // Группируем по nm_id
        const groups = {};
        const order = [];
        prods.forEach(p => {
            const key = p.nm_id;
            if (!groups[key]) { groups[key] = []; order.push(key); }
            groups[key].push(p);
        });
        
        let html = '';
        order.forEach(nmId => {
            const items = groups[nmId];
            const hasSizes = items.length > 1 || (items.length === 1 && items[0].size_name && items[0].size_name !== '0' && items[0].size_name !== 'ONE SIZE');
            
            // Агрегация для родительской строки
            let totalStock = 0, totalOrders = 0, totalBuyouts = 0, totalReturns = 0;
            let totalImpressions = 0, totalClicks = 0, totalAd = 0;
            items.forEach(p => {
                totalStock += p.stock_qty || 0;
                totalOrders += p.orders_count || 0;
                totalBuyouts += p.buyouts_count || 0;
                totalReturns += p.returns_count || 0;
                totalImpressions += p.impressions || 0;
                totalClicks += p.clicks || 0;
                totalAd += p.ad_cost || 0;
            });
            const avgRating = items.reduce((s,p) => s + (p.rating||0), 0) / items.length;
            const avgPrice = items[0].price || 0;
            const ctr = totalImpressions > 0 ? (totalClicks / totalImpressions * 100).toFixed(1) + '%' : '—';
            const thumb = (items[0].photo_main || '').replace('/hq/', '/c246x328/');
            const stockColor = totalStock <= 0 ? '#e74c3c' : totalStock <= 5 ? '#e17055' : '';
            
            if (hasSizes) {
                // Родительская строка (кликабельная)
                html += '<tr class="group-parent" onclick="toggleGroup(this)" style="cursor:pointer;background:#f8f9ff">' +
                '<td>' + (thumb ? '<img src="' + thumb + '" style="width:36px;height:36px;border-radius:4px;object-fit:cover">' : '') + '</td>' +
                '<td><b>' + nmId + '</b> <span style="font-size:.7em;color:#6c5ce7">▸ ' + items.length + ' разм.</span></td>' +
                '<td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + esc(items[0].product_name) + '">' + esc(items[0].product_name) + '</td>' +
                '<td></td><td></td>' +
                '<td style="color:' + stockColor + ';font-weight:600">' + totalStock + '</td>' +
                '<td>' + totalOrders + '</td><td>' + totalBuyouts + '</td><td>' + totalReturns + '</td>' +
                '<td>' + (avgRating ? avgRating.toFixed(1) : '—') + '</td>' +
                '<td>' + totalImpressions + '</td><td>' + totalClicks + '</td><td>' + ctr + '</td>' +
                '<td>' + fmt(totalAd) + '</td><td>' + fmt(avgPrice) + '</td></tr>';
                
                // Строки размеров (скрыты по умолчанию)
                items.forEach(p => {
                    const sCtr = p.impressions > 0 ? (p.clicks / p.impressions * 100).toFixed(1) + '%' : '—';
                    const sizeLabel = p.size_name && p.size_name !== '0' && p.size_name !== 'ONE SIZE' ? p.size_name : '—';
                    const sStockColor = p.stock_qty <= 0 ? '#e74c3c' : p.stock_qty <= 5 ? '#e17055' : '';
                    html += '<tr class="group-child" style="display:none;font-size:.85em">' +
                    '<td></td><td></td><td></td>' +
                    '<td style="color:#6c5ce7;font-weight:500">' + sizeLabel + '</td>' +
                    '<td style="font-size:.7em;color:#999">' + (p.barcode || '') + '</td>' +
                    '<td style="color:' + sStockColor + ';font-weight:600">' + (p.stock_qty ?? '—') + '</td>' +
                    '<td>' + (p.orders_count ?? '—') + '</td><td>' + (p.buyouts_count ?? '—') + '</td><td>' + (p.returns_count ?? '—') + '</td>' +
                    '<td>' + (p.rating ?? '—') + '</td><td>' + (p.impressions ?? '—') + '</td><td>' + (p.clicks ?? '—') + '</td><td>' + sCtr + '</td>' +
                    '<td>' + fmt(p.ad_cost) + '</td><td>' + fmt(p.price) + '</td></tr>';
                });
            } else {
                // Один размер — обычная строка
                const p = items[0];
                const sizeLabel = p.size_name && p.size_name !== '0' && p.size_name !== 'ONE SIZE' ? p.size_name : '';
                html += '<tr data-entity="' + (p.entity_id||'') + '">' +
                '<td>' + (thumb ? '<img src="' + thumb + '" style="width:36px;height:36px;border-radius:4px;object-fit:cover">' : '') + '</td>' +
                '<td>' + (p.nm_id || '') + '</td><td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + esc(p.product_name) + '">' + esc(p.product_name) + '</td>' +
                '<td style="font-size:.8em;color:#636e72">' + sizeLabel + '</td>' +
                '<td style="font-size:.7em;color:#999">' + (p.barcode || '') + '</td>' +
                '<td style="color:' + stockColor + ';font-weight:600">' + (p.stock_qty ?? '—') + '</td>' +
                '<td>' + (p.orders_count ?? '—') + '</td>' +
                '<td>' + (p.buyouts_count ?? '—') + '</td><td>' + (p.returns_count ?? '—') + '</td>' +
                '<td>' + (p.rating ?? '—') + '</td><td>' + (p.impressions ?? '—') + '</td>' +
                '<td>' + (p.clicks ?? '—') + '</td><td>' + ctr + '</td>' +
                '<td>' + fmt(p.ad_cost) + '</td><td>' + fmt(p.price) + '</td></tr>';
            }
        });
        tbody.innerHTML = html;
    } catch(e) { console.error('loadStats error:', e); }
}

function showAuth() {
    document.getElementById('app-section').style.display = 'none';
    document.getElementById('auth-section').style.display = '';
    document.getElementById('auth-login').style.display = '';
    document.getElementById('auth-register').style.display = 'none';
    TOKEN = null; ORG_ID = null;
    localStorage.removeItem('nl_token');
    localStorage.removeItem('nl_org_id');
}

async function doLogin() {
    const email = document.getElementById('login-email').value;
    const password = document.getElementById('login-password').value;
    const err = document.getElementById('login-error');
    try {
        const res = await fetch('/api/v1/nl/login', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({email, password})
        });
        if (!res.ok) { const d = await res.json(); throw new Error(d.detail || 'Ошибка'); }
        const data = await res.json();
        TOKEN = data.access_token; ORG_ID = data.org_id;
        localStorage.setItem('nl_token', TOKEN);
        localStorage.setItem('nl_org_id', ORG_ID);
        document.getElementById('user-email').textContent = email;
        showApp();
        loadOrgs();
        loadWbKeys();
    } catch(e) { err.textContent = e.message; err.style.display = ''; }
}

async function doRegister() {
    const email = document.getElementById('reg-email').value;
    const password = document.getElementById('reg-password').value;
    const org_name = document.getElementById('reg-org').value;
    const err = document.getElementById('reg-error');
    try {
        const res = await fetch('/api/v1/nl/register', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({email, password, org_name})
        });
        if (!res.ok) { const d = await res.json(); throw new Error(d.detail || 'Ошибка'); }
        const data = await res.json();
        TOKEN = data.access_token; ORG_ID = data.org_id;
        localStorage.setItem('nl_token', TOKEN);
        localStorage.setItem('nl_org_id', ORG_ID);
        document.getElementById('user-email').textContent = email;
        showApp();
        loadOrgs();
        loadWbKeys();
    } catch(e) { err.textContent = e.message; err.style.display = ''; }
}

function doLogout() { showAuth(); }

function switchTab(name, el) { navTo(name, el); }

function navTo(name, el) {
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    if (el) el.classList.add('active');
    document.querySelectorAll('.page-section').forEach(t => t.classList.remove('active'));
    var target = document.getElementById('page-' + name);
    if (target) target.classList.add('active');
    // Update page title
    var titles = {stats:'Основные показатели',rnp:'РНП',opiu:'ОПиУ',analytics:'Аналитика по товарам',
        costprice:'Справочник',warehouses:'Склады',opexpenses:'Опер. расходы',ads:'Реклама',
        unitecon:'Юнит Экономика',connectors:'Подключения',subscription:'Подписка',settings:'Настройки',help:'Помощь'};
    document.getElementById('page-title').textContent = titles[name] || name;
    // Update top-bar filters visibility
    var topFilters = document.getElementById('top-filters');
    if (topFilters) topFilters.style.display = (name === 'stats' || name === 'analytics' || name === 'rnp' || name === 'opiu') ? 'flex' : 'none';
    // Load data for the tab
    if (name === 'stats') loadStats();
    else if (name === 'analytics') loadAnalytics();
    else if (name === 'rnp') loadRnp();
    else if (name === 'opiu') loadOpiu();
    else if (name === 'costprice') loadCostPrices();
    else if (name === 'warehouses') loadWarehouses();
    else if (name === 'opexpenses') loadOpEx();
    else if (name === 'ads') loadAds();
    else if (name === 'unitecon') loadUnitEcon();
}

async function loadAds() {
    const days = document.getElementById('ads-period').value;
    try {
        const r = await fetch('/api/v1/nl/ad-stats?org_id=' + ORG_ID + '&days=' + days, {headers:{'Authorization':'Bearer '+TOKEN}});
        const d = await r.json();
        // Totals
        const t = d.totals || {};
        const fmt = (v, s='') => v != null ? (v >= 1000 ? v.toLocaleString('ru-RU', {maximumFractionDigits:0}) : v.toFixed(2)) + s : '—';
        document.getElementById('ad-views').textContent = fmt(t.views);
        document.getElementById('ad-clicks').textContent = fmt(t.clicks);
        document.getElementById('ad-spent').textContent = fmt(t.spent, ' ₽');
        document.getElementById('ad-ctr').textContent = t.ctr ? t.ctr + '%' : '—';
        document.getElementById('ad-cpc').textContent = fmt(t.cpc, ' ₽');
        document.getElementById('ad-orders').textContent = fmt(t.orders);
        document.getElementById('ad-cr').textContent = t.cr ? t.cr + '%' : '—';
        document.getElementById('ad-atbs').textContent = fmt(t.atbs);
        // Balance
        if (d.balance) {
            const bal = d.balance.balance || d.balance.balanceXdiscount || d.balance;
            document.getElementById('ad-balance').textContent = typeof bal === 'number' ? fmt(bal, ' ₽') : JSON.stringify(bal);
        } else {
            document.getElementById('ad-balance').textContent = '—';
        }
        // Daily table
        const daily = d.daily || [];
        const db = document.getElementById('ads-daily-body');
        if (!daily.length) {
            db.innerHTML = '<tr><td colspan="9" class="empty">Нет данных за период. Запустите синхронизацию.</td></tr>';
        } else {
            db.innerHTML = daily.map(r => '<tr><td>'+r.date+'</td><td class="r">'+r.views.toLocaleString('ru-RU')+'</td><td class="r">'+r.clicks.toLocaleString('ru-RU')+'</td><td class="r">'+r.ctr+'%</td><td class="r">'+r.cpc.toFixed(2)+' ₽</td><td class="r">'+r.spent.toLocaleString('ru-RU',{maximumFractionDigits:2})+' ₽</td><td class="r">'+r.orders+'</td><td class="r">'+r.cr+'%</td><td class="r">'+r.atbs+'</td></tr>').join('');
        }
        // Campaigns table
        const camps = d.top_campaigns || [];
        const cb = document.getElementById('ads-campaigns-body');
        if (!camps.length) {
            cb.innerHTML = '<tr><td colspan="8" class="empty">Нет данных по кампаниям</td></tr>';
        } else {
            cb.innerHTML = camps.map(c => '<tr><td>'+c.name+'</td><td>'+c.campaign_id+'</td><td class="r">'+c.views.toLocaleString('ru-RU')+'</td><td class="r">'+c.clicks.toLocaleString('ru-RU')+'</td><td class="r">'+c.ctr+'%</td><td class="r">'+c.spent.toLocaleString('ru-RU',{maximumFractionDigits:2})+' ₽</td><td class="r">'+c.orders+'</td><td class="r">'+c.atbs+'</td></tr>').join('');
        }
        document.getElementById('ads-updated').textContent = 'Обновлено: ' + new Date().toLocaleTimeString('ru-RU');
    } catch(e) {
        console.error('loadAds error:', e);
        document.getElementById('ads-daily-body').innerHTML = '<tr><td colspan="9" class="empty">Ошибка загрузки: '+e.message+'</td></tr>';
    }
}

async function loadDates() {
    if (!ORG_ID) return [];
    const res = await fetch('/api/v1/nl/dates?org_id=' + ORG_ID);
    if (!res.ok) return [];
    const dates = await res.json();
    // Fill both date selectors
    ['ref-date', 'stats-date', 'analytics-date', 'wh-date', 'opiu-period'].forEach(id => {
        const sel = document.getElementById(id);
        if (!sel || sel.tagName !== 'SELECT') return;
        // Keep period selects
        if (id === 'opiu-period') return;
        sel.innerHTML = '';
        if (!dates.length) { sel.innerHTML = '<option>Нет данных</option>'; return; }
        dates.forEach(d => {
            const opt = document.createElement('option');
            opt.value = d;
            const dt = new Date(d + 'T00:00:00');
            opt.textContent = dt.toLocaleDateString('ru-RU', {day:'numeric', month:'short', year:'numeric'});
            sel.appendChild(opt);
        });
    });
    return dates[0];
}

async function loadAnalytics() {
    if (!ORG_ID) return;
    const sel = document.getElementById('analytics-date') || document.getElementById('ref-date');
    const dateVal = sel ? sel.value : '';
    if (!dateVal || dateVal === 'Нет данных') return;
    const search = document.getElementById('analytics-search')?.value || '';
    try {
        const res = await fetch('/api/v1/nl/analytics?org_id=' + ORG_ID + '&target_date=' + dateVal + (search ? '&search=' + encodeURIComponent(search) : ''));
        if (!res.ok) { document.getElementById('analytics-body').innerHTML = '<tr><td colspan="35" class="empty">Ошибка загрузки</td></tr>'; return; }
        const data = await res.json();
        const prods = data.products || [];
        document.getElementById('analytics-count').textContent = prods.length + ' товаров';
        if (!prods.length) { document.getElementById('analytics-body').innerHTML = '<tr><td colspan="35" class="empty">Нет данных</td></tr>'; return; }
        const fmt = (v, s) => { if (v == null) return '—'; return Number(v).toLocaleString('ru-RU', {maximumFractionDigits:2}) + (s || ''); };
        const size = parseInt(document.getElementById('analytics-pagesize')?.value || '25');
        document.getElementById('analytics-body').innerHTML = prods.slice(0, size).map(p => {
            const thumb = (p.photo_main || '').replace('/hq/', '/c246x328/');
            return '<tr>' +
            '<td>' + (thumb ? '<img src="' + thumb + '" style="width:32px;height:32px;border-radius:4px;object-fit:cover">' : '') + '</td>' +
            '<td>' + esc(p.vendor_code || '') + '</td>' +
            '<td>' + (p.nm_id || '') + '</td>' +
            '<td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + esc(p.product_name || '') + '</td>' +
            '<td>' + (p.stock_qty ?? '—') + '</td>' +
            '<td>' + (p.orders_count ?? '—') + '</td>' +
            '<td>' + (p.buyouts_count ?? '—') + '</td>' +
            '<td>—</td>' +
            '<td>' + (p.returns_count ?? '—') + '</td>' +
            '<td>' + (p.buyout_percent || '—') + '%</td>' +
            '<td>' + fmt(p.price) + '</td>' +
            '<td>' + fmt(p.price_discount) + '</td>' +
            '<td>' + (p.tariff_percent || '—') + '%</td>' +
            '<td>' + fmt(p.commission) + '</td>' +
            '<td>' + fmt(p.logistics) + '</td>' +
            '<td>' + fmt(p.ad_cost) + '</td>' +
            '<td>' + (p.drr || '—') + '%</td>' +
            '<td>' + fmt(p.fines) + '</td>' +
            '<td>' + fmt(p.storage) + '</td>' +
            '<td>' + fmt(p.reception) + '</td>' +
            '<td>' + fmt(p.other_deductions) + '</td>' +
            '<td>' + fmt(p.avg_check) + '</td>' +
            '<td>' + fmt(p.revenue) + '</td>' +
            '<td>' + fmt(p.payout) + '</td>' +
            '<td>' + fmt(p.cost_price) + '</td>' +
            '<td>' + fmt(p.margin) + '</td>' +
            '<td>' + fmt(p.margin_per_unit) + '</td>' +
            '<td>' + (p.profitability || '—') + '%</td>' +
            '<td>' + (p.roi || '—') + '%</td>' +
            '<td>' + (p.rating || '—') + '</td>' +
            '<td>' + (p.impressions ?? '—') + '</td>' +
            '<td>' + (p.clicks ?? '—') + '</td>' +
            '<td>' + (p.ctr || '—') + '%</td>' +
            '<td>' + (p.turnover || '—') + '</td>' +
            '<td>' + (p.in_transit || '—') + '</td>' +
            '</tr>';
        }).join('');
    } catch(e) { console.error('loadAnalytics error:', e); }
}

async function loadOpiu() {
    if (!ORG_ID) return;
    const period = document.getElementById('opiu-period')?.value || '4';
    try {
        const res = await fetch('/api/v1/nl/opiu?org_id=' + ORG_ID + '&period=' + period);
        if (!res.ok) return;
        const data = await res.json();
        const weeks = data.weeks || [];
        const total = data.total || {};
        const fmt = (v) => v != null ? Number(v).toLocaleString('ru-RU', {maximumFractionDigits:0}) : '—';
        
        // Строим таблицу
        const thead = document.querySelector('#opiu-table thead tr');
        thead.innerHTML = '<th>Статья</th><th>Итого ₽</th><th>Итого %</th>';
        weeks.forEach(w => { thead.innerHTML += '<th>' + w.label + ' ₽</th><th>%</th>'; });
        
        const tbody = document.getElementById('opiu-body');
        const rows = [
            {name: 'Реализация', key: 'revenue'},
            {name: 'Продажи', key: 'buyouts'},
            {name: 'Возвраты', key: 'returns'},
            {name: 'Расходы', key: 'ad_cost'},
            {name: 'Комиссия ВБ', key: null},
            {name: 'Реклама', key: 'ad_cost'},
            {name: 'Логистика', key: null},
        ];
        
        const totalRev = total.revenue || 1;
        let html = '';
        rows.forEach(r => {
            const t = r.key ? total[r.key] : 0;
            const tp = totalRev ? (t / totalRev * 100).toFixed(1) : '—';
            html += '<tr><td><strong>' + r.name + '</strong></td><td>' + fmt(t) + '</td><td>' + tp + '%</td>';
            weeks.forEach(w => {
                const v = r.key ? w[r.key] : 0;
                const p = totalRev ? (v / totalRev * 100).toFixed(1) : '—';
                html += '<td>' + fmt(v) + '</td><td>' + p + '%</td>';
            });
            html += '</tr>';
        });
        
        // Чистая прибыль
        const profit = (total.revenue || 0) - (total.ad_cost || 0);
        const profitP = totalRev ? (profit / totalRev * 100).toFixed(1) : '—';
        html += '<tr style="font-weight:700;background:#f0edfc"><td>Чистая прибыль</td><td>' + fmt(profit) + '</td><td>' + profitP + '%</td>';
        weeks.forEach(w => {
            const p = (w.revenue || 0) - (w.ad_cost || 0);
            const pp = totalRev ? (p / totalRev * 100).toFixed(1) : '—';
            html += '<td>' + fmt(p) + '</td><td>' + pp + '%</td>';
        });
        html += '</tr>';
        
        tbody.innerHTML = html || '<tr><td colspan="3" class="empty">Нет данных</td></tr>';
    } catch(e) { console.error('loadOpiu error:', e); }
}

async function exportOpiu() { alert('Экспорт ОПиУ в разработке'); }

async function loadOpiu() {
    if (!ORG_ID) return;
    const period = document.getElementById('opiu-period')?.value || '4';
    try {
        const res = await fetch('/api/v1/nl/opiu?org_id=' + ORG_ID + '&period=' + period);
        if (!res.ok) return;
        const data = await res.json();
        const weeks = data.weeks || [];
        const total = data.total || {};
        const fmt = (v) => v != null ? Number(v).toLocaleString('ru-RU', {maximumFractionDigits:0}) : '—';
        const thead = document.querySelector('#opiu-table thead tr');
        thead.innerHTML = '<th>Статья</th><th>Итого</th><th>%</th>';
        weeks.forEach(w => { thead.innerHTML += '<th>' + w.label + '</th>'; });
        const tbody = document.getElementById('opiu-body');
        const tr = total.revenue || 1;
        const rows = [
            ['Реализация', total.revenue], ['Продажи (шт)', total.buyouts],
            ['Возвраты (шт)', total.returns], ['Реклама', total.ad_cost],
        ];
        let html = '';
        rows.forEach(([name, val]) => {
            html += '<tr><td><strong>' + name + '</strong></td><td>' + fmt(val) + '</td><td>' + (tr ? (val/tr*100).toFixed(1) : '—') + '%</td>';
            weeks.forEach(w => { const v = name.includes('Реал') ? w.revenue : name.includes('Прод') ? w.buyouts : name.includes('Воз') ? w.returns : w.ad_cost; html += '<td>' + fmt(v) + '</td>'; });
            html += '</tr>';
        });
        const profit = (total.revenue || 0) - (total.ad_cost || 0);
        html += '<tr style="font-weight:700;background:#f0edfc"><td>Чистая прибыль</td><td>' + fmt(profit) + '</td><td>' + (tr ? (profit/tr*100).toFixed(1) : '—') + '%</td>';
        weeks.forEach(w => { const p = (w.revenue||0)-(w.ad_cost||0); html += '<td>' + fmt(p) + '</td>'; });
        html += '</tr>';
        tbody.innerHTML = html || '<tr><td colspan="3" class="empty">Нет данных</td></tr>';
    } catch(e) { console.error('loadOpiu', e); }
}
async function exportOpiu() { alert('В разработке'); }

async function loadCostPrices() {
    if (!ORG_ID) return;
    const search = document.getElementById('cost-search')?.value || document.getElementById('cp-article')?.value || '';
    try {
        // Load products from latest tech_status date
        const datesRes = await fetch('/api/v1/nl/dates?org_id=' + ORG_ID);
        const dates = datesRes.ok ? await datesRes.json() : [];
        if (!dates.length) { document.getElementById('cost-body').innerHTML = '<tr><td colspan="18" class="empty">Нет данных</td></tr>'; return; }
        
        const prodsRes = await fetch('/api/v1/nl/control?org_id=' + ORG_ID + '&target_date=' + dates[0]);
        if (!prodsRes.ok) return;
        const prodsData = await prodsRes.json();
        let products = prodsData.products || [];
        
        // Load existing cost prices
        const costRes = await fetch('/api/v1/nl/cost-prices?org_id=' + ORG_ID);
        const costMap = {};
        if (costRes.ok) {
            const costs = await costRes.json();
            costs.forEach(c => { if (c.entity_id) costMap[c.entity_id] = c; else costMap[c.nm_id] = c; });
        }
        
        // Filter by search
        if (search) {
            const q = search.toLowerCase();
            products = products.filter(p => 
                (p.product_name||'').toLowerCase().includes(q) || 
                String(p.nm_id).includes(q) || 
                (p.vendor_code||'').toLowerCase().includes(q)
            );
        }
        
        document.getElementById('cost-count').textContent = products.length + ' товаров';
        
        // Calculate totals
        let totalCost = 0, filled = 0;
        
        const tbody = document.getElementById('cost-body');
        tbody.innerHTML = products.map(p => {
            const c = costMap[p.entity_id] || costMap[p.nm_id] || {};
            const purchase = c.purchase_cost || '';
            const logistics = c.logistics_cost || '';
            const packaging = c.packaging_cost || '';
            const other = c.other_costs || '';
            const costPrice = c.cost_price || '';
            const vat = c.vat || '';
            const validFrom = c.valid_from || new Date().toISOString().split('T')[0];
            if (costPrice) { totalCost += parseFloat(costPrice); filled++; }
            
            const thumb = (p.photo_main || '').replace('/hq/', '/c246x328/');
            return '<tr data-nm="' + p.nm_id + '" data-entity-id="' + (p.entity_id||'') + '" data-barcode="' + (p.barcode||'') + '" data-vc="' + esc(p.vendor_code||'') + '">' +
                '<td>' + (thumb ? '<img src="' + thumb + '" style="width:32px;height:32px;border-radius:4px;object-fit:cover">' : '') + '</td>' +
                '<td>' + p.nm_id + '</td>' +
                '<td>' + esc(p.vendor_code||'') + '</td>' +
                '<td>' + esc(p.size_name||'—') + '</td>' +
                '<td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + esc(p.product_name||'') + '">' + esc(p.product_name||'') + '</td>' +
                '<td style="font-size:.8em">' + (p.barcode||'') + '</td>' +
                '<td><input type="number" class="cost-input" data-field="purchase" value="' + purchase + '" style="width:70px" placeholder="0"></td>' +
                '<td><input type="number" class="cost-input" data-field="logistics" value="' + logistics + '" style="width:70px" placeholder="0"></td>' +
                '<td><input type="number" class="cost-input" data-field="packaging" value="' + packaging + '" style="width:70px" placeholder="0"></td>' +
                '<td><input type="number" class="cost-input" data-field="other" value="' + other + '" style="width:70px" placeholder="0"></td>' +
                '<td><input type="number" class="cost-input" data-field="cost_price" value="' + costPrice + '" style="width:80px;font-weight:600" placeholder="0"></td>' +
                '<td><input type="number" class="cost-input" data-field="vat" value="' + vat + '" style="width:50px" placeholder="0"></td>' +
                '<td><input type="date" class="cost-input" data-field="valid_from" value="' + validFrom + '" style="width:110px;font-size:.8em"></td>' +
                '<td><input type="text" class="cost-input" data-field="product_class" value="' + esc(c.product_class||'') + '" style="width:80px" placeholder="-"></td>' +
                '<td><input type="text" class="cost-input" data-field="brand" value="' + esc(c.brand||'') + '" style="width:80px" placeholder="-"></td>' +
                '<td><select class="cost-input" data-field="tax_system" style="width:90px;font-size:.8em"><option value="">-</option><option value="usn"' + (c.tax_system==='usn'?' selected':'') + '>УСН</option><option value="usn_dr"' + (c.tax_system==='usn_dr'?' selected':'') + '>Доходы-Расходы</option><option value="osn"' + (c.tax_system==='osn'?' selected':'') + '>ОСН</option></select></td>' +
                '<td><input type="number" class="cost-input" data-field="tax_rate" value="' + (c.tax_rate||'') + '" style="width:60px" placeholder="0"></td>' +
                '<td><input type="number" class="cost-input" data-field="vat_rate" value="' + (c.vat_rate||'') + '" style="width:60px" placeholder="0"></td>' +
                '</tr>';
        }).join('');
        
        document.getElementById('cost-summary').innerHTML = 
            '<span>💰 Заполнено: <strong>' + filled + '/' + products.length + '</strong></span>' +
            '<span>📊 Сумма себестоимости: <strong>' + totalCost.toLocaleString('ru-RU') + ' ₽</strong></span>' +
            (filled > 0 ? '<span>📐 Средняя: <strong>' + Math.round(totalCost/filled).toLocaleString('ru-RU') + ' ₽</strong></span>' : '');
    } catch(e) { console.error('loadCostPrices', e); }
}

async function saveAllCostPrices() {
    const rows = document.querySelectorAll('#cost-body tr[data-nm]');
    let saved = 0;
    for (const row of rows) {
        const costInput = row.querySelector('[data-field="cost_price"]');
        if (!costInput || !costInput.value) continue;
        const data = {
            nm_id: parseInt(row.dataset.nm),
            entity_id: row.dataset.entityId || undefined,
            barcode: row.dataset.barcode,
            vendor_code: row.dataset.vc,
            purchase_cost: parseFloat(row.querySelector('[data-field="purchase"]')?.value || '0'),
            logistics_cost: parseFloat(row.querySelector('[data-field="logistics"]')?.value || '0'),
            packaging_cost: parseFloat(row.querySelector('[data-field="packaging"]')?.value || '0'),
            other_costs: parseFloat(row.querySelector('[data-field="other"]')?.value || '0'),
            cost_price: parseFloat(costInput.value),
            vat: parseFloat(row.querySelector('[data-field="vat"]')?.value || '0'),
            valid_from: row.querySelector('[data-field="valid_from"]')?.value || new Date().toISOString().split('T')[0],
            product_class: row.querySelector('[data-field="product_class"]')?.value || '',
            brand: row.querySelector('[data-field="brand"]')?.value || '',
            tax_system: row.querySelector('[data-field="tax_system"]')?.value || '',
            tax_rate: parseFloat(row.querySelector('[data-field="tax_rate"]')?.value || '0'),
            vat_rate: parseFloat(row.querySelector('[data-field="vat_rate"]')?.value || '0'),
            source: 'manual'
        };
        try {
            await fetch('/api/v1/nl/cost-prices?org_id=' + ORG_ID, {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(data)
            });
            saved++;
        } catch(e) { console.error('save error', e); }
    }
    alert('Сохранено: ' + saved + ' записей');
    loadCostPrices();
}

async function uploadCostExcel(input) {
    if (!input.files.length) return;
    const file = input.files[0];
    const body = await file.arrayBuffer();
    try {
        const res = await fetch('/api/v1/nl/cost-prices/upload?org_id=' + ORG_ID, {
            method: 'POST', headers: {'Content-Type': 'application/octet-stream', 'x-filename': file.name},
            body: body
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || 'Ошибка');
        alert('Загружено: ' + data.updated + ' из ' + data.total);
        loadCostPrices();
    } catch(e) { alert('Ошибка: ' + e.message); }
    input.value = '';
}

function exportCostTemplate() {
    // Собираем товары из таблицы если есть, или отдаём пустой шаблон
    const rows = document.querySelectorAll('#cost-body tr[data-nm]');
    let csv = 'Арт WB;Арт продавца;Баркод;Название;Закупка;Логистика;Упаковка;Прочее;Себестоимость;НДС %';
    if (rows.length) {
        csv += String.fromCharCode(10);
        rows.forEach(row => {
            const nm = row.dataset.nm || '';
            const vc = row.dataset.vc || '';
            const bc = row.dataset.barcode || '';
            const name = row.querySelector('td:nth-child(4)')?.textContent || '';
            csv += [nm, vc, bc, '"' + name + '"', '', '', '', '', '', ''].join(';') + String.fromCharCode(10);
        });
    }
    const blob = new Blob(['﻿' + csv], {type: 'text/csv;charset=utf-8'});
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'template_sebestoimost.csv';
    a.click();
    URL.revokeObjectURL(a.href);
}

async function loadWarehouses() {
    if (!ORG_ID) return;
    const sel = document.getElementById('wh-date') || document.getElementById('ref-date');
    const d = sel?.value;
    if (!d || d === 'Нет данных') return;
    try {
        const res = await fetch('/api/v1/nl/warehouses?org_id=' + ORG_ID + '&target_date=' + d);
        if (!res.ok) return;
        const items = await res.json();
        document.getElementById('wh-count').textContent = items.length + ' записей';
        if (!items.length) { document.getElementById('wh-body').innerHTML = '<tr><td colspan="6" class="empty">Нет данных</td></tr>'; return; }
        document.getElementById('wh-body').innerHTML = items.map(i =>
            '<tr><td>' + (i.nm_id||'') + '</td><td>' + esc(i.vendor_code||'') + '</td><td>' + esc(i.product_name||'') +
            '</td><td>' + (i.warehouse||'—') + '</td><td>' + (i.qty||0) + '</td><td>—</td></tr>'
        ).join('');
    } catch(e) { console.error('loadWarehouses', e); }
}

function showOpExDialog() { document.getElementById('opex-dialog').style.display = 'flex'; document.getElementById('opex-date').value = new Date().toISOString().split('T')[0]; }
function hideOpExDialog() { document.getElementById('opex-dialog').style.display = 'none'; }
async function saveOpEx() {
    const data = {date: document.getElementById('opex-date').value, category: document.getElementById('opex-category').value, description: document.getElementById('opex-desc').value, amount: parseFloat(document.getElementById('opex-amount').value), vat: parseFloat(document.getElementById('opex-vat').value||'0')};
    if (!data.date || !data.amount) { alert('Заполните дату и сумму'); return; }
    try {
        await fetch('/api/v1/nl/operating-expenses?org_id=' + ORG_ID, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(data)});
        hideOpExDialog(); loadOpEx();
    } catch(e) { alert('Ошибка: ' + e.message); }
}
async function loadOpEx() {
    if (!ORG_ID) return;
    try {
        const res = await fetch('/api/v1/nl/operating-expenses?org_id=' + ORG_ID);
        if (!res.ok) return;
        const items = await res.json();
        document.getElementById('opex-count').textContent = items.length + ' записей';
        if (!items.length) { document.getElementById('opex-body').innerHTML = '<tr><td colspan="7" class="empty">Нет записей. Нажмите добавить.</td></tr>'; return; }
        document.getElementById('opex-body').innerHTML = items.map(i =>
            '<tr><td>' + (i.date||'') + '</td><td>' + esc(i.category||'') + '</td><td>' + esc(i.description||'') +
            '</td><td>' + (i.amount||0) + '</td><td>' + (i.vat||0) + '%</td><td>—</td><td style="color:#e74c3c;cursor:pointer">🗑</td></tr>'
        ).join('');
    } catch(e) { console.error('loadOpEx', e); }
}

async function loadRnp() {
    if (!ORG_ID) return;
    const days = document.getElementById('rnp-days')?.value || '10';
    const search = document.getElementById('rnp-search')?.value || '';
    try {
        const res = await fetch('/api/v1/nl/rnp?org_id=' + ORG_ID + '&days=' + days + (search ? '&search=' + encodeURIComponent(search) : ''));
        if (!res.ok) { document.getElementById('rnp-body').innerHTML = '<tr><td colspan="14" class="empty">Ошибка</td></tr>'; return; }
        const data = await res.json();
        const prods = data.products || [];
        document.getElementById('rnp-count').textContent = prods.length + ' товаров за ' + data.days + ' дней';
        if (!prods.length) { document.getElementById('rnp-body').innerHTML = '<tr><td colspan="14" class="empty">Нет данных</td></tr>'; return; }
        const fmt = (v,s) => { if (v==null) return '—'; return Number(v).toLocaleString('ru-RU',{maximumFractionDigits:2})+(s||''); };
        document.getElementById('rnp-body').innerHTML = prods.map(p =>
            '<tr>' +
            '<td>' + esc(p.vendor_code||'') + '</td><td>' + (p.nm_id||'') + '</td><td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + esc(p.product_name||'') + '</td>' +
            '<td>' + p.orders + '</td><td>' + p.buyouts + '</td><td>' + p.returns + '</td><td>' + p.buyout_pct + '%</td>' +
            '<td>' + fmt(p.revenue) + '</td><td>' + fmt(p.expenses) + '</td>' +
            '<td style="color:' + (p.margin>=0?'#00b894':'#e74c3c') + '">' + fmt(p.margin) + '</td>' +
            '<td>' + fmt(p.margin_per_unit) + '</td><td>' + p.profitability + '%</td>' +
            '<td>' + p.stock + '</td><td>' + p.turnover + '</td></tr>'
        ).join('');
    } catch(e) { console.error('loadRnp', e); }
}

async function loadRefData() {
    if (!ORG_ID) { document.getElementById('ref-body').innerHTML = '<tr><td colspan="13" class="empty">Нет данных. Добавьте WB API ключ в настройках.</td></tr>'; return; }
    const dateVal = document.getElementById('ref-date').value;
    const target_date = dateVal && dateVal !== 'Нет данных' ? '&target_date=' + dateVal : '';
    const [prodRes, refRes] = await Promise.all([
        fetch('/api/v1/nl/products?org_id=' + ORG_ID + target_date),
        fetch('/api/v1/nl/reference?org_id=' + ORG_ID + target_date)
    ]);
    const products = await prodRes.json();
    const refData = await refRes.json();
    const refMap = {};
    refData.forEach(r => refMap[r.nm_id] = r);
    const tbody = document.getElementById('ref-body');
    tbody.innerHTML = '';
    document.getElementById('ref-stats').textContent = products.length + ' товаров';
    if (!products.length) { tbody.innerHTML = '<tr><td colspan="13" class="empty">Нет товаров на эту дату. Подключите WB API ключ и дождитесь синхронизации.</td></tr>'; return; }
    products.forEach(p => {
        const ref = refMap[p.nm_id] || {};
        const thumb = (p.photo_main || '').replace('/hq/', '/c246x328/');
        const img = thumb ? '<img class="photo" src="' + thumb + '" loading="lazy">' : '📦';
        const tr = document.createElement('tr');
        tr.innerHTML =
            '<td>' + img + '</td>' +
            '<td><b>' + p.nm_id + '</b></td>' +
            '<td>' + (p.vendor_code||'') + '</td>' +
            '<td>' + (p.product_name||'').substring(0,30) + '</td>' +
            '<td style="font-size:.78em;color:#666">' + (p.barcode||'—') + '</td>' +
            '<td style="font-size:.78em;color:#666">' + (p.sku||'—') + '</td>' +
            '<td><input type="number" data-field="cost_price" value="' + (ref.cost_price||'') + '" step="0.01" style="width:80px;font-weight:600;background:' + (ref.cost_price ? '#f0fff0' : '#fff8f0') + '"></td>' +
            '<td><input type="number" data-field="purchase_price" value="' + (ref.purchase_price||'') + '" step="0.01" style="width:75px"></td>' +
            '<td><input type="number" data-field="packaging_cost" value="' + (ref.packaging_cost||'') + '" step="0.01" style="width:70px"></td>' +
            '<td><input type="number" data-field="logistics_cost" value="' + (ref.logistics_cost||'') + '" step="0.01" style="width:70px"></td>' +
            '<td><input type="number" data-field="other_costs" value="' + (ref.other_costs||'') + '" step="0.01" style="width:65px"></td>' +
            '<td><input type="text" data-field="notes" value="' + esc(ref.notes) + '" style="width:100px"></td>' +
            '<td><button class="save-btn" data-nm="' + p.nm_id + '" data-vc="' + (p.vendor_code || '') + '" data-pn="' + (p.product_name || '').replace(/"/g, '&quot;') + '" onclick="saveRowBtn(this)">💾</button></td>';
        tbody.appendChild(tr);
    });
}

async function saveAllRows() {
    const btns = document.querySelectorAll('#ref-body .save-btn');
    for (const btn of btns) { await saveRowBtn(btn); }
}

async function saveRowBtn(btn) {
    const nmId = parseInt(btn.dataset.nm);
    const vc = btn.dataset.vc || '';
    const pn = btn.dataset.pn || '';
    const dateVal = document.getElementById('ref-date').value;
    const row = btn.closest('tr');
    const inputs = row.querySelectorAll('input');
    const data = {nm_id: nmId, vendor_code: vc, product_name: pn, target_date: dateVal};
    inputs.forEach(inp => {
        const f = inp.dataset.field;
        if (f) data[f] = inp.type === 'number' ? (parseFloat(inp.value)||null) : inp.value;
    });
    const res = await fetch('/api/v1/nl/reference?org_id=' + ORG_ID, {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data)
    });
    if (res.ok) { btn.textContent='✅'; btn.classList.add('saved'); setTimeout(()=>{btn.textContent='💾';btn.classList.remove('saved');},1500); }
}

// Org & WB key management
async function loadOrgs() {
    const res = await fetch('/api/v1/nl/organizations?token=' + TOKEN);
    if (!res.ok) return;
    const orgs = await res.json();
    const sel = document.getElementById('org-select');
    sel.innerHTML = '';
    orgs.forEach(o => {
        const opt = document.createElement('option');
        opt.value = o.id;
        opt.textContent = o.name + (o.wb_seller_id ? ' (ID ' + o.wb_seller_id + ')' : '') + (o.wb_keys_count ? ' ' + o.wb_keys_count + '🔑' : '');
        if (o.id === ORG_ID) opt.selected = true;
        sel.appendChild(opt);
    });
    // Show seller ID in settings
    const current = orgs.find(o => o.id === ORG_ID);
    const sid = document.getElementById('seller-id-display');
    if (sid && current && current.wb_seller_id) sid.innerHTML = '<strong>ID ' + current.wb_seller_id + '</strong>';
    else if (sid) sid.innerHTML = '';
    if (!ORG_ID && orgs.length) {
        ORG_ID = orgs[0].id;
        localStorage.setItem('nl_org_id', ORG_ID);
        sel.value = ORG_ID;
    }
}

function switchOrg() {
    ORG_ID = document.getElementById('org-select').value;
    localStorage.setItem('nl_org_id', ORG_ID);
    showApp();
}

function showNewOrgDialog() {
    const d = document.getElementById('new-org-dialog');
    d.style.display = 'flex';
}
function hideNewOrgDialog() {
    document.getElementById('new-org-dialog').style.display = 'none';
}

async function createNewOrg() {
    const name = document.getElementById('new-org-name').value || 'Новый магазин';
    const res = await fetch('/api/v1/nl/organizations?token=' + TOKEN, {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({name})
    });
    if (res.ok) {
        const data = await res.json();
        ORG_ID = data.id;
        localStorage.setItem('nl_org_id', ORG_ID);
        hideNewOrgDialog();
        loadOrgs();
        loadRefData();
        loadWbKeys();
    }
}

async function loadWbKeys() {
    if (!ORG_ID) return;
    const res = await fetch('/api/v1/nl/wb-keys?org_id=' + ORG_ID);
    const keys = await res.json();
    const el = document.getElementById('wb-keys-list');
    if (!keys.length) {
        el.innerHTML = '<div style="color:#999;font-size:.9em;padding:8px">Ключи не добавлены</div>';
        return;
    }
    el.innerHTML = keys.map(k =>
        '<div class="wb-key-item"><span class="name">🔑 ' + k.name + '</span><span class="date">' + (k.created_at||'').substring(0,10) + '</span><span class="del" data-keyid="' + k.id + '" onclick="deleteWbKeyBtn(this)">✕</span></div>'
    ).join('');
}

async function addWbKey() {
    const name = document.getElementById('wb-key-name').value || 'WB Key';
    const api_key = document.getElementById('wb-key-value').value;
    if (!api_key) return;
    const res = await fetch('/api/v1/nl/wb-keys?org_id=' + ORG_ID, {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({name, api_key})
    });
    if (res.ok) {
        document.getElementById('wb-key-name').value = '';
        document.getElementById('wb-key-value').value = '';
        loadWbKeys();
        loadOrgs(); // update key count
    } else {
        const err = await res.json();
        alert(err.detail || 'Ошибка');
    }
}

async function deleteWbKeyBtn(btn) { await deleteWbKey(btn.dataset.keyid); }
async function deleteWbKey(id) {
    if (!confirm('Удалить ключ?')) return;
    await fetch('/api/v1/nl/wb-keys/' + id + '?org_id=' + ORG_ID, {method: 'DELETE'});
    loadWbKeys();
    loadOrgs();
}



// ─── OPERATIONAL CONTROL ──────────────────────────────────

async function loadControl() {
    const dateVal = document.getElementById('ctrl-date').value;
    if (!dateVal || dateVal === 'Нет данных') return;
    const res = await fetch('/api/v1/nl/control?org_id=' + ORG_ID + '&target_date=' + dateVal);
    const data = await res.json();
    const s = data.summary;

    // Alerts
    const alerts = document.getElementById('ctrl-alerts');
    let alertHtml = '';
    if (s.zero_stock_count > 0) alertHtml += '<div style="background:#fff3f3;border:1px solid #ffcdd2;border-radius:6px;padding:8px 12px;margin-bottom:6px;color:#c62828">🔴 Нет в наличии: ' + s.zero_stock_count + ' товаров</div>';
    if (s.low_stock_count > 0) alertHtml += '<div style="background:#fff8e1;border:1px solid #ffe082;border-radius:6px;padding:8px 12px;margin-bottom:6px;color:#e65100">🟡 Низкий остаток (≤5): ' + s.low_stock_count + ' товаров</div>';
    if (s.low_rating_count > 0) alertHtml += '<div style="background:#fff8e1;border:1px solid #ffe082;border-radius:6px;padding:8px 12px;margin-bottom:6px;color:#e65100">⚠️ Низкий рейтинг (<4): ' + s.low_rating_count + ' товаров</div>';
    alerts.innerHTML = alertHtml;

    // Cards
    const cards = document.getElementById('ctrl-cards');
    const metrics = [
        {label: '📦 Товаров', value: s.total_products, color: '#6c5ce7'},
        {label: '🏪 Остаток', value: s.total_stock, color: s.total_stock > 0 ? '#00b894' : '#e74c3c'},
        {label: '📋 Заказы', value: s.total_orders, color: '#0984e3'},
        {label: '✅ Выкупы', value: s.total_buyouts, color: '#00b894'},
        {label: '↩️ Возвраты', value: s.total_returns, color: '#d63031'},
        {label: '👁 Показы', value: s.total_impressions, color: '#636e72'},
        {label: '👆 Клики', value: s.total_clicks, color: '#0984e3'},
        {label: '📊 CTR', value: s.ctr + '%', color: s.ctr > 5 ? '#00b894' : s.ctr > 2 ? '#fdcb6e' : '#e74c3c'},
        {label: '💰 Реклама ₽', value: s.total_ad_cost ? s.total_ad_cost.toFixed(0) : '0', color: '#e17055'},
        {label: '⭐ Ср. рейтинг', value: s.avg_rating || '—', color: '#fdcb6e'},
    ];
    cards.innerHTML = metrics.map(m => '<div style="background:#fff;border-radius:8px;padding:12px;box-shadow:0 1px 3px rgba(0,0,0,.08);text-align:center"><div style="font-size:.75em;color:#999;margin-bottom:4px">' + m.label + '</div><div style="font-size:1.3em;font-weight:700;color:' + m.color + '">' + m.value + '</div></div>').join('');

    // Products table
    const tbody = document.getElementById('ctrl-body');
    if (!data.products.length) { tbody.innerHTML = '<tr><td colspan="15" class="empty">Нет данных</td></tr>'; return; }
    tbody.innerHTML = data.products.map(p => {
        const thumb = (p.photo_main || '').replace('/hq/', '/c246x328/');
        const img = thumb ? '<img class="photo" src="' + thumb + '" loading="lazy">' : '📦';
        const ctr = p.impressions > 0 ? (p.clicks / p.impressions * 100).toFixed(1) + '%' : '—';
        const stockColor = p.stock_qty <= 0 ? '#e74c3c' : p.stock_qty <= 5 ? '#e17055' : '#00b894';
        const sizeLabel = p.size_name && p.size_name !== '0' && p.size_name !== 'ONE SIZE' ? p.size_name : '';
        return '<tr data-entity="' + (p.entity_id||'') + '">' +
            '<td>' + img + '</td>' +
            '<td><b>' + p.nm_id + '</b></td>' +
            '<td>' + (p.product_name || '').substring(0, 25) + '</td>' +
            '<td style="font-size:.8em;color:#636e72">' + sizeLabel + '</td>' +
            '<td style="font-size:.7em;color:#999">' + (p.barcode || '') + '</td>' +
            '<td style="color:' + stockColor + ';font-weight:600">' + (p.stock_qty ?? '—') + '</td>' +
            '<td>' + (p.orders_count ?? '—') + '</td>' +
            '<td>' + (p.buyouts_count ?? '—') + '</td>' +
            '<td>' + (p.returns_count ?? '—') + '</td>' +
            '<td>' + (p.rating ? p.rating.toFixed(1) : '—') + '</td>' +
            '<td>' + (p.impressions ?? '—') + '</td>' +
            '<td>' + (p.clicks ?? '—') + '</td>' +
            '<td>' + ctr + '</td>' +
            '<td>' + (p.ad_cost ? p.ad_cost.toFixed(0) : '—') + '</td>' +
            '<td>' + (p.price ? p.price.toFixed(0) : '—') + '</td>' +
            '<td>' + (p.price_discount ? p.price_discount.toFixed(0) : '—') + '</td>' +
            '<td>' + (p.tariff ? p.tariff.toFixed(0) + '%' : '—') + '</td>' +
            '</tr>';
    }).join('');
}

// Sync ctrl dates with ref dates
function syncCtrlDates() {
    const refSel = document.getElementById('ref-date');
    const ctrlSel = document.getElementById('ctrl-date');
    if (!refSel || !ctrlSel) return;
    ctrlSel.innerHTML = refSel.innerHTML;
    ctrlSel.value = refSel.value;
}

// Override loadDates to also sync
const origLoadDates = loadDates;
loadDates = async function() {
    const result = await origLoadDates();
    syncCtrlDates();
    return result;
};

// ─── EXCEL IMPORT/EXPORT ──────────────────────────────────

async function importExcel(input) {
    if (!input.files.length) return;
    const file = input.files[0];
    const dateVal = document.getElementById('ref-date').value;
    const fd = new FormData();
    fd.append('file', file);
    const res = await fetch('/api/v1/nl/reference/import?org_id=' + ORG_ID, {
        method: 'POST',
        body: file,
        headers: {'X-Filename': file.name, 'X-Target-Date': dateVal}
    });
    if (res.ok) {
        const data = await res.json();
        alert('Импортировано: ' + data.imported + ' строк');
        loadRefData();
    } else {
        const err = await res.json();
        alert('Ошибка: ' + err.detail);
    }
    input.value = '';
}

function exportExcel() {
    // Генерируем CSV шаблон с текущими товарами
    const rows = document.querySelectorAll('#ref-body tr');
    rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length < 3) return;
        const nmId = cells[1].textContent.trim();
        const vc = cells[2].textContent.trim();
        const name = cells[3].textContent.trim();
        const dateVal = document.getElementById('ref-date').value;
        const inputs = row.querySelectorAll('input');
        let costs = [];
        inputs.forEach(inp => {
            if (inp.dataset.field === 'notes') costs.push(inp.value);
            else if (inp.type === 'number') costs.push(inp.value);
        });
        if (nmId) csv += nmId + ';' + vc + ';' + name + ';' + dateVal + ';' + costs.join(';') + String.fromCharCode(10);
    });
    const blob = new Blob(['\ufeff' + csv], {type: 'text/csv;charset=utf-8'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'reference_template.csv'; a.click();
    URL.revokeObjectURL(url);
}

// Auto-login
try {
    if (TOKEN && ORG_ID) {
        fetch('/api/v1/nl/me?token=' + TOKEN).then(r => {
            if (r.ok) return r.json(); throw '';
        }).then(d => {
            document.getElementById('user-email').textContent = d.email;
            showApp();
            loadOrgs();
            loadWbKeys();
        }).catch(() => {
            TOKEN = null; ORG_ID = null;
            localStorage.removeItem('nl_token');
            localStorage.removeItem('nl_org_id');
        });
    }
} catch(e) { console.error('Auto-login error:', e); }

// === Сортировка таблиц ===
function parseVal(v) {
    if (v == null || v === '—' || v === '') return null;
    var s = String(v).replace(/[\s₽%]/g,'').replace(',','.');
    var n = parseFloat(s);
    return isNaN(n) ? v : n;
}

function addSorting(tableId) {
    var table = document.getElementById(tableId);
    if (!table) return;
    var headers = table.querySelectorAll('thead th');
    headers.forEach(function(th, ci) {
        th.classList.add('sortable');
        th.addEventListener('click', function() {
            var tbody = table.querySelector('tbody');
            var rows = Array.from(tbody.querySelectorAll('tr'));
            if (!rows.length) return;
            // Check if first row is empty message
            if (rows[0].querySelector('.empty')) return;
            var asc = th.classList.contains('asc') ? false : (th.classList.contains('desc') ? true : true);
            headers.forEach(h => h.classList.remove('asc','desc'));
            th.classList.add(asc ? 'asc' : 'desc');
            rows.sort(function(a, b) {
                var va = parseVal(a.children[ci] ? a.children[ci].textContent.trim() : '');
                var vb = parseVal(b.children[ci] ? b.children[ci].textContent.trim() : '');
                if (va == null && vb == null) return 0;
                if (va == null) return 1;
                if (vb == null) return -1;
                if (typeof va === 'number' && typeof vb === 'number') return asc ? va - vb : vb - va;
                return asc ? String(va).localeCompare(String(vb),'ru') : String(vb).localeCompare(String(va),'ru');
            });
            rows.forEach(r => tbody.appendChild(r));
        });
    });
}

function initSorting() {
    // Stats products table
    var sp = document.getElementById('stats-products');
    if (sp && sp.closest('table')) addSorting(sp.closest('table').id || undefined);
    // All tables with thead
    document.querySelectorAll('table').forEach(function(t) {
        if (t.querySelector('thead') && t.id) addSorting(t.id);
    });
}

// Call initSorting after DOM is ready (deferred)
setTimeout(initSorting, 500);

// === UNIT ECONOMICS ===
let ueData = [];

async function loadUnitEcon() {
    if (!ORG_ID) return;
    const search = document.getElementById('ue-search')?.value || document.getElementById('ue-article')?.value || '';
    try {
        const res = await fetch('/api/v1/nl/unit-economics?org_id=' + ORG_ID + (search ? '&search=' + encodeURIComponent(search) : ''));
        if (!res.ok) return;
        const data = await res.json();
        ueData = data.items || [];
        document.getElementById('ue-count').textContent = ueData.length + ' товаров';

        const tbody = document.getElementById('ue-body');
        tbody.innerHTML = ueData.map((p, idx) => {
            const thumb = p.photo || '';
            const profitClass = p.profit_fact > 0 ? 'color:#00b894' : p.profit_fact < 0 ? 'color:#d63031' : '';
            const profitPlanClass = p.profit_plan > 0 ? 'color:#00b894' : p.profit_plan < 0 ? 'color:#d63031' : '';
            const fmt = (v) => v != null ? (typeof v === 'number' ? v.toFixed(2) : v) : '—';
            const fmtI = (v) => v != null ? (typeof v === 'number' ? v.toFixed(0) : v) : '—';

            return '<tr data-idx="' + idx + '">' +
                '<td style="position:sticky;left:0;z-index:1;background:#fff">' + (thumb ? '<img src="' + thumb + '" style="width:32px;height:32px;border-radius:4px;object-fit:cover">' : '') + '</td>' +
                '<td style="position:sticky;left:40px;z-index:1;background:#fff;font-weight:600">' + p.nm_id + '</td>' +
                '<td>' + esc(p.vendor_code || '') + '</td>' +
                '<td style="max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + esc(p.product_name || '') + '">' + esc(p.product_name || '') + '</td>' +
                '<td>' + esc(p.product_class || '') + '</td>' +
                '<td>' + esc(p.brand || '') + '</td>' +
                '<td>' + fmt(p.cost_price) + '</td>' +
                '<td><input type="number" class="ue-input" data-field="extra_costs" value="' + (p.extra_costs || '') + '" style="width:60px" placeholder="0"></td>' +
                '<td style="background:#f0f0ff">' + fmtI(p.mp_base_pct) + '</td>' +
                '<td style="background:#f0f0ff"><input type="number" class="ue-input" data-field="mp_correction_pct" value="' + (p.mp_correction_pct || '') + '" style="width:50px" step="0.1" placeholder="0"></td>' +
                '<td style="background:#f0f0ff;font-weight:600">' + fmtI(p.mp_total_pct) + '</td>' +
                '<td style="background:#f0f0ff"><input type="number" class="ue-input" data-field="buyout_niche_pct" value="' + (p.buyout_niche_pct || '') + '" style="width:50px" placeholder="0"></td>' +
                '<td>' + fmtI(p.buyout_fact_pct) + '</td>' +
                '<td style="background:#fff3f0">' + fmt(p.logistics_tariff) + '</td>' +
                '<td style="background:#fff3f0">' + fmt(p.logistics_actual) + '</td>' +
                '<td style="background:#fff3f0">' + fmt(p.storage_tariff) + '</td>' +
                '<td style="background:#fff3f0">' + fmt(p.storage_actual) + '</td>' +
                '<td>' + fmt(p.price_with_spp * 0.015) + '</td>' +
                '<td>' + fmt(p.acceptance_avg) + '</td>' +
                '<td>' + (p.tax_rate || '—') + '</td>' +
                '<td>' + (p.vat_rate || '—') + '</td>' +
                '<td>' + fmt(p.tax_total) + '</td>' +
                '<td style="background:#f0fff0">' + fmt(p.ad_fact_rub) + '</td>' +
                '<td style="background:#f0fff0"><input type="number" class="ue-input" data-field="ad_plan_rub" value="' + (p.ad_plan_rub || '') + '" style="width:60px" placeholder="0"></td>' +
                '<td style="background:#e8f4fd">' + fmt(p.price_before_spp) + '</td>' +
                '<td style="background:#e8f4fd">' + fmtI(p.spp_pct) + '</td>' +
                '<td style="background:#e8f4fd;font-weight:600">' + fmt(p.price_with_spp) + '</td>' +
                '<td style="background:#fff8e0"><input type="number" class="ue-input" data-field="wb_club_discount_pct" value="' + (p.wb_club_discount_pct || '') + '" style="width:50px" placeholder="0"></td>' +
                '<td style="background:#ffe8e8">' + fmt(p.expenses_fact) + '</td>' +
                '<td style="background:#ffe8e8;font-weight:600;' + profitClass + '">' + fmt(p.profit_fact) + '</td>' +
                '<td style="background:#ffe8e8">' + fmtI(p.margin_fact) + '</td>' +
                '<td style="background:#ffe8e8">' + fmtI(p.roi_fact) + '</td>' +
                '<td style="background:#ffe8e8">' + fmt(p.to_account_fact) + '</td>' +
                '<td style="background:#e8f0ff"><input type="number" class="ue-input" data-field="price_before_spp_plan" value="' + (p.price_before_spp_plan || '') + '" style="width:70px" placeholder="0"></td>' +
                '<td style="background:#e8f0ff">' + fmt(p.expenses_plan) + '</td>' +
                '<td style="background:#e8f0ff;font-weight:600;' + profitPlanClass + '">' + fmt(p.profit_plan) + '</td>' +
                '<td style="background:#e8f0ff">' + fmtI(p.margin_plan) + '</td>' +
                '<td style="background:#e8f0ff">' + fmtI(p.roi_plan) + '</td>' +
                '<td style="background:#e8f0ff">' + fmt(p.to_account_plan) + '</td>' +
                '<td style="background:#f0e8ff"><input type="date" class="ue-input" data-field="change_date" value="' + (p.change_date || '') + '" style="width:100px;font-size:.9em"></td>' +
                '<td style="background:#f0e8ff"><input type="number" class="ue-input" data-field="price_before_spp_change" value="' + (p.price_before_spp_change || '') + '" style="width:70px" placeholder="0"></td>' +
                '<td style="background:#f0e8ff;font-weight:600">' + fmt(p.profit_change) + '</td>' +
                '<td style="background:#f0e8ff">' + fmtI(p.roi_change) + '</td>' +
                '<td><select class="ue-input" data-field="tariff_type" style="width:70px;font-size:.9em"><option value="box"' + (p.tariff_type==='box'?' selected':'') + '>Короб</option><option value="pallet"' + (p.tariff_type==='pallet'?' selected':'') + '>Палета</option></select></td>' +
                '</tr>';
        }).join('');

        // Summary
        let totalProfit = 0, totalCost = 0, posCount = 0;
        ueData.forEach(p => {
            totalProfit += (p.profit_fact || 0);
            totalCost += (p.cost_price || 0);
            if (p.profit_fact > 0) posCount++;
        });
        document.getElementById('ue-summary').innerHTML =
            '<span>📊 Товаров: <strong>' + ueData.length + '</strong></span>' +
            '<span>💰 Прибыльных: <strong>' + posCount + '/' + ueData.length + '</strong></span>' +
            '<span>📈 Сумма прибыли: <strong>' + totalProfit.toFixed(0) + ' ₽</strong></span>' +
            '<span>📐 Ср. ROI: <strong>' + (totalCost ? (totalProfit / totalCost * 100).toFixed(1) : 0) + '%</strong></span>';
    } catch(e) { console.error('loadUnitEcon', e); }
}

async function saveAllUnitEcon() {
    const rows = document.querySelectorAll('#ue-body tr[data-idx]');
    let saved = 0, errors = 0;
    for (const row of rows) {
        const idx = parseInt(row.dataset.idx);
        const p = ueData[idx];
        if (!p) continue;
        const inputs = row.querySelectorAll('.ue-input');
        const data = { nm_id: p.nm_id, barcode: p.barcode || null, entity_id: p.entity_id || null };
        inputs.forEach(inp => { data[inp.dataset.field] = inp.type === 'number' ? parseFloat(inp.value) || 0 : inp.value; });
        try {
            const res = await fetch('/api/v1/nl/unit-economics?org_id=' + ORG_ID, {
                method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data)
            });
            if (res.ok) saved++; else errors++;
        } catch(e) { errors++; }
    }
    alert('Сохранено: ' + saved + (errors ? ', ошибок: ' + errors : ''));
    loadUnitEcon();
}

function exportUnitEcon() {
    if (!ueData.length) return;
    let csv = 'Арт WB;Арт продавца;Название;Класс;Бренд;Себестоимость;Доп затраты;Баз % МП;Корр % МП;Итог % МП;% выкупа ниши;% выкупа факт;Лог тариф;Лог факт;Хран тариф;Хран факт;Эквайринг;Приёмка;Налог %;НДС %;Налог руб;Рекл факт;Рекл план;Цена до СПП;СПП %;Цена с СПП;Скидка ВБ Клуб %;Расходы;Прибыль;Маржа %;ROI %;На Р/С;Цена план;Расходы план;Прибыль план;Маржа план %;ROI план %;На Р/С план;Дата правок;Цена к изм;Прибыль изм;ROI изм;Тариф тип';
    csv += String.fromCharCode(10);
    ueData.forEach(p => {
        csv += [p.nm_id, p.vendor_code, p.product_name, p.product_class, p.brand,
            p.cost_price, p.extra_costs, p.mp_base_pct, p.mp_correction_pct, p.mp_total_pct,
            p.buyout_niche_pct, p.buyout_fact_pct, p.logistics_tariff, p.logistics_actual,
            p.storage_tariff, p.storage_actual, (p.price_with_spp*0.015).toFixed(2), p.acceptance_avg,
            p.tax_rate, p.vat_rate, p.tax_total, p.ad_fact_rub, p.ad_plan_rub,
            p.price_before_spp, p.spp_pct, p.price_with_spp, p.wb_club_discount_pct,
            p.expenses_fact, p.profit_fact, p.margin_fact, p.roi_fact, p.to_account_fact,
            p.price_before_spp_plan, p.expenses_plan, p.profit_plan, p.margin_plan, p.roi_plan, p.to_account_plan,
            p.change_date, p.price_before_spp_change, p.profit_change, p.roi_change, p.tariff_type
        ].join(';') + String.fromCharCode(10);
    });
    const blob = new Blob(['﻿' + csv], {type: 'text/csv;charset=utf-8'});
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a'); a.href = url; a.download = 'unit_economics.csv'; a.click();
    URL.revokeObjectURL(url);
}

</script>
</body>
</html>
"""
    response = HTMLResponse(html)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    return response
