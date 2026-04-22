"""API для справочного листа, авторизации и фронтенд НЛ"""
from fastapi import APIRouter, Depends, Query, Request, HTTPException, status
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, date

from core.database import get_db
from core.security import verify_password, get_password_hash, create_access_token, decode_token
from core.dependencies import get_current_user
from models.user import User
from models.reference import ReferenceSheet
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
    """Справочный лист на дату"""
    from datetime import datetime as dt
    q = select(ReferenceSheet).where(ReferenceSheet.organization_id == org_id)
    if target_date:
        q = q.where(ReferenceSheet.target_date == dt.strptime(target_date, "%Y-%m-%d").date())
    result = await db.execute(q)
    items = result.scalars().all()
    return [{
        "nm_id": i.nm_id,
        "vendor_code": i.vendor_code,
        "product_name": i.product_name,
        "target_date": str(i.target_date),
        "cost_price": float(i.cost_price) if i.cost_price else None,
        "purchase_price": float(i.purchase_price) if i.purchase_price else None,
        "packaging_cost": float(i.packaging_cost) if i.packaging_cost else None,
        "logistics_cost": float(i.logistics_cost) if i.logistics_cost else None,
        "other_costs": float(i.other_costs) if i.other_costs else None,
        "notes": i.notes,
    } for i in items]


@router.post("/api/v1/nl/reference")
async def save_reference(item: RefItem, org_id: str, db: AsyncSession = Depends(get_db)):
    """Сохранить строку справочного листа"""
    from datetime import datetime as dt_mod
    t_date = dt_mod.strptime(item.target_date, "%Y-%m-%d").date() if item.target_date else date.today()
    ins = pg_insert(ReferenceSheet).values(
        organization_id=org_id, nm_id=item.nm_id, vendor_code=item.vendor_code,
        product_name=item.product_name, target_date=t_date,
        cost_price=item.cost_price,
        purchase_price=item.purchase_price, packaging_cost=item.packaging_cost,
        logistics_cost=item.logistics_cost, other_costs=item.other_costs, notes=item.notes,
    )
    stmt = ins.on_conflict_do_update(
        constraint="reference_sheet_org_nm_date_key",
        set_={
            "vendor_code": ins.excluded.vendor_code, "product_name": ins.excluded.product_name,
            "cost_price": ins.excluded.cost_price, "purchase_price": ins.excluded.purchase_price,
            "packaging_cost": ins.excluded.packaging_cost, "logistics_cost": ins.excluded.logistics_cost,
            "other_costs": ins.excluded.other_costs, "notes": ins.excluded.notes,
            "updated_at": date.today(),
        }
    )
    await db.execute(stmt)
    await db.commit()
    return {"status": "ok"}


@router.get("/api/v1/nl/products")
async def get_products(org_id: str, target_date: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    """Список уникальных карточек из ТС на дату"""
    from datetime import datetime as dt_mod
    q = select(
        TechStatus.nm_id, TechStatus.vendor_code, TechStatus.product_name,
        TechStatus.photo_main, TechStatus.barcode, TechStatus.sku
    ).where(TechStatus.organization_id == org_id, TechStatus.nm_id.isnot(None))
    if target_date:
        q = q.where(TechStatus.target_date == dt_mod.strptime(target_date, "%Y-%m-%d").date())
    q = q.distinct()
    result = await db.execute(q)
    return [{"nm_id": r[0], "vendor_code": r[1], "product_name": r[2], "photo_main": r[3], "barcode": r[4], "sku": r[5]} for r in result.all()]




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
    from models.organization import WbApiKey
    name = data.get("name", "WB Key")
    api_key = data.get("api_key", "")
    if not api_key:
        raise HTTPException(400, "API ключ обязателен")
    encrypted = encrypt_data(api_key)
    key = WbApiKey(organization_id=org_id, name=name, api_key=encrypted)
    db.add(key)
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

    # Детализация по товарам
    products_detail = await db.execute(
        select(
            TechStatus.nm_id, TechStatus.vendor_code, TechStatus.product_name,
            TechStatus.photo_main, TechStatus.stock_qty, TechStatus.orders_count,
            TechStatus.buyouts_count, TechStatus.returns_count, TechStatus.rating,
            TechStatus.impressions, TechStatus.clicks, TechStatus.ad_cost,
            TechStatus.price, TechStatus.price_discount, TechStatus.tariff,
        ).where(TechStatus.organization_id == org_id, TechStatus.target_date == d)
        .order_by(TechStatus.orders_count.desc().nullslast())
    )

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
            "avg_rating": round(float(row.avg_rating), 2) if row.avg_rating else None,
            "zero_stock_count": safe_int(zero_stock.scalar()) or 0,
            "low_stock_count": safe_int(low_stock.scalar()) or 0,
            "low_rating_count": safe_int(low_rating.scalar()) or 0,
        },
        "products": [{
            "nm_id": r[0],
            "vendor_code": r[1],
            "product_name": r[2],
            "photo_main": r[3],
            "stock_qty": safe_int(r[4]),
            "orders_count": safe_int(r[5]),
            "buyouts_count": safe_int(r[6]),
            "returns_count": safe_int(r[7]),
            "rating": safe_float(r[8]),
            "impressions": safe_int(r[9]),
            "clicks": safe_int(r[10]),
            "ad_cost": safe_float(r[11]),
            "price": safe_float(r[12]),
            "price_discount": safe_float(r[13]),
            "tariff": safe_float(r[14]),
        } for r in products_detail.all()]
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

            ins = pg_insert(ReferenceSheet).values(
                organization_id=org_id, nm_id=nm_id, vendor_code=vendor_code,
                product_name=product_name, target_date=t_date,
                cost_price=cost_price, purchase_price=purchase_price,
                packaging_cost=packaging_cost, logistics_cost=logistics_cost,
                other_costs=other_costs, notes=notes,
            )
            stmt = ins.on_conflict_do_update(
                constraint="reference_sheet_org_nm_date_key",
                set_={
                    "vendor_code": ins.excluded.vendor_code,
                    "product_name": ins.excluded.product_name,
                    "cost_price": ins.excluded.cost_price,
                    "purchase_price": ins.excluded.purchase_price,
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
<div style="text-align:center;margin-top:16px;font-size:.85em"><a href="/nl/register" style="color:#6c5ce7;text-decoration:none">Нет аккаунта? Зарегистрироваться</a></div>
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
<a class="nav-item" onclick="navTo('costprice',this)"><span class="icon">💰</span>Себестоимость</a>
<a class="nav-item" onclick="navTo('warehouses',this)"><span class="icon">📦</span>Склады</a>
<a class="nav-item" onclick="navTo('opexpenses',this)"><span class="icon">📝</span>Опер. расходы</a>
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
<div class="filters">
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
<table>
<thead><tr><th>Фото</th><th>Арт WB</th><th>Название</th><th>Остаток</th><th>Заказы</th><th>Выкупы</th><th>Возвраты</th><th>Рейтинг</th><th>Показы</th><th>Клики</th><th>CTR</th><th>Реклама ₽</th><th>Цена</th></tr></thead>
<tbody id="stats-products"><tr><td colspan="13" class="empty">Выберите дату</td></tr></tbody>
</table>
</div>
<div id="page-analytics" class="page-section">
<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap">
<div style="display:flex;align-items:center;gap:6px">
<label style="font-size:.85em;color:#666">📅 Дата:</label>
<select id="ctrl-date" onchange="loadControl()" style="border:1px solid #e0e0e0;border-radius:4px;padding:6px 10px;font-size:.9em;cursor:pointer"></select>
</div>
<button class="btn" onclick="loadControl()" style="padding:6px 14px;font-size:.85em">🔄 Обновить</button>
</div>
<div id="ctrl-alerts" style="margin-bottom:16px"></div>
<div id="ctrl-cards" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:20px"></div>
<table id="ctrl-table" style="width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.08)">
<thead><tr>
<th>Фото</th><th>Арт WB</th><th>Название</th><th>Остаток</th><th>Заказы</th><th>Выкупы</th><th>Возвраты</th><th>Рейтинг</th><th>Показы</th><th>Клики</th><th>CTR</th><th>Реклама ₽</th><th>Цена</th><th>Скидка</th><th>Комиссия</th>
</tr></thead>
<tbody id="ctrl-body"><tr><td colspan="15" class="empty">Выберите дату</td></tr></tbody>
</table>
</div>

<div id="page-settings" class="page-section">
<h3 style="margin-bottom:12px;color:#6c5ce7">🔑 WB API ключи</h3>
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
        if (ORG_ID) { await loadDates(); loadStats(); }
    } catch(e) { console.error('loadDates error:', e); }
}

async function loadStats() {
    if (!ORG_ID) return;
    const sel = document.getElementById('stats-date') || document.getElementById('ref-date');
    const dateVal = sel ? sel.value : '';
    if (!dateVal || dateVal === 'Нет данных') return;
    try {
        const res = await fetch('/api/v1/nl/control?org_id=' + ORG_ID + '&target_date=' + dateVal);
        if (!res.ok) return;
        const data = await res.json();
        const s = data.summary || {};
        // Заполняем карточки
        const fmt = (v, suffix) => { if (v == null) return '—'; return Number(v).toLocaleString('ru-RU', {maximumFractionDigits:2}) + (suffix || ''); };
        document.getElementById('v-profit').textContent = fmt(s.total_buyouts, ' ₽');
        document.getElementById('v-sold').textContent = fmt(s.total_orders);
        document.getElementById('v-returned').textContent = fmt(s.total_returns);
        document.getElementById('v-stock-total').textContent = fmt(s.total_stock, ' шт');
        document.getElementById('v-ads').textContent = fmt(s.total_ad_cost, ' ₽');
        document.getElementById('v-buyout').textContent = s.total_orders ? (s.total_buyouts / s.total_orders * 100).toFixed(1) + '%' : '—';
        // Алерты
        let alerts = '';
        if (s.zero_stock_count > 0) alerts += '<div class="alert-card red">🔴 Нет в наличии: ' + s.zero_stock_count + ' товаров</div>';
        if (s.low_stock_count > 0) alerts += '<div class="alert-card yellow">🟡 Низкий остаток (≤5): ' + s.low_stock_count + ' товаров</div>';
        document.getElementById('stats-alerts').innerHTML = alerts;
        // Таблица товаров
        const tbody = document.getElementById('stats-products');
        const prods = data.products || [];
        if (!prods.length) { tbody.innerHTML = '<tr><td colspan="13" class="empty">Нет данных</td></tr>'; return; }
        tbody.innerHTML = prods.map(p => {
            const thumb = (p.photo_main || '').replace('/big/', '/c246x328/');
            const ctr = p.impressions > 0 ? (p.clicks / p.impressions * 100).toFixed(1) + '%' : '—';
            return '<tr><td>' + (thumb ? '<img src="' + thumb + '" style="width:36px;height:36px;border-radius:4px;object-fit:cover">' : '') + '</td>' +
            '<td>' + (p.nm_id || '') + '</td><td>' + esc(p.product_name) + '</td>' +
            '<td>' + (p.stock_qty ?? '—') + '</td><td>' + (p.orders_count ?? '—') + '</td>' +
            '<td>' + (p.buyouts_count ?? '—') + '</td><td>' + (p.returns_count ?? '—') + '</td>' +
            '<td>' + (p.rating ?? '—') + '</td><td>' + (p.impressions ?? '—') + '</td>' +
            '<td>' + (p.clicks ?? '—') + '</td><td>' + ctr + '</td>' +
            '<td>' + fmt(p.ad_cost) + '</td><td>' + fmt(p.price) + '</td></tr>';
        }).join('');
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
    document.querySelectorAll('.nav-item').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.page-section').forEach(t => t.classList.remove('active'));
    if (el) el.classList.add('active');
    const page = document.getElementById('page-' + name);
    if (page) page.classList.add('active');
    // Update title
    const titles = {stats:'Основные показатели',rnp:'Рука на пульсе',opiu:'ОПиУ',analytics:'Аналитика по товарам',costprice:'Себестоимость',warehouses:'Склады',opexpenses:'Операционные расходы',connectors:'Подключения',subscription:'Подписка',settings:'Настройки',help:'Помощь'};
    document.getElementById('page-title').textContent = titles[name] || name;
}

async function loadDates() {
    if (!ORG_ID) return [];
    const res = await fetch('/api/v1/nl/dates?org_id=' + ORG_ID);
    if (!res.ok) return [];
    const dates = await res.json();
    const sel = document.getElementById('ref-date');
    sel.innerHTML = '';
    if (!dates.length) { sel.innerHTML = '<option>Нет данных</option>'; return; }
    dates.forEach(d => {
        const opt = document.createElement('option');
        opt.value = d;
        const dt = new Date(d + 'T00:00:00');
        opt.textContent = dt.toLocaleDateString('ru-RU', {day:'numeric', month:'short', year:'numeric'});
        sel.appendChild(opt);
    });
    return dates[0];
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
        const thumb = (p.photo_main || '').replace('/big/', '/c246x328/');
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
        opt.textContent = o.name + (o.wb_keys_count ? ' (' + o.wb_keys_count + '🔑)' : '');
        if (o.id === ORG_ID) opt.selected = true;
        sel.appendChild(opt);
    });
    if (!ORG_ID && orgs.length) {
        ORG_ID = orgs[0].id;
        localStorage.setItem('nl_org_id', ORG_ID);
        sel.value = ORG_ID;
    }
}

function switchOrg() {
    ORG_ID = document.getElementById('org-select').value;
    localStorage.setItem('nl_org_id', ORG_ID);
    loadRefData();
    loadWbKeys();
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
        const thumb = (p.photo_main || '').replace('/big/', '/c246x328/');
        const img = thumb ? '<img class="photo" src="' + thumb + '" loading="lazy">' : '📦';
        const ctr = p.impressions > 0 ? (p.clicks / p.impressions * 100).toFixed(1) + '%' : '—';
        const stockColor = p.stock_qty <= 0 ? '#e74c3c' : p.stock_qty <= 5 ? '#e17055' : '#00b894';
        return '<tr>' +
            '<td>' + img + '</td>' +
            '<td><b>' + p.nm_id + '</b></td>' +
            '<td>' + (p.product_name || '').substring(0, 25) + '</td>' +
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
    let csv = 'Арт WB;Арт поставщика;Название;Дата;Себестоимость;Закупочная;Упаковка;Логистика;Прочее;Заметки\\n';
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
        if (nmId) csv += nmId + ';' + vc + ';' + name + ';' + dateVal + ';' + costs.join(';') + '\\n';
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
</script>
</body>
</html>
"""
    response = HTMLResponse(html)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    return response
