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
async def get_reference(org_id: str, db: AsyncSession = Depends(get_db)):
    """Справочный лист"""
    result = await db.execute(select(ReferenceSheet).where(ReferenceSheet.organization_id == org_id))
    items = result.scalars().all()
    return [{
        "nm_id": i.nm_id,
        "vendor_code": i.vendor_code,
        "product_name": i.product_name,
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
    ins = pg_insert(ReferenceSheet).values(
        organization_id=org_id, nm_id=item.nm_id, vendor_code=item.vendor_code,
        product_name=item.product_name, cost_price=item.cost_price,
        purchase_price=item.purchase_price, packaging_cost=item.packaging_cost,
        logistics_cost=item.logistics_cost, other_costs=item.other_costs, notes=item.notes,
    )
    stmt = ins.on_conflict_do_update(
        constraint="reference_sheet_organization_id_nm_id_key",
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
async def get_products(org_id: str, db: AsyncSession = Depends(get_db)):
    """Список уникальных карточек из ТС"""
    result = await db.execute(
        select(TechStatus.nm_id, TechStatus.vendor_code, TechStatus.product_name, TechStatus.photo_main)
        .where(TechStatus.organization_id == org_id, TechStatus.nm_id.isnot(None))
        .distinct()
    )
    return [{"nm_id": r[0], "vendor_code": r[1], "product_name": r[2], "photo_main": r[3]} for r in result.all()]


# ─── FRONTEND ──────────────────────────────────────────────

@router.get("/nl", response_class=HTMLResponse)
async def nl_page():
    """НЛ — главная страница"""
    html = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>НЛ — Аналитика</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f7fa;color:#1a1a2e}

.header{background:#fff;border-bottom:1px solid #e0e0e0;padding:12px 20px;display:flex;align-items:center;gap:12px}
.header h1{font-size:1.2em;color:#6c5ce7}
.header .logo{font-size:1.4em}
.header .logout{margin-left:auto;color:#999;cursor:pointer;font-size:0.85em}
.header .logout:hover{color:#e74c3c}
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
.auth-error{color:#e74c3c;font-size:.85em;margin-bottom:10px}
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
<div class="toggle" onclick="toggleAuth()">Нет аккаунта? Зарегистрироваться</div>
</div>

<div id="auth-register" style="display:none">
<h2>📝 Регистрация</h2>
<p>Создайте аккаунт для доступа к аналитике</p>
<div id="reg-error" class="auth-error" style="display:none"></div>
<div class="field"><label>Email</label><input type="email" id="reg-email"></div>
<div class="field"><label>Пароль</label><input type="password" id="reg-password"></div>
<div class="field"><label>Название организации</label><input type="text" id="reg-org" value="Моя организация"></div>
<button class="btn" onclick="doRegister()" style="width:100%">Зарегистрироваться</button>
<div class="toggle" onclick="toggleAuth()">Уже есть аккаунт? Войти</div>
</div>
</div>
</div>

<!-- Main app -->
<div id="app-section" style="display:none">
<div class="header">
<span class="logo">📊</span>
<h1>НЛ — Аналитика</h1>
<span class="user-info" id="user-email"></span>
<span class="logout" onclick="doLogout()">Выйти</span>
</div>
<div class="tabs">
<div class="tab active" onclick="switchTab('reference',this)">📋 Справочный лист</div>
<div class="tab" onclick="switchTab('control',this)">📈 Оперативный контроль</div>
</div>
<div class="content">
<div id="tab-reference" class="tab-content active">
<table id="ref-table">
<thead><tr>
<th>Фото</th><th>Арт WB</th><th>Арт пост.</th><th>Название</th>
<th>Себестоимость</th><th>Закуп. цена</th><th>Упаковка</th><th>Логистика</th><th>Прочее</th><th>Заметки</th><th></th>
</tr></thead>
<tbody id="ref-body"><tr><td colspan="11" class="empty">Загрузка...</td></tr></tbody>
</table>
</div>
<div id="tab-control" class="tab-content">
<div class="empty">📈 Оперативный контроль — в разработке</div>
</div>
</div>
</div>

<script>
let TOKEN = localStorage.getItem('nl_token');
let ORG_ID = localStorage.getItem('nl_org_id');

function toggleAuth() {
    const l = document.getElementById('auth-login');
    const r = document.getElementById('auth-register');
    l.style.display = l.style.display === 'none' ? '' : 'none';
    r.style.display = r.style.display === 'none' ? '' : 'none';
}

function showApp() {
    document.getElementById('auth-section').style.display = 'none';
    document.getElementById('app-section').style.display = '';
    loadRefData();
}

function showAuth() {
    document.getElementById('auth-section').style.display = '';
    document.getElementById('app-section').style.display = 'none';
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
    } catch(e) { err.textContent = e.message; err.style.display = ''; }
}

function doLogout() { showAuth(); }

function switchTab(name, el) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    el.classList.add('active');
    document.getElementById('tab-' + name).classList.add('active');
}

async function loadRefData() {
    const [prodRes, refRes] = await Promise.all([
        fetch('/api/v1/nl/products?org_id=' + ORG_ID),
        fetch('/api/v1/nl/reference?org_id=' + ORG_ID)
    ]);
    const products = await prodRes.json();
    const refData = await refRes.json();
    const refMap = {};
    refData.forEach(r => refMap[r.nm_id] = r);
    const tbody = document.getElementById('ref-body');
    tbody.innerHTML = '';
    if (!products.length) { tbody.innerHTML = '<tr><td colspan="11" class="empty">Нет товаров. Подключите WB API ключ.</td></tr>'; return; }
    products.forEach(p => {
        const ref = refMap[p.nm_id] || {};
        const thumb = (p.photo_main || '').replace('/big/', '/c246x328/');
        const img = thumb ? `<img class="photo" src="${thumb}" loading="lazy">` : '📦';
        const esc = s => (s||'').replace(/'/g, "\\\\'");
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${img}</td>
            <td><b>${p.nm_id}</b></td>
            <td>${p.vendor_code||''}</td>
            <td>${(p.product_name||'').substring(0,30)}</td>
            <td><input type="number" data-field="cost_price" value="${ref.cost_price||''}" step="0.01"></td>
            <td><input type="number" data-field="purchase_price" value="${ref.purchase_price||''}" step="0.01"></td>
            <td><input type="number" data-field="packaging_cost" value="${ref.packaging_cost||''}" step="0.01"></td>
            <td><input type="number" data-field="logistics_cost" value="${ref.logistics_cost||''}" step="0.01"></td>
            <td><input type="number" data-field="other_costs" value="${ref.other_costs||''}" step="0.01"></td>
            <td><input type="text" data-field="notes" value="${esc(ref.notes)}" style="width:120px"></td>
            <td><button class="save-btn" onclick="saveRow(this,${p.nm_id},'${esc(p.vendor_code)}','${esc(p.product_name)}')">💾</button></td>
        `;
        tbody.appendChild(tr);
    });
}

async function saveRow(btn, nmId, vc, name) {
    const row = btn.closest('tr');
    const inputs = row.querySelectorAll('input');
    const data = {nm_id: nmId, vendor_code: vc, product_name: name};
    inputs.forEach(inp => {
        const f = inp.dataset.field;
        if (f) data[f] = inp.type === 'number' ? (parseFloat(inp.value)||null) : inp.value;
    });
    const res = await fetch('/api/v1/nl/reference?org_id=' + ORG_ID, {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data)
    });
    if (res.ok) { btn.textContent='✅'; btn.classList.add('saved'); setTimeout(()=>{btn.textContent='💾';btn.classList.remove('saved');},1500); }
}

// Auto-login
if (TOKEN && ORG_ID) {
    fetch('/api/v1/nl/me?token=' + TOKEN).then(r => {
        if (r.ok) return r.json(); throw '';
    }).then(d => {
        document.getElementById('user-email').textContent = d.email;
        showApp();
    }).catch(() => showAuth());
}
</script>
</body>
</html>
"""
    return HTMLResponse(html)
