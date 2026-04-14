"""API для справочного листа и фронтенд НЛ"""
from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, insert, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from pydantic import BaseModel
from typing import Optional
from datetime import date

from core.database import get_db
from core.dependencies import get_current_user
from models.user import User
from models.reference import ReferenceSheet
from models.raw_data import TechStatus, RawBarcode

router = APIRouter(tags=["nl"])


# ─── API ENDPOINTS ─────────────────────────────────────────

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


@router.get("/api/v1/nl/reference")
async def get_reference(org_id: str, db: AsyncSession = Depends(get_db)):
    """Получить справочный лист организации"""
    result = await db.execute(
        select(ReferenceSheet).where(ReferenceSheet.organization_id == org_id)
    )
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
    """Сохранить/обновить строку справочного листа"""
    ins = pg_insert(ReferenceSheet).values(
        organization_id=org_id,
        nm_id=item.nm_id,
        vendor_code=item.vendor_code,
        product_name=item.product_name,
        cost_price=item.cost_price,
        purchase_price=item.purchase_price,
        packaging_cost=item.packaging_cost,
        logistics_cost=item.logistics_cost,
        other_costs=item.other_costs,
        notes=item.notes,
    )
    stmt = ins.on_conflict_do_update(
        constraint="reference_sheet_organization_id_nm_id_key",
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
    await db.commit()
    return {"status": "ok"}


# ─── FRONTEND ──────────────────────────────────────────────

@router.get("/nl", response_class=HTMLResponse)
async def nl_page(token: str = Query(""), org_id: str = Query(""), db: AsyncSession = Depends(get_db)):
    """НЛ — главная страница для пользователей"""
    html = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>НЛ — Аналитика</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f7fa; color: #1a1a2e; }

/* Header */
.header { background: #fff; border-bottom: 1px solid #e0e0e0; padding: 12px 20px; display: flex; align-items: center; gap: 20px; }
.header h1 { font-size: 1.3em; color: #6c5ce7; }
.header .logo { font-size: 1.5em; }

/* Tabs */
.tabs { display: flex; background: #fff; border-bottom: 2px solid #e0e0e0; padding: 0 20px; }
.tab { padding: 12px 24px; cursor: pointer; color: #666; font-weight: 500; border-bottom: 3px solid transparent; transition: all 0.2s; }
.tab:hover { color: #6c5ce7; }
.tab.active { color: #6c5ce7; border-bottom-color: #6c5ce7; }

/* Content */
.content { padding: 20px; }
.tab-content { display: none; }
.tab-content.active { display: block; }

/* Table */
table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }
th { background: #f8f9fa; padding: 10px 8px; text-align: left; font-size: 0.8em; color: #666; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 2px solid #e0e0e0; }
td { padding: 8px; border-bottom: 1px solid #f0f0f0; font-size: 0.85em; }
tr:hover { background: #f8f9ff; }
.r { text-align: right; }
input[type="number"], input[type="text"] { width: 100%; border: 1px solid #e0e0e0; border-radius: 4px; padding: 4px 6px; font-size: 0.85em; }
input:focus { outline: none; border-color: #6c5ce7; box-shadow: 0 0 0 2px rgba(108,92,231,0.15); }
.save-btn { background: #6c5ce7; color: #fff; border: none; padding: 4px 10px; border-radius: 4px; cursor: pointer; font-size: 0.8em; }
.save-btn:hover { background: #5a4bd1; }
.save-btn.saved { background: #00b894; }
.empty { text-align: center; padding: 40px; color: #999; }
.loading { text-align: center; padding: 40px; color: #999; }
.photo { width: 36px; height: 36px; border-radius: 4px; object-fit: cover; }
</style>
</head>
<body>

<div class="header">
<span class="logo">📊</span>
<h1>НЛ — Аналитика</h1>
</div>

<div class="tabs">
<div class="tab active" onclick="switchTab('reference')">📋 Справочный лист</div>
<div class="tab" onclick="switchTab('control')">📈 Оперативный контроль</div>
</div>

<div class="content">

<!-- Справочный лист -->
<div id="tab-reference" class="tab-content active">
<table id="ref-table">
<thead>
<tr>
<th>Фото</th>
<th>Арт WB</th>
<th>Арт пост.</th>
<th>Название</th>
<th>Себестоимость</th>
<th>Закуп. цена</th>
<th>Упаковка</th>
<th>Логистика</th>
<th>Прочее</th>
<th>Заметки</th>
<th></th>
</tr>
</thead>
<tbody id="ref-body">
<tr><td colspan="11" class="loading">Загрузка...</td></tr>
</tbody>
</table>
</div>

<!-- Оперативный контроль -->
<div id="tab-control" class="tab-content">
<div class="empty">📈 Оперативный контроль — в разработке</div>
</div>

</div>

<script>
const ORG_ID = '""" + org_id + """';

function switchTab(name) {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    event.target.classList.add('active');
    document.getElementById('tab-' + name).classList.add('active');
}

async function loadData() {
    // Загрузить продукты из ТС
    const res = await fetch('/api/v1/nl/products?org_id=' + ORG_ID);
    const products = await res.json();

    // Загрузить справочный лист
    const refRes = await fetch('/api/v1/nl/reference?org_id=' + ORG_ID);
    const refData = await refRes.json();

    const refMap = {};
    refData.forEach(r => refMap[r.nm_id] = r);

    const tbody = document.getElementById('ref-body');
    tbody.innerHTML = '';

    if (!products.length) {
        tbody.innerHTML = '<tr><td colspan="11" class="empty">Нет товаров</td></tr>';
        return;
    }

    products.forEach(p => {
        const ref = refMap[p.nm_id] || {};
        const thumb = (p.photo_main || '').replace('/big/', '/c246x328/');
        const img = thumb ? `<img class="photo" src="${thumb}" loading="lazy">` : '📦';
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${img}</td>
            <td><b>${p.nm_id}</b></td>
            <td>${p.vendor_code || ''}</td>
            <td>${(p.product_name || '').substring(0, 30)}</td>
            <td><input type="number" data-nm="${p.nm_id}" data-field="cost_price" value="${ref.cost_price || ''}" step="0.01"></td>
            <td><input type="number" data-nm="${p.nm_id}" data-field="purchase_price" value="${ref.purchase_price || ''}" step="0.01"></td>
            <td><input type="number" data-nm="${p.nm_id}" data-field="packaging_cost" value="${ref.packaging_cost || ''}" step="0.01"></td>
            <td><input type="number" data-nm="${p.nm_id}" data-field="logistics_cost" value="${ref.logistics_cost || ''}" step="0.01"></td>
            <td><input type="number" data-nm="${p.nm_id}" data-field="other_costs" value="${ref.other_costs || ''}" step="0.01"></td>
            <td><input type="text" data-nm="${p.nm_id}" data-field="notes" value="${ref.notes || ''}" style="width:120px"></td>
            <td><button class="save-btn" onclick="saveRow(this, ${p.nm_id}, '${p.vendor_code || ''}', '${(p.product_name || '').replace(/'/g, "\\\\'")}')">💾</button></td>
        `;
        tbody.appendChild(tr);
    });
}

async function saveRow(btn, nmId, vc, name) {
    const row = btn.closest('tr');
    const inputs = row.querySelectorAll('input');
    const data = { nm_id: nmId, vendor_code: vc, product_name: name };
    inputs.forEach(inp => {
        const field = inp.dataset.field;
        if (field) data[field] = inp.type === 'number' ? (parseFloat(inp.value) || null) : inp.value;
    });

    const res = await fetch('/api/v1/nl/reference?org_id=' + ORG_ID, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
    });

    if (res.ok) {
        btn.textContent = '✅';
        btn.classList.add('saved');
        setTimeout(() => { btn.textContent = '💾'; btn.classList.remove('saved'); }, 1500);
    }
}

loadData();
</script>
</body>
</html>
"""
    return HTMLResponse(html)


@router.get("/api/v1/nl/products")
async def get_products(org_id: str, db: AsyncSession = Depends(get_db)):
    """Получить список уникальных карточек из ТС"""
    from sqlalchemy import distinct
    result = await db.execute(
        select(
            TechStatus.nm_id,
            TechStatus.vendor_code,
            TechStatus.product_name,
            TechStatus.photo_main,
        )
        .where(TechStatus.organization_id == org_id)
        .where(TechStatus.nm_id.isnot(None))
        .distinct()
    )
    rows = result.all()
    return [{
        "nm_id": r[0],
        "vendor_code": r[1],
        "product_name": r[2],
        "photo_main": r[3],
    } for r in rows]
