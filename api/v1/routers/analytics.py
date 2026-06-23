"""Роутер Аналитики — вынесен из api/v1/nl.py

Эндпоинты:
- GET  /api/v1/nl/analytics           — аналитика по товарам
- GET  /api/v1/nl/warehouses           — остатки на складах WB
- GET  /api/v1/nl/operating-expenses   — операционные расходы (заглушка)
- POST /api/v1/nl/operating-expenses   — добавить расход (заглушка)
- GET  /api/v1/nl/fbo-needs            — расчёт потребности FBO
"""
import math
import decimal
from collections import defaultdict
from datetime import datetime, date
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text

from core.database import get_db
from models.raw_data import TechStatus
from services.reference import resolve_org_id

router = APIRouter()


# ============================================================================
# GET /api/v1/nl/analytics — аналитика по товарам
# ============================================================================
@router.get("/api/v1/nl/analytics")
async def get_analytics(org_id: str, target_date: Optional[str] = None, search: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    org_id = await resolve_org_id(org_id, db)
    """Аналитика по товарам — детальная таблица"""

    d = datetime.strptime(target_date, "%Y-%m-%d").date() if target_date else date.today()

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


# ============================================================================
# GET /api/v1/nl/warehouses — остатки на складах WB
# ============================================================================
@router.get("/api/v1/nl/warehouses")
async def get_warehouse_stock(org_id: str, target_date: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    """Остатки на складах WB"""
    d = datetime.strptime(target_date, "%Y-%m-%d").date() if target_date else date.today()
    result = await db.execute(
        select(TechStatus.nm_id, TechStatus.vendor_code, TechStatus.product_name,
               TechStatus.warehouse_name, TechStatus.stock_qty, TechStatus.barcode)
        .where(TechStatus.organization_id == org_id, TechStatus.target_date == d)
        .order_by(TechStatus.stock_qty.desc().nullslast())
    )
    return [{"nm_id": r[0], "vendor_code": r[1], "product_name": r[2],
             "warehouse": r[3], "qty": int(r[4]) if r[4] else 0, "barcode": r[5]} for r in result.all()]


# ============================================================================
# Операционные расходы (заглушки)
# ============================================================================
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


# ============================================================================
# GET /api/v1/nl/fbo-needs — расчёт потребности FBO
# ============================================================================
@router.get("/api/v1/nl/fbo-needs")
async def get_fbo_needs(org_id: str, days: int = 14, db: AsyncSession = Depends(get_db)):
    """Расчёт потребности FBO: остатки + темп заказов по складам"""

    # 1) Остатки по складам (из tech_status — последний snapshot)
    stocks_result = await db.execute(text("""
        SELECT ts.nm_id, ts.warehouse_name, 0 as warehouse_id,
               ts.stock_qty as qty, ts.stock_qty as qty_full
        FROM tech_status ts
        WHERE ts.organization_id = :org
          AND ts.target_date = (SELECT MAX(target_date) FROM tech_status WHERE organization_id = :org)
          AND ts.entity_id IS NOT NULL
    """), {"org": org_id})
    stocks = stocks_result.all()

    # Маппинг: (nm_id, warehouse_name) -> {qty, qty_full, warehouse_id}
    stock_map = {}
    warehouses = {}  # warehouse_name -> warehouse_id
    for s in stocks:
        key = (s[0], s[1])
        stock_map[key] = {"qty": s[3] or 0, "qty_full": s[4] or 0, "warehouse_id": s[2]}
        if s[1] not in warehouses:
            warehouses[s[1]] = s[2]

    # 2) Темп заказов по складам за N дней (из raw_api_data JSONB)
    orders_result = await db.execute(text("""
        SELECT raw_response FROM raw_api_data
        WHERE organization_id = :org AND api_method = 'orders'
          AND target_date >= CURRENT_DATE - make_interval(days => :days_back)
    """), {"org": org_id, "days_back": days})
    raw_orders = orders_result.all()

    # Парсим JSONB: подсчёт заказов по (nmId, warehouseName)
    order_agg = defaultdict(lambda: {"total_qty": 0, "days": set()})
    for row in raw_orders:
        raw = row[0]
        items = raw if isinstance(raw, list) else []
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("isCancel"):
                continue
            nm = item.get("nmId")
            wh = item.get("warehouseName")
            if not nm or not wh:
                continue
            key = (nm, wh)
            order_agg[key]["total_qty"] += 1
            d = item.get("date", "")[:10]
            if d:
                order_agg[key]["days"].add(d)

    order_map = {}
    for key, v in order_agg.items():
        active = max(len(v["days"]), 1)
        order_map[key] = {"total_qty": v["total_qty"], "rate_per_day": round(v["total_qty"] / active, 2), "active_days": len(v["days"])}

    # 3) Все nm_id из entities (чтобы показать даже без остатков)
    entities_result = await db.execute(text("""
        SELECT pe.id, pe.nm_id, pe.size_name, pe.product_name, pe.photo_main
        FROM product_entities pe
        WHERE pe.organization_id = :org
    """), {"org": org_id})
    entities = entities_result.all()

    # Маппинг nm_id -> entity info
    entity_by_nm = {}
    for e in entities:
        if e[1] not in entity_by_nm:
            entity_by_nm[e[1]] = {"entity_id": str(e[0]), "size_name": e[2], "product_name": e[3], "photo_main": e[4]}

    # 4) Справочник: supply_days, min_batch_fbo по entity_id
    ref_result = await db.execute(text("""
        SELECT entity_id, nm_id, supply_days, min_batch_fbo
        FROM reference_book
        WHERE organization_id = :org AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)
    """), {"org": org_id})
    refs = ref_result.all()

    # entity_id -> supply_days, min_batch_fbo; fallback nm_id
    ref_by_entity = {}
    ref_by_nm = {}
    for r in refs:
        d = {"supply_days": r[2], "min_batch_fbo": r[3]}
        if r[0]:
            ref_by_entity[str(r[0])] = d
        ref_by_nm[r[1]] = d

    # 5) Собираем результат — только комбинации с остатками или заказами
    all_keys = set(stock_map.keys()) | set(order_map.keys())
    rows = []
    for key in all_keys:
        nm_id, wname = key
        if nm_id not in entity_by_nm:
            continue
        einfo = entity_by_nm[nm_id]
        eid = einfo["entity_id"]
        wid = warehouses.get(wname, 0)

        qty = stock_map.get(key, {}).get("qty", 0)
        qty_full = stock_map.get(key, {}).get("qty_full", 0)
        ref = ref_by_nm.get(nm_id, ref_by_entity.get(eid, {}))
        supply_days = ref.get("supply_days") or 5
        min_batch = ref.get("min_batch_fbo") or 1

        order_info = order_map.get(key, {})
        rate = order_info.get("rate_per_day", 0)
        total_orders = order_info.get("total_qty", 0)
        active_days = order_info.get("active_days", 0)

        # Расчёт потребности
        need = round(rate * supply_days) - qty
        if need <= 0:
            need = 0
        elif need < min_batch:
            need = min_batch
        else:
            need = math.ceil(need / min_batch) * min_batch

        # Дней до нуля
        days_to_zero = round(qty / rate, 1) if rate > 0 else 999

        rows.append({
            "entity_id": eid,
            "nm_id": nm_id,
            "product_name": einfo["product_name"],
            "size_name": einfo["size_name"],
            "photo_main": einfo["photo_main"],
            "warehouse_name": wname,
            "warehouse_id": wid,
            "stock_qty": qty,
            "stock_qty_full": qty_full,
            "order_rate": rate,
            "orders_total": total_orders,
            "active_days": active_days,
            "supply_days": supply_days,
            "min_batch": min_batch,
            "need": need,
            "days_to_zero": days_to_zero,
        })

    # Сортировка: сначала критичные (days_to_zero меньше)
    rows.sort(key=lambda x: x["days_to_zero"])

    return {"warehouses": list(warehouses.keys()), "rows": rows, "days": days}
