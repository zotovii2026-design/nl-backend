import uuid
import math
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
from core.rate_limit import enforce_rate_limit
from services.reference import resolve_org_id
from core.role_deps import require_organization_role
from core.tenant_auth import require_query_organization_access
from models.organization import Role
from models.user import User
from models.reference_book import ReferenceBook
from models.raw_data import RawApiData, TechStatus
from models.sales_plan import SalesPlan, PlanType, Seasonality
from domain.unit_economics import (
    apply_financial_formulas,
    build_box_tariff_context,
    calculate_delivery,
    calculate_reverse_delivery,
)
from repositories.unit_economics import (
    get_latest_date as get_unit_economics_latest_date,
    get_products as get_unit_economics_products,
    get_supporting_rows as get_unit_economics_supporting_rows,
)

router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)




# ─── ORG ID RESOLVER ─────────────────────────────────────



# ─── API ENDPOINTS ─────────────────────────────────────────








@router.get("/api/v1/nl/products")
async def get_products(org_id: str, target_date: Optional[str] = None, db: AsyncSession = Depends(get_db)):
    org_id = await resolve_org_id(org_id, db)
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
        select(ProductEntity.id, ProductEntity.size_name, ProductEntity.subject_name).where(
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
async def get_control_metrics(
    org_id: str,
    target_date: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    org_id = await resolve_org_id(org_id, db)
    """Оперативный контроль — метрики на дату"""
    from sqlalchemy import func, case, and_
    from datetime import datetime as dt_mod
    import decimal

    if date_from and date_to:
        d_from = dt_mod.strptime(date_from, "%Y-%m-%d").date()
        d_to = dt_mod.strptime(date_to, "%Y-%m-%d").date()
    else:
        d = dt_mod.strptime(target_date, "%Y-%m-%d").date() if target_date else date.today()
        d_from = d
        d_to = d

    latest_result = await db.execute(
        select(func.max(TechStatus.target_date)).where(
            TechStatus.organization_id == org_id,
            TechStatus.target_date >= d_from,
            TechStatus.target_date <= d_to,
        )
    )
    latest_date = latest_result.scalar() or d_to

    # Потоковые метрики за период
    result = await db.execute(
        select(
            func.count(TechStatus.id).label("total_products"),
            func.sum(TechStatus.orders_count).label("total_orders"),
            func.sum(TechStatus.buyouts_count).label("total_buyouts"),
            func.sum(TechStatus.returns_count).label("total_returns"),
            func.sum(TechStatus.impressions).label("total_impressions"),
            func.sum(TechStatus.clicks).label("total_clicks"),
            func.sum(TechStatus.ad_cost).label("total_ad_cost"),
            func.sum(TechStatus.price_discount * TechStatus.buyouts_count).label("total_revenue"),
            func.sum(TechStatus.price_discount * TechStatus.buyouts_count).label("total_revenue_gross"),
        ).where(
            TechStatus.organization_id == org_id,
            TechStatus.target_date >= d_from,
            TechStatus.target_date <= d_to,
        )
    )
    row = result.one()

    funnel_result = await db.execute(
        select(RawApiData.target_date, RawApiData.raw_response).where(
            RawApiData.organization_id == org_id,
            RawApiData.api_method == "sales_funnel",
            RawApiData.status == "ok",
            RawApiData.target_date >= d_from,
            RawApiData.target_date <= d_to,
        )
    )
    funnel_summary = {
        "dates": set(),
        "total_orders": 0,
        "total_buyouts": 0,
        "total_returns": 0,
        "orders_revenue": 0.0,
        "buyouts_revenue": 0.0,
        "cancel_revenue": 0.0,
    }
    funnel_products = {}
    for fdate, raw in funnel_result.all():
        if not isinstance(raw, list):
            continue
        date_is_valid = False
        day_totals = {
            "total_orders": 0,
            "total_buyouts": 0,
            "total_returns": 0,
            "orders_revenue": 0.0,
            "buyouts_revenue": 0.0,
            "cancel_revenue": 0.0,
        }
        for item in raw:
            if not isinstance(item, dict):
                continue
            stat = (item.get("statistic") or {}).get("selected") or {}
            period = stat.get("period") or {}
            if period.get("start") and period.get("end"):
                if period.get("start") != str(fdate) or period.get("end") != str(fdate):
                    continue
            date_is_valid = True
            order_count = int(stat.get("orderCount", 0) or 0)
            buyout_count = int(stat.get("buyoutCount", 0) or 0)
            cancel_count = int(stat.get("cancelCount", 0) or 0)
            order_sum = float(stat.get("orderSum", 0) or 0)
            buyout_sum = float(stat.get("buyoutSum", 0) or 0)
            cancel_sum = float(stat.get("cancelSum", 0) or 0)
            day_totals["total_orders"] += order_count
            day_totals["total_buyouts"] += buyout_count
            day_totals["total_returns"] += cancel_count
            day_totals["orders_revenue"] += order_sum
            day_totals["buyouts_revenue"] += buyout_sum
            day_totals["cancel_revenue"] += cancel_sum

            nm_id = (item.get("product") or {}).get("nmId")
            if nm_id:
                product_totals = funnel_products.setdefault(str(nm_id), {
                    "orders_count": 0,
                    "buyouts_count": 0,
                    "returns_count": 0,
                    "total_orders_revenue": 0.0,
                    "total_buyouts_revenue": 0.0,
                    "total_cancel_revenue": 0.0,
                })
                product_totals["orders_count"] += order_count
                product_totals["buyouts_count"] += buyout_count
                product_totals["returns_count"] += cancel_count
                product_totals["total_orders_revenue"] += order_sum
                product_totals["total_buyouts_revenue"] += buyout_sum
                product_totals["total_cancel_revenue"] += cancel_sum
        if date_is_valid:
            funnel_summary["dates"].add(fdate)
            for key in day_totals:
                funnel_summary[key] += day_totals[key]
    expected_funnel_dates = set()
    _fd = d_from
    while _fd <= d_to:
        expected_funnel_dates.add(_fd)
        _fd += timedelta(days=1)
    has_funnel_summary = bool(expected_funnel_dates) and expected_funnel_dates.issubset(funnel_summary["dates"])

    # Остатки и рейтинг берем на последнюю доступную дату периода
    stock_result = await db.execute(
        select(
            func.count(TechStatus.id).label("total_products"),
            func.sum(TechStatus.stock_qty).label("total_stock"),
            func.sum(TechStatus.stock_fbo_qty).label("total_stock_fbo"),
            func.avg(TechStatus.rating).label("avg_rating"),
        ).where(TechStatus.organization_id == org_id, TechStatus.target_date == latest_date)
    )
    stock_row = stock_result.one()

    # Товары с нулевым остатком
    zero_stock = await db.execute(
        select(func.count(TechStatus.id)).where(
            TechStatus.organization_id == org_id, TechStatus.target_date == latest_date,
            TechStatus.stock_qty <= 0
        )
    )

    # Товары с низким остатком (<=5)
    low_stock = await db.execute(
        select(func.count(TechStatus.id)).where(
            TechStatus.organization_id == org_id, TechStatus.target_date == latest_date,
            TechStatus.stock_qty > 0, TechStatus.stock_qty <= 5
        )
    )

    # Товары по рейтингу (< 4)
    low_rating = await db.execute(
        select(func.count(TechStatus.id)).where(
            TechStatus.organization_id == org_id, TechStatus.target_date == latest_date,
            TechStatus.rating < 4.0
        )
    )

    # Детализация по товарам (с entity_id)
    products_detail = await db.execute(
        select(
            TechStatus.entity_id, TechStatus.nm_id, TechStatus.vendor_code, TechStatus.product_name,
            TechStatus.photo_main, TechStatus.stock_qty, TechStatus.stock_fbo_qty, TechStatus.orders_count,
            TechStatus.buyouts_count, TechStatus.returns_count, TechStatus.rating,
            TechStatus.impressions, TechStatus.clicks, TechStatus.ad_cost,
            TechStatus.price, TechStatus.price_discount, TechStatus.tariff,
            TechStatus.barcode,
        ).where(
            TechStatus.organization_id == org_id,
            TechStatus.target_date >= d_from,
            TechStatus.target_date <= d_to,
        )
        .order_by(TechStatus.nm_id, TechStatus.entity_id, TechStatus.target_date.desc())
    )

    # Маппинг entity_id -> size_name + Д×Ш×В, вес, объём (факт)
    from models.product_entity import ProductEntity
    ent_result = await db.execute(
        select(ProductEntity.id, ProductEntity.size_name,
               ProductEntity.length, ProductEntity.width, ProductEntity.height,
               ProductEntity.weight).where(
            ProductEntity.organization_id == org_id
        )
    )
    _ent_rows = ent_result.all()
    size_map = {str(r[0]): r[1] for r in _ent_rows}
    dims_map = {}
    for r in _ent_rows:
        eid = str(r[0])
        l, w, h = r[2], r[3], r[4]
        wt = r[5]
        vol = round((l * w * h) / 1000, 2) if l and w and h else None
        dims_map[eid] = {"length": l, "width": w, "height": h, "weight": wt, "volume": vol}

    # Маппинг entity_id -> все ШК (для поиска)
    from models.product_entity import EntityBarcode
    bc_result = await db.execute(
        select(EntityBarcode.entity_id, EntityBarcode.barcode).where(
            EntityBarcode.is_active == True
        )
    )
    barcodes_map = {}
    for r in bc_result.all():
        eid = str(r[0])
        if eid not in barcodes_map:
            barcodes_map[eid] = []
        barcodes_map[eid].append(r[1])

    # --- Юнит Экономика для ТС ---
    from sqlalchemy import text

    # Себестоимость и справочник
    ref_result = await db.execute(text(
        "SELECT entity_id, nm_id, cost_price, product_class, brand, tax_system, tax_rate, vat_rate, wb_price_fact "
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
            "wb_price_fact": float(r[8]) if r[8] else None,
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
        return ref_by_nm.get(nm, ref_by_entity.get(eid, {"cost_price":0,"product_class":"","brand":"","tax_system":"","tax_rate":0,"vat_rate":0,"wb_price_fact":None}))

    def _get_snap(nm):
        return snap_by_nm_ts.get(nm, {"logistics_tariff":0,"storage_tariff":0,"commission_pct":0,"buyout_pct_fact":0})

    # subject_map for product display
    _subj_result = await db.execute(
        select(ProductEntity.id, ProductEntity.subject_name)
        .where(ProductEntity.organization_id == org_id)
    )
    subject_map = {str(r[0]): r[1] or "" for r in _subj_result.all()}

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

    def build_product_rows(rows):
        grouped = {}
        order = []
        for r in rows:
            eid = str(r[0]) if r[0] else ""
            key = eid or f"nm:{r[1]}:{r[17] or ''}"
            if key not in grouped:
                grouped[key] = {
                    "entity_id": eid or None,
                    "nm_id": r[1],
                    "vendor_code": r[2],
                    "product_name": r[3],
                    "photo_main": r[4],
                    "stock_qty": 0,
                    "stock_fbo_qty": 0,
                    "orders_count": 0,
                    "buyouts_count": 0,
                    "returns_count": 0,
                    "rating": None,
                    "impressions": 0,
                    "clicks": 0,
                    "ad_cost": 0,
                    "price": None,
                    "price_discount": None,
                    "tariff": None,
                    "_latest_set": False,
                    "barcode": r[17] or "",
                    "barcodes": ", ".join(barcodes_map.get(eid, [])) or (r[17] or ""),
                    "size_name": size_map.get(eid, "") if eid else "",
                    "subject_name": subject_map.get(eid, "") if eid else "",
                    **(dims_map.get(eid, {}) if eid else {}),
                }
                order.append(key)

            item = grouped[key]
            item["orders_count"] += safe_int(r[7]) or 0
            item["buyouts_count"] += safe_int(r[8]) or 0
            item["returns_count"] += safe_int(r[9]) or 0
            item["impressions"] += safe_int(r[11]) or 0
            item["clicks"] += safe_int(r[12]) or 0
            item["ad_cost"] += safe_float(r[13]) or 0

            # Query is ordered newest first inside each entity/nm group, so set point-in-time fields once.
            if not item["_latest_set"]:
                item["stock_qty"] = safe_int(r[5]) or 0
                item["stock_fbo_qty"] = safe_int(r[6]) or 0
                item["rating"] = safe_float(r[10])
                item["price"] = safe_float(r[14])
                item["price_discount"] = safe_float(r[15])
                item["tariff"] = safe_float(r[16])
                item["_latest_set"] = True

        products = []
        for key in order:
            item = grouped[key]
            item.pop("_latest_set", None)
            ref = _get_ref(item["entity_id"] or "", item["nm_id"])
            snap = _get_snap(item["nm_id"])
            item.update(ref)
            item.update({f"snap_{k}": v for k, v in snap.items()})
            if has_funnel_summary:
                funnel_item = funnel_products.get(str(item["nm_id"]))
                if funnel_item:
                    item.update({
                        "orders_count": funnel_item["orders_count"],
                        "buyouts_count": funnel_item["buyouts_count"],
                        "returns_count": funnel_item["returns_count"],
                        "total_orders_revenue": round(funnel_item["total_orders_revenue"], 2),
                        "total_buyouts_revenue": round(funnel_item["total_buyouts_revenue"], 2),
                        "total_cancel_revenue": round(funnel_item["total_cancel_revenue"], 2),
                    })
            item.update(_calc_unit(item.get("price_discount"), item.get("ad_cost"), ref, snap))
            products.append(item)
        products.sort(key=lambda p: p.get("orders_count") or 0, reverse=True)
        return products

    total_clicks = safe_int(row.total_clicks) or 0
    total_impressions = safe_int(row.total_impressions) or 0

    return {
        "date": str(latest_date),
        "date_from": str(d_from),
        "date_to": str(d_to),
        "summary": {
            "total_products": safe_int(stock_row.total_products) or 0,
            "total_stock": (safe_int(stock_row.total_stock) or 0) + (safe_int(stock_row.total_stock_fbo) or 0),
            "total_stock_fbo": safe_int(stock_row.total_stock_fbo) or 0,
            "total_stock_fbs": safe_int(stock_row.total_stock) or 0,
            "total_orders": int(funnel_summary["total_orders"]) if has_funnel_summary else (safe_int(row.total_orders) or 0),
            "total_buyouts": int(funnel_summary["total_buyouts"]) if has_funnel_summary else (safe_int(row.total_buyouts) or 0),
            "total_returns": int(funnel_summary["total_returns"]) if has_funnel_summary else (safe_int(row.total_returns) or 0),
            "total_impressions": total_impressions,
            "total_clicks": total_clicks,
            "ctr": round(total_clicks / total_impressions * 100, 2) if total_impressions > 0 else 0,
            "total_ad_cost": safe_float(row.total_ad_cost) or 0,
            "total_revenue": round(funnel_summary["buyouts_revenue"], 2) if has_funnel_summary else (safe_float(row.total_revenue) or 0),
            "total_orders_revenue": round(funnel_summary["orders_revenue"], 2) if has_funnel_summary else 0,
            "total_buyouts_revenue": round(funnel_summary["buyouts_revenue"], 2) if has_funnel_summary else (safe_float(row.total_revenue) or 0),
            "total_cancel_revenue": round(funnel_summary["cancel_revenue"], 2) if has_funnel_summary else 0,
            "sales_funnel_dates": sorted(str(d) for d in funnel_summary["dates"]),
            "avg_rating": round(float(stock_row.avg_rating), 2) if stock_row.avg_rating else None,
            "zero_stock_count": safe_int(zero_stock.scalar()) or 0,
            "low_stock_count": safe_int(low_stock.scalar()) or 0,
            "low_rating_count": safe_int(low_rating.scalar()) or 0,
        },
        "products": build_product_rows(products_detail.all())
    }



# ─── FRONTEND ──────────────────────────────────────────────




# === РНП — API эндпоинт ===
@router.get("/nl/register", response_class=HTMLResponse)
async def nl_register_page():
    with open("templates/auth_register.html", "r", encoding="utf-8") as f:
        html = f.read()
    resp = HTMLResponse(html)
    resp.headers["Cache-Control"] = "no-cache, no-store"
    return resp

@router.get("/nl/login", response_class=HTMLResponse)
async def nl_login_page():
    with open("templates/auth_login.html", "r", encoding="utf-8") as f:
        html = f.read()
    resp = HTMLResponse(html)
    resp.headers["Cache-Control"] = "no-cache, no-store"
    return resp

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




@router.get("/api/v1/nl/marketer/products")
async def get_marketer_products(
    org_id: str,
    days: str = "30",
    search: Optional[str] = None,
    status: Optional[str] = None,
    abc_class: Optional[str] = None,
    brand: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Стол маркетолога — список товаров с рекламными данными"""
    import decimal as _dec, json as _json

    try:
        days_int = int(days)
    except:
        days_int = 30

    def sf(v):
        if v is None: return 0
        return float(v) if isinstance(v, (_dec.Decimal, int)) else (float(v) if v else 0)

    date_from = f"CURRENT_DATE - make_interval(days => {days_int})"

    # 1) Получаем уникальные nm_id из активных/приостановленных кампаний
    active_statuses = ('7', '9', '11')  # WB status codes: 7=активна, 9=приостановлена, 11=завершена

    # Все РК с их составом (из ad_stats_nm, только spent > 0) за период
    camp_rows = await db.execute(text(f"""
        SELECT c.wb_campaign_id, c.name, c.type, c.status,
               COALESCE(SUM(sn.views),0), COALESCE(SUM(sn.clicks),0),
               COALESCE(SUM(sn.spent),0),
               COALESCE(SUM(sn.orders),0), COALESCE(SUM(sn.atbs),0),
               COALESCE(SUM(sn.sum_price),0)
        FROM ad_campaigns c
        JOIN ad_stats_nm sn ON sn.wb_campaign_id = c.wb_campaign_id
            AND sn.organization_id = c.organization_id
            AND sn.stat_date >= {date_from}
            AND sn.spent > 0
        WHERE c.organization_id = :org
        GROUP BY c.wb_campaign_id, c.name, c.type, c.status
        ORDER BY COALESCE(SUM(sn.spent),0) DESC
    """), {"org": org_id})

    # nm_id → какие РК к ним относятся (из ad_stats_nm, только spent > 0)
    nm_camp_rows = await db.execute(text(f"""
        SELECT sn.wb_campaign_id, sn.nm_id, c.name, c.type, c.status,
               COALESCE(SUM(sn.views),0), COALESCE(SUM(sn.clicks),0),
               COALESCE(SUM(sn.spent),0),
               COALESCE(SUM(sn.orders),0), COALESCE(SUM(sn.atbs),0),
               COALESCE(SUM(sn.sum_price),0)
        FROM ad_stats_nm sn
        JOIN ad_campaigns c ON c.wb_campaign_id = sn.wb_campaign_id
            AND c.organization_id = sn.organization_id
        WHERE sn.organization_id = :org
            AND sn.stat_date >= {date_from}
            AND sn.spent > 0
        GROUP BY sn.wb_campaign_id, sn.nm_id, c.name, c.type, c.status
    """), {"org": org_id})

    nm_to_campaigns = {}  # nm_id -> [{campaign_info}]
    all_campaigns = []
    all_nm_ids = set()
    camp_ids_seen = set()

    for r in nm_camp_rows:
        nm_id = int(r[1])
        camp_info = {
            "campaign_id": r[0],
            "name": r[2] or "Без названия",
            "type": str(r[3]) if r[3] else "",
            "status": str(r[4]) if r[4] else "",
            "views": int(sf(r[5])),
            "clicks": int(sf(r[6])),
            "spent": round(sf(r[7]), 2),
            "orders": int(sf(r[8])),
            "atbs": int(sf(r[9])),
            "sum_price": round(sf(r[10]), 2),
        }
        if nm_id not in nm_to_campaigns:
            nm_to_campaigns[nm_id] = []
        nm_to_campaigns[nm_id].append(camp_info)
        all_nm_ids.add(nm_id)
        if r[0] not in camp_ids_seen:
            camp_ids_seen.add(r[0])
            all_campaigns.append(camp_info)

    # 2) Получаем инфо о товарах (фото, название, бренд, статус, класс)
    product_info = {}
    if all_nm_ids:
        pe_rows = await db.execute(text("""
            SELECT DISTINCT ON (pe.nm_id) pe.nm_id, pe.brand, pe.subject_name, pe.photo_main,
                pe.vendor_code, pe.subject_name
            FROM product_entities pe
            WHERE pe.organization_id = :org AND pe.nm_id = ANY(:nms)
            ORDER BY pe.nm_id, pe.created_at DESC
        """), {"org": org_id, "nms": list(all_nm_ids)})
        for r in pe_rows:
            product_info[r[0]] = {
                "nm_id": r[0],
                "brand": r[1] or "",
                "category": r[2] or "",
                "photo": r[3] or "",
                "vendor_code": r[4] or "",
            }

        # Дополняем из tech_status (цены)
        ts_rows = await db.execute(text(f"""
            SELECT DISTINCT ON (ts.nm_id) ts.nm_id,
                ts.price, ts.price_spp, ts.price_discount
            FROM tech_status ts
            WHERE ts.organization_id = :org AND ts.nm_id = ANY(:nms)
              AND ts.target_date >= {date_from}
            ORDER BY ts.nm_id, ts.target_date DESC
        """), {"org": org_id, "nms": list(all_nm_ids)})
        for r in ts_rows:
            if r[0] in product_info:
                product_info[r[0]]["price"] = sf(r[1])
                product_info[r[0]]["price_spp"] = sf(r[2])
                product_info[r[0]]["price_discount"] = sf(r[3])

        # Дополняем из reference_book (статус, класс)
        rb_rows = await db.execute(text("""
            SELECT DISTINCT ON (rb.nm_id) rb.nm_id,
                rb.product_status as status,
                rb.product_class as abc_class
            FROM reference_book rb
            WHERE rb.organization_id = :org AND rb.nm_id = ANY(:nms)
            ORDER BY rb.nm_id, rb.valid_from DESC
        """), {"org": org_id, "nms": list(all_nm_ids)})
        for r in rb_rows:
            if r[0] in product_info:
                product_info[r[0]]["status"] = r[1] or ""
                product_info[r[0]]["abc_class"] = r[2] or ""

    # 3) Собираем список товаров
    products = []
    for nm_id in sorted(all_nm_ids):
        info = product_info.get(nm_id, {"nm_id": nm_id})
        camps = nm_to_campaigns.get(nm_id, [])

        # Суммарные метрики по всем РК товара
        total_views = sum(c["views"] for c in camps)
        total_clicks = sum(c["clicks"] for c in camps)
        total_spent = sum(c["spent"] for c in camps)
        total_orders = sum(c["orders"] for c in camps)

        active_camps = [c for c in camps if c["status"] in active_statuses]

        products.append({
            "nm_id": nm_id,
            "vendor_code": info.get("vendor_code", ""),
            "brand": info.get("brand", ""),
            "category": info.get("category", ""),
            "photo": info.get("photo", ""),
            "status": info.get("status", ""),
            "abc_class": info.get("abc_class", ""),
            "price": info.get("price", 0),
            "price_spp": info.get("price_spp", 0),
            "price_discount": info.get("price_discount", 0),
            "campaign_count": len(camps),
            "active_campaign_count": len(active_camps),
            "total_views": total_views,
            "total_clicks": total_clicks,
            "total_spent": round(total_spent, 2),
            "total_orders": total_orders,
            "ctr": round(total_clicks / total_views * 100, 2) if total_views else 0,
            "drr": round(total_spent / sum(c.get("sum_price", 0) for c in camps) * 100, 1) if sum(c.get("sum_price", 0) for c in camps) else 0,
            "campaigns": camps,
            "plan_orders": 0,  # TODO: из плана
            "fact_orders": total_orders,
            "plan_pct": 0,  # TODO: расчёт
        })

    # Фильтрация
    if search:
        s = search.lower()
        products = [p for p in products if s in str(p.get("vendor_code","")).lower() or s in str(p.get("nm_id","")) or s in str(p.get("brand","")).lower()]
    if status:
        products = [p for p in products if p.get("status") == status]
    if abc_class:
        products = [p for p in products if p.get("abc_class") == abc_class]
    if brand:
        products = [p for p in products if p.get("brand","").lower() == brand.lower()]

    # Список уникальных брендов для фильтра
    brands = sorted(set(p.get("brand","") for p in products if p.get("brand")))

    return {
        "products": products,
        "brands": brands,
        "total_products": len(products),
        "total_campaigns": len(all_campaigns),
    }


@router.get("/api/v1/nl/marketer/product/{nm_id}")
async def get_marketer_product_detail(
    nm_id: int,
    org_id: str,
    days: str = "30",
    db: AsyncSession = Depends(get_db)
):
    """Стол маркетолога — детальная карточка товара с РК по дням"""
    import decimal as _dec, json as _json

    try:
        days_int = int(days)
    except:
        days_int = 30

    def sf(v):
        if v is None: return 0
        return float(v) if isinstance(v, (_dec.Decimal, int)) else (float(v) if v else 0)

    date_from_sql = f"CURRENT_DATE - make_interval(days => {days_int})"

    # Инфо о товаре
    pe_rows = await db.execute(text("""
        SELECT DISTINCT ON (pe.nm_id) pe.nm_id, pe.brand, pe.subject_name, pe.photo_main,
            pe.vendor_code, pe.color, pe.weight, pe.chrt_id
        FROM product_entities pe
        WHERE pe.organization_id = :org AND pe.nm_id = :nm
        ORDER BY pe.nm_id, pe.created_at DESC
    """), {"org": org_id, "nm": nm_id})
    pe = pe_rows.first()
    if not pe:
        return {"error": "Товар не найден"}

    product = {
        "nm_id": nm_id,
        "brand": pe[1] or "",
        "category": pe[2] or "",
        "photo": pe[3] or "",
        "vendor_code": pe[4] or "",
    }

    # Цены
    ts_rows = await db.execute(text(f"""
        SELECT DISTINCT ON (ts.target_date) ts.target_date,
            ts.price, ts.price_spp, ts.price_discount, ts.impressions, ts.clicks, ts.ad_cost
        FROM tech_status ts
        WHERE ts.organization_id = :org AND ts.nm_id = :nm
          AND ts.target_date >= {date_from_sql}
        ORDER BY ts.target_date DESC, ts.created_at DESC
    """), {"org": org_id, "nm": nm_id})
    prices_by_date = {}
    organic_by_date = {}
    for r in ts_rows:
        prices_by_date[str(r[0])] = {
            "price": sf(r[1]), "price_spp": sf(r[2]), "price_discount": sf(r[3]),
            "organic_impressions": int(r[4] or 0), "organic_clicks": int(r[5] or 0), "organic_ad_cost": sf(r[6]),
        }

    # Статус/класс из справочника
    rb_rows = await db.execute(text("""
        SELECT rb.product_status as status,
               rb.product_class as abc_class
        FROM reference_book rb
        WHERE rb.organization_id = :org AND rb.nm_id = :nm
        ORDER BY rb.valid_from DESC LIMIT 1
    """), {"org": org_id, "nm": nm_id})
    rb = rb_rows.first()
    if rb:
        product["status"] = rb[0] or ""
        product["abc_class"] = rb[1] or ""
    # Акции - TODO: promo_products table not yet created
    product["in_promo"] = False
    product["promo_name"] = ""


    # РК, которые рекламируют этот nm_id
    camp_rows = await db.execute(text(f"""
        SELECT c.wb_campaign_id, c.name, c.type, c.status, c.nm_ids, c.budget,
               c.daily_budget, c.payment_type, c.bid_type
        FROM ad_campaigns c
        WHERE c.organization_id = :org AND c.nm_ids @> CAST(:nm_arr AS jsonb)
        ORDER BY c.status ASC, c.name
    """), {"org": org_id, "nm_arr": _json.dumps([nm_id])})

    active_statuses = ('7', '9')
    campaigns = []

    for r in camp_rows:
        camp_id = r[0]
        # Статистика по дням для этой РК — только по конкретному nm_id
        stat_rows = await db.execute(text(f"""
            SELECT s.stat_date,
                   SUM(s.views) as views, SUM(s.clicks) as clicks, SUM(s.spent) as spent,
                   CASE WHEN SUM(s.views) > 0 THEN ROUND(SUM(s.clicks)::numeric / SUM(s.views) * 100, 2) ELSE 0 END as ctr,
                   CASE WHEN SUM(s.clicks) > 0 THEN ROUND(SUM(s.spent) / SUM(s.clicks), 2) ELSE 0 END as cpc,
                   SUM(s.orders) as orders, SUM(s.atbs) as atbs,
                   CASE WHEN SUM(s.clicks) > 0 THEN ROUND(SUM(s.orders)::numeric / SUM(s.clicks) * 100, 2) ELSE 0 END as cr,
                   SUM(s.sum_price) as sum_price
            FROM ad_stats_nm s
            WHERE s.organization_id = :org AND s.wb_campaign_id = :cid AND s.nm_id = :nm
              AND s.stat_date >= {date_from_sql}
            GROUP BY s.stat_date
            ORDER BY s.stat_date
        """), {"org": org_id, "cid": camp_id, "nm": nm_id})

        daily_stats = []
        for sr in stat_rows:
            daily_stats.append({
                "date": str(sr[0]),
                "views": int(sr[1] or 0),
                "clicks": int(sr[2] or 0),
                "spent": round(sf(sr[3]), 2),
                "ctr": round(sf(sr[4]), 2),
                "cpc": round(sf(sr[5]), 2),
                "orders": int(sr[6] or 0),
                "atbs": int(sr[7] or 0),
                "cr": round(sf(sr[8]), 2),
                "sum_price": round(sf(sr[9]), 2),
            })

        # Итого по РК
        camp_total = {
            "views": sum(d["views"] for d in daily_stats),
            "clicks": sum(d["clicks"] for d in daily_stats),
            "spent": sum(d["spent"] for d in daily_stats),
            "orders": sum(d["orders"] for d in daily_stats),
            "atbs": sum(d["atbs"] for d in daily_stats),
            "sum_price": sum(d["sum_price"] for d in daily_stats),
        }
        camp_total["ctr"] = round(camp_total["clicks"] / camp_total["views"] * 100, 2) if camp_total["views"] else 0
        camp_total["cpc"] = round(camp_total["spent"] / camp_total["clicks"], 2) if camp_total["clicks"] else 0
        camp_total["cr"] = round(camp_total["orders"] / camp_total["clicks"] * 100, 2) if camp_total["clicks"] else 0

        campaigns.append({
            "campaign_id": camp_id,
            "name": r[1] or "Без названия",
            "type": str(r[2]) if r[2] else "",
            "status": str(r[3]) if r[3] else "",
            "nm_ids": [int(n) for n in (_json.loads(r[4]) if isinstance(r[4], str) else (r[4] or [])) if n],
            "budget": sf(r[5]),
            "daily_budget": sf(r[6]),
            "is_active": str(r[3]) in active_statuses,
            "daily": daily_stats,
            "totals": camp_total,
        })

    # Сводка «РК В ОБЩЕМ»
    all_daily = {}  # date -> aggregated
    for camp in campaigns:
        for d in camp["daily"]:
            dt = d["date"]
            if dt not in all_daily:
                all_daily[dt] = {"date": dt, "views": 0, "clicks": 0, "spent": 0, "orders": 0, "atbs": 0, "sum_price": 0}
            all_daily[dt]["views"] += d["views"]
            all_daily[dt]["clicks"] += d["clicks"]
            all_daily[dt]["spent"] += d["spent"]
            all_daily[dt]["orders"] += d["orders"]
            all_daily[dt]["atbs"] += d["atbs"]
            all_daily[dt]["sum_price"] += d["sum_price"]

    # Добавляем органику (из tech_status)
    for dt, info in organic_by_date.items():
        if dt in all_daily:
            all_daily[dt]["organic_impressions"] = info["organic_impressions"]
            all_daily[dt]["organic_clicks"] = info["organic_clicks"]

    summary_daily = sorted(all_daily.values(), key=lambda x: x["date"])

    grand_total = {
        "views": sum(d["views"] for d in summary_daily),
        "clicks": sum(d["clicks"] for d in summary_daily),
        "spent": sum(d["spent"] for d in summary_daily),
        "orders": sum(d["orders"] for d in summary_daily),
    }
    grand_total["ctr"] = round(grand_total["clicks"] / grand_total["views"] * 100, 2) if grand_total["views"] else 0
    grand_total["drr"] = round(grand_total["spent"] / grand_total["sum_price"] * 100, 1) if grand_total.get("sum_price") else 0

    # Лучший период — ищем день с макс profit (orders * price - spent)
    best_day = None
    best_profit = -float('inf')
    for d in summary_daily:
        price_day = prices_by_date.get(d["date"], {}).get("price", 0)
        profit = d["orders"] * price_day - d["spent"]
        if profit > best_profit and d["orders"] > 0:
            best_profit = profit
            best_day = {
                "date": d["date"],
                "orders": d["orders"],
                "spent": d["spent"],
                "views": d["views"],
                "price": price_day,
                "price_spp": prices_by_date.get(d["date"], {}).get("price_spp", 0),
                "profit": round(profit, 2),
                "in_promo": product.get("in_promo", False),
            }

    return {
        "product": product,
        "campaigns": campaigns,
        "summary_daily": summary_daily,
        "grand_total": grand_total,
        "best_period": best_day,
        "prices_by_date": prices_by_date,
    }



# ==================== UNIT ECONOMICS APIs ====================

async def build_unit_economics(
    org_id: str,
    db: AsyncSession,
    search: Optional[str] = None,
    limit: Optional[int] = None,
):
    """Юнит Экономика — сборка всех данных по SKU"""
    # ── Redis-кэш: отдаём готовый результат если есть ──
    import redis as _redis_lib, json as _json
    _redis = _redis_lib.from_url("redis://redis:6379/0")
    _cache_key = f"ue_cache:{org_id}"
    if not search:
        _cached = _redis.get(_cache_key)
        if _cached:
            try:
                _cached_data = _json.loads(_cached)
                if limit and limit > 0:
                    _cached_data["items"] = _cached_data["items"][:limit]
                return _cached_data
            except Exception:
                pass

    latest_date = await get_unit_economics_latest_date(db, org_id)
    if not latest_date:
        return {"items": [], "total": 0}
    products = await get_unit_economics_products(db, org_id, latest_date)
    rb_rows, tsnap_rows, box_rows = await get_unit_economics_supporting_rows(
        org_id,
        db=db,
    )

    # ── Обработка единого запроса reference_book ──────────
    # Колонки rb_rows:
    #  0: entity_id, 1: nm_id
    #  2-9: UE fields (mp_correction...fulfillment_model)
    # 10-12: wb_club_discount_pct, storage_pct, product_status
    # 13-17: mp_base_pct, wb_price_fact, wb_price_retail, wb_discount_pct, wb_prices_updated_at
    # 18-24: cost fields (cost_price...vat)
    # 25-28: product_class, brand, tax_system, tax_rate, vat_rate
    # 29: fbs_warehouse

    ue_by_entity = {}
    ue_by_nm_bc = {}
    cost_by_entity = {}
    cost_by_nm = {}
    ff_by_entity = {}
    ff_by_nm = {}

    for r in rb_rows:
        eid = str(r[0]) if r[0] else None
        nm = r[1]

        # ─ UE fields ─
        ue_fields = {
            "mp_correction_pct": r[2], "buyout_niche_pct": r[3],
            "extra_costs": r[4], "ad_plan_rub": r[5],
            "price_before_spp_plan": r[6], "price_before_spp_change": r[7],
            "change_date": r[8], "tariff_type": r[9],
            "wb_club_discount_pct": r[10],
            "product_status": r[12],
            "mp_base_pct": r[13],
            "wb_price_fact": r[14],
            "wb_price_retail": r[15],
            "wb_discount_pct": r[16],
            "wb_prices_updated_at": r[17],
            "fulfillment_model": r[9],
            "fbs_warehouse": r[29],
        }
        if eid:
            if eid not in ue_by_entity:
                ue_by_entity[eid] = ue_fields
        else:
            key = (nm, "")
            if key not in ue_by_nm_bc:
                ue_by_nm_bc[key] = ue_fields

        # ─ Cost fields ─
        cost_fields = {
            "cost_price": r[18], "purchase_cost": r[19], "logistics_cost": r[20],
            "packaging_cost": r[21], "other_costs": r[22], "vat": r[23],
            "product_class": r[24], "brand": r[25], "tax_system": r[26],
            "tax_rate": r[27], "vat_rate": r[28],
            "product_status": r[12],
        }
        if eid:
            if eid not in cost_by_entity:
                cost_by_entity[eid] = cost_fields
            else:
                for k, v in cost_fields.items():
                    if v is not None and v != 0:
                        cost_by_entity[eid][k] = v
        if nm:
            if nm not in cost_by_nm:
                cost_by_nm[nm] = cost_fields
            else:
                for k, v in cost_fields.items():
                    if v is not None and v != 0:
                        cost_by_nm[nm][k] = v

        # ─ FF fields ─
        fful = r[9]  # fulfillment_model
        fwh = r[29]  # fbs_warehouse
        if eid:
            if eid not in ff_by_entity:
                ff_by_entity[eid] = (fful, fwh)
        if nm:
            if nm not in ff_by_nm:
                ff_by_nm[nm] = (fful, fwh)

    # ── WB-данные из wb_tariff_snapshot ──────────
    snap_by_nm = {}
    for r in tsnap_rows:
        if r[0] not in snap_by_nm:
            snap_by_nm[r[0]] = {
                "logistics_tariff": float(r[1]) if r[1] else 0,
                "storage_tariff": float(r[2]) if r[2] else 0,
                "ad_cost_fact": float(r[3]) if r[3] else 0,
                "buyout_pct_fact": float(r[4]) if r[4] else 0,
                "commission_pct": float(r[5]) if r[5] else 0,
                "price_retail": float(r[6]) if r[6] else 0,
                "price_with_spp": float(r[7]) if r[7] else 0,
                "spp_pct": float(r[8]) if r[8] else 0,
                "commission_fbs_pct": float(r[9]) if r[9] else 0,
            }

    tariff_context = build_box_tariff_context(box_rows)
    box_tariffs = tariff_context["tariffs"]

    # 8) Собираем результат
    items = []
    search_q = search.lower() if search else ""
    
    # size_name и subject_name берутся напрямую из product_entities (индексы 10, 11)

    for p in products:
        entity_id = str(p[0]) if p[0] else None
        nm_id = p[1]
        vendor_code = p[2] or ""
        product_name = p[3] or ""
        photo = p[4] or ""
        main_barcode = p[5] or ""
        price = float(p[6]) if p[6] else 0
        price_discount = float(p[7]) if p[7] else 0
        # size_name и subject_name из product_entities (индексы 10, 11)
        _pe_size_name = p[10] or ""
        _pe_subject_name = p[11] or ""
        # Габариты из product_entities (индексы 12, 13, 14) — в см
        _pe_width = float(p[12]) if p[12] else 0
        _pe_height = float(p[13]) if p[13] else 0
        _pe_length = float(p[14]) if p[14] else 0
        # Объём в литрах (Д×Ш×В см / 1000)
        _volume_liters = round(_pe_width * _pe_height * _pe_length / 1000, 3) if (_pe_width and _pe_height and _pe_length) else 0

        # Фульфилмент модель и склад ФБС
        ff_info = ff_by_entity.get(entity_id, ff_by_nm.get(nm_id, (None, None)))
        _fulfillment_model = ff_info[0] or "fbo"
        _fbs_warehouse = ff_info[1] if ff_info[1] and ff_info[1] != "0" else None

        # Расчёт логистики до клиента
        _delivery_to_client, _delivery_debug = calculate_delivery(
            _volume_liters,
            _fulfillment_model,
            _fbs_warehouse,
            tariff_context,
        )
        _reverse_logistics, _reverse_debug = calculate_reverse_delivery(
            _volume_liters
        )

        # Tooltip расшифровка логистики (детальная WB-методика)
        _logistics_tooltip_parts = []
        if _pe_width and _pe_height and _pe_length:
            _logistics_tooltip_parts.append(f"Габариты: {_pe_length}x{_pe_width}x{_pe_height} см")
            _logistics_tooltip_parts.append(f"Объём: {_volume_liters:.3f} л (окр. {math.ceil(_volume_liters)})")

        _meth = _delivery_debug.get("method", "")
        # Модель отгрузки
        if _fulfillment_model == "fbs":
            _model_label = f"Модель: ФБС (склад: {_fbs_warehouse or 'не указан'})"
        else:
            _model_label = "Модель: ФБО"
        _logistics_tooltip_parts.append(_model_label)
        _logistics_tooltip_parts.append(f"Методика: {_meth}")

        if "сетка" in _meth:
            # <= 1 литр: показываем сетку WB
            _logistics_tooltip_parts.append("")
            _logistics_tooltip_parts.append("WB-сетка (<= 1 л):")
            _logistics_tooltip_parts.append("  0.001-0.200 л → 23 ₽/л")
            _logistics_tooltip_parts.append("  0.201-0.400 л → 26 ₽/л")
            _logistics_tooltip_parts.append("  0.401-0.600 л → 29 ₽/л")
            _logistics_tooltip_parts.append("  0.601-0.800 л → 30 ₽/л")
            _logistics_tooltip_parts.append("  0.801-1.000 л → 32 ₽/л")
            _tr = _delivery_debug.get("tier_rate", 0)
            _logistics_tooltip_parts.append(f"  → Товар {_volume_liters:.3f} л = {_tr:.0f} ₽/л")
            _logistics_tooltip_parts.append("")
            if "ФБО-среднее" in _meth:
                _kd = box_tariffs.get("Коледино", {})
                _kr = box_tariffs.get("Краснодар", {})
                _kz = box_tariffs.get("Казань", {})
                _logistics_tooltip_parts.append(f"Коледино: коэфф. {_kd.get('fbo_coef', 0):.0f}%")
                _logistics_tooltip_parts.append(f"Краснодар: коэфф. {_kr.get('fbo_coef', 0):.0f}%")
                _logistics_tooltip_parts.append(f"Казань: коэфф. {_kz.get('fbo_coef', 0):.0f}%")
                _ac = _delivery_debug.get("avg_coef", 0)
                _logistics_tooltip_parts.append(f"Средний коэфф.: {_ac:.2f}%")
                _logistics_tooltip_parts.append(f"Формула: {_tr:.0f} × {_ac:.2f}% = {_delivery_to_client:.2f} ₽")
            else:
                _wh = _delivery_debug.get("warehouse", "?")
                _coef = _delivery_debug.get("coef", 0)
                _logistics_tooltip_parts.append(f"Склад: {_wh}, коэфф. {_coef:.0f}%")
                _logistics_tooltip_parts.append(f"Формула: {_tr:.0f} × {_coef:.0f}% = {_delivery_to_client:.2f} ₽")
        elif ">1л" in _meth:
            # > 1 литр: показываем формулу
            _logistics_tooltip_parts.append("")
            _vc = _delivery_debug.get("vol_ceil", 0)
            if "ФБС" in _meth:
                # ФБС: конкретный склад
                _b = _delivery_debug.get("base", 0)
                _l = _delivery_debug.get("liter", 0)
                _c = _delivery_debug.get("coef", 0)
                _wh = _delivery_debug.get("warehouse", "?")
                _fw = _delivery_debug.get("warehouse_requested", "")
                if _fw:
                    _logistics_tooltip_parts.append(f"Склад: {_fw} → тариф по {_wh}")
                else:
                    _logistics_tooltip_parts.append(f"Склад: {_wh}")
                _logistics_tooltip_parts.append(f"Базовый тариф: {_b:.2f} ₽ + {_l:.2f} ₽/л, коэфф. {_c:.0f}%")
                _logistics_tooltip_parts.append(f"Формула: {_b:.2f} + ({_vc}-1) × {_l:.2f} = {_delivery_to_client:.2f} ₽")
            elif "ФБО" in _meth:
                _kd = box_tariffs.get("Коледино", {})
                _kr = box_tariffs.get("Краснодар", {})
                _kz = box_tariffs.get("Казань", {})
                _ab = round((_kd.get('fbo_base') or 0) + (_kr.get('fbo_base') or 0) + (_kz.get('fbo_base') or 0), 2)
                _al = round((_kd.get('fbo_liter') or 0) + (_kr.get('fbo_liter') or 0) + (_kz.get('fbo_liter') or 0), 2)
                _logistics_tooltip_parts.append(f"Коледино: {_kd.get('fbo_base', 0):.2f} + {_kd.get('fbo_liter', 0):.2f}/л (коэфф. {_kd.get('fbo_coef', 0):.0f}%)")
                _logistics_tooltip_parts.append(f"Краснодар: {_kr.get('fbo_base', 0):.2f} + {_kr.get('fbo_liter', 0):.2f}/л (коэфф. {_kr.get('fbo_coef', 0):.0f}%)")
                _logistics_tooltip_parts.append(f"Казань: {_kz.get('fbo_base', 0):.2f} + {_kz.get('fbo_liter', 0):.2f}/л (коэфф. {_kz.get('fbo_coef', 0):.0f}%)")
                _logistics_tooltip_parts.append(f"Среднее: {_ab/3:.2f} + {_al/3:.2f}/л")

        _logistics_tooltip_parts.append("")
        _logistics_tooltip_parts.append(f"Итого логистика: {_delivery_to_client:.2f} ₽")
        _logistics_tooltip = chr(10).join(_logistics_tooltip_parts)

        # Фильтр поиска
        if search_q and search_q not in str(nm_id) and search_q not in product_name.lower() and search_q not in vendor_code.lower():
            continue

        cost = cost_by_nm.get(nm_id, cost_by_entity.get(entity_id, {}))
        ue = ue_by_entity.get(entity_id, ue_by_nm_bc.get((nm_id, main_barcode), ue_by_nm_bc.get((nm_id, ""), {})))

        item = {
            "entity_id": entity_id,
            "nm_id": nm_id,
            "vendor_code": vendor_code,
            "product_name": product_name,
            "photo": photo.replace("/hq/", "/c246x328/").replace("/big/", "/c246x328/").replace("/tm/", "/c246x328/") if photo else "",
            "barcode": main_barcode,
            "size_name": _pe_size_name,
            "subject_name": _pe_subject_name or cost.get("subject_name", ""),
            "sku": f"{vendor_code}_{main_barcode}" if vendor_code else str(nm_id),

            # Из справочника / себестоимости
            # Себестоимость в Юните = Итого из справочника (cost_price + extra_costs)
            "cost_price": (float(cost.get("cost_price") or 0)) + (float(ue.get("extra_costs") or 0)),
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
            "mp_base_pct": (lambda _s=snap_by_nm.get(nm_id,{}), _u=ue, _p=p: (float(_u.get("mp_base_pct") or 0) if _u.get("mp_base_pct") else ((_s.get("commission_fbs_pct") if (_u.get("tariff_type") or "fbo") == "fbs" else _s.get("commission_pct")) or float(_p[8] or 0))))(),
            "buyout_fact_pct": snap_by_nm.get(nm_id, {}).get("buyout_pct_fact", 0),
            "logistics_tariff": _delivery_to_client,
            "reverse_logistics": _reverse_logistics,
            "logistics_actual": 0,  # Будет из финотчётов
            "storage_tariff": snap_by_nm.get(nm_id, {}).get("storage_tariff", 0),
            "storage_actual": 0,  # Будет из финотчётов
            "acceptance_avg": 0,  # Будет из API приёмки
            "price_before_spp": float(ue.get("wb_price_fact")) if ue.get("wb_price_fact") else (snap_by_nm.get(nm_id, {}).get("price_retail") or price),
            "spp_pct": 0,  # заглушка, пока нет источника
            "price_with_spp": 0,  # заглушка, пока нет источника
            "ad_fact_pct": 0,  # заглушка, позже из финотчёта
            "ad_fact_rub": 0,  # заглушка, позже из финотчёта
            "wb_club_discount_pct_api": 0,

            # Из справочника
            "product_status": ue.get("product_status") or cost.get("product_status", ""),

            # Ручные вводы
            "mp_correction_pct": float(ue.get("mp_correction_pct") or 0),
            "buyout_niche_pct": float(ue.get("buyout_niche_pct") or 0),
            "extra_costs": 0,  # Уже включена в cost_price (Итого из справочника)
            "ad_plan_pct": min(99, max(0, float(ue.get("ad_plan_rub")) if ue.get("ad_plan_rub") not in (None, "", 0) else 5)),
            "ad_plan_rub": 0,  # рассчитывается ниже по цене
            "price_before_spp_plan": float(ue.get("price_before_spp_plan") or 0),
            "price_before_spp_change": float(ue.get("price_before_spp_change") or 0),
            "change_date": str(ue.get("change_date")) if ue.get("change_date") else None,
            "tariff_type": ue.get("tariff_type") or "box",
            "wb_club_discount_pct": float(ue.get("wb_club_discount_pct") or 0),

            # Логистика до клиента
            "volume_liters": _volume_liters,
            "volume_rounded": math.ceil(_volume_liters) if _volume_liters else 0,
            "fulfillment_model": _fulfillment_model,
            "fbs_warehouse": _fbs_warehouse or "",
            "delivery_to_client": _delivery_to_client,
            "logistics_tooltip": _logistics_tooltip,
        }

        items.append(apply_financial_formulas(item))

    # Сохраняем в Redis-кэш на 30 минут (ПОЛНЫЙ набор)
    _result_full = {"items": items, "total": len(items)}
    if not search:
        try:
            _redis.setex(_cache_key, 1800, _json.dumps(_result_full, ensure_ascii=False, default=str))
        except Exception:
            pass

    # Пагинация: если limit указан — обрезаем items
    if limit and limit > 0:
        return {"items": items[:limit], "total": len(items)}
    return _result_full


@router.get("/api/v1/nl/unit-economics")
async def get_unit_economics(
    org_id: str,
    search: Optional[str] = None,
    limit: Optional[int] = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Юнит Экономика — только для участников организации."""
    org_id = await resolve_org_id(org_id, db)
    await require_organization_role(org_id, Role.VIEWER, current_user, db)
    return await build_unit_economics(org_id, db, search=search, limit=limit)


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

class UnitEconBatchItem(BaseModel):
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


class UnitEconBatchSave(BaseModel):
    items: list[UnitEconBatchItem]



@router.post("/api/v1/nl/unit-economics")
async def save_unit_economics(
    data: UnitEconSave,
    org_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Сохранить ручные вводы Юнит Экономики"""
    org_id = await resolve_org_id(org_id, db)
    await require_organization_role(org_id, Role.ADMIN, current_user, db)
    from models.reference_book import ReferenceBook
    from datetime import datetime as dt_mod

    change_date = date.today()

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
        change_date=date.today(),
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
            "change_date": date.today(),
            "fulfillment_model": ins.excluded.fulfillment_model,
            "wb_club_discount_pct": ins.excluded.wb_club_discount_pct,
        }
    )
    await db.execute(stmt)
    await db.commit()
    try:
        import redis as _redis_lib
        _redis_lib.from_url("redis://redis:6379/0").delete(f"ue_cache:{org_id}")
    except Exception:
        pass
    return {"ok": True}


@router.post("/api/v1/nl/unit-economics/batch")
async def save_unit_economics_batch(
    payload: UnitEconBatchSave,
    org_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Пакетное сохранение ручных вводов Юнит Экономики"""
    org_id = await resolve_org_id(org_id, db)
    await require_organization_role(org_id, Role.ADMIN, current_user, db)
    from models.reference_book import ReferenceBook

    saved = 0
    for data in payload.items:
        entity_id_ue = data.entity_id if data.entity_id else None
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
            change_date=date.today(),
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
                "change_date": date.today(),
                "fulfillment_model": ins.excluded.fulfillment_model,
                "wb_club_discount_pct": ins.excluded.wb_club_discount_pct,
            }
        )
        await db.execute(stmt)
        saved += 1

    await db.commit()
    try:
        import redis as _redis_lib
        _redis_lib.from_url("redis://redis:6379/0").delete(f"ue_cache:{org_id}")
    except Exception:
        pass
    return {"ok": True, "saved": saved}


# ─── ОБНОВЛЕНИЕ ЦЕН ИЗ WB API ─────────────────────────────

# Кулдаун: минимальный интервал между обновлениями цен (секунды)
PRICES_REFRESH_COOLDOWN = 15 * 60  # 15 минут


@router.post("/api/v1/nl/prices/refresh")
async def refresh_prices_from_wb(org_id: str, db: AsyncSession = Depends(get_db)):
    """
    Обновить цены из WB Prices API и сохранить в reference_book.
    
    Тянет discountedPrice (цена со скидкой, реально на витрине),
    price (цена до скидки), discount (скидка %).
    
    Кулдаун 15 мин — защита от бана WB API.
    """
    from services.wb_api.keys import get_all_wb_keys as _get_keys
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from core.config import settings
    from datetime import datetime as _dt, timezone as _tz
    
    org_id = await resolve_org_id(org_id, db)
    
    # Проверяем кулдаун — когда последний раз обновляли цены
    cooldown_sql = "SELECT MAX(wb_prices_updated_at) FROM reference_book WHERE organization_id = :org AND wb_prices_updated_at IS NOT NULL"
    cooldown_result = await db.execute(text(cooldown_sql), {"org": org_id})
    last_update_row = cooldown_result.first()
    last_update = last_update_row[0] if last_update_row else None
    
    if last_update:
        now_utc = _dt.now(_tz.utc)
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=_tz.utc)
        elapsed = (now_utc - last_update).total_seconds()
        remaining = PRICES_REFRESH_COOLDOWN - elapsed
        if remaining > 0:
            mins = int(remaining // 60)
            secs = int(remaining % 60)
            raise HTTPException(429, f"Кулдаун. Доступно через {mins}:{secs:02d}")
    
    # Получаем API ключи
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    try:
        all_keys = await _get_keys(sf)
    finally:
        await engine.dispose()
    
    # Находим ключ для этой организации
    api_key = None
    for oid, key in all_keys:
        if oid == org_id:
            api_key = key
            break
    
    if not api_key:
        raise HTTPException(400, "Нет WB API ключа для этой организации")
    
    # Запрашиваем цены из WB API
    from services.wb_api.client import WBApiClient
    
    try:
        async with WBApiClient(api_key) as client:
            prices_data = await client.get_all_prices()
    except Exception as e:
        raise HTTPException(502, f"Ошибка WB API: {str(e)}")
    
    items = prices_data if isinstance(prices_data, list) else prices_data.get("items", [])
    if not items:
        raise HTTPException(404, "WB API вернул пустой список товаров")
    
    # Строим маппинг nm_id -> цены
    price_map = {}
    for item in items:
        nm_id = item.get("nmID") or item.get("nmId") or item.get("nm_id")
        if not nm_id:
            continue
        nm_id = int(nm_id)
        discount = item.get("discount", 0)
        sizes = item.get("sizes", [])
        if sizes:
            sz = sizes[0]
            price_retail = float(sz.get("price", 0))
            price_fact = float(sz.get("discountedPrice", 0))
            if price_retail > 0:
                price_map[nm_id] = {
                    "price_retail": price_retail,
                    "price_fact": price_fact,
                    "discount": discount,
                }
    
    # Обновляем reference_book
    now = _dt.now(_tz.utc)
    updated_count = 0
    
    for nm_id, prices in price_map.items():
        update_sql = (
            "UPDATE reference_book "
            "SET wb_price_fact = :pf, "
            "    wb_price_retail = :pr, "
            "    wb_discount_pct = :disc, "
            "    wb_prices_updated_at = :now "
            "WHERE organization_id = :org "
            "  AND nm_id = :nm "
            "  AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)"
        )
        result = await db.execute(text(update_sql), {
            "pf": prices["price_fact"],
            "pr": prices["price_retail"],
            "disc": prices["discount"],
            "now": now,
            "org": org_id,
            "nm": nm_id,
        })
        updated_count += result.rowcount
    
    await db.commit()
    
    return {
        "ok": True,
        "updated": updated_count,
        "total_items": len(items),
        "total_with_prices": len(price_map),
        "updated_at": now.isoformat(),
        "cooldown_seconds": PRICES_REFRESH_COOLDOWN,
    }


@router.get("/api/v1/nl/prices/last-refresh")
async def get_last_prices_refresh(org_id: str, db: AsyncSession = Depends(get_db)):
    """Когда последний раз обновляли цены из WB API"""
    org_id = await resolve_org_id(org_id, db)
    last_sql = "SELECT MAX(wb_prices_updated_at) FROM reference_book WHERE organization_id = :org AND wb_prices_updated_at IS NOT NULL"
    result = await db.execute(text(last_sql), {"org": org_id})
    row = result.first()
    last_update = row[0] if row else None
    
    remaining = 0
    if last_update:
        from datetime import datetime as _dt2, timezone as _tz2
        now_utc = _dt2.now(_tz2.utc)
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=_tz2.utc)
        elapsed = (now_utc - last_update).total_seconds()
        if elapsed < PRICES_REFRESH_COOLDOWN:
            remaining = int(PRICES_REFRESH_COOLDOWN - elapsed)
    
    return {
        "last_update": last_update.isoformat() if last_update else None,
        "cooldown_remaining_seconds": remaining,
        "can_refresh": remaining == 0,
    }


@router.get("/nl/v2", response_class=HTMLResponse)
async def nl_page():
    """НЛ — главная страница"""
    with open("templates/nl_v2.html", "r", encoding="utf-8") as f:
        html = f.read()
    response = HTMLResponse(html)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    return response
