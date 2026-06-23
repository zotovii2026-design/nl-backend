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




@router.get("/nl/v2", response_class=HTMLResponse)
async def nl_page():
    """НЛ — главная страница"""
    with open("templates/nl_v2.html", "r", encoding="utf-8") as f:
        html = f.read()
    response = HTMLResponse(html)
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    return response
