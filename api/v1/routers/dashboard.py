"""Dashboard API routes — products, dates, control metrics."""
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func
from typing import Optional
from datetime import datetime, date, timedelta

from core.database import get_db
from core.tenant_auth import require_query_organization_access
from services.reference import resolve_org_id
from models.raw_data import RawApiData
from models.sales_plan import SalesPlan, PlanType, Seasonality
from models.reference_book import ReferenceBook
from models.product_entity import ProductEntity, EntityBarcode
from models.raw_data import TechStatus

router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)

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



