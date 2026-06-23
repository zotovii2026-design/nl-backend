"""Marketer dashboard API routes."""
import json
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional

from core.database import get_db
from core.tenant_auth import require_query_organization_access

router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)

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



