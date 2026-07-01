"""
Рекламная статистика WB — маршруты вынесены из api/v1/nl.py
Контракты (URL, параметры, JSON) сохранены без изменений.
"""
import decimal as _dec
from datetime import date, datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.rate_limit import get_rate_limit_redis
from core.tenant_auth import require_query_organization_access
from services.reference import resolve_org_id
from tasks.celery_app import celery_app


router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)

VALID_AD_STATUSES = ("-1", "4", "7", "8", "9", "11")
DEFAULT_AD_STATUSES = ["9", "11"]
ADS_REFRESH_DAYS_BACK = 9
ADS_REFRESH_COOLDOWN_SECONDS = 60 * 60
AD_TYPE_NAMES = {
    "4": "Автоматическая",
    "5": "Поиск",
    "6": "Каталог",
    "7": "Таргет",
    "8": "Рек. в рекомендациях",
    "9": "Аукцион",
}
AD_BID_TYPE_NAMES = {
    "unified": "Единая",
    "manual": "Ручная",
}
AD_PAYMENT_TYPE_NAMES = {
    "cpm": "CPM",
    "cpc": "CPC",
}


def _sf(v):
    """Безопасное преобразование в float."""
    if v is None:
        return 0
    return float(v) if not isinstance(v, _dec.Decimal) else float(v)


def _parse_date_range(days: str, date_from: Optional[str], date_to: Optional[str]):
    """Парсинг диапазона дат — общий для обоих маршрутов."""
    if date_from and date_to:
        d_from = date_from
        d_to = date_to
    else:
        try:
            days_int = int(days)
        except Exception:
            days_int = 7
        if days_int == 1:
            d_from = date.today().isoformat()
            d_to = date.today().isoformat()
        elif days_int == 2:
            d = date.today() - timedelta(days=1)
            d_from = d.isoformat()
            d_to = d.isoformat()
        else:
            d_from = (date.today() - timedelta(days=days_int)).isoformat()
            d_to = date.today().isoformat()
    return (
        datetime.strptime(d_from, "%Y-%m-%d").date(),
        datetime.strptime(d_to, "%Y-%m-%d").date(),
    )


def _parse_statuses(statuses: Optional[str]):
    if not statuses:
        return DEFAULT_AD_STATUSES
    return [
        s.strip()
        for s in statuses.split(",")
        if s.strip() and s.strip() in VALID_AD_STATUSES
    ]


def _normalize_ad_attr(value):
    if value is None:
        return ""
    return str(value).strip().lower()


def _ad_type_label(raw_type, bid_type=None, payment_type=None):
    bid_label = AD_BID_TYPE_NAMES.get(_normalize_ad_attr(bid_type), "")
    payment_label = AD_PAYMENT_TYPE_NAMES.get(_normalize_ad_attr(payment_type), "")

    if bid_label and payment_label:
        return f"{bid_label} / {payment_label}"
    if bid_label:
        return bid_label
    if payment_label:
        return payment_label
    return AD_TYPE_NAMES.get(str(raw_type) if raw_type is not None else "", str(raw_type) if raw_type else "")


def _ads_product_filter_sql(
    product_status: Optional[str],
    product_class: Optional[str],
    brand: Optional[str],
    search: Optional[str],
    params: dict,
):
    """SQL join/where for product-level ads filters over the primary ad_stats_nm rows."""
    filters = []
    if product_status:
        params["product_status"] = product_status
        filters.append("COALESCE(rb.product_status, '') = :product_status")
    if product_class:
        params["product_class"] = product_class
        filters.append("COALESCE(rb.product_class, '') = :product_class")
    if brand:
        params["brand"] = brand
        filters.append("COALESCE(NULLIF(rb.brand, ''), pe.brand, '') = :brand")
    if search:
        params["search_like"] = f"%{search.strip()}%"
        filters.append("""(
            sn.nm_id::text ILIKE :search_like
            OR COALESCE(rb.vendor_code, pe.vendor_code, '') ILIKE :search_like
            OR COALESCE(pe.product_name, '') ILIKE :search_like
        )""")

    if not filters:
        return "", ""

    join_sql = """
        LEFT JOIN (
            SELECT DISTINCT ON (nm_id)
                   nm_id,
                   product_status,
                   product_class,
                   brand,
                   vendor_code
            FROM reference_book
            WHERE organization_id = :org
              AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)
            ORDER BY nm_id, valid_from DESC, created_at DESC NULLS LAST
        ) rb ON rb.nm_id = sn.nm_id
        LEFT JOIN (
            SELECT DISTINCT ON (nm_id)
                   nm_id,
                   vendor_code,
                   product_name,
                   brand
            FROM product_entities
            WHERE organization_id = :org
            ORDER BY nm_id, created_at DESC
        ) pe ON pe.nm_id = sn.nm_id
    """
    return join_sql, " AND " + " AND ".join(filters)


def _ads_total_revenue_filter_sql(
    product_status: Optional[str],
    product_class: Optional[str],
    brand: Optional[str],
    search: Optional[str],
    params: dict,
):
    """SQL join/where for total cabinet order revenue over raw WB orders."""
    filters = []
    if product_status:
        params["total_product_status"] = product_status
        filters.append("COALESCE(rb_total.product_status, '') = :total_product_status")
    if product_class:
        params["total_product_class"] = product_class
        filters.append("COALESCE(rb_total.product_class, '') = :total_product_class")
    if brand:
        params["total_brand"] = brand
        filters.append("COALESCE(NULLIF(rb_total.brand, ''), pe_total.brand, '') = :total_brand")
    if search:
        params["total_search_like"] = f"%{search.strip()}%"
        filters.append("""(
            ro.nm_id::text ILIKE :total_search_like
            OR COALESCE(ro.vendor_code, rb_total.vendor_code, pe_total.vendor_code, '') ILIKE :total_search_like
            OR COALESCE(pe_total.product_name, '') ILIKE :total_search_like
        )""")

    join_sql = ""
    if filters:
        join_sql = """
            LEFT JOIN (
                SELECT DISTINCT ON (nm_id)
                       nm_id,
                       product_status,
                       product_class,
                       brand,
                       vendor_code
                FROM reference_book
                WHERE organization_id = :org
                  AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)
                ORDER BY nm_id, valid_from DESC, created_at DESC NULLS LAST
            ) rb_total ON rb_total.nm_id = ro.nm_id
            LEFT JOIN (
                SELECT DISTINCT ON (nm_id)
                       nm_id,
                       vendor_code,
                       product_name,
                       brand
                FROM product_entities
                WHERE organization_id = :org
                ORDER BY nm_id, created_at DESC
            ) pe_total ON pe_total.nm_id = ro.nm_id
        """

    return join_sql, " AND " + " AND ".join(filters) if filters else ""


async def _get_total_orders_revenue_by_day(
    db: AsyncSession,
    params: dict,
    product_status: Optional[str],
    product_class: Optional[str],
    brand: Optional[str],
    search: Optional[str],
):
    """Total cabinet order revenue by day for overall DRR denominator."""
    total_params = dict(params)
    join_sql, filter_sql = _ads_total_revenue_filter_sql(
        product_status, product_class, brand, search, total_params
    )
    rows = await db.execute(text("""
        WITH raw_orders AS (
            SELECT
                r.target_date,
                COALESCE(o.elem->>'srid', '') AS srid,
                COALESCE(NULLIF(o.elem->>'nmId', '')::bigint, NULLIF(o.elem->>'nm_id', '')::bigint) AS nm_id,
                COALESCE(o.elem->>'supplierArticle', '') AS vendor_code,
                LEFT(o.elem->>'date', 10)::date AS order_date,
                COALESCE(
                    NULLIF(o.elem->>'priceWithDisc', '')::numeric,
                    NULLIF(o.elem->>'totalPrice', '')::numeric,
                    NULLIF(o.elem->>'price', '')::numeric,
                    0
                ) AS order_revenue
            FROM raw_api_data r
            CROSS JOIN LATERAL jsonb_array_elements(r.raw_response) AS o(elem)
            WHERE r.organization_id = :org
              AND r.api_method = 'orders'
              AND r.status = 'ok'
              AND r.target_date >= :d_from AND r.target_date <= :d_to
        ),
        dedup_non_empty_orders AS (
            SELECT DISTINCT ON (srid) *
            FROM raw_orders
            WHERE srid <> ''
            ORDER BY srid, target_date
        ),
        dedup_orders AS (
            SELECT *
            FROM dedup_non_empty_orders
            UNION ALL
            SELECT *
            FROM raw_orders
            WHERE srid = ''
        )
        SELECT ro.order_date,
               COALESCE(SUM(ro.order_revenue), 0) as orders_revenue
        FROM dedup_orders ro
        """ + join_sql + """
        WHERE ro.order_date >= :d_from AND ro.order_date <= :d_to
          """ + filter_sql + """
        GROUP BY ro.order_date
    """), total_params)
    return {str(r[0]): round(_sf(r[1]), 2) for r in rows}


async def _get_total_orders_revenue_by_nm(
    db: AsyncSession,
    org_id: str,
    d_from: date,
    d_to: date,
    nm_ids: list[int],
):
    """Total product order revenue by nm_id for product-level DRR."""
    if not nm_ids:
        return {}
    rows = await db.execute(text("""
        WITH raw_orders AS (
            SELECT
                r.target_date,
                COALESCE(o.elem->>'srid', '') AS srid,
                COALESCE(NULLIF(o.elem->>'nmId', '')::bigint, NULLIF(o.elem->>'nm_id', '')::bigint) AS nm_id,
                LEFT(o.elem->>'date', 10)::date AS order_date,
                COALESCE(
                    NULLIF(o.elem->>'priceWithDisc', '')::numeric,
                    NULLIF(o.elem->>'totalPrice', '')::numeric,
                    NULLIF(o.elem->>'price', '')::numeric,
                    0
                ) AS order_revenue
            FROM raw_api_data r
            CROSS JOIN LATERAL jsonb_array_elements(r.raw_response) AS o(elem)
            WHERE r.organization_id = :org
              AND r.api_method = 'orders'
              AND r.status = 'ok'
              AND r.target_date >= :d_from AND r.target_date <= :d_to
        ),
        dedup_non_empty_orders AS (
            SELECT DISTINCT ON (srid) *
            FROM raw_orders
            WHERE srid <> ''
            ORDER BY srid, target_date
        ),
        dedup_orders AS (
            SELECT *
            FROM dedup_non_empty_orders
            UNION ALL
            SELECT *
            FROM raw_orders
            WHERE srid = ''
        )
        SELECT ro.nm_id,
               COUNT(*) as orders_all,
               COALESCE(SUM(ro.order_revenue), 0) as revenue_all
        FROM dedup_orders ro
        WHERE ro.order_date >= :d_from AND ro.order_date <= :d_to
          AND ro.nm_id = ANY(:nm_ids)
        GROUP BY ro.nm_id
    """), {"org": org_id, "d_from": d_from, "d_to": d_to, "nm_ids": nm_ids})
    return {
        int(r[0]): {
            "orders_all": int(r[1] or 0),
            "revenue_all": round(_sf(r[2]), 2),
        }
        for r in rows
    }


def _ads_refresh_key(org_id: str) -> str:
    return f"nl:ads-refresh:{org_id}"


async def _ads_refresh_status(org_id: str, db: AsyncSession):
    cooldown_remaining = 0
    triggered_at = None
    try:
        redis = get_rate_limit_redis()
        ttl = await redis.ttl(_ads_refresh_key(org_id))
        cooldown_remaining = ttl if ttl and ttl > 0 else 0
        triggered_at = await redis.get(_ads_refresh_key(org_id))
    except Exception:
        cooldown_remaining = 0

    row = await db.execute(text("""
        SELECT
            (SELECT MAX(fetched_at)
             FROM raw_api_data
             WHERE organization_id = :org AND api_method = 'ad_balance' AND status = 'ok') AS last_sync_at,
            (SELECT MAX(stat_date)
             FROM ad_stats_nm
             WHERE organization_id = :org) AS last_stat_date
    """), {"org": org_id})
    latest = row.first()
    last_sync_at = latest[0] if latest else None
    last_stat_date = latest[1] if latest else None

    return {
        "can_refresh": cooldown_remaining == 0,
        "cooldown_remaining_seconds": int(cooldown_remaining),
        "cooldown_seconds": ADS_REFRESH_COOLDOWN_SECONDS,
        "days_back": ADS_REFRESH_DAYS_BACK,
        "last_sync_at": last_sync_at.isoformat() if last_sync_at else None,
        "last_stat_date": last_stat_date.isoformat() if last_stat_date else None,
        "triggered_at": triggered_at,
    }


@router.get("/api/v1/nl/ad-stats/refresh-status")
async def get_ad_stats_refresh_status(
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    org_id = await resolve_org_id(org_id, db)
    return await _ads_refresh_status(org_id, db)


@router.post("/api/v1/nl/ad-stats/refresh")
async def refresh_ad_stats(
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Запустить сбор рекламы за 9 дней с понятным кулдауном WB."""
    org_id = await resolve_org_id(org_id, db)
    state = await _ads_refresh_status(org_id, db)
    if not state["can_refresh"]:
        raise HTTPException(status_code=429, detail=state)

    triggered_at = datetime.now(timezone.utc).isoformat()
    try:
        redis = get_rate_limit_redis()
        await redis.set(
            _ads_refresh_key(org_id),
            triggered_at,
            ex=ADS_REFRESH_COOLDOWN_SECONDS,
        )
    except Exception:
        raise HTTPException(
            status_code=503,
            detail="Не удалось поставить таймер обновления рекламы",
        )

    task = celery_app.send_task(
        "wb.sched.ad_stats",
        kwargs={"days_back": ADS_REFRESH_DAYS_BACK, "org_id": org_id},
    )
    state = await _ads_refresh_status(org_id, db)
    return {
        "ok": True,
        "task_id": task.id,
        "message": f"Запущен сбор рекламы за {ADS_REFRESH_DAYS_BACK} дней",
        **state,
    }


@router.get("/api/v1/nl/ad-stats")
async def get_ad_stats(
    org_id: str,
    days: str = "7",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    statuses: Optional[str] = None,
    product_status: Optional[str] = None,
    product_class: Optional[str] = None,
    brand: Optional[str] = None,
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Рекламная статистика — из ad_stats_nm (те же данные что по артикулам)."""
    d_from, d_to = _parse_date_range(days, date_from, date_to)
    params = {"org": org_id, "d_from": d_from, "d_to": d_to}
    status_list = _parse_statuses(statuses)
    status_cond = "AND (c.wb_campaign_id IS NULL OR c.status = ANY(:statuses))"
    params["statuses"] = status_list
    product_join, product_cond = _ads_product_filter_sql(
        product_status, product_class, brand, search, params
    )
    total_revenue_by_day = await _get_total_orders_revenue_by_day(
        db, params, product_status, product_class, brand, search
    )
    total_revenue_period = round(sum(total_revenue_by_day.values()), 2)

    # ═══ Статистика по дням (из ad_stats_nm) ═══
    daily_rows = await db.execute(text("""
        SELECT sn.stat_date,
               SUM(sn.views) as views,
               SUM(sn.clicks) as clicks,
               SUM(sn.spent) as spent,
               SUM(sn.orders) as orders,
               SUM(sn.atbs) as atbs
        FROM ad_stats_nm sn
        LEFT JOIN ad_campaigns c ON c.wb_campaign_id = sn.wb_campaign_id
            AND c.organization_id = sn.organization_id
        """ + product_join + """
        WHERE sn.organization_id = :org
            AND sn.stat_date >= :d_from AND sn.stat_date <= :d_to
            AND sn.spent > 0
            """ + status_cond + """
            """ + product_cond + """
        GROUP BY sn.stat_date
        ORDER BY sn.stat_date DESC
    """), params)

    # ═══ ДРР по дням: sum_price из ad_stats_nm по составу РК ═══
    sum_price_by_day = await db.execute(text("""
        SELECT sn.stat_date, COALESCE(SUM(sn.sum_price), 0) as sum_price
        FROM ad_stats_nm sn
        LEFT JOIN ad_campaigns c ON c.wb_campaign_id = sn.wb_campaign_id
            AND c.organization_id = sn.organization_id
        """ + product_join + """
        WHERE sn.organization_id = :org
            AND sn.stat_date >= :d_from AND sn.stat_date <= :d_to
            AND sn.spent > 0
            """ + status_cond + """
            """ + product_cond + """
        GROUP BY sn.stat_date
    """), params)
    sp_by_date = {}
    for r in sum_price_by_day:
        sp_by_date[str(r[0])] = round(_sf(r[1]), 2)

    daily = []
    for r in daily_rows:
        views = int(r[1] or 0)
        clicks = int(r[2] or 0)
        spent = round(_sf(r[3]), 2)
        orders = int(r[4] or 0)
        atbs = int(r[5] or 0)
        date_str = str(r[0])
        sum_price_day = sp_by_date.get(date_str, 0)
        total_revenue_day = total_revenue_by_day.get(date_str, 0)
        drr_day = round(spent / sum_price_day * 100, 1) if sum_price_day else 0
        drr_total_day = round(spent / total_revenue_day * 100, 1) if total_revenue_day else 0
        daily.append({
            "date": date_str,
            "views": views,
            "clicks": clicks,
            "spent": spent,
            "ctr": round(clicks / views * 100, 2) if views else 0,
            "cpc": round(spent / clicks, 2) if clicks else 0,
            "orders": orders,
            "atbs": atbs,
            "cr": round(orders / clicks * 100, 2) if clicks else 0,
            "sum_price": sum_price_day,
            "total_revenue": total_revenue_day,
            "drr": drr_day,
            "drr_total": drr_total_day,
        })

    # ═══ Список кампаний: только фактические строки рекламы за период ═══
    camp_rows = await db.execute(text("""
        SELECT sn.wb_campaign_id,
               COALESCE(NULLIF(c.name, ''), 'Кампания ' || sn.wb_campaign_id::text) as name,
               c.status,
               c.type,
               c.bid_type,
               c.payment_type,
               SUM(sn.views) as views,
               SUM(sn.clicks) as clicks,
               SUM(sn.spent) as spent,
               SUM(sn.orders) as orders,
               SUM(sn.atbs) as atbs,
               COUNT(DISTINCT sn.nm_id) as nm_count,
               ARRAY_REMOVE(ARRAY_AGG(DISTINCT sn.nm_id), NULL) as nm_ids,
               COALESCE(SUM(sn.sum_price), 0) as sum_price,
               CASE WHEN c.wb_campaign_id IS NULL THEN 'stats_only' ELSE 'both' END as source_side
        FROM ad_stats_nm sn
        LEFT JOIN ad_campaigns c ON c.wb_campaign_id = sn.wb_campaign_id
            AND c.organization_id = sn.organization_id
        """ + product_join + """
        WHERE sn.organization_id = :org
            AND sn.stat_date >= :d_from AND sn.stat_date <= :d_to
            AND sn.spent > 0
            """ + status_cond + """
            """ + product_cond + """
        GROUP BY sn.wb_campaign_id, c.wb_campaign_id, c.name, c.status, c.type, c.bid_type, c.payment_type
        ORDER BY SUM(sn.spent) DESC, c.status, COALESCE(NULLIF(c.name, ''), 'Кампания ' || sn.wb_campaign_id::text)
    """), params)

    all_campaign_rows = list(camp_rows)
    all_nm_ids = sorted({int(nm) for r in all_campaign_rows for nm in (r[10] or []) if nm})
    all_campaign_ids = sorted({int(r[0]) for r in all_campaign_rows if r[0]})
    product_by_nm = {}
    if all_nm_ids:
        prod_row = await db.execute(text("""
            SELECT raw_response FROM raw_api_data
            WHERE api_method = 'products' AND organization_id = :org
            ORDER BY fetched_at DESC LIMIT 1
        """), {"org": org_id})
        pr = prod_row.first()
        if pr and pr[0]:
            cards_data = pr[0] if isinstance(pr[0], list) else (pr[0].get("cards", []) if isinstance(pr[0], dict) else [])
            nm_set = set(all_nm_ids)
            for cd in cards_data:
                if not isinstance(cd, dict):
                    continue
                nm = cd.get("nmID")
                if nm and int(nm) in nm_set:
                    photos = cd.get("photos") or []
                    photo_url = ""
                    if photos:
                        photo_url = photos[0].get("c246x328", "") or photos[0].get("big", "") or photos[0].get("hq", "")
                    product_by_nm[int(nm)] = {
                        "nm_id": int(nm),
                        "vendor_code": cd.get("vendorCode", ""),
                        "name": cd.get("title", ""),
                        "brand": cd.get("brand", ""),
                        "photo": photo_url,
                    }
        ref_rows = await db.execute(text("""
            SELECT DISTINCT ON (nm_id)
                   nm_id,
                   product_status,
                   product_class,
                   brand,
                   vendor_code
            FROM reference_book
            WHERE organization_id = :org
              AND nm_id = ANY(:nm_ids)
              AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)
            ORDER BY nm_id, valid_from DESC, created_at DESC NULLS LAST
        """), {"org": org_id, "nm_ids": all_nm_ids})
        for r in ref_rows:
            nm = int(r[0])
            product_by_nm.setdefault(nm, {"nm_id": nm})
            product_by_nm[nm]["product_status"] = r[1] or ""
            product_by_nm[nm]["product_class"] = r[2] or ""
            if r[3]:
                product_by_nm[nm]["brand"] = r[3]
            if r[4] and not product_by_nm[nm].get("vendor_code"):
                product_by_nm[nm]["vendor_code"] = r[4]

    products_by_campaign = {}
    if all_campaign_ids:
        camp_product_rows = await db.execute(text("""
            SELECT sn.wb_campaign_id,
                   sn.nm_id,
                   SUM(sn.spent) as spent,
                   SUM(sn.views) as views,
                   SUM(sn.clicks) as clicks,
                   SUM(sn.orders) as orders,
                   SUM(sn.atbs) as atbs,
                   COALESCE(SUM(sn.sum_price), 0) as sum_price
            FROM ad_stats_nm sn
            LEFT JOIN ad_campaigns c ON c.wb_campaign_id = sn.wb_campaign_id
                AND c.organization_id = sn.organization_id
            """ + product_join + """
            WHERE sn.organization_id = :org
                AND sn.stat_date >= :d_from AND sn.stat_date <= :d_to
                AND sn.wb_campaign_id = ANY(:campaign_ids)
                AND sn.spent > 0
                """ + status_cond + """
                """ + product_cond + """
            GROUP BY sn.wb_campaign_id, sn.nm_id
            ORDER BY SUM(sn.spent) DESC
        """), {**params, "campaign_ids": all_campaign_ids})
        for r in camp_product_rows:
            cid = int(r[0])
            nm_id = int(r[1])
            info = product_by_nm.get(nm_id, {"nm_id": nm_id})
            product = {
                **info,
                "nm_id": nm_id,
                "spent_share": round(_sf(r[2]), 2),
                "views": int(r[3] or 0),
                "clicks": int(r[4] or 0),
                "orders": int(r[5] or 0),
                "atbs": int(r[6] or 0),
                "sum_price": round(_sf(r[7]), 2),
            }
            products_by_campaign.setdefault(cid, []).append(product)

    campaigns = []
    for r in all_campaign_rows:
        views = int(r[6] or 0)
        clicks = int(r[7] or 0)
        spent = round(_sf(r[8]), 2)
        orders = int(r[9] or 0)
        atbs = int(r[10] or 0)
        nm_count = int(r[11] or 0)
        nm_ids = [int(n) for n in (r[12] or []) if n]

        # Инфо о товарах
        products = products_by_campaign.get(int(r[0]), [])
        sum_price_val = round(_sf(r[13]), 2)

        drr_rk = round(spent / sum_price_val * 100, 1) if sum_price_val else 0
        drr_total = round(spent / total_revenue_period * 100, 1) if total_revenue_period else 0

        campaigns.append({
            "campaign_id": int(r[0]) if r[0] else None,
            "name": r[1] or "Без названия",
            "status": str(r[2]) if r[2] else "",
            "type": str(r[3]) if r[3] else "",
            "type_label": _ad_type_label(r[3], r[4], r[5]),
            "bid_type": str(r[4]) if r[4] else "",
            "payment_type": str(r[5]) if r[5] else "",
            "views": views,
            "clicks": clicks,
            "spent": spent,
            "ctr": round(clicks / views * 100, 2) if views else 0,
            "cpc": round(spent / clicks, 2) if clicks else 0,
            "orders": orders,
            "atbs": atbs,
            "nm_count": nm_count,
            "products": products,
            "sum_price": sum_price_val,
            "cr": round(orders / clicks * 100, 2) if clicks else 0,
            "total_orders": orders,
            "total_revenue": sum_price_val,
            "total_revenue_period": total_revenue_period,
            "drr": drr_rk,
            "drr_total": drr_total,
            "source_side": r[14] or "both",
        })

    # ═══ Баланс ═══
    balance = None
    bal_row = await db.execute(text("""
        SELECT raw_response FROM raw_api_data
        WHERE api_method = 'ad_balance' AND status = 'ok' AND organization_id = :org
        ORDER BY fetched_at DESC LIMIT 1
    """), {"org": org_id})
    br = bal_row.first()
    if br and br[0]:
        balance = br[0]

    # ═══ Итого ═══
    totals = {"views": 0, "clicks": 0, "spent": 0, "orders": 0, "atbs": 0}
    for d in daily:
        for k in totals:
            totals[k] += d.get(k, 0)
    totals["ctr"] = round(totals["clicks"] / totals["views"] * 100, 2) if totals["views"] else 0
    totals["cpc"] = round(totals["spent"] / totals["clicks"], 2) if totals["clicks"] else 0
    totals["cr"] = round(totals["orders"] / totals["clicks"] * 100, 2) if totals["clicks"] else 0
    all_sum_price = sum(d.get("sum_price", 0) for d in daily)
    totals["drr"] = round(totals["spent"] / all_sum_price * 100, 1) if all_sum_price else 0
    totals["sum_price"] = round(all_sum_price, 2)
    totals["total_revenue"] = total_revenue_period
    totals["drr_total"] = round(totals["spent"] / total_revenue_period * 100, 1) if total_revenue_period else 0

    return {
        "daily": daily,
        "campaigns": campaigns,
        "top_campaigns": campaigns[:20],
        "totals": totals,
        "balance": balance,
        "statuses": status_list,
    }


@router.get("/api/v1/nl/ad-stats/by-art")
async def get_ad_stats_by_art(
    org_id: str,
    days: str = "30",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    statuses: Optional[str] = None,
    product_status: Optional[str] = None,
    product_class: Optional[str] = None,
    brand: Optional[str] = None,
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Рекламная статистика по артикулам — данные из ad_stats_nm (разбивка WB по nm_id)."""
    d_from, d_to = _parse_date_range(days, date_from, date_to)
    params = {"org": org_id, "d_from": d_from, "d_to": d_to}

    status_list = _parse_statuses(statuses)
    status_cond = "AND (c.wb_campaign_id IS NULL OR c.status = ANY(:statuses))"
    params["statuses"] = status_list
    product_join, product_cond = _ads_product_filter_sql(
        product_status, product_class, brand, search, params
    )
    total_revenue_by_day = await _get_total_orders_revenue_by_day(
        db, params, product_status, product_class, brand, search
    )
    total_revenue_period = round(sum(total_revenue_by_day.values()), 2)

    # ═══ Основной запрос: агрегация по nm_id из ad_stats_nm ═══
    rows = await db.execute(text("""
        SELECT
            sn.nm_id,
            SUM(sn.spent) as total_spent,
            SUM(sn.views) as total_views,
            SUM(sn.clicks) as total_clicks,
            SUM(sn.orders) as total_orders,
            SUM(sn.atbs) as total_atbs
        FROM ad_stats_nm sn
        LEFT JOIN ad_campaigns c ON c.wb_campaign_id = sn.wb_campaign_id
            AND c.organization_id = sn.organization_id
        """ + product_join + """
        WHERE sn.organization_id = :org
            AND sn.stat_date >= :d_from AND sn.stat_date <= :d_to
            AND sn.spent > 0
            """ + status_cond + """
            """ + product_cond + """
        GROUP BY sn.nm_id
        HAVING SUM(sn.spent) > 0
        ORDER BY SUM(sn.spent) DESC
    """), params)

    art_data = {}
    for r in rows:
        nm_id = int(r[0])
        art_data[nm_id] = {
            "spent": round(_sf(r[1]), 2),
            "views": int(r[2] or 0),
            "clicks": int(r[3] or 0),
            "orders": int(r[4] or 0),
            "atbs": int(r[5] or 0),
        }

    all_nm_ids = list(art_data.keys())
    product_orders_by_nm = await _get_total_orders_revenue_by_nm(
        db, org_id, d_from, d_to, all_nm_ids
    )

    # ═══ Для каждого артикула — список РК с данными по этому nm_id ═══
    nm_campaigns = {}
    if all_nm_ids:
        camp_rows = await db.execute(text("""
            SELECT
                sn.nm_id,
                sn.wb_campaign_id,
                COALESCE(c.name, 'Кампания ' || sn.wb_campaign_id::text) as name,
                c.status,
                c.type,
                c.bid_type,
                c.payment_type,
                SUM(sn.spent) as camp_spent,
                SUM(sn.views) as camp_views,
                SUM(sn.clicks) as camp_clicks,
                SUM(sn.orders) as camp_orders,
                SUM(sn.atbs) as camp_atbs,
                SUM(sn.sum_price) as camp_sum_price
            FROM ad_stats_nm sn
            LEFT JOIN ad_campaigns c ON c.wb_campaign_id = sn.wb_campaign_id
                AND c.organization_id = sn.organization_id
            """ + product_join + """
            WHERE sn.organization_id = :org
                AND sn.stat_date >= :d_from AND sn.stat_date <= :d_to
                AND sn.nm_id = ANY(:nm_ids)
                AND sn.spent > 0
                """ + status_cond + """
                """ + product_cond + """
            GROUP BY sn.nm_id, sn.wb_campaign_id, c.name, c.status, c.type, c.bid_type, c.payment_type
            HAVING SUM(sn.spent) > 0
            ORDER BY SUM(sn.spent) DESC
        """), {**params, "nm_ids": all_nm_ids})

        for r in camp_rows:
            nm_id = int(r[0])
            if nm_id not in nm_campaigns:
                nm_campaigns[nm_id] = []
            nm_campaigns[nm_id].append({
                "campaign_id": int(r[1]),
                "name": r[2] or "Без названия",
                "status": str(r[3]) if r[3] else "",
                "type": str(r[4]) if r[4] else "",
                "type_label": _ad_type_label(r[4], r[5], r[6]),
                "bid_type": str(r[5]) if r[5] else "",
                "payment_type": str(r[6]) if r[6] else "",
                "spent_share": round(_sf(r[7]), 2),
                "views": int(r[8] or 0),
                "clicks": int(r[9] or 0),
                "ctr": round((int(r[9] or 0) / int(r[8] or 0)) * 100, 2) if int(r[8] or 0) else 0,
                "orders": int(r[10] or 0),
                "atbs": int(r[11] or 0),
                "sum_price": round(_sf(r[12]), 2),
            })

    # ═══ Собираем items ═══
    items = []
    for nm_id in all_nm_ids:
        d = art_data[nm_id]
        spent = d["spent"]
        views = d["views"]
        clicks = d["clicks"]
        orders = d["orders"]
        campaigns = nm_campaigns.get(nm_id, [])
        sum_price_art = sum(c.get("sum_price", 0) for c in campaigns) if campaigns else 0
        product_orders = product_orders_by_nm.get(nm_id, {"orders_all": 0, "revenue_all": 0})
        total_orders_product = product_orders["orders_all"]
        total_revenue_product = product_orders["revenue_all"]
        drr_art = round(spent / sum_price_art * 100, 1) if sum_price_art else 0
        drr_product = round(spent / total_revenue_product * 100, 1) if total_revenue_product else 0
        items.append({
            "nm_id": nm_id,
            "spent": spent,
            "views": views,
            "clicks": clicks,
            "ctr": round(clicks / views * 100, 2) if views else 0,
            "cpc": round(spent / clicks, 2) if clicks else 0,
            "orders": orders,
            "cr": round(orders / clicks * 100, 2) if clicks else 0,
            "campaigns_count": len(campaigns),
            "campaigns": campaigns,
            "total_orders": orders,
            "total_revenue": round(sum_price_art, 2),
            "total_orders_product": total_orders_product,
            "total_revenue_product": total_revenue_product,
            "drr": drr_art,
            "drr_product": drr_product,
        })

    # ═══ Информация о товарах (название, фото, vendor_code) ═══
    nm_to_info = {}
    if all_nm_ids:
        prod_row = await db.execute(text("""
            SELECT raw_response FROM raw_api_data
            WHERE api_method = 'products' AND organization_id = :org
            ORDER BY fetched_at DESC LIMIT 1
        """), {"org": org_id})
        pr = prod_row.first()
        if pr and pr[0]:
            cards_data = pr[0] if isinstance(pr[0], list) else (pr[0].get("cards", []) if isinstance(pr[0], dict) else [])
            nm_set = set(all_nm_ids)
            for c in cards_data:
                if not isinstance(c, dict):
                    continue
                nm = c.get("nmID")
                if nm and int(nm) in nm_set:
                    photos = c.get("photos") or []
                    photo_url = ""
                    if photos:
                        photo_url = photos[0].get("c246x328", "") or photos[0].get("big", "") or photos[0].get("hq", "")
                    nm_to_info[int(nm)] = {
                        "name": c.get("title", ""),
                        "brand": c.get("brand", ""),
                        "vendor_code": c.get("vendorCode", ""),
                        "photo": photo_url,
                    }
        ref_rows = await db.execute(text("""
            SELECT DISTINCT ON (nm_id)
                   nm_id,
                   product_status,
                   product_class,
                   brand,
                   vendor_code
            FROM reference_book
            WHERE organization_id = :org
              AND nm_id = ANY(:nm_ids)
              AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)
            ORDER BY nm_id, valid_from DESC, created_at DESC NULLS LAST
        """), {"org": org_id, "nm_ids": all_nm_ids})
        for r in ref_rows:
            nm = int(r[0])
            nm_to_info.setdefault(nm, {})
            nm_to_info[nm]["product_status"] = r[1] or ""
            nm_to_info[nm]["product_class"] = r[2] or ""
            if r[3]:
                nm_to_info[nm]["brand"] = r[3]
            if r[4] and not nm_to_info[nm].get("vendor_code"):
                nm_to_info[nm]["vendor_code"] = r[4]

    for item in items:
        info = nm_to_info.get(item["nm_id"], {})
        item["name"] = info.get("name", "")
        item["brand"] = info.get("brand", "")
        item["vendor_code"] = info.get("vendor_code", "")
        item["photo"] = info.get("photo", "")
        item["product_status"] = info.get("product_status", "")
        item["product_class"] = info.get("product_class", "")

    totals = {
        "spent": round(sum(i["spent"] for i in items), 2),
        "views": sum(i["views"] for i in items),
        "clicks": sum(i["clicks"] for i in items),
        "orders": sum(i["orders"] for i in items),
        "atbs": sum(sum(c.get("atbs", 0) for c in i.get("campaigns", [])) for i in items),
        "ctr": round(sum(i["clicks"] for i in items) / max(sum(i["views"] for i in items), 1) * 100, 2),
        "cpc": round(sum(i["spent"] for i in items) / max(sum(i["clicks"] for i in items), 1), 2),
        "cr": round(sum(i["orders"] for i in items) / max(sum(i["clicks"] for i in items), 1) * 100, 2),
        "items_count": len(items),
        "campaigns_count": sum(i["campaigns_count"] for i in items),
        "total_orders": sum(i["total_orders"] for i in items),
        "total_revenue": round(sum(i["total_revenue"] for i in items), 2),
        "total_orders_product": sum(i["total_orders_product"] for i in items),
        "total_revenue_product": round(sum(i["total_revenue_product"] for i in items), 2),
        "total_revenue_period": total_revenue_period,
        "sum_price": round(sum(c.get("sum_price", 0) for i in items for c in i.get("campaigns", [])), 2),
        "drr": round(sum(i["spent"] for i in items) / max(sum(c.get("sum_price", 0) for i in items for c in i.get("campaigns", [])), 1) * 100, 1),
        "drr_product": round(sum(i["spent"] for i in items) / max(sum(i["total_revenue_product"] for i in items), 1) * 100, 1),
        "drr_total": round(sum(i["spent"] for i in items) / total_revenue_period * 100, 1) if total_revenue_period else 0,
        "statuses": status_list,
    }

    return {"items": items, "totals": totals}
