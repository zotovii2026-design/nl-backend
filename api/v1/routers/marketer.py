"""Marketer dashboard API routes."""
import json
from datetime import date as date_cls
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional

from core.database import get_db
from core.tenant_auth import require_query_organization_access

router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)


MARKETER_ENTITY_LEVELS = [
    {
        "key": "store",
        "label": "Магазин",
        "default_scope": "all_filtered",
        "description": "Верхний график по всей выборке из стандартных фильтров.",
    },
    {
        "key": "product",
        "label": "Товар",
        "default_scope": "filtered_products",
        "description": "Нижние графики по товарам, которые попали в фильтр.",
    },
    {
        "key": "size",
        "label": "Размер",
        "default_scope": "grouped_by_product",
        "description": "Размерные товары по умолчанию сгруппированы; размеры раскрываются кнопкой.",
    },
]


MARKETER_FILTER_CONTRACT = {
    "source": "top_page_filters",
    "filters": ["organization", "date_range", "brand", "group", "product", "article"],
    "top_chart": {
        "no_product_filter": "store_filtered_aggregate",
        "with_product_filter": "average_of_filtered_products",
        "with_group_filter": "average_of_group_products",
    },
    "lower_charts": {
        "scope": "products_matching_current_filters",
        "default_metrics": "mirror_top_chart",
        "can_override_metrics": True,
    },
    "selected_metric_set": {
        "persist": True,
        "levels": ["user", "organization"],
    },
}


MARKETER_METRIC_CATALOG = [
    {
        "key": "economy",
        "label": "Экономика",
        "metrics": [
            {"key": "lifetime_profit", "label": "Сколько заработали всего за всю историю", "unit": "rub", "aggregation": "sum"},
            {"key": "period_profit", "label": "Сколько за период", "unit": "rub", "aggregation": "sum"},
            {"key": "price", "label": "Цена", "unit": "rub", "aggregation": "avg"},
            {"key": "price_spp", "label": "Цена с СПП", "unit": "rub", "aggregation": "avg"},
            {"key": "cost_price", "label": "Себестоимость", "unit": "rub", "aggregation": "avg"},
            {"key": "cost_price_pct", "label": "Себестоимость в % от цены товара", "unit": "percent", "aggregation": "avg"},
            {"key": "margin_income", "label": "Маржинальный доход", "unit": "rub", "aggregation": "sum"},
            {"key": "penalties", "label": "Штрафы", "unit": "rub", "aggregation": "sum"},
            {"key": "acquiring", "label": "Эквайринг", "unit": "rub", "aggregation": "sum"},
            {"key": "extra_costs", "label": "Доп расходы", "unit": "rub", "aggregation": "sum"},
            {"key": "marketplace_costs_pct_total", "label": "% затрат на маркетплейс итого", "unit": "percent", "aggregation": "avg"},
            {"key": "taxes_total", "label": "Налоги итого", "unit": "rub", "aggregation": "sum"},
            {"key": "marketplace_costs_pct_unit", "label": "% затрат на маркетплейс на 1 ед", "unit": "percent", "aggregation": "avg"},
            {"key": "stock_value", "label": "Стоимость остатка", "unit": "rub", "aggregation": "sum"},
            {"key": "roi", "label": "Коэффициент рентабельности инвестиций", "unit": "ratio", "aggregation": "avg"},
            {"key": "pricing_strategy", "label": "Стратегия ценообразования", "unit": "text", "aggregation": "last"},
        ],
    },
    {
        "key": "logistics_layout",
        "label": "Логистика и раскладка",
        "metrics": [
            {"key": "logistics_per_unit", "label": "В руб на 1 ед", "unit": "rub", "aggregation": "avg"},
            {"key": "logistics_with_buyout", "label": "В руб с учетом выкупа", "unit": "rub", "aggregation": "avg"},
            {"key": "buyout_pct", "label": "Выкуп в %", "unit": "percent", "aggregation": "avg"},
            {"key": "stock_qty", "label": "Остаток", "unit": "pcs", "aggregation": "sum"},
            {"key": "warehouse_count", "label": "Кол-во складов", "unit": "pcs", "aggregation": "sum"},
            {"key": "sold_costs_total", "label": "Затраты итого от проданного", "unit": "rub", "aggregation": "sum"},
            {"key": "deficit_surplus", "label": "Дефицит / профицит", "unit": "pcs", "aggregation": "sum"},
            {"key": "local_sales", "label": "Локальные продажи", "unit": "pcs", "aggregation": "sum"},
            {"key": "storage", "label": "Хранение", "unit": "rub", "aggregation": "sum"},
            {"key": "order_costs_total", "label": "Затраты итого от заказа", "unit": "rub", "aggregation": "sum"},
            {"key": "to_order_total", "label": "Итого к заказу", "unit": "pcs", "aggregation": "sum"},
        ],
    },
    {
        "key": "ads_traffic",
        "label": "Реклама и трафик",
        "metrics": [
            {"key": "views", "label": "Показы", "unit": "pcs", "aggregation": "sum"},
            {"key": "ad_views", "label": "Показы в РК", "unit": "pcs", "aggregation": "sum"},
            {"key": "ctr_total", "label": "CTR итого", "unit": "percent", "aggregation": "avg"},
            {"key": "ctr_ad", "label": "CTR в РК", "unit": "percent", "aggregation": "avg"},
            {"key": "cpm", "label": "CPM", "unit": "rub", "aggregation": "avg"},
            {"key": "cpm_min", "label": "CPM минимальный", "unit": "rub", "aggregation": "min"},
            {"key": "cpc", "label": "CPC", "unit": "rub", "aggregation": "avg"},
            {"key": "click_to_purchase", "label": "Клик покупка (какой клик = покупка)", "unit": "ratio", "aggregation": "avg"},
            {"key": "organic_views_pct", "label": "Показы органика в % от всего трафика", "unit": "percent", "aggregation": "avg"},
            {"key": "views_total", "label": "Показы итого", "unit": "pcs", "aggregation": "sum"},
            {"key": "ad_strategy", "label": "Рекламная стратегия (номер)", "unit": "text", "aggregation": "last"},
            {"key": "cv", "label": "CV", "unit": "percent", "aggregation": "avg"},
            {"key": "ad_cost_sum", "label": "Сумма затрат", "unit": "rub", "aggregation": "sum"},
            {"key": "drr", "label": "ДРР", "unit": "percent", "aggregation": "avg"},
            {"key": "cart_count", "label": "Кол-во корзин", "unit": "pcs", "aggregation": "sum"},
            {"key": "orders_count", "label": "Кол-во заказов", "unit": "pcs", "aggregation": "sum"},
            {"key": "query_frequency_sum", "label": "Суммарная частота запросов", "unit": "pcs", "aggregation": "sum"},
            {"key": "marketplace_card_count", "label": "Кол-во карточек на МП", "unit": "pcs", "aggregation": "sum"},
        ],
    },
    {
        "key": "other",
        "label": "Иное",
        "metrics": [
            {"key": "seo_change_milestone", "label": "Веха смена SEO по стратегии", "unit": "event", "aggregation": "event"},
            {"key": "extra_fields_change_milestone", "label": "Веха смена доп полей", "unit": "event", "aggregation": "event"},
            {"key": "main_photo_change_milestone", "label": "Веха смена главного фото", "unit": "event", "aggregation": "event"},
            {"key": "infographic_change_milestone", "label": "Веха смены инфографики", "unit": "event", "aggregation": "event"},
            {"key": "promo_state", "label": "В акции и какой (да/нет)", "unit": "text", "aggregation": "last"},
            {"key": "seasonality", "label": "Сезонность товара", "unit": "index", "aggregation": "avg"},
            {"key": "query_trend", "label": "Тренд запросов", "unit": "index", "aggregation": "avg"},
            {"key": "sales_plan", "label": "План продаж", "unit": "pcs", "aggregation": "sum"},
            {"key": "sales_fact", "label": "Факт продаж", "unit": "pcs", "aggregation": "sum"},
        ],
    },
]


MARKETER_AI_PANEL_CONTRACT = {
    "position": "right",
    "enabled": True,
    "fields": ["summary", "recommendations", "source", "generated_at"],
    "note": "Reserved for n8n/AI output; chart analysis remains usable without it.",
}


MARKETER_CHART_METRICS = {
    "period_profit": {
        "label": "Сколько за период",
        "unit": "rub",
        "expr": "SUM(COALESCE(ts.price_discount, ts.price_spp, ts.price, 0) * COALESCE(ts.buyouts_count, 0))",
        "point_aggregation": "sum",
    },
    "price": {
        "label": "Цена",
        "unit": "rub",
        "expr": "AVG(ts.price)",
        "point_aggregation": "avg",
    },
    "price_spp": {
        "label": "Цена с СПП",
        "unit": "rub",
        "expr": "AVG(ts.price_spp)",
        "point_aggregation": "avg",
    },
    "buyout_pct": {
        "label": "Выкуп в %",
        "unit": "percent",
        "expr": "CASE WHEN SUM(COALESCE(ts.orders_count, 0)) > 0 THEN SUM(COALESCE(ts.buyouts_count, 0))::numeric / SUM(COALESCE(ts.orders_count, 0)) * 100 ELSE 0 END",
        "point_aggregation": "ratio",
    },
    "stock_qty": {
        "label": "Остаток",
        "unit": "pcs",
        "expr": "SUM(COALESCE(ts.stock_qty, 0))",
        "point_aggregation": "sum",
    },
    "views": {
        "label": "Показы",
        "unit": "pcs",
        "expr": "SUM(COALESCE(ts.impressions, 0))",
        "point_aggregation": "sum",
    },
    "clicks": {
        "label": "Клики",
        "unit": "pcs",
        "expr": "SUM(COALESCE(ts.clicks, 0))",
        "point_aggregation": "sum",
    },
    "ctr_total": {
        "label": "CTR итого",
        "unit": "percent",
        "expr": "CASE WHEN SUM(COALESCE(ts.impressions, 0)) > 0 THEN SUM(COALESCE(ts.clicks, 0))::numeric / SUM(COALESCE(ts.impressions, 0)) * 100 ELSE 0 END",
        "point_aggregation": "ratio",
    },
    "ad_cost_sum": {
        "label": "Сумма затрат",
        "unit": "rub",
        "expr": "SUM(COALESCE(ts.ad_cost, 0))",
        "point_aggregation": "sum",
    },
    "orders_count": {
        "label": "Кол-во заказов",
        "unit": "pcs",
        "expr": "SUM(COALESCE(ts.orders_count, 0))",
        "point_aggregation": "sum",
    },
    "sales_fact": {
        "label": "Факт продаж",
        "unit": "pcs",
        "expr": "SUM(COALESCE(ts.buyouts_count, 0))",
        "point_aggregation": "sum",
    },
}


@router.get("/api/v1/nl/marketer/metric-catalog")
async def get_marketer_metric_catalog(org_id: str):
    """Стол маркетолога — расширяемый каталог разделов и метрик графика."""
    return {
        "sections": MARKETER_METRIC_CATALOG,
        "chart_metrics": _metric_meta(list(MARKETER_CHART_METRICS.keys())),
        "entity_levels": MARKETER_ENTITY_LEVELS,
        "filter_contract": MARKETER_FILTER_CONTRACT,
        "ai_panel": MARKETER_AI_PANEL_CONTRACT,
    }


def _split_ints(value: Optional[str]) -> list[int]:
    if not value:
        return []
    items = []
    for raw in value.split(","):
        raw = raw.strip()
        if not raw:
            continue
        try:
            items.append(int(raw))
        except ValueError:
            continue
    return items


def _split_strings(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _resolve_chart_metrics(metrics: Optional[str]) -> list[str]:
    requested = _split_strings(metrics)
    if not requested:
        requested = ["orders_count", "stock_qty", "price", "price_spp", "buyout_pct", "views", "ctr_total"]
    resolved = [key for key in requested if key in MARKETER_CHART_METRICS]
    return resolved or ["orders_count", "stock_qty", "price", "price_spp"]


def _metric_meta(metric_keys: list[str]) -> list[dict]:
    return [
        {
            "key": key,
            "label": MARKETER_CHART_METRICS[key]["label"],
            "unit": MARKETER_CHART_METRICS[key]["unit"],
            "aggregation": MARKETER_CHART_METRICS[key]["point_aggregation"],
        }
        for key in metric_keys
    ]


def _build_marketer_filter_sql(
    *,
    nm_ids: list[int],
    brand: Optional[str],
    group: Optional[str],
    category: Optional[str],
    article: Optional[str],
) -> tuple[str, dict]:
    where = []
    params = {}
    if nm_ids:
        where.append("ts.nm_id = ANY(CAST(:nm_ids AS integer[]))")
        params["nm_ids"] = nm_ids
    if brand:
        where.append("LOWER(COALESCE(ref.brand, pe.brand, '')) = LOWER(:brand)")
        params["brand"] = brand
    if group:
        where.append("(LOWER(COALESCE(ref.product_class, '')) = LOWER(:group) OR LOWER(COALESCE(ref.product_status, '')) = LOWER(:group))")
        params["group"] = group
    if category:
        where.append("LOWER(COALESCE(ref.subject_name, pe.subject_name, '')) = LOWER(:category)")
        params["category"] = category
    if article:
        where.append("(CAST(ts.nm_id AS text) ILIKE :article_like OR COALESCE(ts.vendor_code, pe.vendor_code, '') ILIKE :article_like OR COALESCE(ts.product_name, pe.product_name, '') ILIKE :article_like)")
        params["article_like"] = f"%{article}%"
    return ("\n      AND " + "\n      AND ".join(where)) if where else "", params


def _format_chart_points(rows, metric_keys: list[str]) -> list[dict]:
    points = []
    for row in rows:
        mapping = row._mapping
        point = {"date": str(mapping["date"])}
        for key in metric_keys:
            value = mapping[key]
            point[key] = float(value) if value is not None else 0
        points.append(point)
    return points


def _resolve_chart_date(value: Optional[str], default_date):
    if not value:
        return default_date
    try:
        return date_cls.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(400, "date must be in YYYY-MM-DD format") from exc


@router.get("/api/v1/nl/marketer/chart-data")
async def get_marketer_chart_data(
    org_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    nm_ids: Optional[str] = None,
    brand: Optional[str] = None,
    group: Optional[str] = None,
    category: Optional[str] = None,
    article: Optional[str] = None,
    metrics: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Стол маркетолога — данные верхнего и нижних графиков по текущим фильтрам."""
    from datetime import date, timedelta

    metric_keys = _resolve_chart_metrics(metrics)
    product_ids = _split_ints(nm_ids)
    has_selection_filter = bool(product_ids or brand or group or category or article)
    filter_sql, filter_params = _build_marketer_filter_sql(
        nm_ids=product_ids,
        brand=brand,
        group=group,
        category=category,
        article=article,
    )
    params = {
        "org": org_id,
        "date_from": _resolve_chart_date(date_from, date.today() - timedelta(days=29)),
        "date_to": _resolve_chart_date(date_to, date.today()),
        **filter_params,
    }

    latest_ref_sql = """
        SELECT DISTINCT ON (organization_id, nm_id)
            organization_id, nm_id, product_class, product_status, brand, subject_name
        FROM reference_book
        WHERE organization_id = :org
        ORDER BY organization_id, nm_id, valid_from DESC
    """
    base_from_sql = f"""
        FROM tech_status ts
        LEFT JOIN product_entities pe ON pe.id = ts.entity_id
        LEFT JOIN ({latest_ref_sql}) ref
            ON ref.organization_id = ts.organization_id
           AND ref.nm_id = ts.nm_id
        WHERE ts.organization_id = :org
          AND ts.target_date BETWEEN CAST(:date_from AS date) AND CAST(:date_to AS date)
          AND ts.nm_id IS NOT NULL
          {filter_sql}
    """

    product_metric_columns = ",\n               ".join(
        f"{MARKETER_CHART_METRICS[key]['expr']} AS {key}" for key in metric_keys
    )
    average_columns = ",\n           ".join(
        f"AVG(product_daily.{key}) AS {key}" for key in metric_keys
    )
    if has_selection_filter:
        top_scope = "average_of_filtered_products"
        top_rows = await db.execute(text(f"""
            WITH product_daily AS (
                SELECT ts.target_date AS date,
                       ts.nm_id,
                       {product_metric_columns}
                {base_from_sql}
                GROUP BY ts.target_date, ts.nm_id
            )
            SELECT date,
                   {average_columns}
            FROM product_daily
            GROUP BY date
            ORDER BY date
        """), params)
    else:
        top_scope = "store_filtered_aggregate"
        top_rows = await db.execute(text(f"""
            SELECT ts.target_date AS date,
                   {product_metric_columns}
            {base_from_sql}
            GROUP BY ts.target_date
            ORDER BY ts.target_date
        """), params)

    product_rows = await db.execute(text(f"""
        WITH product_daily AS (
            SELECT ts.target_date AS date,
                   ts.nm_id,
                   MAX(COALESCE(ts.vendor_code, pe.vendor_code, '')) AS vendor_code,
                   MAX(COALESCE(ts.product_name, pe.product_name, '')) AS product_name,
                   MAX(COALESCE(ts.photo_main, pe.photo_main, '')) AS photo,
                   MAX(COALESCE(ref.brand, pe.brand, '')) AS brand,
                   MAX(COALESCE(ref.subject_name, pe.subject_name, '')) AS category,
                   BOOL_OR(COALESCE(pe.size_name, '') NOT IN ('', '0', 'ONE SIZE')) AS has_sizes,
                   {product_metric_columns}
            {base_from_sql}
            GROUP BY ts.target_date, ts.nm_id
        )
        SELECT *
        FROM product_daily
        ORDER BY nm_id, date
    """), params)

    products = {}
    for row in product_rows:
        m = row._mapping
        nm_id = int(m["nm_id"])
        product = products.setdefault(
            nm_id,
            {
                "nm_id": nm_id,
                "vendor_code": m["vendor_code"] or "",
                "product_name": m["product_name"] or "",
                "photo": m["photo"] or "",
                "brand": m["brand"] or "",
                "category": m["category"] or "",
                "has_sizes": bool(m["has_sizes"]),
                "size_state": "grouped",
                "points": [],
            },
        )
        point = {"date": str(m["date"])}
        for key in metric_keys:
            value = m[key]
            point[key] = float(value) if value is not None else 0
        product["points"].append(point)

    return {
        "metrics": _metric_meta(metric_keys),
        "filters": {
            "org_id": org_id,
            "date_from": params["date_from"],
            "date_to": params["date_to"],
            "nm_ids": product_ids,
            "brand": brand,
            "group": group,
            "category": category,
            "article": article,
        },
        "top_chart": {
            "scope": top_scope,
            "points": _format_chart_points(top_rows.all(), metric_keys),
        },
        "product_charts": list(products.values()),
    }


@router.get("/api/v1/nl/marketer/product/{nm_id}/sizes")
async def get_marketer_product_sizes(
    nm_id: int,
    org_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    metrics: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Стол маркетолога — раскрытие размерной сущности товара."""
    from datetime import date, timedelta

    metric_keys = _resolve_chart_metrics(metrics)
    params = {
        "org": org_id,
        "nm": nm_id,
        "date_from": _resolve_chart_date(date_from, date.today() - timedelta(days=29)),
        "date_to": _resolve_chart_date(date_to, date.today()),
    }
    product_metric_columns = ",\n               ".join(
        f"{MARKETER_CHART_METRICS[key]['expr']} AS {key}" for key in metric_keys
    )

    rows = await db.execute(text(f"""
        SELECT ts.target_date AS date,
               ts.entity_id::text AS entity_id,
               COALESCE(pe.size_name, ts.barcode, '') AS size_name,
               {product_metric_columns}
        FROM tech_status ts
        LEFT JOIN product_entities pe ON pe.id = ts.entity_id
        WHERE ts.organization_id = :org
          AND ts.nm_id = :nm
          AND ts.target_date BETWEEN CAST(:date_from AS date) AND CAST(:date_to AS date)
          AND ts.entity_id IS NOT NULL
        GROUP BY ts.target_date, ts.entity_id, pe.size_name, ts.barcode
        ORDER BY size_name, date
    """), params)

    sizes = {}
    for row in rows:
        m = row._mapping
        entity_id = m["entity_id"]
        size = sizes.setdefault(
            entity_id,
            {
                "entity_id": entity_id,
                "size_name": m["size_name"] or "",
                "points": [],
            },
        )
        point = {"date": str(m["date"])}
        for key in metric_keys:
            value = m[key]
            point[key] = float(value) if value is not None else 0
        size["points"].append(point)

    return {
        "nm_id": nm_id,
        "metrics": _metric_meta(metric_keys),
        "sizes": list(sizes.values()),
    }

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
