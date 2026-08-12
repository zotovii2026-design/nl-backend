import asyncio
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import async_session


LATEST_DATE_SQL = """
    SELECT DISTINCT target_date
    FROM tech_status
    WHERE organization_id = :org
    ORDER BY target_date DESC
    LIMIT 1
"""

PRODUCTS_FROM_SQL = """
    FROM product_entities pe
    LEFT JOIN LATERAL (
        SELECT product_name, photo_main, price, price_discount, tariff, ad_cost
        FROM tech_status ts
        WHERE ts.organization_id = :org
          AND ts.nm_id = pe.nm_id
          AND ts.target_date = :dt
        LIMIT 1
    ) ts ON true
    WHERE pe.organization_id = :org
      AND (
          :search = ''
          OR LOWER(COALESCE(ts.product_name, pe.product_name, '')) LIKE :search_like
          OR LOWER(COALESCE(pe.vendor_code, '')) LIKE :search_like
          OR CAST(pe.nm_id AS TEXT) LIKE :search_like
          OR EXISTS (
              SELECT 1
              FROM entity_barcodes eb_search
              WHERE eb_search.entity_id = pe.id
                AND eb_search.is_active = true
                AND eb_search.barcode LIKE :search_like
          )
      )
"""

PRODUCTS_SQL = """
    SELECT pe.id AS entity_id, pe.nm_id, pe.vendor_code,
           COALESCE(ts.product_name, pe.product_name) AS product_name,
           COALESCE(ts.photo_main, pe.photo_main) AS photo_main,
           (
               SELECT string_agg(eb.barcode, ', ')
               FROM entity_barcodes eb
               WHERE eb.entity_id = pe.id AND eb.is_active = true
           ) AS barcode,
           ts.price, ts.price_discount, ts.tariff, ts.ad_cost,
           pe.size_name, pe.subject_name, pe.width, pe.height, pe.length
""" + PRODUCTS_FROM_SQL + """
    ORDER BY pe.nm_id, pe.size_name
    LIMIT :limit
"""

PRODUCTS_COUNT_SQL = """
    SELECT COUNT(*)
""" + PRODUCTS_FROM_SQL + """
"""

REFERENCE_BOOK_SQL = """
    SELECT DISTINCT ON (entity_id, nm_id)
           entity_id, nm_id,
           mp_correction_pct, buyout_niche_pct, extra_costs, ad_plan_rub,
           price_before_spp_plan, price_before_spp_change, change_date,
           fulfillment_model, wb_club_discount_pct, storage_pct, product_status,
           mp_base_pct, wb_price_fact, wb_price_retail, wb_discount_pct,
           wb_prices_updated_at, cost_price, purchase_cost, logistics_cost,
           packaging_cost, other_costs, vat, product_class, brand, tax_system,
           tax_rate, vat_rate, fbs_warehouse, tags
    FROM reference_book
    WHERE organization_id = :org
      AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)
      {product_filter}
    ORDER BY entity_id NULLS LAST, nm_id, valid_from DESC
"""

TARIFF_SNAPSHOT_SQL = """
    SELECT DISTINCT ON (nm_id) nm_id, logistics_tariff, storage_tariff, ad_cost_fact,
           buyout_pct_fact, commission_pct, price_retail, price_with_spp,
           spp_pct, commission_fbs_pct
    FROM wb_tariff_snapshot
    WHERE organization_id = :org
      {product_filter}
    ORDER BY nm_id, target_date DESC
"""

BOX_TARIFFS_SQL = """
    SELECT warehouse_name, box_delivery_base, box_delivery_liter,
           box_delivery_marketplace_base, box_delivery_marketplace_liter,
           box_delivery_coef, box_delivery_marketplace_coef
    FROM wb_box_tariffs
    WHERE organization_id = :org
      AND snapshot_date = (
          SELECT MAX(snapshot_date)
          FROM wb_box_tariffs
          WHERE organization_id = :org
      )
"""


async def get_latest_date(db: AsyncSession, org_id: str):
    result = await db.execute(text(LATEST_DATE_SQL), {"org": org_id})
    row = result.first()
    return row[0] if row else None


async def get_products(
    db: AsyncSession,
    org_id: str,
    latest_date,
    search: str | None = None,
    limit: int | None = None,
) -> list[Any]:
    search_value = (search or "").strip().lower()
    result = await db.execute(
        text(PRODUCTS_SQL),
        {
            "org": org_id,
            "dt": latest_date,
            "search": search_value,
            "search_like": f"%{search_value}%",
            "limit": limit if limit and limit > 0 else None,
        },
    )
    return result.all()


async def count_products(
    db: AsyncSession,
    org_id: str,
    latest_date,
    search: str | None = None,
) -> int:
    search_value = (search or "").strip().lower()
    result = await db.execute(
        text(PRODUCTS_COUNT_SQL),
        {
            "org": org_id,
            "dt": latest_date,
            "search": search_value,
            "search_like": f"%{search_value}%",
        },
    )
    row = result.first()
    return int(row[0]) if row else 0


async def _run_query(
    query: str,
    params: dict[str, Any],
    db: AsyncSession | None = None,
) -> list[Any]:
    if db is not None:
        result = await db.execute(text(query), params)
        return result.all()
    async with async_session() as session:
        result = await session.execute(text(query), params)
        return result.all()


def _product_scope(products: list[Any] | None) -> tuple[list[str], list[int]]:
    if not products:
        return [], []
    entity_ids = []
    nm_ids = []
    for product in products:
        if product[0]:
            entity_ids.append(str(product[0]))
        if product[1]:
            nm_ids.append(int(product[1]))
    return sorted(set(entity_ids)), sorted(set(nm_ids))


def _scoped_reference_query(entity_ids: list[str], nm_ids: list[int]) -> str:
    if not entity_ids and not nm_ids:
        return REFERENCE_BOOK_SQL.format(product_filter="")
    return REFERENCE_BOOK_SQL.format(
        product_filter=(
            "AND (entity_id::text = ANY(CAST(:entity_ids AS text[])) "
            "OR nm_id = ANY(CAST(:nm_ids AS integer[])))"
        )
    )


def _scoped_snapshot_query(nm_ids: list[int]) -> str:
    if not nm_ids:
        return TARIFF_SNAPSHOT_SQL.format(product_filter="")
    return TARIFF_SNAPSHOT_SQL.format(
        product_filter="AND nm_id = ANY(CAST(:nm_ids AS integer[]))"
    )


async def get_supporting_rows(
    org_id: str,
    products: list[Any] | None = None,
    db: AsyncSession | None = None,
) -> tuple[list[Any], list[Any], list[Any]]:
    entity_ids, nm_ids = _product_scope(products)
    params = {"org": org_id, "entity_ids": entity_ids, "nm_ids": nm_ids}
    reference_query = _scoped_reference_query(entity_ids, nm_ids)
    snapshot_query = _scoped_snapshot_query(nm_ids)
    if db is not None:
        return (
            await _run_query(reference_query, params, db),
            await _run_query(snapshot_query, params, db),
            await _run_query(BOX_TARIFFS_SQL, params, db),
        )
    return await asyncio.gather(
        _run_query(reference_query, params),
        _run_query(snapshot_query, params),
        _run_query(BOX_TARIFFS_SQL, params),
    )
