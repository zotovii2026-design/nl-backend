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
    ORDER BY pe.nm_id, pe.size_name
"""

REFERENCE_BOOK_SQL = """
    SELECT entity_id, nm_id,
           mp_correction_pct, buyout_niche_pct, extra_costs, ad_plan_rub,
           price_before_spp_plan, price_before_spp_change, change_date,
           fulfillment_model, wb_club_discount_pct, storage_pct, product_status,
           mp_base_pct, wb_price_fact, wb_price_retail, wb_discount_pct,
           wb_prices_updated_at, cost_price, purchase_cost, logistics_cost,
           packaging_cost, other_costs, vat, product_class, brand, tax_system,
           tax_rate, vat_rate, fbs_warehouse
    FROM reference_book
    WHERE organization_id = :org
      AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)
    ORDER BY entity_id NULLS LAST, valid_from DESC
"""

TARIFF_SNAPSHOT_SQL = """
    SELECT nm_id, logistics_tariff, storage_tariff, ad_cost_fact,
           buyout_pct_fact, commission_pct, price_retail, price_with_spp,
           spp_pct, commission_fbs_pct
    FROM wb_tariff_snapshot
    WHERE organization_id = :org
    ORDER BY target_date DESC
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
) -> list[Any]:
    result = await db.execute(
        text(PRODUCTS_SQL),
        {"org": org_id, "dt": latest_date},
    )
    return result.all()


async def _run_query(query: str, params: dict[str, Any]) -> list[Any]:
    async with async_session() as session:
        result = await session.execute(text(query), params)
        return result.all()


async def get_supporting_rows(
    org_id: str,
) -> tuple[list[Any], list[Any], list[Any]]:
    params = {"org": org_id}
    return await asyncio.gather(
        _run_query(REFERENCE_BOOK_SQL, params),
        _run_query(TARIFF_SNAPSHOT_SQL, params),
        _run_query(BOX_TARIFFS_SQL, params),
    )
