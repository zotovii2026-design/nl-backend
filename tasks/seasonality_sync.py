"""Seasonality data collection and calculation tasks."""

import asyncio
import logging
from typing import Optional

from celery import shared_task
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.celery import celery_app
from models.organization import Organization

_log = logging.getLogger(__name__)


@shared_task(name="seasonality.collect", bind=True)
def collect_seasonality_task(
    self,
    org_id: Optional[str] = None,
    nm_id: Optional[int] = None,
    nm_ids: Optional[list[int]] = None,
    source: Optional[str] = None,
):
    """
    Collect seasonality data for all organizations or a specific one.
    
    This task:
    1. Collects keyword seasonality from Evirma API
    2. Calculates product seasonality profiles
    3. Updates reference_book with seasonal coefficients
    
    Args:
        org_id: Optional organization ID. If None, processes all organizations.
        nm_id/nm_ids: Optional product filters for off-schedule reference updates.
    """
    async def _collect():
        async for db in get_db():
            try:
                if org_id:
                    orgs = [org_id]
                else:
                    # Get all active organizations
                    result = await db.execute(
                        select(Organization.id).where(Organization.is_active == True)
                    )
                    orgs = [row[0] for row in result.all()]
                
                product_filter = _normalize_nm_ids(nm_id=nm_id, nm_ids=nm_ids)
                _log.info(
                    "Starting seasonality collection for %s organization(s), nm_ids=%s, source=%s",
                    len(orgs),
                    product_filter,
                    source,
                )
                
                for org in orgs:
                    await _collect_for_org(org, nm_ids=product_filter)
                
                return {"status": "completed", "organizations": len(orgs), "nm_ids": product_filter}
            except Exception as e:
                _log.error(f"Seasonality collection failed: {e}")
                raise
    
    return asyncio.run(_collect())


def _normalize_nm_ids(
    nm_id: Optional[int] = None,
    nm_ids: Optional[list[int]] = None,
) -> Optional[list[int]]:
    values = []
    if nm_id is not None:
        values.append(nm_id)
    if nm_ids:
        values.extend(nm_ids)
    normalized = sorted({int(value) for value in values if value is not None})
    return normalized or None


def _nm_ids_in_sql(nm_ids: list[int]) -> str:
    normalized = _normalize_nm_ids(nm_ids=nm_ids)
    if not normalized:
        raise ValueError("nm_ids must not be empty")
    return ", ".join(str(nm_id) for nm_id in normalized)


async def _collect_for_org(org_id: str, nm_ids: Optional[list[int]] = None):
    """Collect seasonality data for a single organization."""
    import sys
    import os
    sys.path.insert(0, "/app")
    os.chdir("/app")
    
    from scripts.collect_evirma_seasonality import collect as collect_keywords
    from scripts.calculate_product_seasonality import calculate_product_seasonality as calculate_products
    
    _log.info(f"Collecting seasonality for org {org_id} nm_ids={nm_ids}")
    
    # Step 1: Collect keyword seasonality
    try:
        keyword_result = await collect_keywords(org_id, test_mode=False, dry_run=False, nm_ids=nm_ids)
        _log.info(f"Keyword seasonality collected for org {org_id}")
    except Exception as e:
        _log.error(f"Failed to collect keywords for org {org_id}: {e}")
        return

    if nm_ids:
        if not keyword_result or keyword_result.get("processed", 0) <= 0:
            _log.warning("No keywords processed for org %s nm_ids=%s; keeping previous seasonality", org_id, nm_ids)
            return
        await _reset_product_seasonality(org_id, nm_ids)
    
    # Step 2: Calculate product seasonality
    try:
        await calculate_products(org_id, dry_run=False, nm_ids=nm_ids)
        _log.info(f"Product seasonality calculated for org {org_id}")
    except Exception as e:
        _log.error(f"Failed to calculate products for org {org_id}: {e}")
        return
    
    # Step 3: Update reference_book with seasonal coefficients
    try:
        await _update_reference_book(org_id, nm_ids=nm_ids)
        _log.info(f"Reference book updated with seasonality for org {org_id}")
    except Exception as e:
        _log.error(f"Failed to update reference book for org {org_id}: {e}")


async def _update_reference_book(org_id: str, nm_ids: Optional[list[int]] = None):
    """Update reference_book with seasonality coefficients from product profiles."""
    async for db in get_db():
        # Update reference_book with seasonal coefficients
        sql = """
            UPDATE reference_book rb
            SET 
                season_jan = (ps.seasonality_coefficients->>'1')::numeric,
                season_feb = (ps.seasonality_coefficients->>'2')::numeric,
                season_mar = (ps.seasonality_coefficients->>'3')::numeric,
                season_apr = (ps.seasonality_coefficients->>'4')::numeric,
                season_may = (ps.seasonality_coefficients->>'5')::numeric,
                season_jun = (ps.seasonality_coefficients->>'6')::numeric,
                season_jul = (ps.seasonality_coefficients->>'7')::numeric,
                season_aug = (ps.seasonality_coefficients->>'8')::numeric,
                season_sep = (ps.seasonality_coefficients->>'9')::numeric,
                season_oct = (ps.seasonality_coefficients->>'10')::numeric,
                season_nov = (ps.seasonality_coefficients->>'11')::numeric,
                season_dec = (ps.seasonality_coefficients->>'12')::numeric,
                updated_at = NOW()
            FROM wb_product_seasonality ps
            WHERE rb.nm_id = ps.nm_id
              AND rb.organization_id = :org_id
              AND ps.organization_id = :org_id
              AND jsonb_typeof(ps.seasonality_coefficients) = 'object'
              AND ps.seasonality_coefficients <> '{}'::jsonb
        """
        params = {"org_id": org_id}
        if nm_ids:
            sql += f" AND rb.nm_id IN ({_nm_ids_in_sql(nm_ids)})"

        result = await db.execute(text(sql), params)
        
        await db.commit()
        _log.info("Updated reference_book seasonality fields for org %s rows=%s", org_id, result.rowcount)


async def _reset_product_seasonality(org_id: str, nm_ids: list[int]):
    """Clear stale product profiles and put visible fallback coefficients before recalculation."""
    async for db in get_db():
        await db.execute(text("""
            DELETE FROM wb_product_seasonality
            WHERE organization_id = :org_id
              AND nm_id IN (""" + _nm_ids_in_sql(nm_ids) + """)
        """), {"org_id": org_id})
        reset_result = await db.execute(text("""
            UPDATE reference_book
            SET
                season_jan = 1.00,
                season_feb = 1.00,
                season_mar = 1.00,
                season_apr = 1.00,
                season_may = 1.00,
                season_jun = 1.00,
                season_jul = 1.00,
                season_aug = 1.00,
                season_sep = 1.00,
                season_oct = 1.00,
                season_nov = 1.00,
                season_dec = 1.00
            WHERE organization_id = :org_id
              AND nm_id IN (""" + _nm_ids_in_sql(nm_ids) + """)
        """), {"org_id": org_id})
        await db.commit()
        _log.info("Reset stale product seasonality for org=%s nm_ids=%s rows=%s", org_id, nm_ids, reset_result.rowcount)


@shared_task(name="seasonality.test", bind=True)
def test_seasonality_task(self, org_id: str):
    """Test seasonality collection with a small subset of keywords."""
    async def _test():
        async for db in get_db():
            try:
                import sys
                import os
                sys.path.insert(0, "/app")
                os.chdir("/app")
                
                from scripts.collect_evirma_seasonality import collect as collect_keywords
                
                _log.info(f"Testing seasonality collection for org {org_id}")
                
                # Test with 3 keywords
                await collect_keywords(org_id, test_mode=True, dry_run=False)
                
                return {"status": "completed", "org_id": org_id}
            except Exception as e:
                _log.error(f"Seasonality test failed: {e}")
                raise
    
    return asyncio.run(_test())
