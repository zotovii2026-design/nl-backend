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
def collect_seasonality_task(self, org_id: Optional[str] = None):
    """
    Collect seasonality data for all organizations or a specific one.
    
    This task:
    1. Collects keyword seasonality from Evirma API
    2. Calculates product seasonality profiles
    3. Updates reference_book with seasonal coefficients
    
    Args:
        org_id: Optional organization ID. If None, processes all organizations.
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
                
                _log.info(f"Starting seasonality collection for {len(orgs)} organization(s)")
                
                for org in orgs:
                    await _collect_for_org(org)
                
                return {"status": "completed", "organizations": len(orgs)}
            except Exception as e:
                _log.error(f"Seasonality collection failed: {e}")
                raise
    
    return asyncio.run(_collect())


async def _collect_for_org(org_id: str):
    """Collect seasonality data for a single organization."""
    import sys
    import os
    sys.path.insert(0, "/app")
    os.chdir("/app")
    
    from scripts.collect_evirma_seasonality import collect as collect_keywords
    from scripts.calculate_product_seasonality import calculate_product_seasonality as calculate_products
    
    _log.info(f"Collecting seasonality for org {org_id}")
    
    # Step 1: Collect keyword seasonality
    try:
        await collect_keywords(org_id, test_mode=False, dry_run=False)
        _log.info(f"Keyword seasonality collected for org {org_id}")
    except Exception as e:
        _log.error(f"Failed to collect keywords for org {org_id}: {e}")
        return
    
    # Step 2: Calculate product seasonality
    try:
        await calculate_products(org_id, dry_run=False)
        _log.info(f"Product seasonality calculated for org {org_id}")
    except Exception as e:
        _log.error(f"Failed to calculate products for org {org_id}: {e}")
        return
    
    # Step 3: Update reference_book with seasonal coefficients
    try:
        await _update_reference_book(org_id)
        _log.info(f"Reference book updated with seasonality for org {org_id}")
    except Exception as e:
        _log.error(f"Failed to update reference book for org {org_id}: {e}")


async def _update_reference_book(org_id: str):
    """Update reference_book with seasonality coefficients from product profiles."""
    async for db in get_db():
        # Update reference_book with seasonal coefficients
        await db.execute(text("""
            UPDATE reference_book rb
            SET 
                season_jan = ps.seasonality_coefficients->>'1',
                season_feb = ps.seasonality_coefficients->>'2',
                season_mar = ps.seasonality_coefficients->>'3',
                season_apr = ps.seasonality_coefficients->>'4',
                season_may = ps.seasonality_coefficients->>'5',
                season_jun = ps.seasonality_coefficients->>'6',
                season_jul = ps.seasonality_coefficients->>'7',
                season_aug = ps.seasonality_coefficients->>'8',
                season_sep = ps.seasonality_coefficients->>'9',
                season_oct = ps.seasonality_coefficients->>'10',
                season_nov = ps.seasonality_coefficients->>'11',
                season_dec = ps.seasonality_coefficients->>'12'
            FROM wb_product_seasonality ps
            WHERE rb.nm_id = ps.nm_id
              AND rb.organization_id = :org_id
              AND ps.organization_id = :org_id
        """), {"org_id": org_id})
        
        await db.commit()
        _log.info(f"Updated reference_book seasonality fields for org {org_id}")


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