"""Seasonality data collection and calculation tasks."""

import logging
from typing import Optional

from celery import shared_task
from sqlalchemy import select, text

from models.organization import WbApiKey
from tasks.sync.utils import _run

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
    return _run(lambda sf: _collect(sf, org_id=org_id, nm_id=nm_id, nm_ids=nm_ids, source=source))


async def _collect(
    session_factory,
    org_id: Optional[str] = None,
    nm_id: Optional[int] = None,
    nm_ids: Optional[list[int]] = None,
    source: Optional[str] = None,
) -> dict:
    if org_id:
        orgs = [str(org_id)]
    else:
        async with session_factory() as db:
            result = await db.execute(
                select(WbApiKey.organization_id)
                .where(WbApiKey.is_active.is_(True))
                .distinct()
            )
            orgs = [str(row[0]) for row in result.all()]

    product_filter = _normalize_nm_ids(nm_id=nm_id, nm_ids=nm_ids)
    _log.info(
        "Starting seasonality collection for %s organization(s), nm_ids=%s, source=%s",
        len(orgs),
        product_filter,
        source,
    )

    results = []
    errors = []
    for org in orgs:
        try:
            results.append(await _collect_for_org(session_factory, org, nm_ids=product_filter))
        except Exception as exc:
            _log.exception("Seasonality collection failed for org=%s", org)
            error = {"org_id": org, "status": "error", "error": str(exc)}
            results.append(error)
            errors.append(error)
            if org_id:
                raise

    partials = [result for result in results if result.get("status") == "partial"]
    status = "ok"
    if errors:
        status = "failed" if len(errors) == len(orgs) else "partial"
    elif partials:
        status = "partial"
    return {
        "status": status,
        "organizations": len(orgs),
        "nm_ids": product_filter,
        "results": results,
        "errors": errors,
    }


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


async def _collect_for_org(session_factory, org_id: str, nm_ids: Optional[list[int]] = None):
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
        keyword_result = await collect_keywords(
            org_id,
            test_mode=False,
            dry_run=False,
            nm_ids=nm_ids,
            session_factory=session_factory,
        )
        _log.info(f"Keyword seasonality collected for org {org_id}")
    except Exception as e:
        raise RuntimeError(f"Failed to collect keywords for org {org_id}: {e}") from e

    if not keyword_result or keyword_result.get("total", 0) <= 0:
        _log.warning("No keywords found for org %s nm_ids=%s; skipping seasonality", org_id, nm_ids)
        return {"org_id": org_id, "status": "skipped", "reason": "no_keywords", "keywords": keyword_result}

    if keyword_result.get("status") == "partial":
        raise RuntimeError(
            f"Keyword seasonality partially collected for org {org_id}: {keyword_result}"
        )

    if nm_ids:
        if not keyword_result or keyword_result.get("processed", 0) <= 0:
            _log.warning("No keywords processed for org %s nm_ids=%s; keeping previous seasonality", org_id, nm_ids)
            return {"org_id": org_id, "status": "skipped", "reason": "no_keywords_processed"}
        await _reset_product_seasonality(session_factory, org_id, nm_ids)
    
    # Step 2: Calculate product seasonality
    try:
        product_result = await calculate_products(
            org_id,
            dry_run=False,
            nm_ids=nm_ids,
            session_factory=session_factory,
        )
        _log.info(f"Product seasonality calculated for org {org_id}")
    except Exception as e:
        raise RuntimeError(f"Failed to calculate products for org {org_id}: {e}") from e

    if product_result.get("status") == "skipped":
        return {
            "org_id": org_id,
            "status": "skipped",
            "reason": product_result.get("reason", "no_products"),
            "keywords": keyword_result,
            "products": product_result,
        }
    
    # Step 3: Update reference_book with seasonal coefficients
    try:
        reference_rows = await _update_reference_book(session_factory, org_id, nm_ids=nm_ids)
        _log.info(f"Reference book updated with seasonality for org {org_id}")
    except Exception as e:
        raise RuntimeError(f"Failed to update reference book for org {org_id}: {e}") from e

    status = "ok"
    if product_result.get("status") == "partial" or reference_rows <= 0:
        status = "partial"

    return {
        "org_id": org_id,
        "status": status,
        "keywords": keyword_result,
        "products": product_result,
        "reference_rows": reference_rows,
    }


async def _update_reference_book(session_factory, org_id: str, nm_ids: Optional[list[int]] = None) -> int:
    """Update reference_book with seasonality coefficients from product profiles."""
    async with session_factory() as db:
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
            FROM (
                SELECT DISTINCT ON (nm_id, organization_id)
                    nm_id,
                    organization_id,
                    seasonality_coefficients
                FROM wb_product_seasonality
                WHERE organization_id = :org_id
                  AND jsonb_typeof(seasonality_coefficients) = 'object'
                  AND seasonality_coefficients <> '{}'::jsonb
                ORDER BY nm_id, organization_id, collected_at DESC
            ) ps
            WHERE rb.nm_id = ps.nm_id
              AND rb.organization_id = :org_id
              AND ps.organization_id = :org_id
        """
        params = {"org_id": org_id}
        if nm_ids:
            sql += f" AND rb.nm_id IN ({_nm_ids_in_sql(nm_ids)})"

        result = await db.execute(text(sql), params)
        
        await db.commit()
        _log.info("Updated reference_book seasonality fields for org %s rows=%s", org_id, result.rowcount)
        return result.rowcount


async def _reset_product_seasonality(session_factory, org_id: str, nm_ids: list[int]):
    """Clear stale product profiles and put visible fallback coefficients before recalculation."""
    async with session_factory() as db:
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
    async def _test(session_factory):
        import sys
        import os
        sys.path.insert(0, "/app")
        os.chdir("/app")

        from scripts.collect_evirma_seasonality import collect as collect_keywords

        _log.info(f"Testing seasonality collection for org {org_id}")

        result = await collect_keywords(
            org_id,
            test_mode=True,
            dry_run=False,
            session_factory=session_factory,
        )
        return {"status": "completed", "org_id": org_id, "result": result}

    return _run(_test)
