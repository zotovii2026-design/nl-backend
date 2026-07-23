"""
Initial WB sync for a newly connected organization.

This task scopes existing sync helpers to one org so onboarding does not burn
WB quota for every connected cabinet.
"""

import logging
from typing import Any

from celery import shared_task
from redis.asyncio import Redis
from sqlalchemy import text

from core.config import settings
from tasks.sync import parse_raw, wb_fetch
from tasks.sync.utils import (
    _run,
    reset_wb_key_org_filter,
    set_wb_key_org_filter,
)
from tasks.ue_precompute import run_precompute


logger = logging.getLogger(__name__)

INITIAL_SYNC_LOCK_TTL = 7200


def _lock_key(org_id: str) -> str:
    return f"nl:initial-sync:{org_id}"


async def _has_initial_data(sf, org_id: str) -> bool:
    async with sf() as db:
        result = await db.execute(
            text("""
                SELECT
                    EXISTS (
                        SELECT 1 FROM raw_api_data
                        WHERE organization_id = :org
                          AND api_method = 'products'
                          AND status = 'ok'
                    ) AS has_products,
                    EXISTS (
                        SELECT 1 FROM tech_status
                        WHERE organization_id = :org
                    ) AS has_tech_status
            """),
            {"org": org_id},
        )
        row = result.first()
        return bool(row and row[0] and row[1])


async def _run_step(name: str, fn, sf) -> dict[str, Any]:
    logger.info("[initial_sync] step=%s start", name)
    try:
        result = await fn(sf)
        logger.info("[initial_sync] step=%s done result=%s", name, result)
        return {"status": "ok", "result": result}
    except Exception as exc:
        logger.exception("[initial_sync] step=%s failed", name)
        return {"status": "error", "error": str(exc)}


async def _auto_extract_top_queries(sf, org_id: str) -> dict[str, Any]:
    """Автоизвлечение top_query_1 из product_name после первичной загрузки товаров.

    Берёт первые 2 слова из product_name в tech_status и записывает
    в reference_book.top_query_1 для строк, где он пустой.
    """
    logger.info("[initial_sync] auto-extracting top_query_1 for org=%s", org_id)
    try:
        async with sf() as db:
            result = await db.execute(text("""
                UPDATE reference_book rb
                SET top_query_1 = sub.query_text
                FROM (
                    SELECT DISTINCT ts.nm_id, ts.organization_id,
                        LOWER(SPLIT_PART(TRIM(ts.product_name), ' ', 1) || ' ' ||
                              SPLIT_PART(TRIM(ts.product_name), ' ', 2)) AS query_text
                    FROM tech_status ts
                    WHERE ts.product_name IS NOT NULL
                      AND TRIM(ts.product_name) != ''
                      AND ts.organization_id = :org_id
                ) sub
                WHERE rb.nm_id = sub.nm_id
                  AND rb.organization_id = sub.organization_id
                  AND (rb.top_query_1 IS NULL OR rb.top_query_1 = '')
            """), {"org_id": org_id})
            await db.commit()
            count = result.rowcount
            logger.info("[initial_sync] auto-extracted top_query_1 for %s products org=%s", count, org_id)
            return {"status": "ok", "extracted": count}
    except Exception as exc:
        logger.exception("[initial_sync] auto-extract top_query_1 failed org=%s", org_id)
        return {"status": "error", "error": str(exc)}


async def _do_initial_sync(sf, org_id: str, task_id: str | None = None) -> dict[str, Any]:
    org_id = str(org_id)
    redis = Redis.from_url(settings.REDIS_URL, encoding="utf-8", decode_responses=True)
    lock_key = _lock_key(org_id)
    lock_value = task_id or "manual"

    try:
        if await _has_initial_data(sf, org_id):
            return {"status": "skipped", "reason": "initial_data_already_exists", "org_id": org_id}

        acquired = await redis.set(lock_key, lock_value, ex=INITIAL_SYNC_LOCK_TTL, nx=True)
        if not acquired:
            return {
                "status": "skipped",
                "reason": "initial_sync_already_running",
                "org_id": org_id,
                "ttl": await redis.ttl(lock_key),
            }

        filter_token = set_wb_key_org_filter(org_id)
        try:
            steps = [
                ("products", wb_fetch._do_products),
                ("warehouses", wb_fetch._do_warehouses),
                ("stocks_fbo", wb_fetch._do_stocks_fbo),
                ("sales", wb_fetch._do_sales),
                ("orders", wb_fetch._do_orders),
                ("tariffs", wb_fetch._do_tariffs),
                ("adverts", wb_fetch._do_adverts),
                ("prices", wb_fetch._do_prices),
                ("sales_funnel", wb_fetch._do_sales_funnel),
                ("parse_raw", parse_raw._do_parse_raw),
                ("auto_extract_queries", lambda sf: _auto_extract_top_queries(sf, org_id)),
                ("tariff_snapshot", wb_fetch._do_tariff_snapshot),
            ]

            results: dict[str, Any] = {}
            for name, fn in steps:
                results[name] = await _run_step(name, fn, sf)

            has_errors = any(step.get("status") == "error" for step in results.values())
            return {
                "status": "partial" if has_errors else "ok",
                "org_id": org_id,
                "steps": results,
            }
        finally:
            reset_wb_key_org_filter(filter_token)
    finally:
        try:
            current = await redis.get(lock_key)
            if current == lock_value:
                await redis.delete(lock_key)
        finally:
            await redis.aclose()


@shared_task(name="wb.initial_sync", bind=True, soft_time_limit=7200, time_limit=7500)
def initial_sync(self, org_id: str):
    """Run first WB data sync for one newly connected organization."""
    result = _run(lambda sf: _do_initial_sync(sf, org_id, self.request.id))
    if result.get("status") in ("ok", "partial"):
        try:
            run_precompute([str(org_id)])
        except Exception as exc:
            logger.warning("[initial_sync] ue_precompute skipped org=%s: %s", org_id, exc)
        # Запускаем сбор сезонности после успешного initial_sync
        try:
            from tasks.celery_app import celery_app
            celery_app.send_task("seasonality.collect", kwargs={"org_id": str(org_id)})
            logger.info("[initial_sync] seasonality.collect queued for org=%s", org_id)
        except Exception as exc:
            logger.warning("[initial_sync] failed to queue seasonality for org=%s: %s", org_id, exc)
    return result
