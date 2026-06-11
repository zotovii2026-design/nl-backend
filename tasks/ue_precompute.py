"""
Precompute UE-кэша: после sync-задач прогревает Redis для всех организаций.
Вызывается из Celery-тасков (prices, parse_raw, box_tariffs).
"""

import asyncio
import logging

logger = logging.getLogger(__name__)


async def precompute_ue_cache(org_ids: list[str]):
    """
    Прогревает Redis-кэш UE для указанных организаций.
    Вызывает внутренний расчёт напрямую, не обходя HTTP-авторизацию.
    """
    from api.v1.nl import build_unit_economics
    from core.database import async_session

    for org_id in org_ids:
        try:
            async with async_session() as db:
                data = await build_unit_economics(str(org_id), db)
            total = data.get("total", 0)
            logger.info(f"[ue_precompute] org={str(org_id)[:8]}: cached {total} items")
        except Exception as e:
            logger.error(f"[ue_precompute] org={str(org_id)[:8]}: error={e}")


def run_precompute(org_ids: list[str]):
    """Sync wrapper для вызова из Celery-тасков"""
    async def _run():
        from core.database import engine

        try:
            await precompute_ue_cache(org_ids)
        finally:
            # Celery creates a fresh event loop for each sync task invocation.
            # Drop pooled asyncpg connections before that loop is closed.
            await engine.dispose()

    try:
        asyncio.run(_run())
    except Exception as e:
        logger.error(f"[ue_precompute] run error: {e}")
