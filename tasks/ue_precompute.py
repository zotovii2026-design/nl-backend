"""
Precompute UE-кэша: после sync-задач прогревает Redis для всех организаций.
Вызывается из Celery-тасков (prices, parse_raw, box_tariffs).
"""

import asyncio
import logging
import os

import redis as redis_lib
import json

logger = logging.getLogger(__name__)

# Port can be overridden via env
UE_HOST = os.environ.get("UE_PRECOMPUTE_HOST", "app")
UE_PORT = os.environ.get("UE_PRECOMPUTE_PORT", "8000")


async def precompute_ue_cache(org_ids: list[str]):
    """
    Прогревает Redis-кэш UE для указанных организаций.
    Делает HTTP GET к локальному API — тот же путь, что и браузер,
    но результат оседает в Redis.
    """
    import httpx

    for org_id in org_ids:
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.get(
                    f"http://{UE_HOST}:{UE_PORT}/api/v1/nl/unit-economics",
                    params={"org_id": org_id},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    total = data.get("total", 0)
                    logger.info(f"[ue_precompute] org={org_id[:8]}: cached {total} items")
                else:
                    logger.warning(f"[ue_precompute] org={org_id[:8]}: status={resp.status_code}")
        except Exception as e:
            logger.error(f"[ue_precompute] org={org_id[:8]}: error={e}")


def run_precompute(org_ids: list[str]):
    """Sync wrapper для вызова из Celery-тасков"""
    try:
        asyncio.run(precompute_ue_cache(org_ids))
    except Exception as e:
        logger.error(f"[ue_precompute] run error: {e}")
