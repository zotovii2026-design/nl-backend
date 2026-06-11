"""
Precompute UE-кэша: после sync-задач прогревает Redis для всех организаций.
Вызывается из Celery-тасков (prices, parse_raw, box_tariffs).
"""

import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from core.config import settings

logger = logging.getLogger(__name__)


async def precompute_ue_cache(org_ids: list[str], session_factory, build_fn):
    """
    Прогревает Redis-кэш UE для указанных организаций.
    Вызывает внутренний расчёт напрямую, не обходя HTTP-авторизацию.
    """
    for org_id in org_ids:
        try:
            async with session_factory() as db:
                data = await build_fn(str(org_id), db)
            total = data.get("total", 0)
            logger.info(f"[ue_precompute] org={str(org_id)[:8]}: cached {total} items")
        except Exception as e:
            logger.error(f"[ue_precompute] org={str(org_id)[:8]}: error={e}")


def run_precompute(org_ids: list[str]):
    """Sync wrapper для вызова из Celery-тасков"""
    async def _run():
        from api.v1.nl import build_unit_economics

        engine = create_async_engine(
            settings.DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=300,
        )
        session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        try:
            await precompute_ue_cache(
                org_ids,
                session_factory,
                build_unit_economics,
            )
        finally:
            await engine.dispose()

    try:
        asyncio.run(_run())
    except Exception as e:
        logger.error(f"[ue_precompute] run error: {e}")
