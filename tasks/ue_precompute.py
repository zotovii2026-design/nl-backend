"""
Precompute UE-кэша: после sync-задач прогревает Redis для всех организаций.
Вызывается из Celery-тасков (prices, parse_raw, box_tariffs).
"""

import asyncio
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from api.v1.nl import build_unit_economics
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


async def _get_org_ids(session_factory) -> list[str]:
    async with session_factory() as db:
        result = await db.execute(text("SELECT id FROM organizations"))
        return [str(row[0]) for row in result.all()]


async def _run_precompute(org_ids: list[str] | None = None):
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
        selected_org_ids = org_ids
        if selected_org_ids is None:
            selected_org_ids = await _get_org_ids(session_factory)
        await precompute_ue_cache(
            selected_org_ids,
            session_factory,
            build_unit_economics,
        )
    finally:
        await engine.dispose()


def run_precompute(org_ids: list[str] | None = None):
    """Sync wrapper для вызова из Celery-тасков"""
    try:
        asyncio.run(_run_precompute(org_ids))
    except Exception as e:
        logger.error(f"[ue_precompute] run error: {e}")
