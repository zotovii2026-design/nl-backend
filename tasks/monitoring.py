import asyncio
from datetime import datetime, timedelta, timezone

from celery import shared_task
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from core.config import settings
from models.organization import WbApiKey
from models.raw_data import RawApiData


FRESHNESS_LIMITS = {
    "products": timedelta(hours=30),
    # Sales run at 08:00 and 14:05 Moscow time; the longest gap is 17h55m.
    "sales": timedelta(hours=20),
    # Orders run once daily at 08:05 Moscow time.
    "orders": timedelta(hours=27),
    "stocks_fbo": timedelta(hours=16),
    "adverts": timedelta(hours=30),
}


def _run(coro):
    async def wrapper():
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
            return await coro(session_factory)
        finally:
            await engine.dispose()

    return asyncio.run(wrapper())


@shared_task(name="wb.sched.freshness")
def check_raw_data_freshness():
    return _run(_check_raw_data_freshness)


async def _check_raw_data_freshness(session_factory):
    now = datetime.now(timezone.utc)
    async with session_factory() as db:
        organization_ids = (
            await db.execute(select(WbApiKey.organization_id).distinct())
        ).scalars().all()
        rows = (
            await db.execute(
                select(
                    RawApiData.organization_id,
                    RawApiData.api_method,
                    func.max(RawApiData.fetched_at),
                )
                .where(RawApiData.api_method.in_(FRESHNESS_LIMITS))
                .group_by(RawApiData.organization_id, RawApiData.api_method)
            )
        ).all()

    latest = {
        (str(organization_id), api_method): fetched_at
        for organization_id, api_method, fetched_at in rows
    }
    results = {}
    for organization_id in organization_ids:
        org_key = str(organization_id)
        org_result = {}
        for api_method, max_age in FRESHNESS_LIMITS.items():
            fetched_at = latest.get((org_key, api_method))
            if fetched_at is None:
                org_result[api_method] = {
                    "status": "stale",
                    "reason": "no successful fetch recorded",
                }
                continue
            if fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=timezone.utc)
            age = now - fetched_at
            org_result[api_method] = {
                "status": "stale" if age > max_age else "ok",
                "age_minutes": int(age.total_seconds() // 60),
                "fetched_at": fetched_at.isoformat(),
            }
        results[org_key[:8]] = org_result

    return {
        "status": "ok",
        "checked_at": now.isoformat(),
        "organizations": results,
    }


__all__ = ["FRESHNESS_LIMITS", "check_raw_data_freshness"]
