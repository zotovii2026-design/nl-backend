"""Celery tasks for WB finance report synchronization."""

import asyncio
from datetime import date, datetime
from zoneinfo import ZoneInfo

from celery import shared_task
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from core.config import settings
from services.opiu import sync_finance_period
from services.wb_api.keys import get_all_wb_keys


def _session_factory():
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        future=True,
        pool_pre_ping=True,
        pool_recycle=300,
    )
    return engine, async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )


def _run(coroutine_factory):
    async def wrapper():
        engine, session_factory = _session_factory()
        try:
            return await coroutine_factory(session_factory)
        finally:
            await engine.dispose()

    return asyncio.run(wrapper())


async def _sync_one(
    session_factory,
    organization_id: str,
    date_from: date,
    date_to: date,
):
    keys = await get_all_wb_keys(session_factory)
    token = next(
        (token for org_id, token in keys if org_id == organization_id),
        None,
    )
    if not token:
        return {"status": "skipped", "reason": "finance_token_not_found"}
    return await sync_finance_period(
        session_factory,
        organization_id,
        token,
        date_from,
        date_to,
    )


@shared_task(name="wb.opiu.sync_org")
def sync_opiu_org(
    organization_id: str,
    date_from: str,
    date_to: str,
):
    start = date.fromisoformat(date_from)
    end = date.fromisoformat(date_to)
    if end < start:
        raise ValueError("date_to must not be earlier than date_from")
    return _run(
        lambda session_factory: _sync_one(
            session_factory,
            organization_id,
            start,
            end,
        )
    )


@shared_task(name="wb.sched.opiu_finance")
def sync_opiu_all():
    async def execute(session_factory):
        today = datetime.now(ZoneInfo("Europe/Moscow")).date()
        date_from = today.replace(day=1)
        keys = await get_all_wb_keys(session_factory)
        unique_keys = dict(keys)
        results = {}
        for organization_id, token in unique_keys.items():
            try:
                results[organization_id] = await sync_finance_period(
                    session_factory,
                    organization_id,
                    token,
                    date_from,
                    today,
                )
            except Exception as error:
                results[organization_id] = {
                    "status": "error",
                    "error": str(error),
                }
        return results

    return _run(execute)
