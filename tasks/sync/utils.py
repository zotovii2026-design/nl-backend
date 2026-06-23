"""
Общие утилиты для sync-тасков.
Извлечено из scheduled_sync.py без изменения логики.
"""

import asyncio
import logging
import random
from datetime import datetime
from typing import Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.config import settings
from models.raw_data import RawApiData
from services.wb_api.keys import get_all_wb_keys as _get_all_keys_imported

logger = logging.getLogger(__name__)

PAUSE_SEC = 30
RETRY_DELAYS = [30, 60, 120]  # base delays for exponential backoff


def _get_retry_delay(attempt: int, response=None) -> float:
    """Вычислить задержку с учётом Retry-After + jitter."""
    base = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
    # Проверяем Retry-After заголовок от WB
    if response is not None:
        ra = response.headers.get("retry-after") or response.headers.get("Retry-After")
        if ra:
            try:
                server_delay = float(ra)
                base = max(base, server_delay)
            except ValueError:
                pass
    # Jitter ±20% чтобы параллельные задачи не стучали одновременно
    jitter = base * 0.2 * (random.random() * 2 - 1)
    return max(1.0, base + jitter)


async def _fetch_with_retry(coro_factory, label="", max_retries=3):
    """Retry async call on transient WB errors with Retry-After + jitter."""
    import httpx
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            result = coro_factory()
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except httpx.HTTPStatusError as e:
            last_exc = e
            is_transient = e.response.status_code == 429 or e.response.status_code >= 500
            if is_transient and attempt < max_retries:
                delay = _get_retry_delay(attempt, e.response)
                logger.warning(
                    f"[retry] {label} HTTP {e.response.status_code} "
                    f"(attempt {attempt+1}/{max_retries}), waiting {delay:.1f}s "
                    f"[Retry-After: {e.response.headers.get('retry-after', 'none')}]"
                )
                await asyncio.sleep(delay)
            else:
                raise
        except Exception as e:
            resp = getattr(e, 'response', None)
            if resp is not None and getattr(resp, 'status_code', None) == 429:
                last_exc = e
                if attempt < max_retries:
                    delay = _get_retry_delay(attempt, resp)
                    logger.warning(f"[retry] {label} 429 wrapped (attempt {attempt+1}/{max_retries}), waiting {delay:.1f}s")
                    await asyncio.sleep(delay)
                    continue
            raise
    raise last_exc


def _make_session():
    """Создаёт свежий engine + sessionmaker для текущего event loop"""
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        future=True,
        pool_pre_ping=True,
        pool_recycle=300,
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _run(coro):
    """Запуск async из Celery — каждый раз чистый loop"""
    async def wrapper():
        engine, session_factory = _make_session()
        try:
            return await coro(session_factory)
        finally:
            await engine.dispose()

    return asyncio.run(wrapper())


async def _get_all_keys(sf):
    """Delegate to services.wb_api.keys"""
    return await _get_all_keys_imported(sf)


async def _save_raw(db, org_id, method, target, response, count=None, status="ok", error=None):
    """Upsert сырых данных"""
    stmt = pg_insert(RawApiData).values(
        organization_id=org_id,
        api_method=method,
        target_date=target,
        raw_response=response,
        status=status,
        error_message=error,
        records_count=count,
        fetched_at=datetime.utcnow(),
    ).on_conflict_do_update(
        constraint="raw_api_data_organization_id_api_method_target_date_key",
        set_={
            "raw_response": pg_insert(RawApiData).excluded.raw_response,
            "status": status,
            "records_count": count,
            "fetched_at": datetime.utcnow(),
        }
    )
    await db.execute(stmt)
    await db.commit()
