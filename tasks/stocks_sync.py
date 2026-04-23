"""Задача Celery для синхронизации остатков WB по складам"""

from datetime import datetime
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from celery import shared_task
import asyncio

from core.database import get_db
from models.wb_data import WbStock, SyncLog
from models.organization import WbApiKey
from services.wb_api.client import WBApiClient
from core.security import decrypt_data


def run_async(coro):
    return asyncio.run(coro)


@shared_task(name="wb.sync_stocks")
def sync_stocks_task(api_key_id: str, organization_id: str):
    """Синхронизация остатков со складов WB"""
    return run_async(_sync_stocks(api_key_id, organization_id))


async def _sync_stocks(api_key_id: str, organization_id: str):
    log_id = None
    try:
        # Получаем ключ и personal_token
        async for db in get_db():
            result = await db.execute(select(WbApiKey).where(WbApiKey.id == api_key_id))
            key_rec = result.scalar_one_or_none()
            if not key_rec:
                raise Exception(f"API key {api_key_id} not found")

            standard_key = decrypt_data(key_rec.api_key)
            personal_token = decrypt_data(key_rec.personal_token) if key_rec.personal_token else None
            break

        if not personal_token:
            raise Exception("Personal token not set for this API key. Add personal_token to use stocks sync.")

        # Лог
        async for db in get_db():
            log = SyncLog(
                organization_id=organization_id,
                task_name="wb.sync_stocks",
                status="running",
                started_at=datetime.utcnow()
            )
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id
            break

        # Запрос к analytics API через personal token
        client = WBApiClient(personal_token)
        stocks_data = await client.get_stocks_warehouses(is_archive=False)
        await client.client.aclose()

        # Сохраняем в БД (upsert по nm_id + warehouse_name)
        count = 0
        async for db in get_db():
            for item in stocks_data:
                nm_id = item.get("nmID") or item.get("nmId")
                wh_name = item.get("warehouseName", "unknown")
                if not nm_id:
                    continue

                result = await db.execute(
                    select(WbStock).where(
                        WbStock.nm_id == nm_id,
                        WbStock.warehouse_name == wh_name,
                        WbStock.organization_id == organization_id
                    )
                )
                existing = result.scalar_one_or_none()

                if existing:
                    existing.quantity = item.get("quantity", 0)
                    existing.quantity_full = item.get("quantityFull") or item.get("quantity_full", 0)
                    existing.in_way_to_client = item.get("inWayToClient", 0)
                    existing.in_way_from_client = item.get("inWayFromClient", 0)
                    existing.vendor_code = item.get("vendorCode") or existing.vendor_code
                    existing.category = item.get("category") or existing.category
                    existing.subject = item.get("subject") or existing.subject
                    existing.brand = item.get("brand") or existing.brand
                    existing.synced_at = datetime.utcnow()
                else:
                    stock = WbStock(
                        organization_id=organization_id,
                        nm_id=nm_id,
                        vendor_code=item.get("vendorCode"),
                        warehouse_name=wh_name,
                        warehouse_id=item.get("warehouseId"),
                        quantity=item.get("quantity", 0),
                        quantity_full=item.get("quantityFull") or item.get("quantity_full", 0),
                        in_way_to_client=item.get("inWayToClient", 0),
                        in_way_from_client=item.get("inWayFromClient", 0),
                        category=item.get("category"),
                        subject=item.get("subject"),
                        brand=item.get("brand"),
                        synced_at=datetime.utcnow()
                    )
                    db.add(stock)
                count += 1

            await db.commit()
            break

        # Успех
        async for db in get_db():
            log = await db.get(SyncLog, log_id)
            if log:
                log.status = "success"
                log.finished_at = datetime.utcnow()
                log.synced_count = count
                await db.commit()
            break

        return {"status": "success", "stocks_count": count}

    except Exception as e:
        async for db in get_db():
            log = await db.get(SyncLog, log_id) if log_id else None
            if log:
                log.status = "error"
                log.finished_at = datetime.utcnow()
                log.error_message = str(e)
                await db.commit()
            break
        raise
