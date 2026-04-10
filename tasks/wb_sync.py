"""Задачи Celery для синхронизации данных из WB API"""

from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from celery import shared_task
import asyncio

from core.database import get_db
from models.wb_data import WbProduct, WbSale, SyncLog
from services.wb_api.client import get_wb_client
from core.security import decrypt_data


def run_async(coro):
    """Запуск асинхронной функции"""
    return asyncio.run(coro)


@shared_task(name="wb.sync_products")
def sync_products_task(api_key_id: str, organization_id: str):
    """
    Задача синхронизации товаров из WB API
    
    Args:
        api_key_id: ID ключа WB API
        organization_id: ID организации
    """
    return run_async(_sync_products_task(api_key_id, organization_id))


async def _sync_products_task(api_key_id: str, organization_id: str):
    """Асинхронная реализация синхронизации товаров"""
    log_id = None
    
    try:
        # Получаем API ключ из БД
        async for db in get_db():
            from models.organization import WbApiKey
            result = await db.execute(
                select(WbApiKey).where(WbApiKey.id == api_key_id)
            )
            api_key_record = result.scalar_one_or_none()
            
            if not api_key_record:
                raise Exception(f"API key {api_key_id} not found")
            
            # Расшифровываем ключ
            decrypted_key = decrypt_data(api_key_record.api_key)
            
            break
        
        # Создаём лог синхронизации
        async for db in get_db():
            log = SyncLog(
                organization_id=organization_id,
                task_name="wb.sync_products",
                status="running",
                started_at=datetime.utcnow()
            )
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id
            break
        
        # Получаем карточки из WB API
        client = await get_wb_client(decrypted_key)
        cards = await client.get_all_cards()
        
        # Сохраняем карточки в БД
        async for db in get_db():
            for card in cards:
                # Проверяем, существует ли карточка
                result = await db.execute(
                    select(WbProduct).where(
                        WbProduct.nm_id == card.get("nmID"),
                        WbProduct.organization_id == organization_id
                    )
                )
                existing_product = result.scalar_one_or_none()
                
                # Получаем цену из размеров
                price = 0
                if card.get("sizes") and len(card["sizes"]) > 0:
                    price = card["sizes"][0].get("price", 0) or 0
                
                # Получаем главное фото
                photo_url = ""
                if card.get("photos") and len(card["photos"]) > 0:
                    photo_url = card["photos"][0].get("big", "")
                
                if existing_product:
                    # Обновляем существующую карточку
                    existing_product.name = card.get("title", "")
                    existing_product.vendor_code = card.get("vendorCode", "")
                    existing_product.brand = card.get("brand", "")
                    existing_product.subject = card.get("subjectName", "")
                    existing_product.description = card.get("description", "")
                    existing_product.price = float(price)
                    existing_product.photo_url = photo_url
                    existing_product.need_kiz = card.get("needKiz", False)
                    existing_product.kiz_marked = card.get("kizMarked", False)
                    existing_product.synced_at = datetime.utcnow()
                else:
                    # Создаём новую карточку
                    product = WbProduct(
                        nm_id=card.get("nmID", 0),
                        vendor_code=card.get("vendorCode", ""),
                        name=card.get("title", ""),
                        brand=card.get("brand", ""),
                        subject=card.get("subjectName", ""),
                        description=card.get("description", ""),
                        price=float(price),
                        photo_url=photo_url,
                        organization_id=organization_id,
                        need_kiz=card.get("needKiz", False),
                        kiz_marked=card.get("kizMarked", False),
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                        synced_at=datetime.utcnow()
                    )
                    db.add(product)
            
            await db.commit()
            break
        
        # Обновляем лог синхронизации
        async for db in get_db():
            log = await db.get(SyncLog, log_id)
            if log:
                log.status = "success"
                log.finished_at = datetime.utcnow()
                log.synced_count = len(cards)
                log.error_message = None
                await db.commit()
            break
        
        return {"status": "success", "cards_count": len(cards)}
    
    except Exception as e:
        # Логируем ошибку
        async for db in get_db():
            log = await db.get(SyncLog, log_id)
            if log:
                log.status = "error"
                log.finished_at = datetime.utcnow()
                log.error_message = str(e)
                await db.commit()
            break
        
        raise


@shared_task(name="wb.sync_sales")
def sync_sales_task(api_key_id: str, organization_id: str, days: int = 7):
    """
    Задача синхронизации продаж из WB API
    
    Args:
        api_key_id: ID ключа WB API
        organization_id: ID организации
        days: количество дней для синхронизации
    """
    return run_async(_sync_sales_task(api_key_id, organization_id, days))


async def _sync_sales_task(api_key_id: str, organization_id: str, days: int = 7):
    """Асинхронная реализация синхронизации продаж"""
    log_id = None
    
    try:
        # Получаем API ключ из БД
        async for db in get_db():
            from models.organization import WbApiKey
            result = await db.execute(
                select(WbApiKey).where(WbApiKey.id == api_key_id)
            )
            api_key_record = result.scalar_one_or_none()
            
            if not api_key_record:
                raise Exception(f"API key {api_key_id} not found")
            
            # Расшифровываем ключ
            decrypted_key = decrypt_data(api_key_record.api_key)
            
            break
        
        # Создаём лог синхронизации
        async for db in get_db():
            log = SyncLog(
                organization_id=organization_id,
                task_name="wb.sync_sales",
                status="running",
                started_at=datetime.utcnow()
            )
            db.add(log)
            await db.commit()
            await db.refresh(log)
            log_id = log.id
            break
        
        # Получаем продажи из WB API
        # TODO: Реализовать метод get_sales в WBApiClient
        sales = []
        
        # Сохраняем продажи в БД
        async for db in get_db():
            for sale_data in sales:
                # Проверяем, существует ли продажа
                result = await db.execute(
                    select(WbSale).where(
                        WbSale.sale_id == sale_data.get("sale_id"),
                        WbSale.organization_id == organization_id
                    )
                )
                existing_sale = result.scalar_one_or_none()
                
                if not existing_sale:
                    # Создаём новую продажу
                    sale = WbSale(
                        sale_id=sale_data.get("sale_id", ""),
                        date_from=datetime.fromisoformat(sale_data.get("date_from")),
                        date_to=datetime.fromisoformat(sale_data.get("date_to")),
                        income=float(sale_data.get("income", 0)),
                        penalty=float(sale_data.get("penalty", 0)),
                        reward=float(sale_data.get("reward", 0)),
                        organization_id=organization_id,
                        synced_at=datetime.utcnow()
                    )
                    db.add(sale)
            
            await db.commit()
            break
        
        # Обновляем лог синхронизации
        async for db in get_db():
            log = await db.get(SyncLog, log_id)
            if log:
                log.status = "success"
                log.finished_at = datetime.utcnow()
                log.synced_count = len(sales)
                log.error_message = None
                await db.commit()
            break
        
        return {"status": "success", "sales_count": len(sales)}
    
    except Exception as e:
        # Логируем ошибку
        async for db in get_db():
            log = await db.get(SyncLog, log_id)
            if log:
                log.status = "error"
                log.finished_at = datetime.utcnow()
                log.error_message = str(e)
                await db.commit()
            break
        
        raise
