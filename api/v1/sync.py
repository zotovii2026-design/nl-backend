from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from typing import Optional
from datetime import datetime

from core.database import get_db
from models.user import User
from models.wb_data import WbProduct, SyncLog
from core.dependencies import get_current_user
from sqlalchemy import select
from services.wb_api.client import get_wb_client
from core.security import decrypt_data
from models.organization import WbApiKey

router = APIRouter(prefix="/sync", tags=["Synchronization"])


@router.post("/products")
async def sync_products(
    api_key_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Запустить синхронизацию товаров (синхронно)"""
    log_id = None
    
    try:
        # Получаем API ключ из БД
        result = await db.execute(
            select(WbApiKey).where(WbApiKey.id == api_key_id)
        )
        api_key_record = result.scalar_one_or_none()
        
        if not api_key_record:
            raise Exception(f"API key {api_key_id} not found")
        
        # Расшифровываем ключ
        decrypted_key = decrypt_data(api_key_record.api_key)
        
        # Создаём лог синхронизации
        log = SyncLog(
            organization_id=str(api_key_record.organization_id),
            task_name="wb.sync_products",
            status="running",
            started_at=datetime.utcnow()
        )
        db.add(log)
        await db.commit()
        await db.refresh(log)
        log_id = log.id
        
        # Получаем карточки из WB API
        client = await get_wb_client(decrypted_key)
        cards = await client.get_all_cards()
        # client.close() removed
        
        # Сохраняем карточки в БД
        synced_count = 0
        for card in cards:
            # Проверяем, существует ли карточка
            result = await db.execute(
                select(WbProduct).where(
                    WbProduct.nm_id == card.get("nmID"),
                    WbProduct.organization_id == str(api_key_record.organization_id)
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
                    organization_id=str(api_key_record.organization_id),
                    need_kiz=card.get("needKiz", False),
                    kiz_marked=card.get("kizMarked", False),
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                    synced_at=datetime.utcnow()
                )
                db.add(product)
                synced_count += 1
        
        await db.commit()
        
        # Обновляем лог синхронизации
        log.status = "success"
        log.finished_at = datetime.utcnow()
        log.synced_count = len(cards)
        log.error_message = None
        await db.commit()
        
        return {
            "status": "success",
            "cards_count": len(cards),
            "synced_count": synced_count
        }
    
    except Exception as e:
        # Логируем ошибку
        if log_id:
            log = await db.get(SyncLog, log_id)
            if log:
                log.status = "error"
                log.finished_at = datetime.utcnow()
                log.error_message = str(e)
                await db.commit()
        
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sales")
async def sync_sales(
    api_key_id: UUID,
    days: int = 7,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Запустить минимальную синхронизацию агрегированных продаж WB"""
    log_id = None

    try:
        result = await db.execute(select(WbApiKey).where(WbApiKey.id == api_key_id))
        api_key_record = result.scalar_one_or_none()
        if not api_key_record:
            raise Exception(f"API key {api_key_id} not found")

        decrypted_key = decrypt_data(api_key_record.api_key)

        log = SyncLog(
            organization_id=str(api_key_record.organization_id),
            task_name="wb.sync_sales",
            status="running",
            started_at=datetime.utcnow()
        )
        db.add(log)
        await db.commit()
        await db.refresh(log)
        log_id = log.id

        from datetime import timedelta
        from models.wb_data import WbSale
        date_to = datetime.utcnow().date()
        date_from = date_to - timedelta(days=max(days - 1, 0))

        client = await get_wb_client(decrypted_key)
        products = await client.get_sales_funnel_products(
            date_from=date_from.isoformat(),
            date_to=date_to.isoformat()
        )

        synced_count = 0
        for item in products:
            product = item.get("product", {})
            statistic = item.get("statistic", {})
            selected = statistic.get("selected", {})
            nm_id = product.get("nmId")
            sale_id = f"analytics:{date_from.isoformat()}:{date_to.isoformat()}:{nm_id}"

            existing = await db.execute(select(WbSale).where(WbSale.sale_id == sale_id))
            if existing.scalar_one_or_none():
                continue

            row = WbSale(
                sale_id=sale_id,
                organization_id=str(api_key_record.organization_id),
                date_from=datetime.fromisoformat(date_from.isoformat()),
                date_to=datetime.fromisoformat(date_to.isoformat()),
                income=float(selected.get("orderSum", 0) or 0),
                penalty=float(selected.get("cancelSum", 0) or 0),
                reward=float(selected.get("buyoutSum", 0) or 0),
                quantity=int(selected.get("orderCount", 0) or 0),
                total_price=float(selected.get("orderSum", 0) or 0),
                price_with_disc=float(selected.get("avgPrice", 0) or 0),
                nm_id=nm_id,
                subject=product.get("subjectName", ""),
                brand=product.get("brandName", ""),
                g_number=product.get("vendorCode", ""),
                supplier_oper_name="analytics_sales_funnel",
                synced_at=datetime.utcnow()
            )
            db.add(row)
            synced_count += 1

        await db.commit()

        log.status = "success"
        log.finished_at = datetime.utcnow()
        log.synced_count = synced_count
        log.error_message = None
        await db.commit()

        return {
            "status": "success",
            "sales_count": synced_count,
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat()
        }

    except Exception as e:
        if log_id:
            log = await db.get(SyncLog, log_id)
            if log:
                log.status = "error"
                log.finished_at = datetime.utcnow()
                log.error_message = str(e)
                await db.commit()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs")
async def get_sync_logs(
    limit: int = 50,
    offset: int = 0,
    sync_type: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Получить логи синхронизации"""
    from sqlalchemy import select, desc
    
    query = select(SyncLog).order_by(desc(SyncLog.started_at))
    
    if sync_type:
        query = query.where(SyncLog.sync_type == sync_type)
    
    result = await db.execute(query.offset(offset).limit(limit))
    logs = result.scalars().all()
    
    return {
        "logs": [
            {
                "id": str(log.id),
                "task_name": log.task_name,
                "status": log.status,
                "synced_count": log.synced_count,
                "error_message": log.error_message,
                "started_at": log.started_at.isoformat() if log.started_at else None,
                "finished_at": log.finished_at.isoformat() if log.finished_at else None,
                "duration_seconds": log.duration_seconds
            }
            for log in logs
        ]
    }
