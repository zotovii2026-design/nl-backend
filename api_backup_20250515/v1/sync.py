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
from services.wb_api.client import get_wb_client, WBApiClient
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
        
        # Используем Personal token (приоритет), либо обычный API key
        import sys
        from core.security import decrypt_data as dd
        token = None
        if api_key_record.personal_token:
            token = dd(api_key_record.personal_token)
        if not token:
            token = decrypt_data(api_key_record.api_key)
        
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
        # DEBUG
        import sys
        
        client = WBApiClient(token)
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
        import traceback, logging
        logging.error(f"SYNC PRODUCTS ERROR: {traceback.format_exc()}")
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

        from core.security import decrypt_data as dd
        token = None
        if api_key_record.personal_token:
            token = dd(api_key_record.personal_token)
        if not token:
            token = decrypt_data(api_key_record.api_key)

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

        client = WBApiClient(token)
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





@router.post("/orders")
async def sync_orders(
    api_key_id: UUID,
    days: int = 7,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Синхронизация заказов WB через Statistics API"""
    log_id = None
    try:
        result = await db.execute(select(WbApiKey).where(WbApiKey.id == api_key_id))
        api_key_record = result.scalar_one_or_none()
        if not api_key_record:
            raise Exception(f"API key {api_key_id} not found")

        from core.security import decrypt_data as dd
        token = None
        if api_key_record.personal_token:
            token = dd(api_key_record.personal_token)
        if not token:
            token = decrypt_data(api_key_record.api_key)

        log = SyncLog(
            organization_id=str(api_key_record.organization_id),
            task_name="wb.sync_orders",
            status="running",
            started_at=datetime.utcnow()
        )
        db.add(log)
        await db.commit()
        await db.refresh(log)
        log_id = log.id

        from datetime import timedelta
        from models.wb_data import WbOrder
        import httpx

        date_from = (datetime.utcnow().date() - timedelta(days=days)).isoformat()

        # Прямой запрос к WB Statistics API
        async with httpx.AsyncClient(timeout=30.0) as http:
            r = await http.get(
                "https://statistics-api.wildberries.ru/api/v1/supplier/orders",
                params={"dateFrom": date_from},
                headers={"Authorization": f"Bearer {token}"}
            )
            r.raise_for_status()
            orders_data = r.json()

        synced_count = 0
        for item in orders_data:
            # WB возвращает srid как уникальный ID заказа
            order_id = str(item.get("srid", ""))
            if not order_id:
                continue

            existing = await db.execute(
                select(WbOrder).where(WbOrder.order_id == order_id)
            )
            if existing.scalar_one_or_none():
                continue

            raw_date = item.get("date", "")
            raw_change = item.get("lastChangeDate", "")

            order = WbOrder(
                organization_id=str(api_key_record.organization_id),
                order_id=order_id,
                g_number=str(item.get("gNumber", "")),
                date=datetime.fromisoformat(raw_date) if raw_date else datetime.utcnow(),
                last_change_date=datetime.fromisoformat(raw_change) if raw_change else None,
                status="cancel" if item.get("isCancel") else "active",
                nm_id=item.get("nmId"),
                subject=item.get("subject", ""),
                brand=item.get("brand", ""),
                tech_size=item.get("techSize", ""),
                total_price=item.get("totalPrice"),
                quantity=1,
                barcode=item.get("barcode", ""),
                warehouse_name=item.get("warehouseName", ""),
                region_name=item.get("regionName", ""),
                is_supply=str(item.get("isSupply", "")),
                synced_at=datetime.utcnow()
            )
            db.add(order)
            synced_count += 1

        await db.commit()

        log.status = "success"
        log.finished_at = datetime.utcnow()
        log.synced_count = synced_count
        log.error_message = None
        await db.commit()

        return {
            "status": "success",
            "orders_count": synced_count,
            "date_from": date_from
        }

    except HTTPException:
        raise
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


@router.post("/stocks")
async def sync_stocks(
    api_key_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Синхронизация остатков WB по складам (требуется Personal token)"""
    log_id = None
    try:
        result = await db.execute(select(WbApiKey).where(WbApiKey.id == api_key_id))
        key_rec = result.scalar_one_or_none()
        if not key_rec:
            raise Exception(f"API key {api_key_id} not found")

        from core.security import decrypt_data as dd
        personal_token = dd(key_rec.personal_token) if key_rec.personal_token else None
        if not personal_token:
            raise HTTPException(status_code=400, detail="Personal token not set. Set it via POST /organizations/{org_id}/wb-keys/{key_id}/personal-token")

        log = SyncLog(
            organization_id=str(key_rec.organization_id),
            task_name="wb.sync_stocks",
            status="running",
            started_at=datetime.utcnow()
        )
        db.add(log)
        await db.commit()
        await db.refresh(log)
        log_id = log.id

        from services.wb_api.client import WBApiClient
        client = WBApiClient(personal_token)
        stocks_data = await client.get_stocks_warehouses(is_archive=False)
        await client.client.aclose()

        from models.wb_data import WbStock
        count = 0
        for item in stocks_data:
            nm_id = item.get("nmID") or item.get("nmId")
            wh_name = item.get("warehouseName", "unknown")
            if not nm_id:
                continue
            existing = await db.execute(
                select(WbStock).where(
                    WbStock.nm_id == nm_id,
                    WbStock.warehouse_name == wh_name,
                    WbStock.organization_id == str(key_rec.organization_id)
                )
            )
            row = existing.scalar_one_or_none()
            if row:
                row.quantity = item.get("quantity", 0)
                row.quantity_full = item.get("quantityFull") or item.get("quantity_full", 0)
                row.in_way_to_client = item.get("inWayToClient", 0)
                row.in_way_from_client = item.get("inWayFromClient", 0)
                row.vendor_code = item.get("vendorCode") or row.vendor_code
                row.category = item.get("category") or row.category
                row.subject = item.get("subject") or row.subject
                row.brand = item.get("brand") or row.brand
                row.synced_at = datetime.utcnow()
            else:
                stock = WbStock(
                    organization_id=str(key_rec.organization_id),
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

        log.status = "success"
        log.finished_at = datetime.utcnow()
        log.synced_count = count
        await db.commit()

        return {"status": "success", "stocks_count": count}

    except HTTPException:
        raise
    except Exception as e:
        if log_id:
            log = await db.get(SyncLog, log_id)
            if log:
                log.status = "error"
                log.finished_at = datetime.utcnow()
                log.error_message = str(e)
                await db.commit()
        raise HTTPException(status_code=500, detail=str(e))
