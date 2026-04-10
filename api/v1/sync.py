from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from typing import Optional
from datetime import datetime

from core.database import get_db
from models.user import User
from models.wb_data import SyncLog
from core.dependencies import get_current_user

router = APIRouter(prefix="/sync", tags=["Synchronization"])


@router.post("/products")
async def sync_products(
    api_key_id: UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Запустить синхронизацию товаров"""
    from tasks.wb_sync import sync_products_task
    
    result = sync_products_task.delay(str(api_key_id), str(current_user.id))
    
    return {
        "status": "started",
        "task_id": result.id,
        "message": "Синхронизация товаров запущена"
    }


@router.post("/sales")
async def sync_sales(
    api_key_id: UUID,
    days: int = 7,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Запустить синхронизацию продаж"""
    from tasks.wb_sync import sync_sales_task
    
    result = sync_sales_task.delay(str(api_key_id), str(current_user.id), days)
    
    return {
        "status": "started",
        "task_id": result.id,
        "message": "Синхронизация продаж запущена"
    }


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
