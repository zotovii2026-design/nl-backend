from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from core.database import get_db
from core.dependencies import get_current_user
from core.role_deps import require_organization_role
from models.user import User
from models.organization import Role

router = APIRouter(prefix="/organizations/{org_id}/sync", tags=["Sync"])


@router.post("/products")
async def trigger_products_sync(
    org_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Запуск синхронизации товаров вручную (admin+)"""
    from tasks.celery_app_new import sync_wb_products
    
    # Проверка прав
    await require_organization_role(org_id, Role.ADMIN, current_user, db)
    
    # Запуск задачи
    task = sync_wb_products.delay(organization_id=org_id)
    
    return {
        "message": "Products sync started",
        "task_id": task.id,
        "status": "pending"
    }


@router.post("/sales")
async def trigger_sales_sync(
    org_id: str,
    date_from: str = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Запуск синхронизации продаж вручную (admin+)"""
    from tasks.celery_app_new import sync_wb_sales
    
    # Проверка прав
    await require_organization_role(org_id, Role.ADMIN, current_user, db)
    
    # Запуск задачи
    task = sync_wb_sales.delay(organization_id=org_id, date_from=date_from)
    
    return {
        "message": "Sales sync started",
        "task_id": task.id,
        "status": "pending",
        "date_from": date_from
    }


@router.post("/orders")
async def trigger_orders_sync(
    org_id: str,
    date_from: str = None,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Запуск синхронизации заказов вручную (admin+)"""
    from tasks.celery_app_new import sync_wb_orders
    
    # Проверка прав
    await require_organization_role(org_id, Role.ADMIN, current_user, db)
    
    # Запуск задачи
    task = sync_wb_orders.delay(organization_id=org_id, date_from=date_from)
    
    return {
        "message": "Orders sync started",
        "task_id": task.id,
        "status": "pending",
        "date_from": date_from
    }


@router.post("/full")
async def trigger_full_sync(
    org_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Запуск полной синхронизации (товары + продажи + заказы) (admin+)"""
    from tasks.celery_app_new import sync_organization_data
    
    # Проверка прав
    await require_organization_role(org_id, Role.ADMIN, current_user, db)
    
    # Запуск задачи
    task = sync_organization_data.delay(organization_id=org_id)
    
    return {
        "message": "Full sync started",
        "task_id": task.id,
        "status": "pending"
    }


@router.get("/status/{task_id}")
async def get_sync_status(
    org_id: str,
    task_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Получение статуса задачи синхронизации (viewer+)"""
    from celery.result import AsyncResult
    
    # Проверка прав
    await require_organization_role(org_id, Role.VIEWER, current_user, db)
    
    # Получение статуса задачи
    result = AsyncResult(task_id)
    
    return {
        "task_id": task_id,
        "status": result.status,
        "result": result.result if result.ready() else None,
        "failed": result.failed(),
        "info": result.info
    }


@router.get("/logs")
async def list_sync_logs(
    org_id: str,
    limit: int = 50,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db)
):
    """Получение логов синхронизации организации (viewer+)"""
    # Проверка прав
    await require_organization_role(org_id, Role.VIEWER, current_user, db)
    
    # TODO: Реализовать запрос логов из БД
    
    return {
        "organization_id": org_id,
        "logs": [],
        "limit": limit
    }
