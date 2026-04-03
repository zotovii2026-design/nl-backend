from celery import shared_task
from celery import Celery
from core.config import settings
import logging

logger = logging.getLogger(__name__)

# Создание Celery приложения
celery_app = Celery(
    "nl_backend_sync",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=["tasks.sync_tasks"]
)

# Настройки Celery
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    beat_schedule={
        # Синхронизация товаров каждые 30 минут
        "sync-wb-products": {
            "task": "tasks.sync_tasks.sync_wb_products",
            "schedule": 30 * 60,  # 30 минут
        },
        # Синхронизация продаж каждые 1 час
        "sync-wb-sales": {
            "task": "tasks.sync_tasks.sync_wb_sales",
            "schedule": 60 * 60,  # 1 час
        },
        # Синхронизация заказов каждые 2 часа
        "sync-wb-orders": {
            "task": "tasks.sync_tasks.sync_wb_orders",
            "schedule": 2 * 60 * 60,  # 2 часа
        },
    },
)


@shared_task(name="sync_wb_products")
def sync_wb_products(organization_id: str):
    """Синхронизация товаров Wildberries"""
    from services.wb_api.keys import get_decrypted_wb_api_key
    from services.wb_api.client import get_wb_client
    from core.database import async_session
    from sqlalchemy import select
    from models.sync import SyncLog
    
    logger.info(f"Starting WB products sync for organization: {organization_id}")
    
    try:
        # TODO: Реализовать логику синхронизации товаров
        # 1. Получить WB API ключ организации
        # 2. Расшифровать ключ
        # 3. Создать клиент WB API
        # 4. Загрузить товары
        # 5. Сохранить в БД
        
        logger.info(f"Successfully synced WB products for organization: {organization_id}")
        
        return {
            "status": "completed",
            "organization_id": organization_id,
            "synced_count": 0  # TODO: заменить на реальное количество
        }
        
    except Exception as e:
        logger.error(f"Failed to sync WB products for organization {organization_id}: {str(e)}")
        
        return {
            "status": "failed",
            "organization_id": organization_id,
            "error": str(e)
        }


@shared_task(name="sync_wb_sales")
def sync_wb_sales(organization_id: str, date_from: str = None):
    """Синхронизация продаж Wildberries"""
    from services.wb_api.keys import get_decrypted_wb_api_key
    from services.wb_api.client import get_wb_client
    import logging
    
    logger.info(f"Starting WB sales sync for organization: {organization_id}")
    
    try:
        # TODO: Реализовать логику синхронизации продаж
        # 1. Получить WB API ключ организации
        # 2. Расшифровать ключ
        # 3. Создать клиент WB API
        # 4. Загрузить продажи за период
        # 5. Сохранить в БД
        
        logger.info(f"Successfully synced WB sales for organization: {organization_id}")
        
        return {
            "status": "completed",
            "organization_id": organization_id,
            "synced_count": 0  # TODO: заменить на реальное количество
        }
        
    except Exception as e:
        logger.error(f"Failed to sync WB sales for organization {organization_id}: {str(e)}")
        
        return {
            "status": "failed",
            "organization_id": organization_id,
            "error": str(e)
        }


@shared_task(name="sync_wb_orders")
def sync_wb_orders(organization_id: str, date_from: str = None):
    """Синхронизация заказов Wildberries"""
    from services.wb_api.keys import get_decrypted_wb_api_key
    from services.wb_api.client import get_wb_client
    import logging
    
    logger.info(f"Starting WB orders sync for organization: {organization_id}")
    
    try:
        # TODO: Реализовать логику синхронизации заказов
        # 1. Получить WB API ключ организации
        # 2. Расшифровать ключ
        # 3. Создать клиент WB API
        # 4. Загрузить заказы за период
        # 5. Сохранить в БД
        
        logger.info(f"Successfully synced WB orders for organization: {organization_id}")
        
        return {
            "status": "completed",
            "organization_id": organization_id,
            "synced_count": 0  # TODO: заменить на реальное количество
        }
        
    except Exception as e:
        logger.error(f"Failed to sync WB orders for organization {organization_id}: {str(e)}")
        
        return {
            "status": "failed",
            "organization_id": organization_id,
            "error": str(e)
        }


@shared_task(name="sync_organization_data")
def sync_organization_data(organization_id: str):
    """Полная синхронизация данных организации (товары + продажи + заказы)"""
    from celery import group
    
    logger.info(f"Starting full sync for organization: {organization_id}")
    
    # Запуск всех задач синхронизации параллельно
    sync_group = group(
        sync_wb_products.s(organization_id),
        sync_wb_sales.s(organization_id),
        sync_wb_orders.s(organization_id)
    )
    
    sync_group.apply_async()
    
    return {
        "status": "started",
        "organization_id": organization_id,
        "message": "Started parallel sync for products, sales, and orders"
    }


@shared_task(name="cleanup_old_sync_logs")
def cleanup_old_sync_logs(days: int = 7):
    """Очистка старых логов синхронизации"""
    # TODO: Реализовать логику очистки старых логов
    
    logger.info(f"Cleaned up sync logs older than {days} days")
    
    return {
        "status": "completed",
        "days": days
    }
