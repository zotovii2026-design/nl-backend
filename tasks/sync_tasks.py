from celery import shared_task
from sqlalchemy.ext.asyncio import AsyncSession
from core.database import async_session
import logging

logger = logging.getLogger(__name__)


@shared_task(name="sync_wb_data")
def sync_wb_data():
    """Синхронизация данных с Wildberries API"""
    # TODO: Реализовать логику синхронизации
    logger.info("Starting WB data sync...")
    return {"status": "completed", "synced": 0}


@shared_task(name="sync_organization_data")
def sync_organization_data(organization_id: str):
    """Синхронизация данных для конкретной организации"""
    # TODO: Реализовать логику синхронизации для организации
    logger.info(f"Syncing data for organization: {organization_id}")
    return {"status": "completed", "organization_id": organization_id}
