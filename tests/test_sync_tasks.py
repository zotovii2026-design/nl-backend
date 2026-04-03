from fastapi.testclient import TestClient
import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from models.sync import SyncLog
from tasks.celery_app_new import (
    sync_wb_products,
    sync_wb_sales,
    sync_wb_orders
)


@pytest.mark.asyncio
async def test_sync_products_task():
    """Тест задачи синхронизации товаров"""
    result = sync_wb_products.delay(organization_id="test-org-id")
    
    # Ожидание выполнения задачи
    task_result = result.get(timeout=10)
    
    assert task_result is not None
    assert "status" in task_result
    assert task_result["organization_id"] == "test-org-id"


@pytest.mark.asyncio
async def test_sync_sales_task():
    """Тест задачи синхронизации продаж"""
    result = sync_wb_sales.delay(organization_id="test-org-id", date_from="2026-01-01")
    
    # Ожидание выполнения задачи
    task_result = result.get(timeout=10)
    
    assert task_result is not None
    assert "status" in task_result


@pytest.mark.asyncio
async def test_sync_orders_task():
    """Тест задачи синхронизации заказов"""
    result = sync_wb_orders.delay(organization_id="test-org-id", date_from="2026-01-01")
    
    # Ожидание выполнения задачи
    task_result = result.get(timeout=10)
    
    assert task_result is not None
    assert "status" in task_result


@pytest.mark.asyncio
async def test_full_sync():
    """Тест полной синхронизации"""
    from tasks.celery_app_new import sync_organization_data
    
    result = sync_organization_data.delay(organization_id="test-org-id")
    
    # Ожидание выполнения задачи
    task_result = result.get(timeout=10)
    
    assert task_result is not None
    assert task_result["status"] in ["started", "completed"]


@pytest.mark.asyncio
async def test_cleanup_old_sync_logs():
    """Тест очистки старых логов"""
    from tasks.celery_app_new import cleanup_old_sync_logs
    
    result = cleanup_old_sync_logs.delay(days=7)
    
    # Ожидание выполнения задачи
    task_result = result.get(timeout=10)
    
    assert task_result is not None
    assert "status" in task_result
