from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.tenant_auth import require_query_organization_access
from schemas.sales_plan import SalesPlanItem
from services.sales_plans import (
    list_sales_plans,
    patch_sales_plan,
    remove_sales_plan,
    save_sales_plan_batch,
    save_sales_plan_item,
    summarize_sales_plans,
)


router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)


@router.get("/api/v1/nl/sales-plans")
async def get_sales_plans(
    org_id: str,
    period: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """План продаж — список по организации с фильтром по периоду"""
    return await list_sales_plans(org_id, period, db)


@router.post("/api/v1/nl/sales-plans")
async def save_sales_plan(
    data: SalesPlanItem,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Создать/обновить план продаж"""
    return await save_sales_plan_item(org_id, data, db)


@router.put("/api/v1/nl/sales-plans/{plan_id}")
async def update_sales_plan(
    plan_id: str,
    data: dict,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Обновить отдельные поля плана продаж"""
    return await patch_sales_plan(org_id, plan_id, data, db)


@router.delete("/api/v1/nl/sales-plans/{plan_id}")
async def delete_sales_plan(
    plan_id: str,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Удалить план продаж"""
    return await remove_sales_plan(org_id, plan_id, db)


@router.post("/api/v1/nl/sales-plans/batch")
async def batch_sales_plans(
    items: list[SalesPlanItem],
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Массовое создание/обновление планов продаж"""
    return await save_sales_plan_batch(org_id, items, db)


@router.get("/api/v1/nl/sales-plans/summary")
async def sales_plans_summary(
    org_id: str,
    period: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Сводка по плану продаж"""
    return await summarize_sales_plans(org_id, period, db)
