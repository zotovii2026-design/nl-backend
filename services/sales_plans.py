from datetime import datetime
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.organization import Organization
from repositories.sales_plans import (
    delete_sales_plan_by_id,
    fetch_sales_plan_summary,
    fetch_sales_plans,
    find_entity_by_nm,
    find_entity_labels,
    update_sales_plan_fields,
    upsert_sales_plan,
)
from schemas.sales_plan import SalesPlanItem


def _parse_period(period: str):
    return datetime.strptime(period, "%Y-%m-%d").date()


def _parse_month_period(period: str):
    return _parse_period(period).replace(day=1)


async def resolve_sales_plan_organization_id(
    org_id: str,
    db: AsyncSession,
) -> UUID:
    try:
        return UUID(str(org_id))
    except (TypeError, ValueError):
        try:
            seller_id = int(org_id)
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid organization ID",
            ) from exc

    result = await db.execute(
        select(Organization.id).where(Organization.wb_seller_id == seller_id)
    )
    organization_id = result.scalar_one_or_none()
    if not organization_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Organization not found",
        )
    return organization_id


def _serialize_plan(row):
    plan_value = float(row[7]) if row[7] else 0
    actual_value = float(row[8]) if row[8] else 0
    return {
        "id": str(row[0]),
        "entity_id": str(row[1]) if row[1] else None,
        "nm_id": row[2],
        "vendor_code": row[3],
        "size_name": row[4],
        "period": str(row[5]),
        "plan_type": row[6],
        "plan_value": plan_value,
        "actual_value": actual_value,
        "sales_temp": float(row[9]) if row[9] else None,
        "seasonality": row[10],
        "created_at": str(row[11]) if row[11] else None,
        "updated_at": str(row[12]) if row[12] else None,
        "product_name": row[13],
        "photo_main": row[14],
        "pct_complete": (
            round(actual_value / plan_value * 100, 1) if plan_value > 0 else 0
        ),
    }


async def list_sales_plans(
    org_id: str,
    period: str | None,
    db: AsyncSession,
):
    organization_id = await resolve_sales_plan_organization_id(org_id, db)
    period_date = _parse_period(period) if period else None
    rows = await fetch_sales_plans(db, organization_id, period_date)
    return [_serialize_plan(row) for row in rows]


async def save_sales_plan_item(
    org_id: str,
    item: SalesPlanItem,
    db: AsyncSession,
):
    organization_id = await resolve_sales_plan_organization_id(org_id, db)
    period = _parse_month_period(item.period)
    entity_id = item.entity_id

    if not entity_id:
        entity = await find_entity_by_nm(db, organization_id, item.nm_id)
        entity_id = str(entity[0]) if entity else None

    vendor_code = item.vendor_code
    size_name = item.size_name
    if entity_id and (not vendor_code or not size_name):
        entity = await find_entity_labels(db, entity_id)
        if entity:
            vendor_code = vendor_code or entity[0]
            size_name = size_name or entity[1]

    await upsert_sales_plan(
        db,
        organization_id,
        item,
        period,
        entity_id=entity_id,
        vendor_code=vendor_code,
        size_name=size_name,
        update_identity_fields=True,
    )
    await db.commit()
    return {"status": "ok"}


async def patch_sales_plan(
    org_id: str,
    plan_id: str,
    data: dict,
    db: AsyncSession,
):
    organization_id = await resolve_sales_plan_organization_id(org_id, db)
    changed = await update_sales_plan_fields(db, organization_id, plan_id, data)
    if not changed:
        return {"status": "noop"}
    await db.commit()
    return {"status": "ok"}


async def remove_sales_plan(org_id: str, plan_id: str, db: AsyncSession):
    organization_id = await resolve_sales_plan_organization_id(org_id, db)
    await delete_sales_plan_by_id(db, organization_id, plan_id)
    await db.commit()
    return {"status": "ok"}


async def save_sales_plan_batch(
    org_id: str,
    items: list[SalesPlanItem],
    db: AsyncSession,
):
    organization_id = await resolve_sales_plan_organization_id(org_id, db)
    updated = 0
    for item in items:
        period = _parse_month_period(item.period)
        entity = await find_entity_by_nm(db, organization_id, item.nm_id)
        entity_id = str(entity[0]) if entity else None
        vendor_code = item.vendor_code or (entity[1] if entity else None)
        size_name = item.size_name or (entity[2] if entity else None)

        await upsert_sales_plan(
            db,
            organization_id,
            item,
            period,
            entity_id=entity_id,
            vendor_code=vendor_code,
            size_name=size_name,
            update_identity_fields=False,
        )
        updated += 1
    await db.commit()
    return {"status": "ok", "updated": updated}


async def summarize_sales_plans(
    org_id: str,
    period: str | None,
    db: AsyncSession,
):
    organization_id = await resolve_sales_plan_organization_id(org_id, db)
    period_date = _parse_period(period) if period else None
    rows = await fetch_sales_plan_summary(db, organization_id, period_date)
    return [
        {
            "plan_type": row[0],
            "total_plan": float(row[1]) if row[1] else 0,
            "total_actual": float(row[2]) if row[2] else 0,
            "items_count": row[3],
            "pct_complete": (
                round(float(row[2]) / float(row[1]) * 100, 1)
                if row[1] and float(row[1]) > 0
                else 0
            ),
            "green_count": row[4],
            "yellow_count": row[5],
            "red_count": row[6],
        }
        for row in rows
    ]
