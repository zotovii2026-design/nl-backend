from datetime import datetime
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from models.sales_plan import SalesPlan
from schemas.sales_plan import SalesPlanItem


async def fetch_sales_plans(
    db: AsyncSession,
    organization_id: UUID,
    period=None,
):
    sql = (
        "SELECT sp.id, sp.entity_id, sp.nm_id, sp.vendor_code, sp.size_name, "
        "sp.period, sp.plan_type, sp.plan_value, sp.actual_value, "
        "sp.sales_temp, sp.seasonality, sp.created_at, sp.updated_at, "
        "pe.product_name, pe.photo_main "
        "FROM sales_plans sp "
        "LEFT JOIN product_entities pe ON sp.entity_id = pe.id "
        "WHERE sp.organization_id = :org "
    )
    params = {"org": str(organization_id)}
    if period:
        sql += " AND sp.period = :period"
        params["period"] = period
    sql += " ORDER BY sp.nm_id, sp.period DESC"
    result = await db.execute(text(sql), params)
    return result.all()


async def find_entity_by_nm(
    db: AsyncSession,
    organization_id: UUID,
    nm_id: int,
):
    result = await db.execute(
        text(
            "SELECT id, vendor_code, size_name FROM product_entities "
            "WHERE organization_id = :org AND nm_id = :nm LIMIT 1"
        ),
        {"org": str(organization_id), "nm": nm_id},
    )
    return result.first()


async def find_entity_labels(db: AsyncSession, entity_id):
    result = await db.execute(
        text("SELECT vendor_code, size_name FROM product_entities WHERE id = :eid"),
        {"eid": str(entity_id)},
    )
    return result.first()


async def upsert_sales_plan(
    db: AsyncSession,
    organization_id: UUID,
    item: SalesPlanItem,
    period,
    entity_id=None,
    vendor_code=None,
    size_name=None,
    update_identity_fields: bool = True,
):
    ins = pg_insert(SalesPlan).values(
        organization_id=organization_id,
        entity_id=entity_id,
        nm_id=item.nm_id,
        vendor_code=vendor_code,
        size_name=size_name,
        period=period,
        plan_type=item.plan_type,
        plan_value=item.plan_value,
        actual_value=item.actual_value,
        sales_temp=item.sales_temp,
        seasonality=item.seasonality,
    )
    set_values = {
        "plan_value": ins.excluded.plan_value,
        "actual_value": ins.excluded.actual_value,
        "sales_temp": ins.excluded.sales_temp,
        "seasonality": ins.excluded.seasonality,
        "updated_at": datetime.utcnow(),
    }
    if update_identity_fields:
        set_values.update(
            {
                "vendor_code": ins.excluded.vendor_code,
                "size_name": ins.excluded.size_name,
            }
        )
    stmt = ins.on_conflict_do_update(
        constraint="sales_plans_org_entity_period_type_key",
        set_=set_values,
    )
    await db.execute(stmt)


async def update_sales_plan_fields(
    db: AsyncSession,
    organization_id: UUID,
    plan_id: str,
    data: dict,
):
    fields = []
    params = {"pid": plan_id, "org": str(organization_id)}
    for key in [
        "plan_value",
        "actual_value",
        "sales_temp",
        "plan_type",
        "seasonality",
        "vendor_code",
        "size_name",
    ]:
        if key in data:
            fields.append(f"{key} = :{key}")
            params[key] = data[key]
    if not fields:
        return False
    fields.append("updated_at = NOW()")
    await db.execute(
        text(
            f"UPDATE sales_plans SET {', '.join(fields)} "
            "WHERE id = :pid AND organization_id = :org"
        ),
        params,
    )
    return True


async def delete_sales_plan_by_id(
    db: AsyncSession,
    organization_id: UUID,
    plan_id: str,
):
    await db.execute(
        text("DELETE FROM sales_plans WHERE id = :pid AND organization_id = :org"),
        {"pid": plan_id, "org": str(organization_id)},
    )


async def fetch_sales_plan_summary(
    db: AsyncSession,
    organization_id: UUID,
    period=None,
):
    params = {"org": str(organization_id)}
    where = "WHERE organization_id = :org"
    if period:
        where += " AND period = :period"
        params["period"] = period
    result = await db.execute(
        text(
            f"SELECT plan_type, "
            f"SUM(plan_value) as total_plan, "
            f"SUM(actual_value) as total_actual, "
            f"COUNT(*) as items_count, "
            f"COUNT(*) FILTER (WHERE actual_value / NULLIF(plan_value,0) >= 0.9) as green_count, "
            f"COUNT(*) FILTER (WHERE actual_value / NULLIF(plan_value,0) >= 0.7 AND actual_value / NULLIF(plan_value,0) < 0.9) as yellow_count, "
            f"COUNT(*) FILTER (WHERE actual_value / NULLIF(plan_value,0) < 0.7) as red_count "
            f"FROM sales_plans {where} GROUP BY plan_type"
        ),
        params,
    )
    return result.all()
