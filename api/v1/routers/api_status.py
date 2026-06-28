"""WB API data-source status for the Connections page."""

from datetime import date, datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.tenant_auth import require_query_organization_access
from models.raw_data import RawApiData


router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)


WB_API_REGISTRY: list[dict[str, Any]] = [
    {
        "method": "products",
        "title": "Карточки товаров",
        "endpoint": "POST /content/v2/get/cards/list",
        "sections": ["Справочник", "Юнит Экономика", "Стол маркетолога"],
        "task": "wb.sched.products",
        "schedule": "03:05 МСК",
        "expected_days": 1,
        "stale_hours": 36,
        "storage": "raw_api_data, product_entities, reference_book",
        "update_mode": "перезапись текущего снимка",
    },
    {
        "method": "sales",
        "title": "Продажи",
        "endpoint": "GET /api/v1/supplier/sales",
        "sections": ["РНП", "ОПиУ", "Основные показатели"],
        "task": "wb.sched.sales",
        "schedule": "08:00 / 14:05 МСК",
        "expected_days": 3,
        "stale_hours": 20,
        "storage": "raw_api_data -> tech_status",
        "update_mode": "перезапись дня",
    },
    {
        "method": "orders",
        "title": "Заказы",
        "endpoint": "GET /api/v1/supplier/orders",
        "sections": ["РНП", "ОПиУ", "Основные показатели", "План продаж"],
        "task": "wb.sched.orders",
        "schedule": "08:05 МСК",
        "expected_days": 3,
        "stale_hours": 28,
        "storage": "raw_api_data -> tech_status",
        "update_mode": "перезапись дня",
    },
    {
        "method": "stocks_fbo",
        "title": "Остатки FBO",
        "endpoint": "POST /api/analytics/v1/stocks-report/wb-warehouses",
        "sections": ["Склады", "Потребность FBO", "РНП"],
        "task": "wb.sched.stocks_fbo",
        "schedule": "03:23 / 14:03 МСК",
        "expected_days": 1,
        "stale_hours": 18,
        "storage": "raw_api_data -> tech_status",
        "update_mode": "моментальный срез",
    },
    {
        "method": "prices",
        "title": "Цены и СПП",
        "endpoint": "GET /api/v2/list/goods/filter",
        "sections": ["Справочник", "Юнит Экономика", "РНП"],
        "task": "wb.sched.prices",
        "schedule": "каждый час :30",
        "expected_days": 1,
        "stale_hours": 3,
        "storage": "raw_api_data, tech_status, reference_book",
        "update_mode": "обновление текущих цен",
    },
    {
        "method": "tariffs_commission",
        "title": "Комиссии WB",
        "endpoint": "GET /api/v1/tariffs/commission",
        "sections": ["Справочник", "Юнит Экономика"],
        "task": "wb.sched.commission",
        "schedule": "08:15 МСК",
        "expected_days": 1,
        "stale_hours": 36,
        "storage": "raw_api_data -> wb_tariff_snapshot",
        "update_mode": "снимок тарифов",
    },
    {
        "method": "tariffs_box",
        "title": "Тарифы коробов",
        "endpoint": "GET /api/v1/tariffs/box",
        "sections": ["Юнит Экономика", "Справочник"],
        "task": "wb.sched.tariffs",
        "schedule": "20:00 МСК",
        "expected_days": 1,
        "stale_hours": 36,
        "storage": "raw_api_data -> wb_tariff_snapshot",
        "update_mode": "снимок тарифов",
    },
    {
        "method": "adverts",
        "title": "Список РК",
        "endpoint": "GET /adv/v1/promotion/count",
        "sections": ["Реклама", "Стол маркетолога"],
        "task": "wb.sched.adverts",
        "schedule": "20:05 МСК",
        "expected_days": 1,
        "stale_hours": 36,
        "storage": "raw_api_data, ad_campaigns",
        "update_mode": "снимок кампаний",
    },
    {
        "method": "sales_funnel",
        "title": "Воронка продаж",
        "endpoint": "POST /api/analytics/v3/sales-funnel/products",
        "sections": ["Основные показатели", "Стол маркетолога", "РНП"],
        "task": "wb.sched.sales_funnel",
        "schedule": "21:30 МСК",
        "expected_days": 1,
        "stale_hours": 36,
        "storage": "raw_api_data -> tech_status",
        "update_mode": "снимок дня",
    },
    {
        "method": "ad_balance",
        "title": "Баланс рекламы",
        "endpoint": "GET /adv/v1/balance",
        "sections": ["Реклама"],
        "task": "wb.sched.ad_stats",
        "schedule": "21:00 МСК / Вс 04:30 МСК",
        "expected_days": 1,
        "stale_hours": 36,
        "storage": "raw_api_data, ad_stats",
        "update_mode": "снимок",
    },
]


def _aware(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _status_for(row: RawApiData | None, ok_days: int, expected_days: int, stale_hours: int) -> tuple[str, str]:
    if row is None:
        return "no_data", "нет данных"
    if row.status == "error":
        return "error", "ошибка WB/API"
    fetched_at = _aware(row.fetched_at)
    if fetched_at:
        age_hours = (datetime.now(timezone.utc) - fetched_at).total_seconds() / 3600
        if age_hours > stale_hours:
            return "stale", "устарело"
    if ok_days < expected_days:
        return "partial", f"{ok_days} из {expected_days}"
    if row.status == "partial":
        return "partial", "частично"
    return "ok", "ок"


@router.get("/api/v1/nl/wb-api-status")
async def wb_api_status(org_id: str, db: AsyncSession = Depends(get_db)):
    """Return a read-only WB API passport with live raw-data status."""
    methods = [item["method"] for item in WB_API_REGISTRY]
    max_expected_days = max(item["expected_days"] for item in WB_API_REGISTRY)
    min_date = date.today().toordinal() - max_expected_days + 1
    min_target_date = date.fromordinal(min_date)

    latest_subq = (
        select(
            RawApiData.api_method.label("api_method"),
            func.max(RawApiData.fetched_at).label("max_fetched_at"),
        )
        .where(RawApiData.organization_id == org_id)
        .where(RawApiData.api_method.in_(methods))
        .group_by(RawApiData.api_method)
        .subquery()
    )
    latest_result = await db.execute(
        select(RawApiData)
        .join(
            latest_subq,
            and_(
                RawApiData.api_method == latest_subq.c.api_method,
                RawApiData.fetched_at == latest_subq.c.max_fetched_at,
            ),
        )
        .where(RawApiData.organization_id == org_id)
    )
    latest_by_method: dict[str, RawApiData] = {}
    for row in latest_result.scalars().all():
        latest_by_method.setdefault(row.api_method, row)

    coverage_result = await db.execute(
        select(
            RawApiData.api_method,
            RawApiData.target_date,
            RawApiData.status,
            RawApiData.records_count,
        )
        .where(RawApiData.organization_id == org_id)
        .where(RawApiData.api_method.in_(methods))
        .where(RawApiData.target_date >= min_target_date)
    )
    ok_dates_by_method: dict[str, set[date]] = {method: set() for method in methods}
    records_by_method: dict[str, dict[date, int]] = {method: {} for method in methods}
    for method, target_date, status, records_count in coverage_result.all():
        if status == "ok":
            ok_dates_by_method.setdefault(method, set()).add(target_date)
            method_records = records_by_method.setdefault(method, {})
            method_records[target_date] = method_records.get(target_date, 0) + int(records_count or 0)

    rows = []
    summary = {"ok": 0, "partial": 0, "error": 0, "stale": 0, "no_data": 0}
    for item in WB_API_REGISTRY:
        method = item["method"]
        latest = latest_by_method.get(method)
        expected_days = item["expected_days"]
        method_min_date = date.fromordinal(date.today().toordinal() - expected_days + 1)
        method_ok_dates = {
            target_date
            for target_date in ok_dates_by_method.get(method, set())
            if target_date >= method_min_date
        }
        ok_days = len(method_ok_dates)
        records_count = sum(
            count
            for target_date, count in records_by_method.get(method, {}).items()
            if target_date >= method_min_date
        )
        status, status_label = _status_for(latest, ok_days, expected_days, item["stale_hours"])
        summary[status] += 1
        rows.append({
            **item,
            "status": status,
            "status_label": status_label,
            "last_target_date": latest.target_date.isoformat() if latest and latest.target_date else None,
            "last_fetched_at": latest.fetched_at.isoformat() if latest and latest.fetched_at else None,
            "last_records_count": latest.records_count if latest else None,
            "records_count": records_count,
            "coverage_count": ok_days,
            "coverage_expected": expected_days,
            "coverage_label": f"{ok_days} из {expected_days}",
            "last_error": latest.error_message if latest else None,
        })

    return {
        "org_id": org_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "items": rows,
    }
