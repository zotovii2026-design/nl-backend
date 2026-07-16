"""API endpoints for the rebuilt OPIU report."""

import io
from datetime import date
from decimal import Decimal

import openpyxl
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import StreamingResponse
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.dependencies import get_current_user
from core.role_deps import require_organization_role
from domain.opiu import build_opiu_report, serialize_report
from models.organization import Role
from models.user import User
from models.wb_finance import WbFinanceSync
from tasks.celery_app import celery_app


router = APIRouter(tags=["nl"])

ZERO = Decimal("0")


def _validate_period(date_from: date, date_to: date):
    if date_to < date_from:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="date_to must not be earlier than date_from",
        )
    if (date_to - date_from).days > 366:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="The maximum report period is 366 days",
        )


async def _load_report_rows(
    db: AsyncSession,
    organization_id: str,
    date_from: date,
    date_to: date,
):
    result = await db.execute(
        text(
            """
            SELECT
                fr.entity_id,
                fr.nm_id,
                fr.vendor_code,
                fr.barcode,
                fr.size_name,
                fr.doc_type_name,
                fr.seller_oper_name,
                fr.quantity,
                fr.return_amount,
                fr.retail_price,
                fr.retail_amount,
                fr.for_pay,
                fr.acquiring_fee,
                fr.delivery_service,
                fr.penalty,
                fr.paid_storage,
                fr.deduction,
                fr.paid_acceptance,
                fr.cashback_amount,
                fr.cashback_discount,
                fr.cashback_commission_change,
                pe.product_name,
                pe.photo_main,
                COALESCE(rb.brand, pe.brand, '') AS brand,
                COALESCE(rb.product_class, '') AS product_class,
                COALESCE(rb.product_status, '') AS product_status,
                COALESCE(rb.subject_name, pe.subject_name, '') AS subject_name
            FROM wb_finance_rows fr
            LEFT JOIN product_entities pe ON pe.id = fr.entity_id
            LEFT JOIN LATERAL (
                SELECT
                    ref.brand,
                    ref.product_class,
                    ref.product_status,
                    ref.subject_name
                FROM reference_book ref
                WHERE ref.organization_id = fr.organization_id
                  AND (
                      ref.entity_id = fr.entity_id
                      OR (
                          fr.entity_id IS NULL
                          AND ref.nm_id = fr.nm_id
                      )
                  )
                  AND (ref.valid_to IS NULL OR ref.valid_to >= :date_from)
                ORDER BY
                    CASE WHEN ref.entity_id = fr.entity_id THEN 0 ELSE 1 END,
                    ref.valid_from DESC
                LIMIT 1
            ) rb ON TRUE
            WHERE fr.organization_id = :org
              AND (
                  fr.operation_date::date BETWEEN :date_from AND :date_to
                  OR (
                      fr.operation_date IS NULL
                      AND fr.report_date_from <= :date_to
                      AND fr.report_date_to >= :date_from
                  )
              )
            ORDER BY fr.rrd_id
            """
        ),
        {
            "org": organization_id,
            "date_from": date_from,
            "date_to": date_to,
        },
    )

    names = [
        "entity_id",
        "nm_id",
        "vendor_code",
        "barcode",
        "size_name",
        "doc_type_name",
        "seller_oper_name",
        "quantity",
        "return_amount",
        "retail_price",
        "retail_amount",
        "for_pay",
        "acquiring_fee",
        "delivery_service",
        "penalty",
        "paid_storage",
        "deduction",
        "paid_acceptance",
        "cashback_amount",
        "cashback_discount",
        "cashback_commission_change",
        "product_name",
        "photo_main",
        "brand",
        "product_class",
        "product_status",
        "subject_name",
    ]
    return [dict(zip(names, row)) for row in result.all()]


def _as_decimal(value) -> Decimal:
    if value in (None, ""):
        return ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _money(value) -> float:
    return float(_as_decimal(value).quantize(Decimal("0.01")))


async def _load_ad_spend_by_nm(
    db: AsyncSession,
    organization_id: str,
    date_from: date,
    date_to: date,
):
    result = await db.execute(
        text(
            """
            SELECT sn.nm_id, COALESCE(SUM(sn.spent), 0) AS spent
            FROM ad_stats_nm sn
            WHERE sn.organization_id = :org
              AND sn.stat_date BETWEEN :date_from AND :date_to
              AND sn.nm_id IS NOT NULL
            GROUP BY sn.nm_id
            """
        ),
        {
            "org": organization_id,
            "date_from": date_from,
            "date_to": date_to,
        },
    )
    return {int(nm_id): _as_decimal(spent) for nm_id, spent in result.all()}


async def _load_sales_funnel_orders_by_nm(
    db: AsyncSession,
    organization_id: str,
    date_from: date,
    date_to: date,
):
    result = await db.execute(
        text(
            """
            SELECT target_date, raw_response
            FROM raw_api_data
            WHERE organization_id = :org
              AND api_method = 'sales_funnel'
              AND status = 'ok'
              AND target_date BETWEEN :date_from AND :date_to
            """
        ),
        {
            "org": organization_id,
            "date_from": date_from,
            "date_to": date_to,
        },
    )
    orders_by_nm = {}
    for target_date, raw_response in result.all():
        if not isinstance(raw_response, list):
            continue
        for item in raw_response:
            if not isinstance(item, dict):
                continue
            stat = (item.get("statistic") or {}).get("selected") or {}
            period = stat.get("period") or {}
            if period.get("start") and period.get("end"):
                if (
                    period.get("start") != str(target_date)
                    or period.get("end") != str(target_date)
                ):
                    continue
            nm_id = (item.get("product") or {}).get("nmId")
            if not nm_id:
                continue
            bucket = orders_by_nm.setdefault(
                int(nm_id),
                {"orders_qty": ZERO, "orders_sum": ZERO},
            )
            bucket["orders_qty"] += _as_decimal(stat.get("orderCount"))
            bucket["orders_sum"] += _as_decimal(stat.get("orderSum"))
    return orders_by_nm


async def _load_raw_orders_by_nm(
    db: AsyncSession,
    organization_id: str,
    date_from: date,
    date_to: date,
):
    result = await db.execute(
        text(
            """
            WITH raw_orders AS (
                SELECT
                    r.target_date,
                    COALESCE(o.elem->>'srid', '') AS srid,
                    COALESCE(
                        NULLIF(o.elem->>'nmId', '')::bigint,
                        NULLIF(o.elem->>'nm_id', '')::bigint
                    ) AS nm_id,
                    LEFT(o.elem->>'date', 10)::date AS order_date,
                    COALESCE(
                        NULLIF(o.elem->>'priceWithDisc', '')::numeric,
                        NULLIF(o.elem->>'totalPrice', '')::numeric,
                        NULLIF(o.elem->>'price', '')::numeric,
                        0
                    ) AS order_sum
                FROM raw_api_data r
                CROSS JOIN LATERAL jsonb_array_elements(r.raw_response) AS o(elem)
                WHERE r.organization_id = :org
                  AND r.api_method = 'orders'
                  AND r.status = 'ok'
                  AND r.target_date BETWEEN :date_from AND :date_to
            ),
            dedup_non_empty_orders AS (
                SELECT DISTINCT ON (srid) *
                FROM raw_orders
                WHERE srid <> ''
                ORDER BY srid, target_date
            ),
            dedup_orders AS (
                SELECT *
                FROM dedup_non_empty_orders
                UNION ALL
                SELECT *
                FROM raw_orders
                WHERE srid = ''
            )
            SELECT nm_id,
                   COUNT(*) AS orders_qty,
                   COALESCE(SUM(order_sum), 0) AS orders_sum
            FROM dedup_orders
            WHERE order_date BETWEEN :date_from AND :date_to
              AND nm_id IS NOT NULL
            GROUP BY nm_id
            """
        ),
        {
            "org": organization_id,
            "date_from": date_from,
            "date_to": date_to,
        },
    )
    return {
        int(nm_id): {
            "orders_qty": _as_decimal(orders_qty),
            "orders_sum": _as_decimal(orders_sum),
        }
        for nm_id, orders_qty, orders_sum in result.all()
    }


async def _load_cost_by_item(
    db: AsyncSession,
    organization_id: str,
    date_from: date,
):
    result = await db.execute(
        text(
            """
            SELECT DISTINCT ON (entity_id, nm_id)
                   entity_id,
                   nm_id,
                   COALESCE(cost_price, 0)
                 + COALESCE(purchase_cost, 0)
                 + COALESCE(packaging_cost, 0)
                 + COALESCE(logistics_cost, 0)
                 + COALESCE(other_costs, 0)
                 + COALESCE(extra_costs, 0)
                 + COALESCE(vat, 0) AS unit_cost
            FROM reference_book
            WHERE organization_id = :org
              AND (valid_to IS NULL OR valid_to >= :date_from)
            ORDER BY entity_id, nm_id, valid_from DESC, created_at DESC NULLS LAST
            """
        ),
        {"org": organization_id, "date_from": date_from},
    )
    by_entity = {}
    by_nm = {}
    for entity_id, nm_id, unit_cost in result.all():
        value = _as_decimal(unit_cost)
        if entity_id:
            by_entity[str(entity_id)] = value
        if nm_id:
            by_nm[int(nm_id)] = value
    return by_entity, by_nm


def _item_nm_id(item):
    nm_id = item.get("nm_id")
    if nm_id in (None, "", 0, "0"):
        return None
    return int(nm_id)


def _enrich_serialized_report(
    data: dict,
    ad_spend_by_nm: dict[int, Decimal],
    orders_by_nm: dict[int, dict],
    cost_by_entity: dict[str, Decimal],
    cost_by_nm: dict[int, Decimal],
):
    enriched_items = []
    totals = {
        "marketplace_commission_sum": ZERO,
        "acquiring_sum": ZERO,
        "realized_sum": ZERO,
        "advertising_api_spend": ZERO,
        "orders_qty": ZERO,
        "orders_sum": ZERO,
        "cost_total": ZERO,
        "other_expenses": ZERO,
        "net_profit": ZERO,
    }

    for item in data["items"]:
        nm_id = _item_nm_id(item)
        sales_qty = _as_decimal(item.get("sales_qty"))
        mp_sum = _as_decimal(item.get("marketplace_commission_unit")) * sales_qty
        acquiring_sum = _as_decimal(item.get("acquiring_unit")) * sales_qty
        realized_sum = _as_decimal(item.get("realized_unit")) * sales_qty
        ad_spend = ad_spend_by_nm.get(nm_id, ZERO) if nm_id else ZERO
        orders = orders_by_nm.get(nm_id, {}) if nm_id else {}
        orders_qty = _as_decimal(orders.get("orders_qty"))
        orders_sum = _as_decimal(orders.get("orders_sum"))
        unit_cost = cost_by_entity.get(
            str(item.get("entity_id") or ""),
            cost_by_nm.get(nm_id, ZERO) if nm_id else ZERO,
        )
        cost_total = unit_cost * sales_qty
        other_expenses = (
            _as_decimal(item.get("deduction"))
            + _as_decimal(item.get("acceptance"))
            + _as_decimal(item.get("distributed_other_expenses"))
            + _as_decimal(item.get("loyalty_points"))
            + _as_decimal(item.get("loyalty_participation"))
        )
        net_profit = _as_decimal(item.get("gross_profit")) - ad_spend - cost_total
        drr = ad_spend / orders_sum * 100 if orders_sum else ZERO

        item.update(
            {
                "marketplace_commission_sum": _money(mp_sum),
                "acquiring_sum": _money(acquiring_sum),
                "realized_sum": _money(realized_sum),
                "advertising_api_spend": _money(ad_spend),
                "orders_qty": float(orders_qty),
                "orders_sum": _money(orders_sum),
                "drr": _money(drr),
                "cost_unit": _money(unit_cost),
                "cost_total": _money(cost_total),
                "other_expenses": _money(other_expenses),
                "net_profit": _money(net_profit),
                "is_unassigned": item.get("vendor_code") == "(без артикула)",
            }
        )
        for key in totals:
            totals[key] += {
                "marketplace_commission_sum": mp_sum,
                "acquiring_sum": acquiring_sum,
                "realized_sum": realized_sum,
                "advertising_api_spend": ad_spend,
                "orders_qty": orders_qty,
                "orders_sum": orders_sum,
                "cost_total": cost_total,
                "other_expenses": other_expenses,
                "net_profit": net_profit,
            }[key]
        enriched_items.append(item)

    total = data["total"]
    total.update({key: _money(value) for key, value in totals.items()})
    total["orders_qty"] = float(totals["orders_qty"])
    total["drr"] = _money(
        totals["advertising_api_spend"] / totals["orders_sum"] * 100
        if totals["orders_sum"]
        else ZERO
    )
    total["is_unassigned"] = False
    data["items"] = enriched_items
    data["unassigned_items"] = [
        item for item in enriched_items if item.get("is_unassigned")
    ]
    product_items = [
        item for item in enriched_items if not item.get("is_unassigned")
    ]
    data["product_total"] = _product_total_row(product_items)
    return data


async def _enrichment_context(
    db: AsyncSession,
    organization_id: str,
    date_from: date,
    date_to: date,
):
    ad_spend_by_nm = await _load_ad_spend_by_nm(
        db, organization_id, date_from, date_to
    )
    orders_by_nm = await _load_sales_funnel_orders_by_nm(
        db, organization_id, date_from, date_to
    )
    raw_orders_by_nm = await _load_raw_orders_by_nm(
        db, organization_id, date_from, date_to
    )
    for nm_id, orders in raw_orders_by_nm.items():
        if nm_id not in orders_by_nm or not orders_by_nm[nm_id]["orders_sum"]:
            orders_by_nm[nm_id] = orders
    cost_by_entity, cost_by_nm = await _load_cost_by_item(
        db, organization_id, date_from
    )
    return ad_spend_by_nm, orders_by_nm, cost_by_entity, cost_by_nm


async def _sync_info(
    db: AsyncSession,
    organization_id: str,
    date_from: date,
    date_to: date,
):
    result = await db.execute(
        select(WbFinanceSync)
        .where(
            WbFinanceSync.organization_id == organization_id,
            WbFinanceSync.date_from == date_from,
            WbFinanceSync.date_to == date_to,
        )
        .order_by(WbFinanceSync.started_at.desc())
        .limit(1)
    )
    sync = result.scalar_one_or_none()
    if not sync:
        return None
    return {
        "status": sync.status,
        "rows_count": sync.rows_count,
        "bank_payment_sum": (
            float(sync.bank_payment_sum)
            if sync.bank_payment_sum is not None
            else None
        ),
        "calculated_payment_sum": (
            float(sync.calculated_payment_sum)
            if sync.calculated_payment_sum is not None
            else None
        ),
        "difference": (
            float(sync.difference) if sync.difference is not None else None
        ),
        "started_at": sync.started_at.isoformat(),
        "finished_at": (
            sync.finished_at.isoformat() if sync.finished_at else None
        ),
        "error_message": sync.error_message,
    }


async def _authorized_report(
    organization_id: str,
    date_from: date,
    date_to: date,
    current_user: User,
    db: AsyncSession,
):
    _validate_period(date_from, date_to)
    await require_organization_role(
        organization_id, Role.VIEWER, current_user, db
    )
    rows = await _load_report_rows(
        db, organization_id, date_from, date_to
    )
    return build_opiu_report(rows)


@router.get("/api/v1/nl/opiu/report")
async def get_opiu_report(
    org_id: str,
    date_from: date,
    date_to: date,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    report = await _authorized_report(
        org_id, date_from, date_to, current_user, db
    )
    serialized = serialize_report(report)
    serialized = _enrich_serialized_report(
        serialized,
        *(await _enrichment_context(db, org_id, date_from, date_to)),
    )
    serialized["period"] = {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
    }
    serialized["sync"] = await _sync_info(
        db, org_id, date_from, date_to
    )
    return serialized


@router.post("/api/v1/nl/opiu/sync")
async def trigger_opiu_sync(
    org_id: str,
    date_from: date,
    date_to: date,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    _validate_period(date_from, date_to)
    await require_organization_role(
        org_id, Role.ADMIN, current_user, db
    )
    task = celery_app.send_task(
        "wb.opiu.sync_org",
        args=[org_id, date_from.isoformat(), date_to.isoformat()],
    )
    return {"status": "queued", "task_id": task.id}


EXPORT_COLUMNS = [
    ("Артикул поставщика", "vendor_code"),
    ("Артикул WB", "nm_id"),
    ("Название", "product_name"),
    ("Кол-во реализовано, шт", "sales_qty"),
    ("Вайлдберриз реализовал Товар (Пр), руб", "realized_sum"),
    ("Комиссия ВБ с учетом НДС, %", "marketplace_commission_pct"),
    ("Комиссия ВБ с учетом НДС, сумма, руб", "marketplace_commission_sum"),
    ("Эквайринг, %", "acquiring_pct"),
    ("Эквайринг, сумма, руб", "acquiring_sum"),
    ("Услуги по доставке товара покупателю, руб", "delivery_total"),
    ("Штрафы", "penalty"),
    ("Хранение", "storage"),
    ("Реклама по API рекламы, руб", "advertising_api_spend"),
    ("ДРР, %", "drr"),
    ("Заказы, шт", "orders_qty"),
    ("Заказы, сумма, руб", "orders_sum"),
    ("Удержания WB по фин. отчету, руб", "deduction"),
    ("Прочие затраты", "other_expenses"),
    ("К перечислению на р/с", "net_for_pay"),
    ("Себестоимость", "cost_total"),
    ("Чистая прибыль", "net_profit"),
    ("Баркод", "barcode"),
    ("Размер", "size_name"),
    ("Бренд", "brand"),
    ("Категория", "subject_name"),
]

CONTROL_COLUMNS = [
    ("Группа", "control_group"),
    ("Артикул поставщика", "vendor_code"),
    ("Артикул WB", "nm_id"),
    ("Название", "product_name"),
    ("Штрафы", "penalty"),
    ("Хранение", "storage"),
    ("Удержания WB по фин. отчету, руб", "deduction"),
    ("Прочие затраты", "other_expenses"),
    ("К перечислению на р/с", "net_for_pay"),
    ("Валовая прибыль finance", "gross_profit"),
]

OTHER_EXPENSE_COLUMNS = [
    ("Операция / группа", "operation"),
    ("Поле WB", "source_field"),
    ("Куда попало в отчете", "target_field"),
    ("Сумма, руб", "amount"),
    ("Правило распределения", "allocation"),
    ("Кол-во артикулов", "items_count"),
]

RAW_FINANCE_COLUMNS = [
    ("Дата операции", "operation_date"),
    ("Тип документа", "doc_type_name"),
    ("Операция WB", "seller_oper_name"),
    ("Артикул поставщика", "vendor_code"),
    ("Артикул WB", "nm_id"),
    ("Баркод", "barcode"),
    ("Размер", "size_name"),
    ("Количество", "quantity"),
    ("Вайлдберриз реализовал, руб", "retail_amount"),
    ("К перечислению, руб", "for_pay"),
    ("Эквайринг, руб", "acquiring_fee"),
    ("Доставка, руб", "delivery_service"),
    ("Штрафы", "penalty"),
    ("Хранение", "paid_storage"),
    ("Удержания", "deduction"),
    ("Приёмка", "paid_acceptance"),
    ("Компенсация скидки", "cashback_amount"),
    ("Баллы/скидка лояльности", "cashback_discount"),
    ("Участие в лояльности", "cashback_commission_change"),
]


def _style_worksheet(worksheet, money_from_column: int = 4):
    header_fill = PatternFill("solid", fgColor="5B4B8A")
    for cell in worksheet[1]:
        cell.fill = header_fill
        cell.font = Font(color="FFFFFF", bold=True)
        cell.alignment = Alignment(wrap_text=True, vertical="center")

    if worksheet.max_row > 1:
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = (
            f"A1:{get_column_letter(worksheet.max_column)}{worksheet.max_row}"
        )
    for column in range(1, worksheet.max_column + 1):
        worksheet.column_dimensions[get_column_letter(column)].width = 18
    for row in range(2, worksheet.max_row + 1):
        for column in range(money_from_column, worksheet.max_column + 1):
            worksheet.cell(row, column).number_format = "#,##0.00"


def _append_sheet(workbook, title: str, columns: list[tuple[str, str]], rows):
    worksheet = workbook.create_sheet(title=title)
    worksheet.append([column_title for column_title, _ in columns])
    for item in rows:
        worksheet.append([item.get(field) for _, field in columns])
    _style_worksheet(worksheet)
    return worksheet


def _sum_rows(rows, field: str) -> Decimal:
    return sum((_as_decimal(row.get(field)) for row in rows), ZERO)


def _product_total_row(rows):
    realized_sum = _sum_rows(rows, "realized_sum")
    mp_sum = _sum_rows(rows, "marketplace_commission_sum")
    acquiring_sum = _sum_rows(rows, "acquiring_sum")
    ad_spend = _sum_rows(rows, "advertising_api_spend")
    orders_sum = _sum_rows(rows, "orders_sum")

    total = {
        "vendor_code": "ИТОГО ПО АРТИКУЛАМ",
        "nm_id": "",
        "product_name": "",
        "barcode": "",
        "size_name": "",
        "brand": "",
        "subject_name": "",
    }
    for field in {
        key for _, key in EXPORT_COLUMNS
    } - set(total):
        total[field] = _money(_sum_rows(rows, field))

    total["orders_qty"] = float(_sum_rows(rows, "orders_qty"))
    total["marketplace_commission_pct"] = _money(
        mp_sum / realized_sum * 100 if realized_sum else ZERO
    )
    total["acquiring_pct"] = _money(
        acquiring_sum / realized_sum * 100 if realized_sum else ZERO
    )
    total["drr"] = _money(ad_spend / orders_sum * 100 if orders_sum else ZERO)
    return total


@router.get("/api/v1/nl/opiu/export")
async def export_opiu_report(
    org_id: str,
    date_from: date,
    date_to: date,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    report = await _authorized_report(
        org_id, date_from, date_to, current_user, db
    )
    data = serialize_report(report)
    data = _enrich_serialized_report(
        data,
        *(await _enrichment_context(db, org_id, date_from, date_to)),
    )
    product_rows = [
        item for item in data["items"] if not item.get("is_unassigned")
    ]
    product_rows = [data["product_total"]] + product_rows
    control_rows = [
        {
            **item,
            "control_group": "Нераспределено: нет артикула/nm_id/barcode",
        }
        for item in data.get("unassigned_items", [])
    ]
    raw_rows = await _load_report_rows(db, org_id, date_from, date_to)

    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.title = "ОПиУ по артикулам"
    worksheet.append([title for title, _ in EXPORT_COLUMNS])
    for item in product_rows:
        worksheet.append([item.get(field) for _, field in EXPORT_COLUMNS])
    _style_worksheet(worksheet)

    total_fill = PatternFill("solid", fgColor="D9EAD3")
    for cell in worksheet[2]:
        cell.fill = total_fill
        cell.font = Font(bold=True)

    _append_sheet(
        workbook,
        "Расшифровка прочих затрат",
        OTHER_EXPENSE_COLUMNS,
        data.get("allocations", []),
    )
    _append_sheet(
        workbook,
        "Контроль нераспределено",
        CONTROL_COLUMNS,
        control_rows,
    )
    _append_sheet(
        workbook,
        "Raw finance rows",
        RAW_FINANCE_COLUMNS,
        raw_rows,
    )

    output = io.BytesIO()
    workbook.save(output)
    output.seek(0)
    filename = f"opiu_{date_from.isoformat()}_{date_to.isoformat()}.xlsx"
    return StreamingResponse(
        output,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
