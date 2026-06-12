"""API endpoints for the rebuilt OPIU report."""

import io
import uuid
from datetime import date

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
        uuid.UUID(organization_id), Role.VIEWER, current_user, db
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
        uuid.UUID(org_id), Role.ADMIN, current_user, db
    )
    task = celery_app.send_task(
        "wb.opiu.sync_org",
        args=[org_id, date_from.isoformat(), date_to.isoformat()],
    )
    return {"status": "queued", "task_id": task.id}


EXPORT_COLUMNS = [
    ("Артикул", "vendor_code"),
    ("Артикул WB", "nm_id"),
    ("Баркод", "barcode"),
    ("Размер", "size_name"),
    ("Название", "product_name"),
    ("Класс товара", "product_class"),
    ("Статус", "product_status"),
    ("Бренд", "brand"),
    ("Категория", "subject_name"),
    ("Кол-во продаж", "sales_qty"),
    ("Цена розничная (ед.)", "retail_unit"),
    ("ВБ реализовал товар (ед.)", "realized_unit"),
    ("Комиссия платёжных сервисов (ед.)", "acquiring_unit"),
    ("% комиссии платёжных сервисов", "acquiring_pct"),
    ("Комиссия МП (ед.)", "marketplace_commission_unit"),
    ("% комиссии МП", "marketplace_commission_pct"),
    ("Доставка (всего)", "delivery_total"),
    ("Доставка (ед.)", "delivery_unit"),
    ("Возвраты (шт)", "returns_qty"),
    ("Возвраты (руб)", "returns_rub"),
    ("К перечислению за вычетом возвратов", "net_for_pay"),
    ("Штрафы", "penalty"),
    ("Хранение", "storage"),
    ("Удержания", "deduction"),
    ("Операции на приёмке", "acceptance"),
    ("Компенсация скидки лояльности", "loyalty_compensation"),
    ("Сумма баллов лояльности", "loyalty_points"),
    ("Стоимость участия в программе лояльности", "loyalty_participation"),
    ("Валовая прибыль", "gross_profit"),
]


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
    rows = data["items"] + [data["total"]]

    workbook = openpyxl.Workbook()
    worksheet = workbook.active
    worksheet.title = "ОПиУ"
    worksheet.append([title for title, _ in EXPORT_COLUMNS])
    for item in rows:
        worksheet.append([item.get(field) for _, field in EXPORT_COLUMNS])

    header_fill = PatternFill("solid", fgColor="5B4B8A")
    total_fill = PatternFill("solid", fgColor="D9EAD3")
    for cell in worksheet[1]:
        cell.fill = header_fill
        cell.font = Font(color="FFFFFF", bold=True)
        cell.alignment = Alignment(wrap_text=True, vertical="center")
    for cell in worksheet[worksheet.max_row]:
        cell.fill = total_fill
        cell.font = Font(bold=True)

    worksheet.freeze_panes = "A2"
    worksheet.auto_filter.ref = (
        f"A1:{get_column_letter(worksheet.max_column)}{worksheet.max_row}"
    )
    for column in range(1, worksheet.max_column + 1):
        worksheet.column_dimensions[get_column_letter(column)].width = 18
    for row in range(2, worksheet.max_row + 1):
        for column in range(11, worksheet.max_column + 1):
            worksheet.cell(row, column).number_format = "#,##0.00"

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
