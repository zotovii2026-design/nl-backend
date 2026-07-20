"""WB Finance API synchronization for the OPIU report."""

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterable

import httpx
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from domain.opiu import as_decimal, build_opiu_report, serialize_report
from models.product_entity import EntityBarcode, ProductEntity
from models.wb_finance import (
    WbFinanceRow,
    WbFinanceSync,
    WbOpiuSnapshot,
    WbPaidStorageRow,
    WbPaidStorageSync,
)


logger = logging.getLogger(__name__)

FINANCE_API = "https://finance-api.wildberries.ru"
SELLER_ANALYTICS_API = "https://seller-analytics-api.wildberries.ru"
DETAIL_PATH = "/api/finance/v1/sales-reports/detailed"
LIST_PATH = "/api/finance/v1/sales-reports/list"
PAID_STORAGE_CREATE_PATH = "/api/v1/paid_storage"
PAID_STORAGE_STATUS_PATH = "/api/v1/paid_storage/tasks/{task_id}/status"
PAID_STORAGE_DOWNLOAD_PATH = "/api/v1/paid_storage/tasks/{task_id}/download"
PAGE_LIMIT = 100000
MAX_PAGES = 1000
UPSERT_CHUNK = 1000
RECONCILIATION_TOLERANCE = Decimal("0.01")
FINANCE_FIELDS = [
    "rrdId",
    "reportId",
    "dateFrom",
    "dateTo",
    "nmId",
    "vendorCode",
    "title",
    "brandName",
    "subjectName",
    "sku",
    "techSize",
    "docTypeName",
    "sellerOperName",
    "orderDt",
    "saleDt",
    "quantity",
    "returnAmount",
    "retailPrice",
    "retailAmount",
    "forPay",
    "acquiringFee",
    "deliveryService",
    "penalty",
    "paidStorage",
    "deduction",
    "paidAcceptance",
    "cashbackAmount",
    "cashbackDiscount",
    "cashbackCommissionChange",
    "srid",
]


def _authorization_value(token: str) -> str:
    value = token.strip()
    if value.lower().startswith("bearer "):
        return value[7:].strip()
    return value


def _pick(item: dict, *names, default=None):
    for name in names:
        if name in item and item[name] is not None:
            return item[name]
    return default


def _parse_date(value) -> date | None:
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def _parse_datetime(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        parsed_date = _parse_date(value)
        if parsed_date:
            return datetime.combine(
                parsed_date, datetime.min.time(), tzinfo=timezone.utc
            )
        return None


def _extract_items(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("data", "items", "reports", "rows"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _extract_items(value)
            if nested:
                return nested
    return []


def _extract_task_id(payload: Any) -> str | None:
    if isinstance(payload, dict):
        for key in ("taskId", "task_id", "id"):
            value = payload.get(key)
            if value:
                return str(value)
        for key in ("data", "result"):
            nested = _extract_task_id(payload.get(key))
            if nested:
                return nested
    return None


def _paid_storage_status_done(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    values = []
    for key in ("done", "isDone", "completed", "isCompleted"):
        if payload.get(key) is True:
            return True
    for key in ("status", "state"):
        if payload.get(key) is not None:
            values.append(str(payload.get(key)).strip().lower())
    data = payload.get("data")
    if isinstance(data, dict):
        for key in ("done", "isDone", "completed", "isCompleted"):
            if data.get(key) is True:
                return True
        for key in ("status", "state"):
            if data.get(key) is not None:
                values.append(str(data.get(key)).strip().lower())
    return any(value in {"done", "success", "finished", "completed"} for value in values)


def _paid_storage_date_window(date_from: date, date_to: date) -> tuple[date, date]:
    # WB finance rows usually book storage one day earlier than the
    # paid_storage report date, so a finance period N..M maps to N+1..M+1.
    return date_from + timedelta(days=1), date_to + timedelta(days=1)


async def _request_with_retry(
    client: httpx.AsyncClient,
    path: str,
    payload: dict,
    max_retries: int = 4,
) -> httpx.Response:
    for attempt in range(max_retries + 1):
        response = await client.post(path, json=payload)
        if response.status_code != 429 or attempt >= max_retries:
            response.raise_for_status()
            return response
        retry_after = response.headers.get("Retry-After", "30")
        try:
            delay = max(float(retry_after), 1)
        except ValueError:
            delay = 30
        await asyncio.sleep(delay)
    raise RuntimeError("WB Finance API retry loop exhausted")


async def fetch_paid_storage_rows(
    token: str,
    date_from: date,
    date_to: date,
    poll_interval: float = 5,
    max_polls: int = 60,
) -> tuple[str, list[dict]]:
    async with httpx.AsyncClient(
        base_url=SELLER_ANALYTICS_API,
        headers={
            "Authorization": _authorization_value(token),
            "Content-Type": "application/json",
            "User-Agent": "NL-Table/1.0",
        },
        timeout=120,
    ) as client:
        response = await client.post(
            PAID_STORAGE_CREATE_PATH,
            json={
                "dateFrom": date_from.isoformat(),
                "dateTo": date_to.isoformat(),
            },
        )
        response.raise_for_status()
        task_id = _extract_task_id(response.json())
        if not task_id:
            raise RuntimeError("WB paid_storage task id was not returned")

        for _ in range(max_polls):
            status_response = await client.get(
                PAID_STORAGE_STATUS_PATH.format(task_id=task_id)
            )
            status_response.raise_for_status()
            if _paid_storage_status_done(status_response.json()):
                break
            await asyncio.sleep(poll_interval)
        else:
            raise TimeoutError("WB paid_storage task did not finish in time")

        download_response = await client.get(
            PAID_STORAGE_DOWNLOAD_PATH.format(task_id=task_id)
        )
        download_response.raise_for_status()
        return task_id, _extract_items(download_response.json())


async def fetch_finance_rows(
    token: str,
    date_from: date,
    date_to: date,
    period: str = "daily",
) -> list[dict]:
    rows = []
    chunk_from = date_from
    while chunk_from <= date_to:
        chunk_to = min(chunk_from + timedelta(days=30), date_to)
        rows.extend(
            await _fetch_finance_chunk(
                token, chunk_from, chunk_to, period=period
            )
        )
        chunk_from = chunk_to + timedelta(days=1)
    # rrdId is unique for an organization, including across date chunks.
    return list(
        {
            int(_pick(item, "rrdId", "rrd_id")): item
            for item in rows
            if _pick(item, "rrdId", "rrd_id") is not None
        }.values()
    )


async def _fetch_finance_chunk(
    token: str,
    date_from: date,
    date_to: date,
    period: str = "daily",
) -> list[dict]:
    rows = []
    seen_rrd = set()
    cursor = 0
    async with httpx.AsyncClient(
        base_url=FINANCE_API,
        headers={
            "Authorization": _authorization_value(token),
            "Content-Type": "application/json",
            "User-Agent": "NL-Table/1.0",
        },
        timeout=120,
    ) as client:
        for _ in range(MAX_PAGES):
            response = await _request_with_retry(
                client,
                DETAIL_PATH,
                {
                    "dateFrom": date_from.isoformat(),
                    "dateTo": date_to.isoformat(),
                    "limit": PAGE_LIMIT,
                    "rrdId": cursor,
                    "period": period,
                    "fields": FINANCE_FIELDS,
                },
            )
            if response.status_code == 204:
                break

            page = _extract_items(response.json())
            if not page:
                break

            new_rows = []
            for item in page:
                rrd_id = _pick(item, "rrdId", "rrd_id")
                if rrd_id is None:
                    continue
                rrd_id = int(rrd_id)
                if rrd_id in seen_rrd:
                    continue
                seen_rrd.add(rrd_id)
                new_rows.append(item)
            rows.extend(new_rows)

            next_cursor = int(_pick(page[-1], "rrdId", "rrd_id", default=0))
            if not new_rows or next_cursor <= cursor:
                break
            cursor = next_cursor
            if len(page) < PAGE_LIMIT:
                # The API may still return 204 only on the next request.
                continue
    return rows


async def fetch_bank_payment_sum(
    token: str,
    date_from: date,
    date_to: date,
) -> Decimal | None:
    async with httpx.AsyncClient(
        base_url=FINANCE_API,
        headers={
            "Authorization": _authorization_value(token),
            "Content-Type": "application/json",
            "User-Agent": "NL-Table/1.0",
        },
        timeout=60,
    ) as client:
        response = await _request_with_retry(
            client,
            LIST_PATH,
            {
                "dateFrom": date_from.isoformat(),
                "dateTo": date_to.isoformat(),
                "limit": 1000,
                "offset": 0,
                "period": "daily",
            },
        )
        if response.status_code == 204:
            return None
        reports = _extract_items(response.json())

    matched = []
    for report in reports:
        report_from = _parse_date(_pick(report, "dateFrom", "date_from"))
        report_to = _parse_date(_pick(report, "dateTo", "date_to"))
        if not report_from or not report_to:
            continue
        if report_from < date_from or report_to > date_to:
            continue
        value = _pick(report, "bankPaymentSum", "bank_payment_sum")
        if value is not None:
            matched.append((report_from, report_to, as_decimal(value)))
    if not matched:
        return None
    # A bank payment is comparable only when complete report periods cover
    # the requested range. Partial weekly reports cannot reconcile exactly.
    if min(item[0] for item in matched) != date_from:
        return None
    if max(item[1] for item in matched) != date_to:
        return None
    return sum((item[2] for item in matched), Decimal("0"))


async def _entity_maps(db: AsyncSession, organization_id: str):
    entity_result = await db.execute(
        select(
            ProductEntity.id,
            ProductEntity.nm_id,
            ProductEntity.vendor_code,
            ProductEntity.size_name,
        ).where(ProductEntity.organization_id == organization_id)
    )
    entity_rows = entity_result.all()

    barcode_result = await db.execute(
        select(EntityBarcode.barcode, EntityBarcode.entity_id).where(
            EntityBarcode.organization_id == organization_id
        )
    )
    barcode_map = {
        str(barcode): entity_id
        for barcode, entity_id in barcode_result.all()
        if barcode
    }
    nm_size_map = {
        (int(nm_id), str(size_name or "").strip().lower()): entity_id
        for entity_id, nm_id, _, size_name in entity_rows
        if nm_id
    }
    by_nm = {}
    by_vendor = {}
    for entity_id, nm_id, vendor_code, _ in entity_rows:
        if nm_id:
            by_nm.setdefault(int(nm_id), []).append(entity_id)
        if vendor_code:
            by_vendor.setdefault(str(vendor_code).strip().lower(), []).append(
                entity_id
            )
    return barcode_map, nm_size_map, by_nm, by_vendor


def _resolve_entity(
    item: dict,
    barcode_map,
    nm_size_map,
    by_nm,
    by_vendor,
):
    barcode = str(_pick(item, "sku", "barcode", default="") or "").strip()
    if barcode and barcode in barcode_map:
        return barcode_map[barcode]

    nm_raw = _pick(item, "nmId", "nm_id")
    nm_id = int(nm_raw) if nm_raw not in (None, "") else None
    size_name = str(
        _pick(item, "techSize", "sizeName", "size_name", default="") or ""
    ).strip()
    if nm_id:
        exact = nm_size_map.get((nm_id, size_name.lower()))
        if exact:
            return exact
        candidates = by_nm.get(nm_id, [])
        if len(candidates) == 1:
            return candidates[0]

    vendor_code = str(
        _pick(item, "vendorCode", "vendor_code", default="") or ""
    ).strip().lower()
    candidates = by_vendor.get(vendor_code, [])
    return candidates[0] if len(candidates) == 1 else None


def normalize_finance_row(
    item: dict,
    organization_id: str,
    entity_id,
    date_from: date,
    date_to: date,
) -> dict:
    operation_date = _parse_datetime(
        _pick(
            item,
            "saleDt",
            "saleDate",
            "orderDt",
            "createDt",
            "operationDate",
        )
    )
    return {
        "organization_id": organization_id,
        "entity_id": entity_id,
        "rrd_id": int(_pick(item, "rrdId", "rrd_id")),
        "report_id": _pick(item, "reportId", "realizationreport_id"),
        "report_date_from": _parse_date(
            _pick(item, "dateFrom", "date_from")
        )
        or date_from,
        "report_date_to": _parse_date(_pick(item, "dateTo", "date_to"))
        or date_to,
        "operation_date": operation_date,
        "nm_id": _pick(item, "nmId", "nm_id"),
        "vendor_code": _pick(item, "vendorCode", "supplierArticle"),
        "barcode": str(_pick(item, "sku", "barcode", default="") or ""),
        "size_name": _pick(item, "techSize", "sizeName"),
        "doc_type_name": _pick(item, "docTypeName", "doc_type_name"),
        "seller_oper_name": _pick(
            item, "sellerOperName", "supplier_oper_name"
        ),
        "quantity": as_decimal(_pick(item, "quantity", default=0)),
        "return_amount": as_decimal(
            _pick(item, "returnAmount", "return_amount", default=0)
        ),
        "retail_price": as_decimal(
            _pick(item, "retailPrice", "retail_price", default=0)
        ),
        "retail_amount": as_decimal(
            _pick(item, "retailAmount", "retail_amount", default=0)
        ),
        "for_pay": as_decimal(
            _pick(item, "forPay", "ppvz_for_pay", default=0)
        ),
        "acquiring_fee": as_decimal(
            _pick(item, "acquiringFee", "acquiring_fee", default=0)
        ),
        "delivery_service": as_decimal(
            _pick(item, "deliveryService", "delivery_rub", default=0)
        ),
        "penalty": as_decimal(_pick(item, "penalty", default=0)),
        "paid_storage": as_decimal(
            _pick(item, "paidStorage", "storage_fee", default=0)
        ),
        "deduction": as_decimal(_pick(item, "deduction", default=0)),
        "paid_acceptance": as_decimal(
            _pick(item, "paidAcceptance", "acceptance", default=0)
        ),
        "cashback_amount": as_decimal(
            _pick(item, "cashbackAmount", default=0)
        ),
        "cashback_discount": as_decimal(
            _pick(item, "cashbackDiscount", default=0)
        ),
        "cashback_commission_change": as_decimal(
            _pick(item, "cashbackCommissionChange", default=0)
        ),
        "raw_data": item,
        "fetched_at": datetime.now(timezone.utc),
    }


def _paid_storage_amount(item: dict) -> Decimal:
    return as_decimal(
        _pick(
            item,
            "warehousePrice",
            "storagePrice",
            "storageFee",
            "storageSum",
            "storage",
            "paidStorage",
            "amount",
            "sum",
            "sumPrice",
            default=0,
        )
    )


def normalize_paid_storage_row(
    item: dict,
    organization_id: str,
    entity_id,
    fallback_date: date,
) -> dict | None:
    nm_id = _pick(item, "nmId", "nmID", "nm_id", "nm")
    if nm_id in (None, ""):
        return None
    storage_date = _parse_date(
        _pick(item, "date", "storageDate", "reportDate", "dt")
    ) or fallback_date
    return {
        "organization_id": organization_id,
        "entity_id": entity_id,
        "storage_date": storage_date,
        "nm_id": int(nm_id),
        "vendor_code": _pick(item, "vendorCode", "supplierArticle"),
        "subject_name": _pick(item, "subjectName", "subject"),
        "brand": _pick(item, "brandName", "brand"),
        "storage_amount": _paid_storage_amount(item),
        "raw_data": item,
        "fetched_at": datetime.now(timezone.utc),
    }


def _chunks(values: list[dict], size: int) -> Iterable[list[dict]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


async def _upsert_rows(db: AsyncSession, rows: list[dict]) -> int:
    if not rows:
        return 0
    for chunk in _chunks(rows, UPSERT_CHUNK):
        statement = pg_insert(WbFinanceRow).values(chunk)
        excluded = statement.excluded
        statement = statement.on_conflict_do_update(
            constraint="wb_finance_rows_org_rrd_key",
            set_={
                "entity_id": excluded.entity_id,
                "report_id": excluded.report_id,
                "report_date_from": excluded.report_date_from,
                "report_date_to": excluded.report_date_to,
                "operation_date": excluded.operation_date,
                "nm_id": excluded.nm_id,
                "vendor_code": excluded.vendor_code,
                "barcode": excluded.barcode,
                "size_name": excluded.size_name,
                "doc_type_name": excluded.doc_type_name,
                "seller_oper_name": excluded.seller_oper_name,
                "quantity": excluded.quantity,
                "return_amount": excluded.return_amount,
                "retail_price": excluded.retail_price,
                "retail_amount": excluded.retail_amount,
                "for_pay": excluded.for_pay,
                "acquiring_fee": excluded.acquiring_fee,
                "delivery_service": excluded.delivery_service,
                "penalty": excluded.penalty,
                "paid_storage": excluded.paid_storage,
                "deduction": excluded.deduction,
                "paid_acceptance": excluded.paid_acceptance,
                "cashback_amount": excluded.cashback_amount,
                "cashback_discount": excluded.cashback_discount,
                "cashback_commission_change": (
                    excluded.cashback_commission_change
                ),
                "raw_data": excluded.raw_data,
                "fetched_at": excluded.fetched_at,
            },
        )
        await db.execute(statement)
    await db.commit()
    return len(rows)


async def _upsert_paid_storage_rows(db: AsyncSession, rows: list[dict]) -> int:
    if not rows:
        return 0
    aggregated = {}
    for row in rows:
        key = (
            row["organization_id"],
            row["storage_date"],
            row["nm_id"],
        )
        if key not in aggregated:
            aggregated[key] = row.copy()
            continue
        previous_raw = aggregated[key]["raw_data"]
        previous_items = (
            previous_raw.get("items", [previous_raw])
            if isinstance(previous_raw, dict)
            else [previous_raw]
        )
        aggregated[key]["storage_amount"] += row["storage_amount"]
        aggregated[key]["raw_data"] = {"items": [*previous_items, row["raw_data"]]}
        if not aggregated[key].get("entity_id") and row.get("entity_id"):
            aggregated[key]["entity_id"] = row["entity_id"]

    values = list(aggregated.values())
    for chunk in _chunks(values, UPSERT_CHUNK):
        statement = pg_insert(WbPaidStorageRow).values(chunk)
        excluded = statement.excluded
        statement = statement.on_conflict_do_update(
            constraint="wb_paid_storage_rows_org_date_nm_key",
            set_={
                "entity_id": excluded.entity_id,
                "vendor_code": excluded.vendor_code,
                "subject_name": excluded.subject_name,
                "brand": excluded.brand,
                "storage_amount": excluded.storage_amount,
                "raw_data": excluded.raw_data,
                "fetched_at": excluded.fetched_at,
            },
        )
        await db.execute(statement)
    await db.commit()
    return len(values)


async def sync_paid_storage_period(
    session_factory: async_sessionmaker,
    organization_id: str,
    token: str,
    date_from: date,
    date_to: date,
) -> dict:
    async with session_factory() as db:
        sync = WbPaidStorageSync(
            organization_id=organization_id,
            date_from=date_from,
            date_to=date_to,
            status="running",
        )
        db.add(sync)
        await db.commit()
        await db.refresh(sync)
        sync_id = sync.id

    try:
        task_id, raw_rows = await fetch_paid_storage_rows(
            token,
            date_from,
            date_to,
        )
        async with session_factory() as db:
            maps = await _entity_maps(db, organization_id)
            normalized = []
            for item in raw_rows:
                row = normalize_paid_storage_row(
                    item,
                    organization_id,
                    _resolve_entity(item, *maps),
                    date_from,
                )
                if row is not None:
                    normalized.append(row)
            rows_count = await _upsert_paid_storage_rows(db, normalized)
            total_storage = sum(
                (row["storage_amount"] for row in normalized),
                Decimal("0"),
            )

        async with session_factory() as db:
            sync = await db.get(WbPaidStorageSync, sync_id)
            sync.status = "success"
            sync.rows_count = rows_count
            sync.total_storage = total_storage
            sync.task_id = task_id
            sync.finished_at = datetime.now(timezone.utc)
            await db.commit()
        return {
            "status": "success",
            "task_id": task_id,
            "rows": rows_count,
            "total_storage": float(total_storage),
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
        }
    except Exception as error:
        async with session_factory() as db:
            sync = await db.get(WbPaidStorageSync, sync_id)
            sync.status = "error"
            sync.error_message = str(error)
            sync.finished_at = datetime.now(timezone.utc)
            await db.commit()
        raise


def _snapshot_group_key(item: dict) -> str:
    if item.get("vendor_code") == "ИТОГО":
        return "__total__"
    if item.get("vendor_code") == "(без артикула)":
        return "__unassigned__"
    return "|".join(
        str(item.get(field) or "")
        for field in (
            "entity_id",
            "vendor_code",
            "barcode",
            "size_name",
            "nm_id",
        )
    )


async def _replace_snapshots(
    db: AsyncSession,
    organization_id: str,
    date_from: date,
    date_to: date,
    report: dict,
) -> int:
    serialized = serialize_report(report)
    items = serialized["items"] + [serialized["total"]]
    await db.execute(
        delete(WbOpiuSnapshot).where(
            WbOpiuSnapshot.organization_id == organization_id,
            WbOpiuSnapshot.period_from == date_from,
            WbOpiuSnapshot.period_to == date_to,
        )
    )
    for item in items:
        entity_id = item.get("entity_id") or None
        db.add(
            WbOpiuSnapshot(
                organization_id=organization_id,
                entity_id=entity_id,
                period_from=date_from,
                period_to=date_to,
                group_key=_snapshot_group_key(item),
                is_total=1 if item.get("vendor_code") == "ИТОГО" else 0,
                payload=item,
            )
        )
    await db.commit()
    return len(items)


async def sync_finance_period(
    session_factory: async_sessionmaker,
    organization_id: str,
    token: str,
    date_from: date,
    date_to: date,
) -> dict:
    async with session_factory() as db:
        sync = WbFinanceSync(
            organization_id=organization_id,
            date_from=date_from,
            date_to=date_to,
            status="running",
        )
        db.add(sync)
        await db.commit()
        await db.refresh(sync)
        sync_id = sync.id

    try:
        raw_rows = await fetch_finance_rows(token, date_from, date_to)
        paid_storage_result = None
        storage_from, storage_to = _paid_storage_date_window(date_from, date_to)
        try:
            paid_storage_result = await sync_paid_storage_period(
                session_factory,
                organization_id,
                token,
                storage_from,
                storage_to,
            )
        except Exception as error:
            logger.warning(
                "WB paid_storage sync is unavailable for org=%s: %s",
                organization_id,
                error,
            )
        try:
            bank_payment_sum = await fetch_bank_payment_sum(
                token, date_from, date_to
            )
        except Exception as error:
            logger.warning(
                "WB bank payment control is unavailable for org=%s: %s",
                organization_id,
                error,
            )
            bank_payment_sum = None
        async with session_factory() as db:
            maps = await _entity_maps(db, organization_id)
            normalized = [
                normalize_finance_row(
                    item,
                    organization_id,
                    _resolve_entity(item, *maps),
                    date_from,
                    date_to,
                )
                for item in raw_rows
            ]
            await _upsert_rows(db, normalized)

        report = build_opiu_report(normalized)
        calculated = report["total"]["gross_profit"]
        difference = (
            calculated - bank_payment_sum
            if bank_payment_sum is not None
            else None
        )
        reconciliation_status = (
            "unavailable"
            if difference is None
            else (
                "matched"
                if abs(difference) <= RECONCILIATION_TOLERANCE
                else "mismatch"
            )
        )
        async with session_factory() as db:
            snapshot_count = await _replace_snapshots(
                db, organization_id, date_from, date_to, report
            )
            sync = await db.get(WbFinanceSync, sync_id)
            sync.status = (
                "warning"
                if reconciliation_status == "mismatch"
                else "success"
            )
            sync.rows_count = len(normalized)
            sync.bank_payment_sum = bank_payment_sum
            sync.calculated_payment_sum = calculated
            sync.difference = difference
            sync.finished_at = datetime.now(timezone.utc)
            await db.commit()
        return {
            "status": (
                "error"
                if reconciliation_status == "mismatch"
                else "success"
            ),
            "rows": len(normalized),
            "snapshots": snapshot_count,
            "reconciliation": reconciliation_status,
            "bank_payment_sum": (
                float(bank_payment_sum)
                if bank_payment_sum is not None
                else None
            ),
            "calculated_payment_sum": float(calculated),
            "difference": float(difference) if difference is not None else None,
            "paid_storage": paid_storage_result,
        }
    except Exception as error:
        logger.exception(
            "WB finance sync failed for org=%s period=%s..%s",
            organization_id,
            date_from,
            date_to,
        )
        async with session_factory() as db:
            sync = await db.get(WbFinanceSync, sync_id)
            sync.status = "error"
            sync.error_message = str(error)
            sync.finished_at = datetime.now(timezone.utc)
            await db.commit()
        raise
