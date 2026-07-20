"""Pure OPIU aggregation rules for WB finance rows."""

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP


ZERO = Decimal("0")
MONEY = Decimal("0.01")
QTY = Decimal("0.001")


def as_decimal(value) -> Decimal:
    if value in (None, ""):
        return ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def money(value: Decimal) -> Decimal:
    return value.quantize(MONEY, rounding=ROUND_HALF_UP)


def _empty_group():
    return {
        "sales_qty": ZERO,
        "returns_qty": ZERO,
        "net_sales_qty": ZERO,
        "retail_sum": ZERO,
        "returns_retail_sum": ZERO,
        "realized_sum": ZERO,
        "acquiring_sum": ZERO,
        "sales_for_pay": ZERO,
        "returns_rub": ZERO,
        "net_for_pay": ZERO,
        "delivery_total": ZERO,
        "penalty": ZERO,
        "storage": ZERO,
        "deduction": ZERO,
        "acceptance": ZERO,
        "distributed_other_expenses": ZERO,
        "loyalty_compensation": ZERO,
        "loyalty_points": ZERO,
        "loyalty_participation": ZERO,
    }


def _is_return(row) -> bool:
    doc_type = str(row.get("doc_type_name") or "").strip().lower()
    return "возврат" in doc_type


def _is_sale(row) -> bool:
    operation = str(row.get("seller_oper_name") or "").strip().lower()
    return operation == "продажа" and not _is_return(row)


def _is_wb_promotion_service(row) -> bool:
    operation = str(row.get("seller_oper_name") or "").strip().lower()
    return "wb продвиж" in operation or "вб продвиж" in operation


def _group_key(row):
    article = str(row.get("vendor_code") or "").strip()
    barcode = str(row.get("barcode") or "").strip()
    size_name = str(row.get("size_name") or "").strip()
    nm_id = row.get("nm_id")
    entity_id = str(row.get("entity_id") or "")
    has_nm_id = nm_id not in (None, "", 0, "0")
    if not article and not barcode and not has_nm_id and not entity_id:
        return ("unassigned",)
    return (
        entity_id,
        article,
        barcode,
        size_name,
        str(nm_id) if has_nm_id else "",
    )


def _metadata(row):
    return {
        "entity_id": str(row.get("entity_id") or ""),
        "nm_id": row.get("nm_id"),
        "vendor_code": str(row.get("vendor_code") or "").strip(),
        "barcode": str(row.get("barcode") or "").strip(),
        "size_name": str(row.get("size_name") or "").strip(),
        "product_name": str(row.get("product_name") or "").strip(),
        "photo_main": str(row.get("photo_main") or "").strip(),
        "brand": str(row.get("brand") or "").strip(),
        "product_class": str(row.get("product_class") or "").strip(),
        "product_status": str(row.get("product_status") or "").strip(),
        "subject_name": str(row.get("subject_name") or "").strip(),
    }


def _calculate_item(meta, totals):
    sales_qty = totals["sales_qty"]
    net_sales_qty = totals["net_sales_qty"]

    def per_unit(field):
        return totals[field] / sales_qty if sales_qty else ZERO

    retail_unit = per_unit("retail_sum")
    retail_net_sum = totals["retail_sum"] - totals["returns_retail_sum"]
    realized_unit = per_unit("realized_sum")
    acquiring_unit = per_unit("acquiring_sum")
    seller_payment_unit = per_unit("sales_for_pay")
    acquiring_pct = acquiring_unit / retail_unit * 100 if retail_unit else ZERO
    marketplace_commission_unit = (
        retail_unit - seller_payment_unit - acquiring_unit
    )
    marketplace_commission_pct = (
        marketplace_commission_unit / retail_unit * 100
        if retail_unit
        else ZERO
    )
    delivery_unit = (
        totals["delivery_total"] / sales_qty if sales_qty else ZERO
    )
    gross_profit = (
        totals["net_for_pay"]
        - totals["delivery_total"]
        - totals["penalty"]
        - totals["storage"]
        - totals["deduction"]
        - totals["acceptance"]
        - totals["distributed_other_expenses"]
        - totals["loyalty_points"]
        - totals["loyalty_participation"]
    )

    return {
        **meta,
        "sales_qty": sales_qty,
        "net_sales_qty": net_sales_qty,
        "retail_sum": totals["retail_sum"],
        "retail_unit": retail_unit,
        "returns_retail_sum": totals["returns_retail_sum"],
        "retail_net_sum": retail_net_sum,
        "realized_unit": realized_unit,
        "acquiring_unit": acquiring_unit,
        "acquiring_pct": acquiring_pct,
        "marketplace_commission_unit": marketplace_commission_unit,
        "marketplace_commission_pct": marketplace_commission_pct,
        "delivery_total": totals["delivery_total"],
        "delivery_unit": delivery_unit,
        "returns_qty": totals["returns_qty"],
        "returns_rub": totals["returns_rub"],
        "net_for_pay": totals["net_for_pay"],
        "penalty": totals["penalty"],
        "storage": totals["storage"],
        "deduction": totals["deduction"],
        "acceptance": totals["acceptance"],
        "distributed_other_expenses": totals["distributed_other_expenses"],
        "loyalty_compensation": totals["loyalty_compensation"],
        "loyalty_points": totals["loyalty_points"],
        "loyalty_participation": totals["loyalty_participation"],
        "gross_profit": gross_profit,
        "_raw": totals,
    }


def build_opiu_report(rows):
    grouped = defaultdict(_empty_group)
    metadata = {}

    for row in rows:
        key = _group_key(row)
        totals = grouped[key]
        metadata.setdefault(
            key,
            {
                "vendor_code": "(без артикула)",
                "barcode": "",
                "size_name": "",
                "entity_id": "",
                "nm_id": None,
                "product_name": "",
                "photo_main": "",
                "brand": "",
                "product_class": "",
                "product_status": "",
                "subject_name": "",
            }
            if key == ("unassigned",)
            else _metadata(row),
        )

        quantity = as_decimal(row.get("quantity"))
        if _is_sale(row):
            totals["sales_qty"] += quantity
            totals["net_sales_qty"] += quantity
            totals["retail_sum"] += as_decimal(row.get("retail_price"))
            totals["realized_sum"] += as_decimal(row.get("retail_amount"))
            totals["acquiring_sum"] += as_decimal(row.get("acquiring_fee"))
            totals["sales_for_pay"] += as_decimal(row.get("for_pay"))
        if _is_return(row):
            return_qty = abs(quantity)
            totals["returns_qty"] += return_qty
            totals["net_sales_qty"] -= return_qty
            totals["returns_rub"] += as_decimal(row.get("for_pay"))
            totals["returns_retail_sum"] += abs(
                as_decimal(row.get("return_amount"))
                or as_decimal(row.get("retail_amount"))
                or as_decimal(row.get("for_pay"))
            )

        totals["net_for_pay"] += as_decimal(row.get("for_pay"))
        totals["delivery_total"] += as_decimal(row.get("delivery_service"))
        totals["penalty"] += as_decimal(row.get("penalty"))
        totals["storage"] += as_decimal(row.get("paid_storage"))
        totals["deduction"] += as_decimal(row.get("deduction"))
        totals["acceptance"] += as_decimal(row.get("paid_acceptance"))
        totals["loyalty_compensation"] += as_decimal(
            row.get("cashback_amount")
        )
        totals["loyalty_points"] += as_decimal(row.get("cashback_discount"))
        totals["loyalty_participation"] += as_decimal(
            row.get("cashback_commission_change")
        )

    allocations = []

    items = [
        _calculate_item(metadata[key], totals)
        for key, totals in grouped.items()
        if any(value != ZERO for value in totals.values())
    ]
    items.sort(
        key=lambda item: (
            item["vendor_code"] == "(без артикула)",
            item["vendor_code"],
            item["barcode"],
            item["size_name"],
        )
    )

    all_totals = _empty_group()
    for item in items:
        for field, value in item["_raw"].items():
            all_totals[field] += value
    total = _calculate_item(
        {
            "entity_id": "",
            "nm_id": None,
            "vendor_code": "ИТОГО",
            "barcode": "",
            "size_name": "",
            "product_name": "",
            "photo_main": "",
            "brand": "",
            "product_class": "",
            "product_status": "",
            "subject_name": "",
        },
        all_totals,
    )
    return {"items": items, "total": total, "allocations": allocations}


def _distribute_unassigned_amount(grouped, allocation_keys, target_field, amount):
    if not amount or not allocation_keys:
        return
    total_qty = sum((grouped[key]["net_sales_qty"] for key in allocation_keys), ZERO)
    if total_qty <= ZERO:
        return
    allocated = ZERO
    for key in allocation_keys[:-1]:
        share = money(amount * grouped[key]["net_sales_qty"] / total_qty)
        grouped[key][target_field] += share
        allocated += share
    grouped[allocation_keys[-1]][target_field] += amount - allocated


def serialize_report(report):
    def serialize_item(item):
        result = {}
        for key, value in item.items():
            if key == "_raw":
                continue
            if isinstance(value, Decimal):
                if key in {"sales_qty", "returns_qty", "net_sales_qty"}:
                    result[key] = float(value.quantize(QTY).normalize())
                else:
                    result[key] = float(money(value))
            else:
                result[key] = value
        return result

    return {
        "items": [serialize_item(item) for item in report["items"]],
        "total": serialize_item(report["total"]),
        "allocations": [serialize_item(item) for item in report["allocations"]],
    }
