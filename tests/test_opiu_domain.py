from decimal import Decimal
from datetime import date
from types import SimpleNamespace

import httpx
import pytest

from domain.opiu import build_opiu_report, serialize_report
from api.v1.routers.opiu import _enrich_serialized_report
from services.opiu import (
    FINANCE_FIELDS,
    _aggregate_paid_storage_rows,
    _fetch_finance_chunk,
    normalize_paid_storage_row,
    normalize_finance_row,
)


def test_opiu_matches_reference_report_totals():
    rows = [
        {
            "entity_id": "entity-1",
            "nm_id": 100,
            "vendor_code": "article-1",
            "barcode": "4600000000001",
            "size_name": "0",
            "seller_oper_name": "Продажа",
            "doc_type_name": "Продажа",
            "quantity": Decimal("271"),
            "retail_price": Decimal("321306.00"),
            "retail_amount": Decimal("211788.00"),
            "acquiring_fee": Decimal("8882.00"),
            "for_pay": Decimal("216801.56"),
            "delivery_service": Decimal("24660.65"),
            "paid_acceptance": Decimal("255.00"),
            "cashback_amount": Decimal("178.69"),
            "cashback_discount": Decimal("2297.00"),
            "cashback_commission_change": Decimal("229.70"),
        },
        {
            "nm_id": "0",
            "seller_oper_name": "Хранение",
            "paid_storage": Decimal("3806.09"),
        },
        {
            "nm_id": 0,
            "seller_oper_name": "Удержание",
            "deduction": Decimal("58583.00"),
        },
    ]

    report = build_opiu_report(rows)

    assert report["total"]["net_for_pay"] == Decimal("216801.56")
    assert report["total"]["gross_profit"] == Decimal("185553.12")
    assert report["total"]["wb_promotion_deduction"] == Decimal("58583.00")
    assert len(report["items"]) == 2
    product = report["items"][0]
    unassigned = report["items"][1]
    assert product["storage"] == Decimal("0")
    assert unassigned["storage"] == Decimal("3806.09")
    assert product["distributed_other_expenses"] == Decimal("0")
    assert unassigned["deduction"] == Decimal("0")
    assert unassigned["wb_promotion_deduction"] == Decimal("58583.00")
    assert report["allocations"] == []


def test_opiu_unit_values_use_sales_quantity_not_row_count():
    rows = [
        {
            "entity_id": "entity-1",
            "nm_id": 100,
            "vendor_code": "article-1",
            "barcode": "4600000000001",
            "seller_oper_name": "Продажа",
            "doc_type_name": "Продажа",
            "quantity": 2,
            "retail_price": 2000,
            "retail_amount": 1400,
            "acquiring_fee": 60,
            "for_pay": 1200,
        },
        {
            "entity_id": "entity-1",
            "nm_id": 100,
            "vendor_code": "article-1",
            "barcode": "4600000000001",
            "seller_oper_name": "Логистика",
            "delivery_service": 100,
        },
        {
            "entity_id": "entity-1",
            "nm_id": 100,
            "vendor_code": "article-1",
            "barcode": "4600000000001",
            "seller_oper_name": "Логистика",
            "delivery_service": 50,
        },
    ]

    item = build_opiu_report(rows)["items"][0]

    assert item["retail_unit"] == Decimal("1000")
    assert item["delivery_total"] == Decimal("150")
    assert item["delivery_unit"] == Decimal("75")
    assert item["marketplace_commission_unit"] == Decimal("370")
    assert item["marketplace_commission_pct"] == Decimal("37.00")


def test_opiu_keeps_two_barcodes_of_one_article_separate():
    rows = [
        {
            "vendor_code": "article-1",
            "nm_id": 100,
            "barcode": barcode,
            "seller_oper_name": "Продажа",
            "doc_type_name": "Продажа",
            "quantity": 1,
            "retail_price": 1000,
            "for_pay": 700,
        }
        for barcode in ("4600000000001", "4600000000002")
    ]

    report = build_opiu_report(rows)

    assert len(report["items"]) == 2
    assert {item["barcode"] for item in report["items"]} == {
        "4600000000001",
        "4600000000002",
    }


def test_opiu_merges_storage_detail_by_entity_id():
    rows = [
        {
            "entity_id": "entity-1",
            "vendor_code": "article-1",
            "barcode": "4600000000001",
            "nm_id": 100,
            "seller_oper_name": "Продажа",
            "doc_type_name": "Продажа",
            "quantity": 1,
            "for_pay": 700,
        },
        {
            "entity_id": "entity-1",
            "vendor_code": "article-1",
            "nm_id": 100,
            "seller_oper_name": "Хранение",
            "paid_storage": Decimal("42.10"),
        },
    ]

    report = build_opiu_report(rows)

    assert len(report["items"]) == 1
    assert report["items"][0]["storage"] == Decimal("42.10")


def test_opiu_returns_are_counted_by_quantity_and_net_payment_keeps_sign():
    rows = [
        {
            "entity_id": "entity-1",
            "vendor_code": "article-1",
            "barcode": "4600000000001",
            "seller_oper_name": "Продажа",
            "doc_type_name": "Продажа",
            "quantity": 3,
            "retail_price": 3000,
            "retail_amount": 2100,
            "acquiring_fee": 90,
            "for_pay": 1800,
        },
        {
            "entity_id": "entity-1",
            "vendor_code": "article-1",
            "barcode": "4600000000001",
            "seller_oper_name": "Возврат",
            "doc_type_name": "Возврат",
            "quantity": -2,
            "for_pay": -1200,
        },
    ]

    item = build_opiu_report(rows)["items"][0]

    assert item["sales_qty"] == Decimal("3")
    assert item["returns_qty"] == Decimal("2")
    assert item["returns_rub"] == Decimal("-1200")
    assert item["net_for_pay"] == Decimal("600")


def test_opiu_positive_return_payment_reduces_net_payment():
    rows = [
        {
            "vendor_code": "article-1",
            "barcode": "4600000000001",
            "seller_oper_name": "Продажа",
            "doc_type_name": "Продажа",
            "quantity": 3,
            "for_pay": Decimal("1800.00"),
        },
        {
            "vendor_code": "article-1",
            "barcode": "4600000000001",
            "seller_oper_name": "Возврат",
            "doc_type_name": "Возврат",
            "quantity": 1,
            "for_pay": Decimal("1200.00"),
        },
    ]

    item = build_opiu_report(rows)["items"][0]

    assert item["sales_qty"] == Decimal("3")
    assert item["returns_qty"] == Decimal("1")
    assert item["net_for_pay"] == Decimal("600.00")


def test_serialized_opiu_values_are_rounded_to_two_decimals():
    report = build_opiu_report(
        [
            {
                "vendor_code": "article-1",
                "barcode": "1",
                "seller_oper_name": "Продажа",
                "doc_type_name": "Продажа",
                "quantity": 3,
                "retail_price": 100,
                "for_pay": 60,
            }
        ]
    )

    serialized = serialize_report(report)

    assert serialized["items"][0]["retail_unit"] == 33.33
    assert serialized["items"][0]["marketplace_commission_pct"] == 40.0


def test_opiu_v2_enrichment_keeps_ads_orders_costs_separate():
    report = build_opiu_report(
        [
            {
                "entity_id": "entity-1",
                "nm_id": 100,
                "vendor_code": "article-1",
                "barcode": "1",
                "seller_oper_name": "Продажа",
                "doc_type_name": "Продажа",
                "quantity": 2,
                "retail_amount": 1000,
                "acquiring_fee": 50,
                "for_pay": 800,
            },
            {
                "seller_oper_name": "Удержание",
                "deduction": 300,
            },
        ]
    )

    data = _enrich_serialized_report(
        serialize_report(report),
        {100: Decimal("123.45")},
        {100: {"orders_qty": Decimal("3"), "orders_sum": Decimal("1500")}},
        {
            "entity-1": {
                "unit_cost": Decimal("200"),
                "purchase_cost": Decimal("0"),
                "tax_system": None,
                "tax_rate": Decimal("0"),
                "vat_rate": Decimal("0"),
            }
        },
        {},
    )

    item = next(row for row in data["items"] if row["vendor_code"] == "article-1")

    assert item["advertising_api_spend"] == 123.45
    assert item["orders_qty"] == 3.0
    assert item["orders_sum"] == 1500.0
    assert item["drr"] == 8.23
    assert item["cost_total"] == 400.0
    assert item["other_expenses"] == 0.0
    assert item["net_profit"] == 276.55
    assert data["unassigned_items"][0]["deduction"] == 0.0
    assert data["unassigned_items"][0]["wb_promotion_deduction"] == 300.0


def test_opiu_other_expenses_exclude_separate_acceptance_and_loyalty_columns():
    report = build_opiu_report(
        [
            {
                "entity_id": "entity-1",
                "nm_id": 100,
                "vendor_code": "article-1",
                "barcode": "1",
                "seller_oper_name": "Продажа",
                "doc_type_name": "Продажа",
                "quantity": 1,
                "for_pay": 800,
                "paid_acceptance": 60,
                "cashback_discount": 77.08,
                "cashback_commission_change": 2.80,
            }
        ]
    )

    data = _enrich_serialized_report(
        serialize_report(report),
        {},
        {},
        {},
        {},
    )

    assert data["items"][0]["acceptance"] == 60.0
    assert data["items"][0]["loyalty_points"] == 77.08
    assert data["items"][0]["loyalty_participation"] == 2.8
    assert data["items"][0]["other_expenses"] == 0.0
    assert data["total"]["other_expenses"] == 0.0


def test_opiu_total_tracks_wb_promotion_as_advertising_difference_only():
    report = build_opiu_report(
        [
            {
                "entity_id": "entity-1",
                "nm_id": 100,
                "vendor_code": "article-1",
                "barcode": "1",
                "seller_oper_name": "Продажа",
                "doc_type_name": "Продажа",
                "quantity": 2,
                "retail_amount": 1000,
                "for_pay": 800,
            },
            {
                "seller_oper_name": "Удержание",
                "deduction": 300,
            },
        ]
    )

    data = _enrich_serialized_report(
        serialize_report(report),
        {100: Decimal("123.45")},
        {100: {"orders_qty": Decimal("3"), "orders_sum": Decimal("1500")}},
        {
            "entity-1": {
                "unit_cost": Decimal("200"),
                "purchase_cost": Decimal("0"),
                "tax_system": None,
                "tax_rate": Decimal("0"),
                "vat_rate": Decimal("0"),
            }
        },
        {},
        control_totals={
            "finance_storage": Decimal("0"),
            "wb_promotion_deduction": Decimal("300"),
            "advertising_total": Decimal("123.45"),
        },
    )

    item = next(row for row in data["items"] if row["vendor_code"] == "article-1")

    assert item["advertising_difference_info"] == 0
    assert item["other_expenses"] == 0.0
    assert data["total"]["wb_promotion_deduction"] == 300.0
    assert data["total"]["advertising_api_spend"] == 123.45
    assert data["total"]["advertising_difference_info"] == 176.55
    assert data["total"]["other_expenses"] == 0.0
    assert data["total"]["net_profit"] == 100.0


def test_opiu_total_can_use_bank_payment_control_for_account_payment():
    report = build_opiu_report(
        [
            {
                "entity_id": "entity-1",
                "nm_id": 100,
                "vendor_code": "article-1",
                "barcode": "1",
                "seller_oper_name": "Продажа",
                "doc_type_name": "Продажа",
                "quantity": 1,
                "for_pay": 800,
            }
        ]
    )

    data = _enrich_serialized_report(
        serialize_report(report),
        {},
        {},
        {},
        {},
        control_totals={
            "bank_payment_sum": Decimal("777.77"),
        },
    )

    assert data["items"][0]["net_for_pay"] == 800.0
    assert data["total"]["net_for_pay"] == 777.77


def test_opiu_enrichment_uses_reference_total_cost_and_row_taxes():
    report = build_opiu_report(
        [
            {
                "entity_id": "entity-1",
                "nm_id": 100,
                "vendor_code": "article-1",
                "barcode": "1",
                "seller_oper_name": "Продажа",
                "doc_type_name": "Продажа",
                "quantity": 2,
                "retail_price": 2000,
                "retail_amount": 2000,
                "for_pay": 1600,
            }
        ]
    )

    data = _enrich_serialized_report(
        serialize_report(report),
        {},
        {},
        {
            "entity-1": {
                "unit_cost": Decimal("250"),
                "purchase_cost": Decimal("0"),
                "tax_system": "usn",
                "tax_rate": Decimal("6"),
                "vat_rate": Decimal("5"),
            }
        },
        {},
    )

    item = data["items"][0]

    assert item["cost_unit"] == 250.0
    assert item["cost_total"] == 500.0
    assert item["vat_tax"] == 100.0
    assert item["selected_tax"] == 120.0
    assert item["net_profit"] == 880.0


def test_opiu_enrichment_can_use_historical_cost_total():
    report = build_opiu_report(
        [
            {
                "entity_id": "entity-1",
                "nm_id": 100,
                "vendor_code": "article-1",
                "barcode": "1",
                "seller_oper_name": "Продажа",
                "doc_type_name": "Продажа",
                "quantity": 3,
                "retail_price": 3000,
                "retail_amount": 3000,
                "for_pay": 2400,
            }
        ]
    )

    data = _enrich_serialized_report(
        serialize_report(report),
        {},
        {},
        {
            "entity-1": {
                "unit_cost": Decimal("999"),
                "purchase_cost": Decimal("0"),
                "tax_system": None,
                "tax_rate": Decimal("0"),
                "vat_rate": Decimal("0"),
            }
        },
        {},
        {"entity-1": Decimal("450")},
        {},
    )

    item = data["items"][0]

    assert item["cost_unit"] == 150.0
    assert item["cost_total"] == 450.0
    assert item["net_profit"] == 1950.0


def test_paid_storage_row_normalization_accepts_wb_field_names():
    row = normalize_paid_storage_row(
        {
            "date": "2026-07-13",
            "nmId": 123,
            "vendorCode": "abc",
            "brandName": "Brand",
            "subjectName": "Subject",
            "warehousePrice": "42.155",
        },
        "org-id",
        "entity-id",
        date.fromisoformat("2026-07-13"),
    )

    assert row["storage_date"] == date.fromisoformat("2026-07-13")
    assert row["nm_id"] == 123
    assert row["entity_id"] == "entity-id"
    assert row["storage_amount"] == Decimal("42.16")


def test_paid_storage_rows_aggregate_before_sync_total():
    rows = [
        {
            "organization_id": "org-id",
            "storage_date": date.fromisoformat("2026-07-13"),
            "nm_id": 123,
            "entity_id": None,
            "vendor_code": "abc",
            "subject_name": "Subject",
            "brand": "Brand",
            "storage_amount": Decimal("10.10"),
            "raw_data": {"row": 1},
        },
        {
            "organization_id": "org-id",
            "storage_date": date.fromisoformat("2026-07-13"),
            "nm_id": 123,
            "entity_id": "entity-id",
            "vendor_code": "abc",
            "subject_name": "Subject",
            "brand": "Brand",
            "storage_amount": Decimal("0.27"),
            "raw_data": {"row": 2},
        },
    ]

    aggregated = _aggregate_paid_storage_rows(rows)

    assert len(aggregated) == 1
    assert aggregated[0]["entity_id"] == "entity-id"
    assert aggregated[0]["storage_amount"] == Decimal("10.37")


def test_finance_api_row_normalization_uses_current_field_names():
    row = normalize_finance_row(
        {
            "rrdId": 123,
            "reportId": 456,
            "nmId": 789,
            "vendorCode": "article",
            "sku": "4600000000001",
            "techSize": "M",
            "docTypeName": "Продажа",
            "sellerOperName": "Продажа",
            "quantity": "2",
            "retailPrice": "2000.00",
            "retailAmount": "1400.00",
            "forPay": "1200.00",
            "acquiringFee": "60.00",
            "deliveryService": "100.00",
            "paidStorage": "10.00",
            "deduction": "20.00",
            "paidAcceptance": "30.00",
            "cashbackAmount": "1.00",
            "cashbackDiscount": "2.00",
            "cashbackCommissionChange": "3.00",
            "saleDt": "2026-06-12T10:30:00Z",
        },
        "org-id",
        None,
        date.fromisoformat("2026-06-01"),
        date.fromisoformat("2026-06-12"),
    )

    assert row["rrd_id"] == 123
    assert row["barcode"] == "4600000000001"
    assert row["retail_price"] == Decimal("2000.00")
    assert row["cashback_commission_change"] == Decimal("3.00")


def test_finance_request_includes_required_fields():
    required = {
        "rrdId",
        "reportId",
        "dateFrom",
        "dateTo",
        "nmId",
        "vendorCode",
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
    }

    assert required <= set(FINANCE_FIELDS)


@pytest.mark.asyncio
async def test_finance_pagination_uses_last_rrd_id(monkeypatch):
    requests = []
    responses = [
        httpx.Response(
            200,
            json=[{"rrdId": 1}, {"rrdId": 2}],
            request=httpx.Request("POST", "https://finance.test/details"),
        ),
        httpx.Response(
            200,
            json=[{"rrdId": 2}, {"rrdId": 3}],
            request=httpx.Request("POST", "https://finance.test/details"),
        ),
        httpx.Response(
            204,
            request=httpx.Request("POST", "https://finance.test/details"),
        ),
    ]

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, path, json):
            requests.append(SimpleNamespace(path=path, json=json))
            return responses.pop(0)

    monkeypatch.setattr(
        "services.opiu.httpx.AsyncClient", lambda **kwargs: FakeClient()
    )

    rows = await _fetch_finance_chunk(
        "token",
        date.fromisoformat("2026-06-01"),
        date.fromisoformat("2026-06-12"),
    )

    assert [row["rrdId"] for row in rows] == [1, 2, 3]
    assert [request.json["rrdId"] for request in requests] == [0, 2, 3]
    assert requests[0].json["fields"] == FINANCE_FIELDS
