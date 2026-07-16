from decimal import Decimal
from datetime import date
from types import SimpleNamespace

import httpx
import pytest

from domain.opiu import build_opiu_report, serialize_report
from api.v1.routers.opiu import _enrich_serialized_report
from services.opiu import (
    FINANCE_FIELDS,
    _fetch_finance_chunk,
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
    assert report["total"]["gross_profit"] == Decimal("126970.12")
    assert len(report["items"]) == 1
    assert report["items"][0]["storage"] == Decimal("3806.09")
    assert report["items"][0]["distributed_other_expenses"] == Decimal(
        "58583.00"
    )
    assert report["allocations"] == [
        {
            "operation": "Хранение",
            "source_field": "paid_storage",
            "target_field": "storage",
            "amount": Decimal("3806.09"),
            "allocation": "Равными долями по проданным артикулам",
            "items_count": 1,
        },
        {
            "operation": "Удержания без артикула",
            "source_field": "deduction",
            "target_field": "other_expenses",
            "amount": Decimal("58583.00"),
            "allocation": "Равными долями по проданным артикулам",
            "items_count": 1,
        },
    ]


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
        {"entity-1": Decimal("200")},
        {},
    )

    item = next(row for row in data["items"] if row["vendor_code"] == "article-1")

    assert item["advertising_api_spend"] == 123.45
    assert item["orders_qty"] == 3.0
    assert item["orders_sum"] == 1500.0
    assert item["drr"] == 8.23
    assert item["cost_total"] == 400.0
    assert item["other_expenses"] == 300.0
    assert item["net_profit"] == -23.45
    assert data["unassigned_items"] == []


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
