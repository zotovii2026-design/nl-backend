import inspect
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.v1.routers.promotions import (
    PromoProductSave,
    download_promo_excel,
    download_promo_wb_template,
    get_promotions,
    get_promotion_products,
    save_promotion_products,
)
from main import app
from services.product_pricing import price_before_spp_sql


PROMOTION_ROUTES = {
    ("GET", "/api/v1/nl/promotions"),
    ("GET", "/api/v1/nl/promotions/products"),
    ("POST", "/api/v1/nl/promotions/products/save"),
    ("POST", "/api/v1/nl/promotions/upload-excel"),
    ("GET", "/api/v1/nl/promotions/download-wb-template"),
    ("POST", "/api/v1/nl/promotions/sync-api"),
}


def test_promotions_routes_keep_legacy_paths_without_duplicates():
    registered = []
    for route in app.routes:
        for method in getattr(route, "methods", None) or set():
            key = (method, route.path)
            if key in PROMOTION_ROUTES:
                registered.append(key)

    assert set(registered) == PROMOTION_ROUTES
    assert len(registered) == len(PROMOTION_ROUTES)


def test_promotion_price_before_spp_prefers_effective_seller_price():
    expected_expr = (
        "COALESCE(NULLIF(snp.price_product, 0), "
        "NULLIF(ts.price_discount, 0), "
        "NULLIF(snp.price_basic, 0), "
        "NULLIF(ts.price, 0), "
        "NULLIF(pp.current_price, 0))"
    )
    assert price_before_spp_sql(fallback_sql="pp.current_price") == expected_expr

    for endpoint in (get_promotion_products, download_promo_excel):
        source = inspect.getsource(endpoint)

        assert "price_before_spp_sql" in source
        assert "price_discount" in source
        assert "price_product" in source


def test_promotion_products_exposes_rrc_from_reference_book():
    source = inspect.getsource(get_promotion_products)

    assert "rb.rrc_price" in source
    assert '"rrc_price": rrc_price' in source


def test_promotion_products_exposes_decision_contract():
    products_source = inspect.getsource(get_promotion_products)
    save_source = inspect.getsource(save_promotion_products)
    download_source = inspect.getsource(download_promo_excel)

    assert "pp.decision" in products_source
    assert "'decision', pp3.decision" in products_source
    assert '"decision": row.decision' in products_source
    assert 'decision not in (None, "enter", "exit")' in save_source
    assert "pp.decision IN ('enter', 'exit')" in download_source


def test_promotions_hide_actions_older_than_yesterday_contract():
    promotions_source = inspect.getsource(get_promotions)
    products_source = inspect.getsource(get_promotion_products)
    wb_template_source = inspect.getsource(download_promo_wb_template)

    assert "CURRENT_DATE - INTERVAL '1 day'" in promotions_source
    assert "CURRENT_DATE - INTERVAL '1 day'" in products_source
    assert "CURRENT_DATE - INTERVAL '1 day'" in wb_template_source


def test_promo_wb_template_is_selected_two_column_file_contract():
    source = inspect.getsource(download_promo_wb_template)

    assert "/api/v1/nl/promotions/download-wb-template" in source
    assert 'worksheet.append(["Артикул WB", "Цена"])' in source
    assert "pp.decision IN ('enter', 'exit')" in source
    assert 'worksheet.append([row.nm_id, upload_price])' in source


@pytest.mark.asyncio
async def test_get_promotions_keeps_response_contract():
    promotion_id = uuid.uuid4()
    start_date = datetime(2026, 6, 1, tzinfo=timezone.utc)
    end_date = datetime(2026, 6, 30, tzinfo=timezone.utc)
    promotion = SimpleNamespace(
        id=promotion_id,
        promotion_id=123,
        title="June promo",
        promo_type="regular",
        start_date=start_date,
        end_date=end_date,
        max_price=999.5,
        min_discount=10,
        has_boost=True,
        boost_value=2.5,
        is_active=True,
        importance="high",
        source="api",
        participating_count=7,
    )
    db = AsyncMock()
    db.execute.return_value = SimpleNamespace(all=lambda: [promotion])

    result = await get_promotions(
        "00000000-0000-0000-0000-000000000001",
        is_active=True,
        db=db,
    )

    assert result == [
        {
            "id": str(promotion_id),
            "promotion_id": 123,
            "title": "June promo",
            "promo_type": "regular",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "max_price": 999.5,
            "min_discount": 10,
            "has_boost": True,
            "boost_value": 2.5,
            "is_active": True,
            "importance": "high",
            "source": "api",
            "participating_count": 7,
        }
    ]


@pytest.mark.asyncio
async def test_save_promotion_products_keeps_org_scoped_update_contract():
    promotion_product = SimpleNamespace(plan=False, decision=None, price_in_promo=None)
    db = AsyncMock()
    db.execute.return_value = SimpleNamespace(
        scalar_one_or_none=lambda: promotion_product,
    )

    result = await save_promotion_products(
        PromoProductSave(
            items=[
                {
                    "id": str(uuid.uuid4()),
                    "decision": "enter",
                    "price_in_promo": 749.0,
                }
            ]
        ),
        "00000000-0000-0000-0000-000000000001",
        db=db,
    )

    assert result == {"ok": True, "saved": 1}
    assert promotion_product.plan is True
    assert promotion_product.decision == "enter"
    assert promotion_product.price_in_promo == 749.0
    db.commit.assert_awaited_once()
