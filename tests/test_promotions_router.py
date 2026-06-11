import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.v1.routers.promotions import (
    PromoProductSave,
    get_promotions,
    save_promotion_products,
)
from main import app


PROMOTION_ROUTES = {
    ("GET", "/api/v1/nl/promotions"),
    ("GET", "/api/v1/nl/promotions/products"),
    ("POST", "/api/v1/nl/promotions/products/save"),
    ("POST", "/api/v1/nl/promotions/upload-excel"),
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
    )
    scalar_result = SimpleNamespace(all=lambda: [promotion])
    db = AsyncMock()
    db.execute.return_value = SimpleNamespace(
        scalars=lambda: scalar_result,
    )

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
        }
    ]


@pytest.mark.asyncio
async def test_save_promotion_products_keeps_org_scoped_update_contract():
    promotion_product = SimpleNamespace(plan=False, price_in_promo=None)
    db = AsyncMock()
    db.execute.return_value = SimpleNamespace(
        scalar_one_or_none=lambda: promotion_product,
    )

    result = await save_promotion_products(
        PromoProductSave(
            items=[
                {
                    "id": str(uuid.uuid4()),
                    "plan": True,
                    "price_in_promo": 749.0,
                }
            ]
        ),
        "00000000-0000-0000-0000-000000000001",
        db=db,
    )

    assert result == {"ok": True, "saved": 1}
    assert promotion_product.plan is True
    assert promotion_product.price_in_promo == 749.0
    db.commit.assert_awaited_once()
