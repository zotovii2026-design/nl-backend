from datetime import datetime, timezone

from tasks.promo_sync import _build_snapshot_payload, _is_nomenclature_sync_eligible


NOW = datetime(2026, 6, 12, 12, 0, tzinfo=timezone.utc)


def test_regular_current_promotion_is_eligible():
    promotion = {
        "type": "regular",
        "endDateTime": "2026-06-13T12:00:00Z",
    }

    assert _is_nomenclature_sync_eligible(promotion, {}, NOW)


def test_expired_regular_promotion_is_not_eligible():
    promotion = {
        "type": "regular",
        "endDateTime": "2026-06-04T20:59:59Z",
    }

    assert not _is_nomenclature_sync_eligible(promotion, {}, NOW)


def test_auto_promotion_is_not_eligible():
    promotion = {
        "type": "auto",
        "endDateTime": "2026-06-13T12:00:00Z",
    }

    assert not _is_nomenclature_sync_eligible(promotion, {}, NOW)


def test_details_override_list_type_and_end_date():
    promotion = {
        "type": "regular",
        "endDateTime": "2026-06-13T12:00:00Z",
    }
    detail = {
        "type": "auto",
        "endDateTime": "2026-06-14T12:00:00Z",
    }

    assert not _is_nomenclature_sync_eligible(promotion, detail, NOW)


def test_snapshot_payload_marks_public_promotions_and_prices():
    payload = _build_snapshot_payload(
        {"discount": 15, "sizes": [{"price": 1000, "discountedPrice": 850}]},
        {
            "totalQuantity": 12,
            "promotions": [1006815],
            "sizes": [
                {
                    "saleConditions": [{"id": 1, "name": "auto"}],
                    "price": {"basic": 1000, "product": 850},
                }
            ],
        },
    )

    assert payload["promotions"] == [{"id": 1006815, "source": "card"}]
    assert payload["auto_promotion_ids"] == [{"id": 1006815, "source": "card_auto_marker"}]
    assert payload["available_qty"] == 12
    assert payload["available_to_buy"] is True
    assert payload["regular_in_promo"] is False
    assert payload["auto_in_promo"] is True
    assert payload["in_any_promo"] is True
    assert payload["sale_conditions"]["seller_discount"] == 15
    assert payload["sale_conditions"]["card_returned"] is True
    assert payload["price_basic"] == 1000.0
    assert payload["price_product"] == 850.0


def test_snapshot_payload_keeps_no_promo_as_empty_fact():
    payload = _build_snapshot_payload({}, {"id": 123, "promotions": []})

    assert payload["promotions"] is None
    assert payload["available_qty"] == 0
    assert payload["available_to_buy"] is False
    assert payload["in_any_promo"] is False
    assert payload["sale_conditions"]["card_returned"] is True


def test_snapshot_payload_uses_regular_promotions():
    payload = _build_snapshot_payload(
        {},
        {"id": 123, "totalQuantity": 3, "promotions": [1002605]},
        [{"id": 2642, "title": "Экспресс-скидки", "source": "calendar"}],
    )

    assert payload["available_to_buy"] is True
    assert payload["regular_in_promo"] is True
    assert payload["auto_in_promo"] is False
    assert payload["in_any_promo"] is True
    assert payload["regular_promotion_ids"] == [
        {"id": 2642, "title": "Экспресс-скидки", "source": "calendar"}
    ]
