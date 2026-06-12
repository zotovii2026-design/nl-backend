from datetime import datetime, timezone

from tasks.promo_sync import _is_nomenclature_sync_eligible


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
