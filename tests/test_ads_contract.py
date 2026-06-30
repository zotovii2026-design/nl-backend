from pathlib import Path

from api.v1.routers.ads import DEFAULT_AD_STATUSES, _parse_statuses


def test_ads_default_statuses_are_active_and_paused_only():
    assert DEFAULT_AD_STATUSES == ["9", "11"]
    assert _parse_statuses(None) == ["9", "11"]
    assert "7" not in _parse_statuses(None)


def test_ads_accepts_archive_status_when_explicit():
    assert _parse_statuses("9,11,7") == ["9", "11", "7"]


def test_ads_router_does_not_mix_tech_status_into_ad_conversions():
    source = Path("api/v1/routers/ads.py").read_text(encoding="utf-8")
    assert "FROM tech_status" not in source
    assert "JOIN tech_status" not in source
