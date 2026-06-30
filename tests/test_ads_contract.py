from pathlib import Path

from api.v1.routers.ads import (
    ADS_REFRESH_COOLDOWN_SECONDS,
    ADS_REFRESH_DAYS_BACK,
    DEFAULT_AD_STATUSES,
    _parse_statuses,
)


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


def test_ads_manual_refresh_uses_nine_day_window():
    assert ADS_REFRESH_DAYS_BACK == 9
    assert ADS_REFRESH_COOLDOWN_SECONDS == 60 * 60


def test_ads_manual_refresh_passes_selected_org_to_celery():
    source = Path("api/v1/routers/ads.py").read_text(encoding="utf-8")
    assert 'kwargs={"days_back": ADS_REFRESH_DAYS_BACK, "org_id": org_id}' in source


def test_ads_template_uses_unified_period_and_daily_table():
    source = Path("templates/nl_v2.html").read_text(encoding="utf-8")
    for legacy_id in (
        "adsPeriodPreset",
        "adsCustomDateChange",
        "ads-period",
        "ads-date-from",
        "ads-date-to",
        "toggleDailyTable",
    ):
        assert legacy_id not in source
    assert "ads-daily-total" in source
    assert "ad-atbs" in source


def test_ads_items_include_reference_product_filters():
    source = Path("api/v1/routers/ads.py").read_text(encoding="utf-8")
    assert "FROM reference_book" in source
    assert "product_status" in source
    assert "product_class" in source


def test_ads_campaign_filters_recalculate_from_matching_products():
    source = Path("static/js/ads-grid.js").read_text(encoding="utf-8")
    assert "function buildFilteredCampaign" in source
    assert "productMatchesAdsFilters" in source
    assert "spent_share" in source


def test_ads_product_filters_are_server_side_for_all_ads_views():
    backend = Path("api/v1/routers/ads.py").read_text(encoding="utf-8")
    template = Path("templates/nl_v2.html").read_text(encoding="utf-8")
    arts = Path("static/js/ads-arts-grid.js").read_text(encoding="utf-8")
    grid = Path("static/js/ads-grid.js").read_text(encoding="utf-8")

    assert "def _ads_product_filter_sql" in backend
    assert "product_status: Optional[str] = None" in backend
    assert "product_class: Optional[str] = None" in backend
    assert "COALESCE(rb.product_status, '') = :product_status" in backend
    assert "COALESCE(rb.product_class, '') = :product_class" in backend
    assert "getAdsProductFilterQuery" in grid
    assert "url += getAdsProductFilterQuery()" in template
    assert "url += getAdsProductFilterQuery()" in arts
