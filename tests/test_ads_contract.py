from pathlib import Path

from api.v1.routers.ads import (
    ADS_REFRESH_COOLDOWN_SECONDS,
    ADS_REFRESH_DAYS_BACK,
    DEFAULT_AD_STATUSES,
    _ad_type_label,
    _parse_statuses,
)


def test_ads_default_statuses_are_active_and_paused_only():
    assert DEFAULT_AD_STATUSES == ["9", "11"]
    assert _parse_statuses(None) == ["9", "11"]
    assert "7" not in _parse_statuses(None)


def test_ads_accepts_archive_status_when_explicit():
    assert _parse_statuses("9,11,7") == ["9", "11", "7"]


def test_ads_router_keeps_ad_conversions_on_ad_stats_nm():
    source = Path("api/v1/routers/ads.py").read_text(encoding="utf-8")
    assert "FROM ad_stats_nm sn" in source
    assert "SUM(sn.orders) as orders" in source
    assert "COALESCE(SUM(sn.sum_price), 0) as sum_price" in source
    assert "def _get_total_orders_revenue_by_day" in source


def test_ads_manual_refresh_uses_nine_day_window():
    assert ADS_REFRESH_DAYS_BACK == 9
    assert ADS_REFRESH_COOLDOWN_SECONDS == 60 * 60


def test_ads_manual_refresh_passes_selected_org_to_celery():
    source = Path("api/v1/routers/ads.py").read_text(encoding="utf-8")
    assert 'kwargs={"days_back": ADS_REFRESH_DAYS_BACK, "org_id": org_id}' in source


def test_ads_campaign_type_label_uses_bid_and_payment_type():
    assert _ad_type_label("9", "unified", "cpm") == "Единая / CPM"
    assert _ad_type_label("9", "manual", "cpc") == "Ручная / CPC"
    assert _ad_type_label("9", "manual", "cpm") == "Ручная / CPM"
    assert _ad_type_label("5", "", "") == "Поиск"


def test_ads_sync_persists_wb_bid_and_payment_type():
    source = Path("tasks/ad_sync.py").read_text(encoding="utf-8")
    assert 'item.get("paymentType", item.get("payment_type", ""))' in source
    assert 'item.get("bidType", item.get("bid_type", ""))' in source
    assert "payment_type = EXCLUDED.payment_type, bid_type = EXCLUDED.bid_type" in source


def test_ads_frontend_prefers_api_campaign_type_label():
    backend = Path("api/v1/routers/ads.py").read_text(encoding="utf-8")
    grid = Path("static/js/ads-grid.js").read_text(encoding="utf-8")
    arts = Path("static/js/ads-arts-grid.js").read_text(encoding="utf-8")

    assert '"type_label": _ad_type_label' in backend
    assert "data.type_label || typeMap" in grid
    assert "row.type_label ||" in grid
    assert "c.type_label || typeNames" in arts


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


def test_ads_uses_url_org_and_handles_unauthorized_without_breaking_tabs():
    template = Path("templates/nl_v2.html").read_text(encoding="utf-8")
    arts = Path("static/js/ads-arts-grid.js").read_text(encoding="utf-8")
    grid = Path("static/js/ads-grid.js").read_text(encoding="utf-8")

    assert "function getCurrentOrgId()" in template
    assert "const urlOrg = new URL(location.href).searchParams.get('org');" in template
    assert "selectedOrg || ORG_ID || urlOrg || localStorage.getItem('nl_org_id')" in template
    assert "setCurrentOrgId(new URL(location.href).searchParams.get('org') || data.org_id);" in template
    assert "if (urlOrg && orgs.some(function(o) { return o.id === urlOrg; }))" in template
    assert "showAdsLoadError('Нет доступа к выбранному магазину" in template
    assert "var orgId = (typeof getCurrentOrgId === 'function') ? getCurrentOrgId()" in arts
    assert "encodeURIComponent(orgId)" in arts
    assert "if (typeof getCurrentOrgId === 'function') return getCurrentOrgId();" in grid
    assert "adsmodel11" in template
    assert "adsmodel10" in template


def test_ads_has_separate_campaign_and_total_drr_columns():
    backend = Path("api/v1/routers/ads.py").read_text(encoding="utf-8")
    template = Path("templates/nl_v2.html").read_text(encoding="utf-8")
    grid = Path("static/js/ads-grid.js").read_text(encoding="utf-8")
    arts = Path("static/js/ads-arts-grid.js").read_text(encoding="utf-8")

    assert '"drr": drr_rk' in backend
    assert '"drr_total": drr_total' in backend
    assert "r.api_method = 'orders'" in backend
    assert "DISTINCT ON (srid)" in backend
    assert "priceWithDisc" in backend
    assert "ДРР по РК" in template
    assert "ДРР общий" in template
    assert "field: 'drr_total'" in grid
    assert "ДРР по РК %" in arts
    assert "ДРР товара %" in arts
    assert "field: 'drr_product'" in arts
    assert "field: 'drr_total'" not in arts
    assert '"drr_total": drr_total_art' not in backend
    assert '"drr_product": drr_product' in backend
    assert "total_revenue_product" in backend


def test_ads_org_switch_resets_state_and_ignores_stale_responses():
    template = Path("templates/nl_v2.html").read_text(encoding="utf-8")
    arts = Path("static/js/ads-arts-grid.js").read_text(encoding="utf-8")

    assert "function resetAdsUiForOrgChange()" in template
    assert "resetAdsUiForOrgChange();" in template
    assert "const requestSeq = ++_adsLoadSeq;" in template
    assert "if (requestSeq !== _adsLoadSeq || orgId !== getCurrentOrgId()) return;" in template
    assert "var requestSeq = ++_adsArtsLoadSeq;" in arts
    assert "if (requestSeq !== _adsArtsLoadSeq || orgId !== currentOrgId) return;" in arts
    assert "var requestSeq = ++_adsRefreshStatusSeq;" in arts


def test_ads_filter_options_do_not_shrink_after_server_side_filtering():
    grid = Path("static/js/ads-grid.js").read_text(encoding="utf-8")
    arts = Path("static/js/ads-arts-grid.js").read_text(encoding="utf-8")

    assert "function hasAdsProductFilters()" in grid
    assert "if (!hasAdsProductFilters()) populateAdsFilterOptionsForRK();" in grid
    assert "if (current && values.indexOf(current) < 0) values = values.concat([current]).sort();" in grid
    assert "if (typeof hasAdsProductFilters !== 'function' || !hasAdsProductFilters()) populateAdsFilterOptions();" in arts
