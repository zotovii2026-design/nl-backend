import pytest

from api.v1.routers.marketer import get_marketer_metric_catalog
from main import app


def test_marketer_metric_catalog_route_is_registered():
    routes = {
        (method, route.path)
        for route in app.routes
        for method in (getattr(route, "methods", None) or set())
    }

    assert ("GET", "/api/v1/nl/marketer/metric-catalog") in routes
    assert ("GET", "/api/v1/nl/marketer/chart-data") in routes
    assert ("GET", "/api/v1/nl/marketer/product/{nm_id}/sizes") in routes


@pytest.mark.asyncio
async def test_marketer_metric_catalog_keeps_graph_contract():
    catalog = await get_marketer_metric_catalog(
        "00000000-0000-0000-0000-000000000001",
    )

    sections = {section["key"]: section for section in catalog["sections"]}
    assert "economy" in sections
    assert "logistics_layout" in sections
    assert "ads_traffic" in sections
    assert "other" in sections

    economy_metrics = {metric["key"] for metric in sections["economy"]["metrics"]}
    assert {"period_profit", "price", "price_spp"}.issubset(economy_metrics)

    ad_metrics = {metric["key"] for metric in sections["ads_traffic"]["metrics"]}
    assert {"cv", "cart_count", "drr"}.issubset(ad_metrics)

    chart_metrics = {metric["key"] for metric in catalog["chart_metrics"]}
    assert {"price", "price_spp", "stock_qty", "orders_count", "buyout_pct"}.issubset(
        chart_metrics
    )
    assert "cv" not in chart_metrics

    assert catalog["filter_contract"]["source"] == "top_page_filters"
    assert catalog["filter_contract"]["top_chart"] == {
        "no_product_filter": "store_filtered_aggregate",
        "with_product_filter": "average_of_filtered_products",
        "with_group_filter": "average_of_group_products",
    }
    assert catalog["filter_contract"]["lower_charts"]["default_metrics"] == "mirror_top_chart"

    size_level = next(level for level in catalog["entity_levels"] if level["key"] == "size")
    assert size_level["default_scope"] == "grouped_by_product"
    assert catalog["ai_panel"]["position"] == "right"


def test_marketer_chart_metrics_are_real_backend_metrics():
    from api.v1.routers.marketer import MARKETER_CHART_METRICS, _resolve_chart_metrics

    assert {"price", "price_spp", "stock_qty", "orders_count", "buyout_pct"}.issubset(
        MARKETER_CHART_METRICS
    )
    assert _resolve_chart_metrics("price,cv,orders_count,unknown") == ["price", "orders_count"]
    assert _resolve_chart_metrics("") == [
        "orders_count",
        "stock_qty",
        "price",
        "price_spp",
        "buyout_pct",
        "views",
        "ctr_total",
    ]
