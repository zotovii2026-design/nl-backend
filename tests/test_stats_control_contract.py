from pathlib import Path


DASHBOARD_ROUTER = Path("api/v1/routers/dashboard.py").read_text(encoding="utf-8")
STATS_GRID = Path("static/js/stats-grid.js").read_text(encoding="utf-8")


def test_control_products_are_entity_level_with_separate_card_funnel_metrics():
    build_rows = DASHBOARD_ROUTER.split("def build_product_rows(rows):", 1)[1].split(
        "total_clicks =", 1
    )[0]

    assert 'key = eid or f"nm:{nm}:{r[17] or \'\'}"' in build_rows
    assert '"card_orders_count": 0' in build_rows
    assert '"card_orders_count": funnel_item["orders_count"]' in build_rows
    assert '"orders_count": funnel_item["orders_count"]' not in build_rows
    assert '"sizes": []' not in build_rows
    assert 'item["sizes"] = sizes' not in build_rows


def test_stats_grid_renders_entity_rows_grouped_by_card():
    init_block = STATS_GRID.split("function initStatsProductsGrid()", 1)[1].split(
        "function prepareStatsProducts", 1
    )[0]

    assert "groupBy: 'nm_id'" in init_block
    assert "card_orders_count" in init_block
    assert "statsSizesFormatter" not in STATS_GRID
    assert "title: 'Размер'" in STATS_GRID
    assert "Заказы разм." in STATS_GRID
    assert "Показы карт." in STATS_GRID


def test_parse_raw_uses_entity_level_fbo_and_prices_when_chrt_id_exists():
    parse_raw = Path("tasks/sync/parse_raw.py").read_text(encoding="utf-8")

    assert "SELECT id, nm_id, size_name, chrt_id FROM product_entities" in parse_raw
    assert "entity_by_nm_chrt" in parse_raw
    assert "key = entity_id or nm" in parse_raw
    assert "_date_fbo.get(e_id) or _date_fbo.get(n_id, {})" in parse_raw
    assert "prices_by_entity" in parse_raw
    assert "_wp = prices_by_entity.get(e_id)" in parse_raw
