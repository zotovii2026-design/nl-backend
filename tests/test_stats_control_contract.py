from pathlib import Path


DASHBOARD_ROUTER = Path("api/v1/routers/dashboard.py").read_text(encoding="utf-8")
STATS_GRID = Path("static/js/stats-grid.js").read_text(encoding="utf-8")


def test_control_products_are_card_level_when_funnel_is_used():
    build_rows = DASHBOARD_ROUTER.split("def build_product_rows(rows):", 1)[1].split(
        "total_clicks =", 1
    )[0]

    assert 'key = f"nm:{nm}"' in build_rows
    assert "WB sales_funnel is nm-level" in build_rows
    assert '"sizes": []' in build_rows
    assert 'item["sizes"] = sizes' in build_rows
    assert '"orders_count": funnel_item["orders_count"]' in build_rows
    assert 'key = eid or f"nm:{r[1]}:{r[17] or \'\'}"' not in build_rows


def test_stats_grid_renders_card_rows_without_nm_grouping():
    init_block = STATS_GRID.split("function initStatsProductsGrid()", 1)[1].split(
        "function prepareStatsProducts", 1
    )[0]

    assert "groupBy: 'nm_id'" not in init_block
    assert "statsSizesFormatter" in STATS_GRID
    assert "title: 'Размеры'" in STATS_GRID
