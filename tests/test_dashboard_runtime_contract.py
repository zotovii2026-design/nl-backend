from pathlib import Path
import re


DASHBOARD_SOURCE = Path("templates/nl_v2.html").read_text(encoding="utf-8")
OPIU_SOURCE = Path("static/js/opiu-grid.js").read_text(encoding="utf-8")
COST_GRID_SOURCE = Path("static/js/cost-grid.js").read_text(encoding="utf-8")


def test_marketer_loader_is_defined():
    assert "async function loadMarketer()" in DASHBOARD_SOURCE
    assert "/api/v1/nl/marketer/chart-data?" in DASHBOARD_SOURCE


def test_async_sections_guard_removed_dom_nodes():
    required_guards = (
        "if (!document.getElementById('ad-views')) return;",
        "if (!el || !el.options.length) return;",
        "if (!count || !body) return;",
        "if (!cards || !summary || !count || !header) return;",
        "const el = document.getElementById('wb-keys-list');\n    if (!el) return;",
    )

    for guard in required_guards:
        assert guard in DASHBOARD_SOURCE


def test_opiu_loader_is_external_and_guards_removed_dom():
    assert "/static/js/opiu-grid.js" in DASHBOARD_SOURCE
    assert "async function loadOpiu()" not in DASHBOARD_SOURCE
    assert "function ensureOpiuDom()" in OPIU_SOURCE
    assert "if (!ensureOpiuDom()) return false;" in OPIU_SOURCE
    assert "if (!container || typeof Tabulator === 'undefined') return false;" in OPIU_SOURCE
    assert "if (!ensureOpiuDom()) return;" in OPIU_SOURCE
    assert "ОПиУ по артикулам" in DASHBOARD_SOURCE
    assert "await loadOrgs(); _opiuInited = true;" in DASHBOARD_SOURCE


def test_reference_book_visible_columns_contract_is_stable():
    columns_block = COST_GRID_SOURCE.split("function getCostColumns()", 1)[1].split(
        "/**\n * Подготовить данные", 1
    )[0]
    fields = [
        (match.group(2), match.group(1))
        for match in re.finditer(r"title:\s*'([^']*)'\s*,\s*field:\s*'([^']*)'", columns_block)
    ]

    assert fields == [
        ("_selected", "☑"),
        ("product_status", "Статус товара"),
        ("product_class", "Класс товара"),
        ("brand", "Бренд"),
        ("photo_main", "Фото"),
        ("subject_name", "Категория"),
        ("vendor_code", "Арт продавца"),
        ("_barcodes", "Баркод"),
        ("_sizeList", "Размер"),
        ("nm_id_display", "Арт WB"),
        ("product_name", "Товар"),
        ("fulfillment_model", "Отгрузка"),
        ("fbs_warehouse", "Склад FBS"),
        ("cost_price", "Себестоимость ₽"),
        ("extra_costs", "Доп расходы ₽"),
        ("_total_cost", "Итого ₽"),
        ("_tax_rate_override", "Налог %"),
        ("vat_rate", "НДС от дохода"),
        ("plan_length", "Длина"),
        ("plan_width", "Ширина"),
        ("plan_height", "Высота"),
        ("plan_volume", "Объём, л"),
        ("plan_weight", "Вес, гр"),
        ("_fact_dims", "Д×Ш×В"),
        ("_fact_volume", "Объём, л"),
        ("_fact_weight", "Вес, кг"),
        ("season_jan", "янв"),
        ("season_feb", "фев"),
        ("season_mar", "мар"),
        ("season_apr", "апр"),
        ("season_may", "май"),
        ("season_jun", "июн"),
        ("season_jul", "июл"),
        ("season_aug", "авг"),
        ("season_sep", "сен"),
        ("season_oct", "окт"),
        ("season_nov", "ноя"),
        ("season_dec", "дек"),
        ("top_query_1", "1"),
        ("top_query_2", "2"),
        ("top_query_3", "3"),
        ("buyout_niche_pct", "% выкупа по кат."),
        ("mp_correction_pct", "Корр. комиссии %"),
        ("ad_plan_rub", "Рекл. расходы %"),
        ("supply_days", "Скорость достав., дн"),
        ("min_batch_fbo", "Мин партия"),
        ("rrc_price", "РРЦ"),
        ("min_price", "Мин. цена"),
        ("change_date", "Дата правок"),
        ("valid_from", "Дата начала"),
    ]


def test_reference_book_uses_global_store_selector_only():
    assert 'id="cp-store"' not in DASHBOARD_SOURCE
    assert "switchCostStore" not in DASHBOARD_SOURCE
    assert "loadFbsWarehouses(orgId)" in DASHBOARD_SOURCE
    assert "const orgId = getCurrentOrgId();" in DASHBOARD_SOURCE


def test_top_store_switch_reloads_reference_book():
    switch_top_store = DASHBOARD_SOURCE.split("async function switchTopStore()", 1)[1].split(
        "\n}\n\nfunction showNewOrgDialog", 1
    )[0]

    assert "if (_costDirty && !await confirmDirty()) return;" in switch_top_store
    assert "else if (tabName === 'costprice') { loadTaxSettings(); loadCostPrices(); }" in switch_top_store


def test_expired_session_is_handled_globally():
    assert "function handleAuthExpired()" in DASHBOARD_SOURCE
    assert "Сессия истекла. Войдите заново." in DASHBOARD_SOURCE
    assert "response.status === 401" in DASHBOARD_SOURCE
    assert "AUTH_EXEMPT_PATHS.includes(url.pathname)" in DASHBOARD_SOURCE


def test_reference_upload_ui_advertises_real_file_types():
    assert "Загрузить CSV/XLSX" in DASHBOARD_SOURCE
    assert 'accept=".xlsx,.csv"' in DASHBOARD_SOURCE
    assert "Скачать CSV-шаблон" in DASHBOARD_SOURCE
    assert "encodeURIComponent(orgId)" in DASHBOARD_SOURCE


def test_reference_book_save_payload_does_not_zero_hidden_legacy_fields():
    payload_block = COST_GRID_SOURCE.split("function getCostDataForSave()", 1)[1]
    payload_block = payload_block.split("\n}", 1)[0]

    assert "purchase_cost: null, logistics_cost: null, packaging_cost: null, other_costs: null" in payload_block
    assert "mp_base_pct: null" in payload_block
    assert "storage_pct: null" in payload_block
    assert "price_before_spp_plan: null" in payload_block
    assert "price_before_spp_change: null" in payload_block
    assert "wb_club_discount_pct: null" in payload_block
    assert "vat: null" in payload_block

    forbidden = (
        "purchase_cost: 0",
        "logistics_cost: 0",
        "packaging_cost: 0",
        "other_costs: 0",
        "mp_base_pct: 0",
        "storage_pct: 0",
        "price_before_spp_plan: 0",
        "price_before_spp_change: 0",
        "wb_club_discount_pct: 0",
    )
    for snippet in forbidden:
        assert snippet not in payload_block
