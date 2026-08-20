import pytest

from domain.unit_economics import (
    apply_financial_formulas,
    build_box_tariff_context,
    calculate_delivery,
    calculate_reverse_delivery,
    normalize_fbo_average_warehouses,
)


def _tariff_rows():
    return [
        ("Коледино", 92, 28, 80, 20, 200, 150),
        ("Краснодар", 69, 21, 0, 0, 150, 0),
        ("Казань", 46, 14, 0, 0, 100, 0),
    ]


def _new_rf_tariff_rows():
    return [
        ("Свой склад СГТ РФ", None, None, 46, 14, None, 100),
        ("Свой склад РФ", None, None, 78.2, 23.8, None, 170),
        ("Ташкент", 46, 14, None, None, 100, None),
    ]


def test_fbo_delivery_keeps_average_warehouse_method():
    context = build_box_tariff_context(_tariff_rows())

    cost, debug = calculate_delivery(0.5, "fbo", None, context)

    assert cost == 43.5
    assert debug["method"] == "ФБО-среднее (сетка <=1л)"
    assert debug["avg_coef"] == 150


def test_fbo_delivery_uses_selected_average_warehouses():
    context = build_box_tariff_context(_tariff_rows(), ["Коледино", "Краснодар"])

    cost, debug = calculate_delivery(2.1, "fbo", None, context)

    assert cost == 129.5
    assert debug["warehouses"] == ["Коледино", "Краснодар"]
    assert context["fbo_avg_base"] == 80.5
    assert context["fbo_avg_liter"] == 24.5


def test_fbo_average_warehouses_falls_back_to_default():
    assert normalize_fbo_average_warehouses([]) == ("Коледино", "Краснодар", "Казань")
    assert normalize_fbo_average_warehouses(None) == ("Коледино", "Краснодар", "Казань")


def test_fbo_delivery_falls_back_when_saved_warehouses_are_missing():
    context = build_box_tariff_context(_tariff_rows(), ["Неизвестный склад"])

    cost, debug = calculate_delivery(0.5, "fbo", None, context)

    assert cost == 43.5
    assert debug["warehouses"] == ["Коледино", "Краснодар", "Казань"]


def test_fbs_delivery_uses_selected_warehouse_tariff():
    context = build_box_tariff_context(_tariff_rows())

    cost, debug = calculate_delivery(2.1, "fbs", "Коледино", context)

    assert cost == 120
    assert debug["method"] == "ФБС (>1л)"
    assert debug["warehouse"] == "Коледино"


def test_fbo_delivery_uses_virtual_rf_average_when_wb_hides_russian_warehouses():
    context = build_box_tariff_context(_new_rf_tariff_rows())

    cost, debug = calculate_delivery(0.5, "fbo", None, context)

    assert cost == 78.2
    assert context["fbo_unified_rf"] is True
    assert debug["method"] == "ФБО/FBW РФ единый тариф"
    assert debug["warehouses"] == ["Коледино", "Краснодар", "Казань"]
    assert debug["avg_coef"] == 170


def test_fbo_delivery_keeps_selected_virtual_warehouses_for_rf_unified_tariff():
    context = build_box_tariff_context(_new_rf_tariff_rows(), ["Коледино", "Казань"])

    cost, debug = calculate_delivery(2.1, "fbo", None, context)

    assert cost == 125.8
    assert context["fbo_unified_rf"] is True
    assert debug["warehouses"] == ["Коледино", "Казань"]


def test_fbs_delivery_uses_rf_unified_tariff_without_selected_warehouse():
    context = build_box_tariff_context(_new_rf_tariff_rows())

    cost, debug = calculate_delivery(0.5, "fbs", None, context)

    assert cost == 78.2
    assert debug["method"] == "ФБС РФ единый тариф"
    assert debug["warehouse"] == "Свой склад РФ"


@pytest.mark.parametrize(
    ("volume", "expected"),
    [(0.2, 23), (0.5, 29), (2.1, 74)],
)
def test_reverse_delivery_keeps_wb_base_grid(volume, expected):
    cost, _ = calculate_reverse_delivery(volume)
    assert cost == expected


def test_financial_formulas_keep_legacy_contract():
    item = {
        "mp_base_pct": 10,
        "mp_correction_pct": 2,
        "price_before_spp": 1000,
        "ad_plan_pct": 5,
        "price_with_spp": 900,
        "ad_fact_pct": 3,
        "tax_system": "usn",
        "tax_rate": 6,
        "vat_rate": 0,
        "purchase_cost": 0,
        "cost_price": 300,
        "buyout_fact_pct": 80,
        "buyout_niche_pct": 0,
        "delivery_to_client": 50,
        "reverse_logistics": 20,
        "logistics_cost": 10,
        "packaging_cost": 5,
        "other_costs": 2,
        "logistics_actual": 0,
        "storage_actual": 0,
        "acceptance_avg": 0,
        "price_before_spp_plan": 1100,
        "price_before_spp_change": 1200,
        "spp_pct": 0,
    }

    result = apply_financial_formulas(item)

    # 20.08.2026: комиссия и эквайринг — от цены ДО СПП; налог УСН Д — от цены с СПП
    assert result["mp_total_pct"] == 12
    assert result["ad_plan_rub"] == 50
    assert result["mp_commission"] == 120  # 1000 * 12%
    assert result["acquiring_rub"] == 15  # 1000 * 1.5%
    assert result["tax_total"] == 54  # 900 * 6%
    assert result["tax_label"] == "УСН Д, 6%"
    assert result["logistics_with_buyout"] == 54
    assert result["expenses_fact"] == 553.0
    assert result["profit_fact"] == 347.0
    assert result["plan_price_spp"] == 1100
    assert result["profit_plan"] == 493.5
    assert result["profit_change"] == 602
