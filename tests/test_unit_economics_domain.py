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


def _formula_item(**overrides):
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
        "wb_club_discount_pct": 0,
        "loyalty_cashback_pct": 0,
        "other_deductions_pct": 0,
        "acquiring_pct": 1.5,
        "storage_tariff": 0,
        "price_before_spp_plan": 1100,
        "price_before_spp_change": 1200,
        "spp_pct": 0,
    }
    item.update(overrides)
    return item


def test_financial_formulas_keep_legacy_contract():
    item = _formula_item()

    result = apply_financial_formulas(item)

    # 20.08.2026: комиссия и эквайринг — от цены ДО СПП; налог УСН Д — от цены с СПП
    assert result["mp_total_pct"] == 12
    assert result["ad_plan_rub"] == 50
    assert result["mp_commission"] == 120  # 1000 * 12%
    assert result["acquiring_rub"] == 15  # 1000 * 1.5%
    assert result["tax_total"] == 54  # 900 * 6%
    assert result["tax_label"] == "УСН Д, 6%"
    assert result["logistics_with_buyout"] == 54
    # Новые формулы (ТЗ 31.08.2026)
    # total_wb_deductions = 120 (комиссия) + 54 (лог. с выкупом) + 0 (хранение) + 15 (эквайринг)
    #   + 50 (реклама план) + 0 (прочие) + 0 (WB клуб) + 0 (кэшбэк) = 239
    assert result["total_wb_deductions"] == 239
    # expenses = 239 - 300 (себестоимость) - 54 (налог) = -115 (может быть отрицательным)
    assert result["expenses"] == -115
    assert result["profit"] == 1115  # 1000 - (-115)
    assert result["margin_pct"] == 111.5  # 1115 / 1000 * 100
    assert result["roi_pct"] == 371.67  # 1115 / 300 * 100
    assert result["to_account"] == 761  # 1000 - 239
    # После правок: цена 1200, комиссия 144, эквайринг 18, реклама 60
    # total = 144 + 54 + 18 + 60 = 276; expenses = 276 - 300 - 54 = -78; profit = 1200 + 78 = 1278
    assert result["profit_change"] == 1278


def test_new_formulas_numeric_example_with_club_and_cashback():
    """Числовой пример ТЗ 31.08.2026: скидка WB клуба, кэшбэк, отрицательные расходы."""
    item = _formula_item(
        price_before_spp=2000,
        wb_club_discount_pct=5,
        loyalty_cashback_pct=3,
        other_deductions_pct=1,
        storage_tariff=7.3,
        cost_price=600,
    )

    result = apply_financial_formulas(item)

    # комиссия 12% от 2000 = 240; лог. с выкупом = 50 + 20*0.2 = 54
    assert result["mp_commission"] == 240
    assert result["logistics_with_buyout"] == 54
    # эквайринг 1.5% = 30; реклама план 5% = 100; прочие 1% = 20
    assert result["acquiring_rub"] == 30
    assert result["ad_plan_rub"] == 100
    assert result["other_deductions_finreport"] == 20
    # клуб и кэшбэк от цены до СПП
    assert result["wb_club_discount_rub"] == 100  # 2000 * 5%
    assert result["loyalty_cashback_rub"] == 60  # 2000 * 3%
    # налог УСН Д 6% от цены с СПП (900) = 54
    assert result["tax_total"] == 54
    # total_wb_deductions = 240 + 54 + 7.3 + 30 + 100 + 20 + 100 + 60 = 611.3
    assert result["total_wb_deductions"] == 611.3
    # expenses = 611.3 - 600 - 54 = -42.7 (отрицательные — допустимы)
    assert result["expenses"] == -42.7
    # profit = 2000 - (-42.7) = 2042.7
    assert result["profit"] == 2042.7
    assert result["margin_pct"] == 102.13  # 2042.7 / 2000 * 100 = 102.135 → 102.13
    assert result["roi_pct"] == 340.45  # 2042.7 / 600 * 100
    # to_account = 2000 - 611.3 = 1388.7
    assert result["to_account"] == 1388.7


def test_new_formulas_zero_price_and_zero_cost_guards():
    result = apply_financial_formulas(
        _formula_item(price_before_spp=0, cost_price=0)
    )

    assert result["margin_pct"] == 0  # нулевая цена — маржа 0
    assert result["roi_pct"] == 0  # нулевая себестоимость — ROI 0
