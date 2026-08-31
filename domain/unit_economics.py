import math
from typing import Any


FBO_WAREHOUSE_NAMES = ("Коледино", "Краснодар", "Казань")
DEFAULT_FBO_AVERAGE_WAREHOUSES = FBO_WAREHOUSE_NAMES
WB_TIER_RATES = (
    (0.001, 0.200, 23.0),
    (0.201, 0.400, 26.0),
    (0.401, 0.600, 29.0),
    (0.601, 0.800, 30.0),
    (0.801, 1.000, 32.0),
)
WB_BASE_FIRST_LITER = 46.0
WB_BASE_NEXT_LITER = 14.0
WB_RF_UNIFIED_WAREHOUSE = "Свой склад РФ"
WB_RF_UNIFIED_SGT_WAREHOUSE = "Свой склад СГТ РФ"


def normalize_fbo_average_warehouses(
    warehouse_names: list[str] | tuple[str, ...] | None,
) -> tuple[str, ...]:
    selected = []
    seen = set()
    for warehouse_name in warehouse_names or []:
        clean = str(warehouse_name or "").strip()
        key = clean.lower()
        if clean and key not in seen:
            selected.append(clean)
            seen.add(key)
    return tuple(selected[:5]) or DEFAULT_FBO_AVERAGE_WAREHOUSES


def build_box_tariff_context(
    rows: list[Any],
    fbo_warehouse_names: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    tariffs = {}
    fbo_delivery_sum = 0.0
    fbo_liter_sum = 0.0
    fbo_coef_sum = 0.0
    fbo_count = 0
    selected_fbo_warehouses = normalize_fbo_average_warehouses(fbo_warehouse_names)
    selected_fbo_lookup = {name.lower() for name in selected_fbo_warehouses}
    matched_fbo_warehouses = []

    for row in rows:
        warehouse_name = row[0]
        tariffs[warehouse_name] = {
            "fbo_base": float(row[1]) if row[1] else None,
            "fbo_liter": float(row[2]) if row[2] else None,
            "fbs_base": float(row[3]) if row[3] else None,
            "fbs_liter": float(row[4]) if row[4] else None,
            "fbo_coef": float(row[5]) if row[5] else None,
            "fbs_coef": float(row[6]) if len(row) > 6 and row[6] else None,
        }
        if (
            warehouse_name.lower() in selected_fbo_lookup
            and row[1] is not None
            and row[2] is not None
        ):
            fbo_delivery_sum += float(row[1])
            fbo_liter_sum += float(row[2])
            fbo_coef_sum += float(row[5]) if len(row) > 5 and row[5] else 0
            fbo_count += 1
            matched_fbo_warehouses.append(warehouse_name)

    if not fbo_count and selected_fbo_warehouses != DEFAULT_FBO_AVERAGE_WAREHOUSES:
        default_delivery_sum = 0.0
        default_liter_sum = 0.0
        default_coef_sum = 0.0
        default_count = 0
        default_matched_warehouses = []
        default_lookup = {name.lower() for name in DEFAULT_FBO_AVERAGE_WAREHOUSES}
        for row in rows:
            warehouse_name = row[0]
            if (
                warehouse_name.lower() in default_lookup
                and row[1] is not None
                and row[2] is not None
            ):
                default_delivery_sum += float(row[1])
                default_liter_sum += float(row[2])
                default_coef_sum += float(row[5]) if len(row) > 5 and row[5] else 0
                default_count += 1
                default_matched_warehouses.append(warehouse_name)
        if default_count:
            fbo_delivery_sum = default_delivery_sum
            fbo_liter_sum = default_liter_sum
            fbo_coef_sum = default_coef_sum
            fbo_count = default_count
            matched_fbo_warehouses = default_matched_warehouses
            selected_fbo_warehouses = DEFAULT_FBO_AVERAGE_WAREHOUSES

    fbo_unified_rf = False
    rf_unified = tariffs.get(WB_RF_UNIFIED_WAREHOUSE)
    if not fbo_count and rf_unified and rf_unified.get("fbs_base"):
        # WB changed Russian FBW/FBO tariffs from warehouse-level rows to one
        # country-wide 170% row. Keep user-selected warehouses as virtual
        # averaging targets so the product model can switch back when WB returns
        # warehouse rows.
        fbo_delivery_sum = float(rf_unified.get("fbs_base") or 0)
        fbo_liter_sum = float(rf_unified.get("fbs_liter") or 0)
        fbo_coef_sum = float(rf_unified.get("fbs_coef") or 0)
        fbo_count = 1
        fbo_unified_rf = True
        matched_fbo_warehouses = list(selected_fbo_warehouses)

    return {
        "tariffs": tariffs,
        "fbo_warehouses": list(selected_fbo_warehouses),
        "fbo_matched_warehouses": matched_fbo_warehouses,
        "fbo_avg_base": round(fbo_delivery_sum / fbo_count, 2) if fbo_count else 0,
        "fbo_avg_liter": round(fbo_liter_sum / fbo_count, 2) if fbo_count else 0,
        "fbo_avg_coef": round(fbo_coef_sum / fbo_count, 2) if fbo_count else 0,
        "fbo_unified_rf": fbo_unified_rf,
    }


def wb_rate_per_liter(volume_liters: float) -> float:
    for lower, upper, rate in WB_TIER_RATES:
        if lower <= volume_liters <= upper:
            return rate
    return WB_BASE_FIRST_LITER


def _box_delivery_cost(base: float, liter: float, volume_ceil: int) -> float:
    return round(base + max(volume_ceil - 1, 0) * liter, 2)


def calculate_delivery(
    volume_liters: float,
    fulfillment_model: str,
    fbs_warehouse: str | None,
    context: dict[str, Any],
) -> tuple[float, dict[str, Any]]:
    if not volume_liters or volume_liters <= 0:
        return 0, {}

    tariffs = context["tariffs"]
    volume_ceil = math.ceil(volume_liters)

    if fulfillment_model == "fbs":
        warehouse_tariffs = None
        warehouse_found = None
        if fbs_warehouse:
            for warehouse_name in tariffs:
                if fbs_warehouse in warehouse_name or warehouse_name in fbs_warehouse:
                    warehouse_tariffs = tariffs[warehouse_name]
                    warehouse_found = warehouse_name
                    break
        if not warehouse_tariffs:
            warehouse_tariffs = tariffs.get(WB_RF_UNIFIED_WAREHOUSE)
            warehouse_found = WB_RF_UNIFIED_WAREHOUSE if warehouse_tariffs else None

        if warehouse_tariffs:
            base = (
                warehouse_tariffs.get("fbs_base")
                or warehouse_tariffs.get("fbo_base")
                or 0
            )
            liter = (
                warehouse_tariffs.get("fbs_liter")
                or warehouse_tariffs.get("fbo_liter")
                or 0
            )
            coef = (
                warehouse_tariffs.get("fbs_coef")
                or warehouse_tariffs.get("fbo_coef")
                or 100
            )
            if base:
                if warehouse_found in (WB_RF_UNIFIED_WAREHOUSE, WB_RF_UNIFIED_SGT_WAREHOUSE):
                    cost = _box_delivery_cost(base, liter, volume_ceil)
                    debug = {
                        "method": "ФБС РФ единый тариф",
                        "warehouse": warehouse_found,
                        "vol_ceil": volume_ceil,
                        "base": base,
                        "liter": liter,
                        "coef": coef,
                        "formula": f"{base} + {max(volume_ceil - 1, 0)}×{liter}",
                        "result": cost,
                    }
                elif volume_liters <= 1.0:
                    rate = wb_rate_per_liter(volume_liters)
                    cost = round(rate * (coef / 100), 2)
                    debug = {
                        "method": "ФБС (сетка <=1л)",
                        "warehouse": warehouse_found,
                        "vol": volume_liters,
                        "tier_rate": rate,
                        "coef": coef,
                        "coef_source": (
                            "fbs" if warehouse_tariffs.get("fbs_coef") else "fbo"
                        ),
                        "formula": f"{rate} × {coef}%",
                        "result": cost,
                    }
                else:
                    cost = _box_delivery_cost(base, liter, volume_ceil)
                    debug = {
                        "method": "ФБС (>1л)",
                        "warehouse": warehouse_found,
                        "vol_ceil": volume_ceil,
                        "base": base,
                        "liter": liter,
                        "coef": coef,
                        "coef_source": (
                            "fbs" if warehouse_tariffs.get("fbs_coef") else "fbo"
                        ),
                        "formula": f"{base} + {volume_ceil - 1}×{liter}",
                        "result": cost,
                    }
                return cost, debug

        fallback = tariffs.get("Коледино")
        if fallback and (fallback.get("fbs_base") or fallback.get("fbs_liter")):
            base = fallback.get("fbs_base") or fallback.get("fbo_base") or 0
            liter = fallback.get("fbs_liter") or fallback.get("fbo_liter") or 0
            coef = fallback.get("fbs_coef") or fallback.get("fbo_coef") or 100
            if volume_liters <= 1.0:
                rate = wb_rate_per_liter(volume_liters)
                cost = round(rate * (coef / 100), 2)
                debug = {
                    "method": "ФБС Коледино (сетка <=1л, fallback)",
                    "warehouse": "Коледино",
                    "warehouse_requested": fbs_warehouse,
                    "fallback_warehouse": "Коледино",
                    "tier_rate": rate,
                    "coef": coef,
                    "formula": f"{rate} × {coef}%",
                    "result": cost,
                }
            else:
                cost = _box_delivery_cost(base, liter, volume_ceil)
                debug = {
                    "method": "ФБС Коледино (>1л, fallback)",
                    "warehouse": "Коледино",
                    "warehouse_requested": fbs_warehouse,
                    "fallback_warehouse": "Коледино",
                    "vol_ceil": volume_ceil,
                    "base": base,
                    "liter": liter,
                    "coef": coef,
                    "formula": f"{base} + {volume_ceil - 1}×{liter}",
                    "result": cost,
                }
            return cost, debug
        return 0, {}

    if context["fbo_avg_base"]:
        if context.get("fbo_unified_rf"):
            cost = _box_delivery_cost(
                context["fbo_avg_base"],
                context["fbo_avg_liter"],
                volume_ceil,
            )
            debug = {
                "method": "ФБО/FBW РФ единый тариф",
                "vol_ceil": volume_ceil,
                "avg_base": context["fbo_avg_base"],
                "avg_liter": context["fbo_avg_liter"],
                "avg_coef": context["fbo_avg_coef"],
                "warehouses": context.get("fbo_matched_warehouses", []),
                "formula": (
                    f"{context['fbo_avg_base']} + "
                    f"{max(volume_ceil - 1, 0)}×{context['fbo_avg_liter']} "
                    f"(единый РФ тариф {context['fbo_avg_coef']}%)"
                ),
                "result": cost,
            }
        elif volume_liters <= 1.0:
            rate = wb_rate_per_liter(volume_liters)
            cost = round(rate * (context["fbo_avg_coef"] / 100), 2)
            debug = {
                "method": "ФБО-среднее (сетка <=1л)",
                "vol": volume_liters,
                "tier_rate": rate,
                "avg_coef": context["fbo_avg_coef"],
                "warehouses": context.get("fbo_matched_warehouses", []),
                "formula": (
                    f"{rate} × {context['fbo_avg_coef']}% "
                    f"(среднее {len(context.get('fbo_matched_warehouses', []))} склад.)"
                ),
                "result": cost,
            }
        else:
            cost = _box_delivery_cost(
                context["fbo_avg_base"],
                context["fbo_avg_liter"],
                volume_ceil,
            )
            debug = {
                "method": "ФБО-среднее (>1л)",
                "vol_ceil": volume_ceil,
                "avg_base": context["fbo_avg_base"],
                "avg_liter": context["fbo_avg_liter"],
                "warehouses": context.get("fbo_matched_warehouses", []),
                "formula": (
                    f"{context['fbo_avg_base']} + "
                    f"{volume_ceil - 1}×{context['fbo_avg_liter']}"
                ),
                "result": cost,
            }
        return cost, debug
    return 0, {}


def calculate_reverse_delivery(
    volume_liters: float,
) -> tuple[float, dict[str, Any]]:
    if not volume_liters or volume_liters <= 0:
        return 0, {}

    volume_ceil = math.ceil(volume_liters)
    if volume_liters <= 1.0:
        rate = wb_rate_per_liter(volume_liters)
        cost = round(rate, 2)
        debug = {
            "method": "Обратная лог. (сетка <=1л)",
            "vol": volume_liters,
            "tier_rate": rate,
            "formula": f"{rate} ₽ (без коэфф. склада)",
            "result": cost,
        }
    else:
        cost = round(
            WB_BASE_FIRST_LITER + (volume_ceil - 1) * WB_BASE_NEXT_LITER,
            2,
        )
        debug = {
            "method": "Обратная лог. (>1л)",
            "vol_ceil": volume_ceil,
            "base": WB_BASE_FIRST_LITER,
            "liter": WB_BASE_NEXT_LITER,
            "formula": (
                f"{WB_BASE_FIRST_LITER} + "
                f"{volume_ceil - 1}×{WB_BASE_NEXT_LITER}"
            ),
            "result": cost,
        }
    return cost, debug


def normalize_tax_system(raw: str | None) -> str | None:
    """Преобразует человекочитаемое название системы налогообложения
    в код, понятный calculate_tax.

    >>> normalize_tax_system("УСН Доходы")
    'usn'
    >>> normalize_tax_system("УСН Доходы-Расходы")
    'usn_dr'
    >>> normalize_tax_system("ОСНО")
    'osn'
    """
    if not raw:
        return None
    s = raw.strip().lower().replace("ё", "е")
    if "усн" in s and "расход" in s:
        return "usn_dr"
    if "усн" in s or "ausn" in s:
        return "usn"
    if "осн" in s:
        return "osn"
    # уже код
    if s in ("usn", "usn_dr", "osn"):
        return s
    return None


def calculate_tax(item: dict[str, Any], price: float, commission: float) -> float:
    """Сумма удержания налога. Система и ставка — из Справочника (fallback — организация).
    П.9 (20.08.2026): если налог не задан — 0."""
    tax_system = normalize_tax_system(item.get("tax_system"))
    if not tax_system or not item.get("tax_rate"):
        return 0
    if tax_system == "usn":
        return round(price * item["tax_rate"] / 100, 2)
    if tax_system == "usn_dr":
        income = price - commission - item["cost_price"]
        return round(max(income, 0) * item["tax_rate"] / 100, 2)
    if tax_system == "osn":
        vat = round(price * item["vat_rate"] / 100, 2)
        input_vat = round(
            item["purchase_cost"] / 120 * item["vat_rate"]
            if item["purchase_cost"]
            else 0,
            2,
        )
        return round(vat - input_vat, 2)
    return 0


def tax_header_label(item: dict[str, Any]) -> str:
    """Подпись вида налога/ставки для шапки колонки (п.9-а, 20.08.2026)."""
    tax_system = normalize_tax_system(item.get("tax_system"))
    rate = item.get("tax_rate")
    if tax_system == "usn":
        return f"УСН Д, {rate}%" if rate else "УСН Д"
    if tax_system == "usn_dr":
        return f"УСН Д-Р, {rate}%" if rate else "УСН Д-Р"
    if tax_system == "osn":
        vat = item.get("vat_rate")
        return f"ОСНО, НДС {vat}%" if vat else "ОСНО"
    return "Налог не задан"


def _wb_deductions_bundle(item: dict[str, Any], price: float, commission: float,
                            acquiring: float, ad_rub: float, other_deductions: float,
                            club_discount_rub: float, cashback_rub: float) -> None:
    """Новые формулы UE (ТЗ 31.08.2026). Пишет в item:
    total_wb_deductions / expenses / profit / margin_pct / roi_pct / to_account.
    """
    total_wb_deductions = round(
        commission
        + item["logistics_with_buyout"]
        + item["storage_tariff"]
        + acquiring
        + ad_rub
        + other_deductions
        + club_discount_rub
        + cashback_rub,
        2,
    )
    expenses = round(total_wb_deductions - item["cost_price"] - item["tax_total"], 2)
    profit = round(price - expenses, 2)
    item["total_wb_deductions"] = total_wb_deductions
    item["expenses"] = expenses
    item["profit"] = profit
    item["margin_pct"] = round(profit / price * 100, 2) if price else 0
    item["roi_pct"] = (
        round(profit / item["cost_price"] * 100, 2) if item["cost_price"] else 0
    )
    item["to_account"] = round(price - total_wb_deductions, 2)


def apply_financial_formulas(item: dict[str, Any]) -> dict[str, Any]:
    mp_total_pct = item["mp_base_pct"] + item["mp_correction_pct"]
    item["mp_total_pct"] = mp_total_pct
    item["ad_plan_rub"] = round(
        item["price_before_spp"] * item["ad_plan_pct"] / 100,
        2,
    )
    item["ad_fact_rub"] = round(
        item["price_with_spp"] * item["ad_fact_pct"] / 100,
        2,
    )

    # П.3 (20.08.2026): комиссия — от цены ДО СПП (деление на 100 — перевод % в долю, обязательно)
    mp_commission = round(item["price_before_spp"] * mp_total_pct / 100, 2)
    item["mp_commission"] = mp_commission
    # П.8 (20.08.2026): эквайринг — цена ДО СПП * 1.5%
    acquiring = round(item["price_before_spp"] * (item.get("acquiring_pct") or 1.5) / 100, 2)
    item["acquiring_rub"] = acquiring
    # П.9: налог от цены с СПП, вид/ставка из Справочника
    tax = calculate_tax(item, item["price_with_spp"], mp_commission)
    item["tax_total"] = tax
    item["tax_label"] = tax_header_label(item)
    # П.10/11 (20.08.2026): прочие удержания = цена до СПП * % из Справочника
    item["other_deductions_finreport"] = round(
        item["price_before_spp"] * float(item.get("other_deductions_pct") or 0) / 100, 2
    )

    reverse_logistics = item["reverse_logistics"] or 0
    buyout_pct = (
        item["buyout_fact_pct"]
        if item["buyout_fact_pct"] and item["buyout_fact_pct"] > 0
        else item["buyout_niche_pct"]
    )
    buyout_ratio = float(buyout_pct) / 100 if buyout_pct else 1
    item["logistics_with_buyout"] = round(
        item["delivery_to_client"] + reverse_logistics * (1 - buyout_ratio),
        2,
    )

    # Скидка WB Клуб и кэшбэк лояльности — от цены до СПП (ТЗ 31.08.2026)
    wb_club_discount_rub = round(
        item["price_before_spp"] * float(item.get("wb_club_discount_pct") or 0) / 100, 2
    )
    loyalty_cashback_rub = round(
        item["price_before_spp"] * float(item.get("loyalty_cashback_pct") or 0) / 100, 2
    )
    item["wb_club_discount_rub"] = wb_club_discount_rub
    item["loyalty_cashback_rub"] = loyalty_cashback_rub

    # ── Основной расчёт (факт) — новые формулы ──
    _wb_deductions_bundle(
        item,
        price=item["price_before_spp"],
        commission=mp_commission,
        acquiring=acquiring,
        ad_rub=item["ad_plan_rub"],
        other_deductions=item["other_deductions_finreport"],
        club_discount_rub=wb_club_discount_rub,
        cashback_rub=loyalty_cashback_rub,
    )

    # ── Блок «После правок»: та же схема на цене к изменению ──
    change_price = float(
        item["price_before_spp_change"] or item["price_before_spp"]
    )
    change_commission = round(change_price * mp_total_pct / 100, 2)
    change_acquiring = round(
        change_price * (item.get("acquiring_pct") or 1.5) / 100, 2
    )
    change_ad_rub = round(change_price * item["ad_plan_pct"] / 100, 2)
    change_other_deductions = round(
        change_price * float(item.get("other_deductions_pct") or 0) / 100, 2
    )
    change_club_rub = round(
        change_price * float(item.get("wb_club_discount_pct") or 0) / 100, 2
    )
    change_cashback_rub = round(
        change_price * float(item.get("loyalty_cashback_pct") or 0) / 100, 2
    )
    change_bundle: dict[str, Any] = dict(item)
    _wb_deductions_bundle(
        change_bundle,
        price=change_price,
        commission=change_commission,
        acquiring=change_acquiring,
        ad_rub=change_ad_rub,
        other_deductions=change_other_deductions,
        club_discount_rub=change_club_rub,
        cashback_rub=change_cashback_rub,
    )
    item["profit_change"] = change_bundle["profit"]
    item["margin_change"] = change_bundle["margin_pct"]
    return item
