import math
from typing import Any


FBO_WAREHOUSE_NAMES = ("Коледино", "Краснодар", "Казань")
WB_TIER_RATES = (
    (0.001, 0.200, 23.0),
    (0.201, 0.400, 26.0),
    (0.401, 0.600, 29.0),
    (0.601, 0.800, 30.0),
    (0.801, 1.000, 32.0),
)
WB_BASE_FIRST_LITER = 46.0
WB_BASE_NEXT_LITER = 14.0


def build_box_tariff_context(rows: list[Any]) -> dict[str, Any]:
    tariffs = {}
    fbo_delivery_sum = 0.0
    fbo_liter_sum = 0.0
    fbo_coef_sum = 0.0
    fbo_count = 0

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
            warehouse_name in FBO_WAREHOUSE_NAMES
            and row[1] is not None
            and row[2] is not None
        ):
            fbo_delivery_sum += float(row[1])
            fbo_liter_sum += float(row[2])
            fbo_coef_sum += float(row[5]) if len(row) > 5 and row[5] else 0
            fbo_count += 1

    return {
        "tariffs": tariffs,
        "fbo_avg_base": round(fbo_delivery_sum / fbo_count, 2) if fbo_count else 0,
        "fbo_avg_liter": round(fbo_liter_sum / fbo_count, 2) if fbo_count else 0,
        "fbo_avg_coef": round(fbo_coef_sum / fbo_count, 2) if fbo_count else 0,
    }


def wb_rate_per_liter(volume_liters: float) -> float:
    for lower, upper, rate in WB_TIER_RATES:
        if lower <= volume_liters <= upper:
            return rate
    return WB_BASE_FIRST_LITER


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

    if fulfillment_model == "fbs" and fbs_warehouse:
        warehouse_tariffs = None
        warehouse_found = None
        for warehouse_name in tariffs:
            if fbs_warehouse in warehouse_name or warehouse_name in fbs_warehouse:
                warehouse_tariffs = tariffs[warehouse_name]
                warehouse_found = warehouse_name
                break

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
                if volume_liters <= 1.0:
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
                    cost = round(base + (volume_ceil - 1) * liter, 2)
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
                cost = round(base + (volume_ceil - 1) * liter, 2)
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
        if volume_liters <= 1.0:
            rate = wb_rate_per_liter(volume_liters)
            cost = round(rate * (context["fbo_avg_coef"] / 100), 2)
            debug = {
                "method": "ФБО-среднее (сетка <=1л)",
                "vol": volume_liters,
                "tier_rate": rate,
                "avg_coef": context["fbo_avg_coef"],
                "kd_coef": tariffs.get("Коледино", {}).get("fbo_coef", 0),
                "kr_coef": tariffs.get("Краснодар", {}).get("fbo_coef", 0),
                "kz_coef": tariffs.get("Казань", {}).get("fbo_coef", 0),
                "formula": (
                    f"{rate} × {context['fbo_avg_coef']}% (среднее 3 складов)"
                ),
                "result": cost,
            }
        else:
            cost = round(
                context["fbo_avg_base"]
                + (volume_ceil - 1) * context["fbo_avg_liter"],
                2,
            )
            debug = {
                "method": "ФБО-среднее (>1л)",
                "vol_ceil": volume_ceil,
                "avg_base": context["fbo_avg_base"],
                "avg_liter": context["fbo_avg_liter"],
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
    tax_system = normalize_tax_system(item.get("tax_system"))
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

    mp_commission = round(item["price_with_spp"] * mp_total_pct / 100, 2)
    item["mp_commission"] = mp_commission
    acquiring = round(item["price_with_spp"] * 0.015, 2)
    tax = calculate_tax(item, item["price_with_spp"], mp_commission)
    item["tax_total"] = tax

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

    expenses_fact = (
        item["cost_price"]
        + item["logistics_cost"]
        + item["packaging_cost"]
        + item["other_costs"]
        + mp_commission
        + item["logistics_actual"]
        + item["storage_actual"]
        + item["acceptance_avg"]
        + acquiring
        + tax
        + item["ad_fact_rub"]
        + reverse_logistics
    )
    profit_fact = round(item["price_with_spp"] - expenses_fact, 2)
    item["expenses_fact"] = round(expenses_fact, 2)
    item["profit_fact"] = profit_fact
    item["margin_fact"] = (
        round(profit_fact / item["price_with_spp"] * 100, 2)
        if item["price_with_spp"]
        else 0
    )
    item["roi_fact"] = (
        round(profit_fact / item["cost_price"] * 100, 2)
        if item["cost_price"]
        else 0
    )
    item["to_account_fact"] = round(
        item["price_with_spp"] - mp_commission - tax,
        2,
    )

    plan_price = float(item["price_before_spp_plan"] or item["price_before_spp"])
    plan_price_spp = (
        round(plan_price * (1 - item["spp_pct"] / 100), 2)
        if item["spp_pct"]
        else plan_price
    )
    plan_commission = round(plan_price_spp * mp_total_pct / 100, 2)
    plan_tax = calculate_tax(item, plan_price_spp, plan_commission)
    expenses_plan = (
        item["cost_price"]
        + item["logistics_cost"]
        + item["packaging_cost"]
        + item["other_costs"]
        + plan_commission
        + item["logistics_actual"]
        + item["storage_actual"]
        + item["acceptance_avg"]
        + round(plan_price_spp * 0.015, 2)
        + plan_tax
        + round(plan_price_spp * item["ad_plan_pct"] / 100, 2)
        + reverse_logistics
    )
    profit_plan = round(plan_price_spp - expenses_plan, 2)
    item["plan_price_spp"] = plan_price_spp
    item["expenses_plan"] = round(expenses_plan, 2)
    item["profit_plan"] = profit_plan
    item["margin_plan"] = (
        round(profit_plan / plan_price_spp * 100, 2) if plan_price_spp else 0
    )
    item["roi_plan"] = (
        round(profit_plan / item["cost_price"] * 100, 2)
        if item["cost_price"]
        else 0
    )
    item["to_account_plan"] = round(
        plan_price_spp - plan_commission - plan_tax,
        2,
    )

    change_price = float(
        item["price_before_spp_change"] or item["price_before_spp"]
    )
    change_price_spp = (
        round(change_price * (1 - item["spp_pct"] / 100), 2)
        if item["spp_pct"]
        else change_price
    )
    change_commission = round(change_price_spp * mp_total_pct / 100, 2)
    change_tax = calculate_tax(item, change_price_spp, change_commission)
    expenses_change = (
        item["cost_price"]
        + item["logistics_cost"]
        + item["packaging_cost"]
        + item["other_costs"]
        + change_commission
        + item["logistics_actual"]
        + item["storage_actual"]
        + item["acceptance_avg"]
        + round(change_price_spp * 0.015, 2)
        + change_tax
        + item["ad_fact_rub"]
        + reverse_logistics
    )
    profit_change = round(change_price_spp - expenses_change, 2)
    item["profit_change"] = profit_change
    item["margin_change"] = (
        round(profit_change / change_price_spp * 100, 2)
        if change_price_spp
        else 0
    )
    item["roi_change"] = (
        round(profit_change / item["cost_price"] * 100, 2)
        if item["cost_price"]
        else 0
    )
    return item
