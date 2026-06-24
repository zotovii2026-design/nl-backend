"""Роутер РНП — Раздел Нормативных Показателей.

Восстановлен из api/v1/nl.py (коммит e6e9994), потерян при рефакторинге.

Эндпоинт:
- GET /api/v1/nl/rnp — данные по карточкам с разбивкой по дням
"""
import calendar
from collections import defaultdict
from datetime import datetime as dt_mod, date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text

from core.database import get_db
from models.product_entity import ProductEntity

router = APIRouter(tags=["nl"])


# ============================================================================
# GET /api/v1/nl/rnp — Раздел Нормативных Показателей
# ============================================================================
@router.get("/api/v1/nl/rnp")
async def get_rnp(
    org_id: str,
    month: Optional[str] = None,          # YYYY-MM (например 2026-05)
    days: Optional[int] = None,           # Количество дней назад (30, 60, 90)
    date_from: Optional[str] = None,      # ISO дата начала (от единого datepicker)
    date_to: Optional[str] = None,        # ISO дата конца (от единого datepicker)
    sort_by: Optional[str] = "orders_revenue",  # orders_revenue, roi, buyout_pct
    filter_status: Optional[str] = None,
    search: Optional[str] = None,
    use_buyout_pct: bool = False,         # чекбокс "Учесть % выкупа"
    db: AsyncSession = Depends(get_db),
):
    """
    РНП — Раздел Нормативных Показателей.
    Данные за период: каждая карточка = строка, дни = столбцы.
    Поддерживает: month (YYYY-MM), days (N), date_from+date_to (ISO).
    """
    # Период: приоритет date_from/date_to > month > days
    today = date.today()
    if date_from and date_to:
        try:
            first_day = dt_mod.fromisoformat(date_from).date()
            last_day = dt_mod.fromisoformat(date_to).date()
            days_in_month = (last_day - first_day).days + 1
        except ValueError:
            first_day = today - timedelta(days=89)
            last_day = today
            days_in_month = 90
    elif month:
        year, mon = month.split("-")
        year, mon = int(year), int(mon)
        first_day = date(year, mon, 1)
        last_day = date(year, mon, calendar.monthrange(year, mon)[1])
        days_in_month = calendar.monthrange(year, mon)[1]
    else:
        num_days = days if days else 90
        last_day = today
        first_day = today - timedelta(days=num_days - 1)
        days_in_month = num_days

    # 1. Список дней (по убыванию)
    day_list = []
    d = min(last_day, today)
    while d >= first_day:
        day_list.append(d)
        d -= timedelta(days=1)

    # 2. Получаем entity_id -> размер
    ent_result = await db.execute(
        select(ProductEntity.id, ProductEntity.size_name, ProductEntity.nm_id, ProductEntity.subject_name)
        .where(ProductEntity.organization_id == org_id)
    )
    _ent_rows = ent_result.all()
    size_map = {str(r[0]): r[1] for r in _ent_rows}

    # 3. Справочник (reference_book) — последние записи по entity
    ref_result = await db.execute(text(
        "SELECT DISTINCT ON (entity_id) entity_id, nm_id, cost_price, purchase_cost, "
        "packaging_cost, logistics_cost, other_costs, extra_costs, vat, "
        "mp_base_pct, mp_correction_pct, tax_system, tax_rate, vat_rate, "
        "product_class, brand, product_status, subject_id, subject_name, "
        "in_promo, ad_shows_organic, ad_shows_paid, ad_strategy, tags, rating_reviews, localization_pct "
        "FROM reference_book "
        "WHERE organization_id = :org AND entity_id IS NOT NULL "
        "AND (valid_to IS NULL OR valid_to >= :fd) "
        "ORDER BY entity_id, valid_from DESC"
    ), {"org": org_id, "fd": first_day})
    ref_map = {}       # entity_id -> dict
    ref_map_nm = {}    # nm_id -> dict (fallback)
    for r in ref_result.all():
        d_item = {
            "cost_price": float(r[2]) if r[2] else 0,
            "purchase_cost": float(r[3]) if r[3] else 0,
            "packaging_cost": float(r[4]) if r[4] else 0,
            "logistics_cost": float(r[5]) if r[5] else 0,
            "other_costs": float(r[6]) if r[6] else 0,
            "extra_costs": float(r[7]) if r[7] else 0,
            "vat": float(r[8]) if r[8] else 0,
            "mp_base_pct": float(r[9]) if r[9] else 0,
            "mp_correction_pct": float(r[10]) if r[10] else 0,
            "tax_system": r[11] or "",
            "tax_rate": float(r[12]) if r[12] else 0,
            "vat_rate": float(r[13]) if r[13] else 0,
            "product_class": r[14] or "",
            "brand": r[15] or "",
            "product_status": r[16] or "",
            "subject_id": r[17],
            "subject_name": r[18] or "",
            "in_promo": bool(r[19]) if r[19] is not None else False,
            "ad_shows_organic": int(r[20]) if r[20] else None,
            "ad_shows_paid": int(r[21]) if r[21] else None,
            "ad_strategy": r[22] or "",
            "tags": r[23] or "",
            "rating_reviews": float(r[24]) if r[24] else None,
            "localization_pct": r[25] or "",
        }
        if r[0]:
            ref_map[str(r[0])] = d_item
        if r[1]:
            ref_map_nm[r[1]] = d_item

    def get_ref(eid, nm):
        return ref_map_nm.get(nm, ref_map.get(eid, {
            "cost_price": 0, "purchase_cost": 0, "packaging_cost": 0,
            "logistics_cost": 0, "other_costs": 0, "extra_costs": 0, "vat": 0,
            "mp_base_pct": 0, "mp_correction_pct": 0, "tax_system": "",
            "tax_rate": 0, "vat_rate": 0, "product_class": "", "brand": "",
            "product_status": "", "in_promo": False, "ad_shows_organic": None,
            "ad_shows_paid": None, "ad_strategy": "", "tags": "",
            "rating_reviews": None, "localization_pct": "",
        }))

    # 4. WB тарифы — последние по nm_id
    snap_result = await db.execute(text(
        "SELECT DISTINCT ON (nm_id) nm_id, logistics_tariff, storage_tariff, "
        "commission_pct, buyout_pct_fact, price_retail, price_with_spp, spp_pct "
        "FROM wb_tariff_snapshot "
        "WHERE organization_id = :org "
        "ORDER BY nm_id, target_date DESC"
    ), {"org": org_id})
    snap_map = {}
    for r in snap_result.all():
        snap_map[r[0]] = {
            "logistics_tariff": float(r[1]) if r[1] else 0,
            "storage_tariff": float(r[2]) if r[2] else 0,
            "commission_pct": float(r[3]) if r[3] else 0,
            "buyout_pct_fact": float(r[4]) if r[4] else 0,
            "price_retail": float(r[5]) if r[5] else 0,
            "price_with_spp": float(r[6]) if r[6] else 0,
            "spp_pct": float(r[7]) if r[7] else 0,
        }

    def get_snap(nm):
        return snap_map.get(nm, {
            "logistics_tariff": 0, "storage_tariff": 0,
            "commission_pct": 0, "buyout_pct_fact": 0,
            "price_retail": 0, "price_with_spp": 0, "spp_pct": 0,
        })

    # 5. План продаж
    plan_result = await db.execute(text(
        "SELECT entity_id, plan_type, plan_value "
        "FROM sales_plans "
        "WHERE organization_id = :org AND period = :period"
    ), {"org": org_id, "period": first_day})
    plan_map = {}  # entity_id -> {quantity: X, revenue: Y}
    for r in plan_result.all():
        eid = str(r[0]) if r[0] else None
        if not eid:
            continue
        if eid not in plan_map:
            plan_map[eid] = {"quantity": 0, "revenue": 0}
        ptype = str(r[1]) if r[1] else "quantity"
        plan_map[eid][ptype] = float(r[2]) if r[2] else 0

    # 6. tech_status за весь период — агрегация по entity_id + target_date
    ts_result = await db.execute(text(
        "SELECT entity_id, target_date, nm_id, vendor_code, product_name, photo_main, barcode, "
        "orders_count, buyouts_count, returns_count, stock_qty, "
        "price, price_discount, price_spp, ad_cost, impressions, clicks, tariff "
        "FROM tech_status "
        "WHERE organization_id = :org AND target_date BETWEEN :fd AND :ld "
        "AND entity_id IS NOT NULL "
        "ORDER BY entity_id, target_date DESC"
    ), {"org": org_id, "fd": first_day, "ld": last_day})
    ts_rows = ts_result.all()

    # Группируем по entity_id
    entities_data = defaultdict(lambda: {"days": {}, "last_row": None})
    for r in ts_rows:
        eid = str(r[0])
        tdate = r[1]
        entities_data[eid]["days"][str(tdate)] = {
            "date": str(tdate),
            "orders_count": int(r[7]) if r[7] else 0,
            "buyouts_count": int(r[8]) if r[8] else 0,
            "returns_count": int(r[9]) if r[9] else 0,
            "stock_qty": int(r[10]) if r[10] else 0,
            "price": float(r[11]) if r[11] else 0,
            "price_discount": float(r[12]) if r[12] else 0,
            "price_spp": float(r[13]) if r[13] else 0,
            "ad_cost": float(r[14]) if r[14] else 0,
            "impressions": int(r[15]) if r[15] else 0,
            "clicks": int(r[16]) if r[16] else 0,
            "tariff": float(r[17]) if r[17] else 0,
        }
        # Сохраняем последнюю строку для идентификации
        if entities_data[eid]["last_row"] is None:
            entities_data[eid]["last_row"] = r

    # 7. Собираем результат
    products = []
    for eid, edata in entities_data.items():
        last = edata["last_row"]
        if not last:
            continue
        nm = last[2]
        ref = get_ref(eid, nm)
        snap = get_snap(nm)
        plan = plan_map.get(eid, {"quantity": 0, "revenue": 0})

        # Фильтр поиска
        if search:
            vc = str(last[3] or "")
            pn = str(last[4] or "")
            if not (search.lower() in vc.lower() or search.lower() in pn.lower() or (search.isdigit() and int(search) == nm)):
                continue

        # Фильтр по статусу
        if filter_status and ref["product_status"] != filter_status:
            continue

        # Последний сток
        last_day_key = max(edata["days"].keys()) if edata["days"] else None
        last_day_data = edata["days"].get(last_day_key, {})
        current_stock = last_day_data.get("stock_qty", 0)

        # Сумма за период
        total_orders = sum(dd["orders_count"] for dd in edata["days"].values())
        total_buyouts = sum(dd["buyouts_count"] for dd in edata["days"].values())
        total_ad_cost = sum(dd["ad_cost"] for dd in edata["days"].values())
        total_orders_revenue = sum(dd["orders_count"] * dd["price_discount"] for dd in edata["days"].values())
        total_buyouts_revenue = sum(dd["buyouts_count"] * dd["price_discount"] for dd in edata["days"].values())

        # Себестоимость единицы
        total_cost = ref["cost_price"] + ref["purchase_cost"] + ref["packaging_cost"] + ref["logistics_cost"] + ref["other_costs"] + ref["extra_costs"] + ref["vat"]

        # Комиссия МП
        mp_pct = (ref["mp_base_pct"] or snap["commission_pct"]) + ref["mp_correction_pct"]

        # Маржа до ДРР (на выкуп)
        commission = total_buyouts_revenue * mp_pct / 100 if total_buyouts_revenue else 0
        logistics = snap["logistics_tariff"] * total_buyouts
        margin_before_drr = total_buyouts_revenue - (total_cost * total_buyouts) - commission - logistics

        # Прибыль расчёт
        profit_calc = margin_before_drr - total_ad_cost

        # Маржа с ДРР
        margin_with_drr = profit_calc

        # ДРР
        drr = round(total_ad_cost / total_orders_revenue * 100, 2) if total_orders_revenue else 0

        # КРРР
        krrr = round(margin_with_drr / margin_before_drr * 100, 1) if margin_before_drr else 0

        # Себестоимость остатков
        cost_of_stock = round(total_cost * current_stock, 2)

        # % выкупа
        buyout_pct = snap["buyout_pct_fact"] or 0

        # План / факт / % выполнения
        plan_val = plan.get("revenue", 0) or plan.get("quantity", 0)
        days_passed = len(edata["days"])
        daily_norm = round(plan_val / days_in_month, 2) if days_in_month else 0
        pct_complete = round(total_orders_revenue / (daily_norm * days_passed) * 100, 1) if daily_norm and days_passed else 0

        # «Хватит на» дней
        avg_orders_day = total_orders / days_passed if days_passed else 0
        if use_buyout_pct and buyout_pct > 0:
            effective_demand = avg_orders_day * buyout_pct / 100
        else:
            effective_demand = avg_orders_day
        enough_days = round(current_stock / effective_demand, 1) if effective_demand > 0 else 999

        # ROI
        total_invested = total_cost * (total_orders if not use_buyout_pct else total_buyouts)
        roi = round(total_buyouts_revenue / total_invested * 100 - 100, 1) if total_invested else 0

        # CPL
        total_clicks = sum(dd["clicks"] for dd in edata["days"].values())
        cpl = round(total_clicks / total_orders, 2) if total_orders else 0

        # CTR
        total_impressions = sum(dd["impressions"] for dd in edata["days"].values())
        ctr = round(total_clicks / total_impressions * 100, 2) if total_impressions else 0

        # Дни для столбцов (по убыванию)
        day_columns = []
        for day in day_list:
            dk = str(day)
            dd = edata["days"].get(dk, {
                "date": dk, "orders_count": 0, "buyouts_count": 0,
                "returns_count": 0, "stock_qty": 0, "price": 0,
                "price_discount": 0, "price_spp": 0, "ad_cost": 0,
                "impressions": 0, "clicks": 0, "tariff": 0,
            })
            o_rev = dd["orders_count"] * dd["price_discount"]
            b_rev = dd["buyouts_count"] * dd["price_discount"]
            dd_comm = b_rev * mp_pct / 100 if b_rev else 0
            dd_logist = snap["logistics_tariff"] * dd["buyouts_count"]
            dd_margin_before = b_rev - (total_cost * dd["buyouts_count"]) - dd_comm - dd_logist
            dd_profit = dd_margin_before - dd["ad_cost"]
            dd_margin_with = dd_profit
            dd_drr = round(dd["ad_cost"] / o_rev * 100, 2) if o_rev else 0

            day_columns.append({
                "date": dk,
                "orders_count": dd["orders_count"],
                "orders_revenue": round(o_rev, 2),
                "buyouts_count": dd["buyouts_count"],
                "buyouts_revenue": round(b_rev, 2),
                "ad_cost": round(dd["ad_cost"], 2),
                "drr": dd_drr,
                "margin_before_drr": round(dd_margin_before, 2),
                "profit_calc": round(dd_profit, 2),
                "margin_with_drr": round(dd_margin_with, 2),
            })

        products.append({
            "entity_id": eid,
            "nm_id": nm,
            "vendor_code": last[3] or "",
            "product_name": last[4] or "",
            "photo_main": last[5] or "",
            "barcode": last[6] or "",
            "size_name": size_map.get(eid, ""),
            # Справочник
            "brand": ref["brand"],
            "product_status": ref["product_status"],
            "product_class": ref["product_class"],
            "in_promo": ref["in_promo"],
            "ad_strategy": ref["ad_strategy"],
            "tags": ref["tags"],
            "rating_reviews": ref["rating_reviews"],
            "localization_pct": ref["localization_pct"],
            "ad_shows_organic": ref["ad_shows_organic"],
            "ad_shows_paid": ref["ad_shows_paid"],
            # Цены
            "price_retail": snap["price_retail"],
            "price_with_spp": snap["price_with_spp"],
            "spp_pct": snap["spp_pct"],
            "buyout_pct": buyout_pct,
            # Себестоимость
            "cost_price": ref["cost_price"],
            "cost_of_stock": cost_of_stock,
            # План
            "plan_value": plan_val,
            "daily_norm": daily_norm,
            "pct_complete": pct_complete,
            # Итоги за период
            "total_orders": total_orders,
            "total_orders_revenue": round(total_orders_revenue, 2),
            "total_buyouts": total_buyouts,
            "total_buyouts_revenue": round(total_buyouts_revenue, 2),
            "total_ad_cost": round(total_ad_cost, 2),
            "drr": drr,
            "margin_before_drr": round(margin_before_drr, 2),
            "profit_calc": round(profit_calc, 2),
            "margin_with_drr": round(margin_with_drr, 2),
            "krrr": krrr,
            "roi": roi,
            "cpl": cpl,
            "ctr": ctr,
            "enough_days": enough_days,
            "current_stock": current_stock,
            # Дни
            "days": day_columns,
        })

    # Сортировка
    sort_key_map = {
        "orders_revenue": lambda x: x["total_orders_revenue"],
        "roi": lambda x: x["roi"],
        "buyout_pct": lambda x: x["buyout_pct"],
    }
    sort_fn = sort_key_map.get(sort_by, sort_key_map["orders_revenue"])
    products.sort(key=sort_fn, reverse=True)

    # Сводка по всем карточкам
    summary = {
        "total_orders": sum(p["total_orders"] for p in products),
        "total_orders_revenue": round(sum(p["total_orders_revenue"] for p in products), 2),
        "total_buyouts": sum(p["total_buyouts"] for p in products),
        "total_buyouts_revenue": round(sum(p["total_buyouts_revenue"] for p in products), 2),
        "total_ad_cost": round(sum(p["total_ad_cost"] for p in products), 2),
        "total_drr": round(
            sum(p["total_ad_cost"] for p in products) / sum(p["total_orders_revenue"] for p in products) * 100, 2
        ) if sum(p["total_orders_revenue"] for p in products) else 0,
        "total_margin_before_drr": round(sum(p["margin_before_drr"] for p in products), 2),
        "total_profit_calc": round(sum(p["profit_calc"] for p in products), 2),
        "total_margin_with_drr": round(sum(p["margin_with_drr"] for p in products), 2),
        "total_products": len(products),
        "total_stock": sum(p["current_stock"] for p in products),
    }

    period_label = f"{year}-{mon:02d}" if month else f"{first_day} — {last_day}"
    return {
        "month": period_label,
        "days_in_month": days_in_month,
        "day_list": [str(d) for d in day_list],
        "summary": summary,
        "products": products,
    }
