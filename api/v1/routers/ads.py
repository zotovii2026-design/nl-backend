"""
Рекламная статистика WB — маршруты вынесены из api/v1/nl.py
Контракты (URL, параметры, JSON) сохранены без изменений.
"""
import decimal as _dec
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.tenant_auth import require_query_organization_access


router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)


def _sf(v):
    """Безопасное преобразование в float."""
    if v is None:
        return 0
    return float(v) if not isinstance(v, _dec.Decimal) else float(v)


def _parse_date_range(days: str, date_from: Optional[str], date_to: Optional[str]):
    """Парсинг диапазона дат — общий для обоих маршрутов."""
    if date_from and date_to:
        d_from = date_from
        d_to = date_to
    else:
        try:
            days_int = int(days)
        except Exception:
            days_int = 7
        if days_int == 1:
            d_from = date.today().isoformat()
            d_to = date.today().isoformat()
        elif days_int == 2:
            d = date.today() - timedelta(days=1)
            d_from = d.isoformat()
            d_to = d.isoformat()
        else:
            d_from = (date.today() - timedelta(days=days_int)).isoformat()
            d_to = date.today().isoformat()
    return (
        datetime.strptime(d_from, "%Y-%m-%d").date(),
        datetime.strptime(d_to, "%Y-%m-%d").date(),
    )


@router.get("/api/v1/nl/ad-stats")
async def get_ad_stats(
    org_id: str,
    days: str = "7",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Рекламная статистика — из ad_stats_nm (те же данные что по артикулам)."""
    d_from, d_to = _parse_date_range(days, date_from, date_to)
    params = {"org": org_id, "d_from": d_from, "d_to": d_to}

    # ═══ Статистика по дням (из ad_stats_nm) ═══
    daily_rows = await db.execute(text("""
        SELECT sn.stat_date,
               SUM(sn.views) as views,
               SUM(sn.clicks) as clicks,
               SUM(sn.spent) as spent,
               SUM(sn.orders) as orders,
               SUM(sn.atbs) as atbs
        FROM ad_stats_nm sn
        WHERE sn.organization_id = :org
            AND sn.stat_date >= :d_from AND sn.stat_date <= :d_to
            AND sn.spent > 0
        GROUP BY sn.stat_date
        ORDER BY sn.stat_date DESC
    """), params)

    # ═══ ДРР по дням: sum_price из ad_stats_nm по составу РК ═══
    sum_price_by_day = await db.execute(text("""
        SELECT sn.stat_date, COALESCE(SUM(sn.sum_price), 0) as sum_price
        FROM ad_stats_nm sn
        WHERE sn.organization_id = :org
            AND sn.stat_date >= :d_from AND sn.stat_date <= :d_to
            AND sn.spent > 0
        GROUP BY sn.stat_date
    """), params)
    sp_by_date = {}
    for r in sum_price_by_day:
        sp_by_date[str(r[0])] = round(_sf(r[1]), 2)

    daily = []
    for r in daily_rows:
        views = int(r[1] or 0)
        clicks = int(r[2] or 0)
        spent = round(_sf(r[3]), 2)
        orders = int(r[4] or 0)
        atbs = int(r[5] or 0)
        date_str = str(r[0])
        sum_price_day = sp_by_date.get(date_str, 0)
        drr_day = round(spent / sum_price_day * 100, 1) if sum_price_day else 0
        daily.append({
            "date": date_str,
            "views": views,
            "clicks": clicks,
            "spent": spent,
            "ctr": round(clicks / views * 100, 2) if views else 0,
            "cpc": round(spent / clicks, 2) if clicks else 0,
            "orders": orders,
            "atbs": atbs,
            "cr": round(orders / clicks * 100, 2) if clicks else 0,
            "sum_price": sum_price_day,
            "drr": drr_day,
        })

    # ═══ Список кампаний (из ad_stats_nm, агрегировано по РК) ═══
    camp_rows = await db.execute(text("""
        SELECT sn.wb_campaign_id, c.name, c.status, c.type,
               SUM(sn.views) as views,
               SUM(sn.clicks) as clicks,
               SUM(sn.spent) as spent,
               SUM(sn.orders) as orders,
               SUM(sn.atbs) as atbs,
               (SELECT COUNT(DISTINCT sn2.nm_id) FROM ad_stats_nm sn2
                    WHERE sn2.organization_id = :org
                    AND sn2.wb_campaign_id = sn.wb_campaign_id
                    AND sn2.spent > 0
                    AND sn2.stat_date >= :d_from AND sn2.stat_date <= :d_to
               ) as nm_count
        FROM ad_stats_nm sn
        JOIN ad_campaigns c ON c.wb_campaign_id = sn.wb_campaign_id
            AND c.organization_id = sn.organization_id
        WHERE sn.organization_id = :org
            AND sn.stat_date >= :d_from AND sn.stat_date <= :d_to
            AND sn.spent > 0
        GROUP BY sn.wb_campaign_id, c.name, c.status, c.type
        ORDER BY SUM(sn.spent) DESC
    """), params)

    campaigns = []
    for r in camp_rows:
        views = int(r[4] or 0)
        clicks = int(r[5] or 0)
        spent = round(_sf(r[6]), 2)
        orders = int(r[7] or 0)
        atbs = int(r[8] or 0)
        nm_count = int(r[9] or 0)

        # Состав РК — nm_id из ad_stats_nm
        nm_ids_row = await db.execute(text("""
            SELECT DISTINCT nm_id FROM ad_stats_nm
            WHERE organization_id = :org AND wb_campaign_id = :cid
                AND stat_date >= :d_from AND stat_date <= :d_to AND spent > 0
            ORDER BY nm_id
        """), {**params, "cid": r[0]})
        nm_ids = [int(n[0]) for n in nm_ids_row]

        # Инфо о товарах
        products = []
        if nm_ids:
            prod_row = await db.execute(text("""
                SELECT raw_response FROM raw_api_data
                WHERE api_method = 'products' AND organization_id = :org
                ORDER BY fetched_at DESC LIMIT 1
            """), {"org": org_id})
            pr = prod_row.first()
            if pr and pr[0]:
                cards_data = pr[0] if isinstance(pr[0], list) else (pr[0].get("cards", []) if isinstance(pr[0], dict) else [])
                nm_set = set(nm_ids)
                for cd in cards_data:
                    if not isinstance(cd, dict):
                        continue
                    nm = cd.get("nmID")
                    if nm and int(nm) in nm_set:
                        photos = cd.get("photos") or []
                        photo_url = ""
                        if photos:
                            photo_url = photos[0].get("c246x328", "") or photos[0].get("big", "") or photos[0].get("hq", "")
                        products.append({
                            "nm_id": int(nm),
                            "vendor_code": cd.get("vendorCode", ""),
                            "name": cd.get("title", ""),
                            "brand": cd.get("brand", ""),
                            "photo": photo_url,
                        })

        # Сумма заказов для ДРР из ad_stats_nm (только состав РК)
        sum_price_row = await db.execute(text("""
            SELECT COALESCE(SUM(sn.sum_price), 0) as sum_price
            FROM ad_stats_nm sn
            WHERE sn.organization_id = :org AND sn.wb_campaign_id = :cid
                AND sn.stat_date >= :d_from AND sn.stat_date <= :d_to
                AND sn.spent > 0
        """), {**params, "cid": r[0]})
        sum_price_val = round(_sf(sum_price_row.scalar()), 2)

        # Общие заказы и выручка по товарам этой РК из tech_status
        total_orders_rk = 0
        total_revenue_rk = 0
        if nm_ids:
            rk_totals_row = await db.execute(text("""
                SELECT COALESCE(SUM(ts.orders_count), 0),
                       COALESCE(SUM(ts.orders_count * ts.price_discount), 0)
                FROM tech_status ts
                WHERE ts.organization_id = :org
                    AND ts.target_date >= :d_from AND ts.target_date <= :d_to
                    AND ts.nm_id = ANY(:nm_ids)
            """), {**params, "nm_ids": nm_ids})
            rk_totals = rk_totals_row.first()
            if rk_totals:
                total_orders_rk = int(rk_totals[0] or 0)
                total_revenue_rk = round(_sf(rk_totals[1]), 2)

        drr_rk = round(spent / sum_price_val * 100, 1) if sum_price_val else 0

        campaigns.append({
            "campaign_id": r[0],
            "name": r[1] or "Без названия",
            "status": str(r[2]) if r[2] else "",
            "type": str(r[3]) if r[3] else "",
            "views": views,
            "clicks": clicks,
            "spent": spent,
            "ctr": round(clicks / views * 100, 2) if views else 0,
            "cpc": round(spent / clicks, 2) if clicks else 0,
            "orders": orders,
            "atbs": atbs,
            "nm_count": nm_count,
            "products": products,
            "sum_price": sum_price_val,
            "cr": round(orders / clicks * 100, 2) if clicks else 0,
            "total_orders": total_orders_rk,
            "total_revenue": total_revenue_rk,
            "drr": drr_rk,
        })

    # ═══ Баланс ═══
    balance = None
    bal_row = await db.execute(text("""
        SELECT raw_response FROM raw_api_data
        WHERE api_method = 'ad_balance' AND status = 'ok' AND organization_id = :org
        ORDER BY fetched_at DESC LIMIT 1
    """), {"org": org_id})
    br = bal_row.first()
    if br and br[0]:
        balance = br[0]

    # ═══ Итого ═══
    totals = {"views": 0, "clicks": 0, "spent": 0, "orders": 0, "atbs": 0}
    for d in daily:
        for k in totals:
            totals[k] += d.get(k, 0)
    totals["ctr"] = round(totals["clicks"] / totals["views"] * 100, 2) if totals["views"] else 0
    totals["cpc"] = round(totals["spent"] / totals["clicks"], 2) if totals["clicks"] else 0
    totals["cr"] = round(totals["orders"] / totals["clicks"] * 100, 2) if totals["clicks"] else 0
    all_sum_price = sum(d.get("sum_price", 0) for d in daily)
    totals["drr"] = round(totals["spent"] / all_sum_price * 100, 1) if all_sum_price else 0
    totals["sum_price"] = round(all_sum_price, 2)

    return {
        "daily": daily,
        "campaigns": campaigns,
        "top_campaigns": campaigns[:20],
        "totals": totals,
        "balance": balance,
    }


@router.get("/api/v1/nl/ad-stats/by-art")
async def get_ad_stats_by_art(
    org_id: str,
    days: str = "30",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    statuses: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Рекламная статистика по артикулам — данные из ad_stats_nm (разбивка WB по nm_id)."""
    d_from, d_to = _parse_date_range(days, date_from, date_to)
    params = {"org": org_id, "d_from": d_from, "d_to": d_to}

    # Parse status filter — только реальные статусы WB: 7, 9, 11
    status_list = []
    if statuses:
        status_list = [s.strip() for s in statuses.split(",") if s.strip() and s.strip() in ("7", "9", "11")]

    status_cond = ""
    if status_list:
        status_cond = "AND c.status = ANY(:statuses)"
        params["statuses"] = status_list

    # ═══ Основной запрос: агрегация по nm_id из ad_stats_nm ═══
    rows = await db.execute(text("""
        SELECT
            sn.nm_id,
            SUM(sn.spent) as total_spent,
            SUM(sn.views) as total_views,
            SUM(sn.clicks) as total_clicks,
            SUM(sn.orders) as total_orders,
            SUM(sn.atbs) as total_atbs
        FROM ad_stats_nm sn
        JOIN ad_campaigns c ON c.wb_campaign_id = sn.wb_campaign_id
            AND c.organization_id = sn.organization_id
        WHERE sn.organization_id = :org
            AND sn.stat_date >= :d_from AND sn.stat_date <= :d_to
            """ + status_cond + """
        GROUP BY sn.nm_id
        HAVING SUM(sn.spent) > 0
        ORDER BY SUM(sn.spent) DESC
    """), params)

    art_data = {}
    for r in rows:
        nm_id = int(r[0])
        art_data[nm_id] = {
            "spent": round(_sf(r[1]), 2),
            "views": int(r[2] or 0),
            "clicks": int(r[3] or 0),
            "orders": int(r[4] or 0),
            "atbs": int(r[5] or 0),
        }

    all_nm_ids = list(art_data.keys())

    # ═══ Для каждого артикула — список РК с данными по этому nm_id ═══
    nm_campaigns = {}
    if all_nm_ids:
        camp_rows = await db.execute(text("""
            SELECT
                sn.nm_id,
                sn.wb_campaign_id,
                c.name,
                c.status,
                c.type,
                SUM(sn.spent) as camp_spent,
                SUM(sn.views) as camp_views,
                SUM(sn.clicks) as camp_clicks,
                AVG(sn.ctr) as camp_ctr,
                SUM(sn.orders) as camp_orders,
                SUM(sn.atbs) as camp_atbs,
                SUM(sn.sum_price) as camp_sum_price
            FROM ad_stats_nm sn
            JOIN ad_campaigns c ON c.wb_campaign_id = sn.wb_campaign_id
                AND c.organization_id = sn.organization_id
            WHERE sn.organization_id = :org
                AND sn.stat_date >= :d_from AND sn.stat_date <= :d_to
                AND sn.nm_id = ANY(:nm_ids)
                """ + status_cond + """
            GROUP BY sn.nm_id, sn.wb_campaign_id, c.name, c.status, c.type
            HAVING SUM(sn.spent) > 0
            ORDER BY SUM(sn.spent) DESC
        """), {**params, "nm_ids": all_nm_ids})

        for r in camp_rows:
            nm_id = int(r[0])
            if nm_id not in nm_campaigns:
                nm_campaigns[nm_id] = []
            nm_campaigns[nm_id].append({
                "campaign_id": int(r[1]),
                "name": r[2] or "Без названия",
                "status": str(r[3]) if r[3] else "",
                "type": str(r[4]) if r[4] else "",
                "spent_share": round(_sf(r[5]), 2),
                "views": int(r[6] or 0),
                "clicks": int(r[7] or 0),
                "ctr": round(_sf(r[8]), 2),
                "orders": int(r[9] or 0),
                "atbs": int(r[10] or 0),
                "sum_price": round(_sf(r[11]), 2),
            })

    # ═══ Общие заказы и цена по nm_id из tech_status (для ДРР) ═══
    nm_orders_price = {}
    if all_nm_ids:
        ts_rows = await db.execute(text("""
            SELECT ts.nm_id,
                   SUM(ts.orders_count) as total_orders,
                   SUM(ts.orders_count * ts.price_discount) as total_revenue
            FROM tech_status ts
            WHERE ts.organization_id = :org
                AND ts.target_date >= :d_from AND ts.target_date <= :d_to
                AND ts.nm_id = ANY(:nm_ids)
            GROUP BY ts.nm_id
        """), {**params, "nm_ids": all_nm_ids})
        for r in ts_rows:
            nm_orders_price[int(r[0])] = {
                "total_orders": int(r[1] or 0),
                "total_revenue": round(_sf(r[2]), 2),
            }

    # ═══ Собираем items ═══
    items = []
    for nm_id in all_nm_ids:
        d = art_data[nm_id]
        spent = d["spent"]
        views = d["views"]
        clicks = d["clicks"]
        orders = d["orders"]
        campaigns = nm_campaigns.get(nm_id, [])
        sum_price_art = sum(c.get("sum_price", 0) for c in campaigns) if campaigns else 0
        op = nm_orders_price.get(nm_id, {"total_orders": 0, "total_revenue": 0})
        drr_art = round(spent / sum_price_art * 100, 1) if sum_price_art else 0
        items.append({
            "nm_id": nm_id,
            "spent": spent,
            "views": views,
            "clicks": clicks,
            "ctr": round(clicks / views * 100, 2) if views else 0,
            "cpc": round(spent / clicks, 2) if clicks else 0,
            "orders": orders,
            "cr": round(orders / clicks * 100, 2) if clicks else 0,
            "campaigns_count": len(campaigns),
            "campaigns": campaigns,
            "total_orders": op["total_orders"],
            "total_revenue": op["total_revenue"],
            "drr": drr_art,
        })

    # ═══ Информация о товарах (название, фото, vendor_code) ═══
    nm_to_info = {}
    if all_nm_ids:
        prod_row = await db.execute(text("""
            SELECT raw_response FROM raw_api_data
            WHERE api_method = 'products' AND organization_id = :org
            ORDER BY fetched_at DESC LIMIT 1
        """), {"org": org_id})
        pr = prod_row.first()
        if pr and pr[0]:
            cards_data = pr[0] if isinstance(pr[0], list) else (pr[0].get("cards", []) if isinstance(pr[0], dict) else [])
            nm_set = set(all_nm_ids)
            for c in cards_data:
                if not isinstance(c, dict):
                    continue
                nm = c.get("nmID")
                if nm and int(nm) in nm_set:
                    photos = c.get("photos") or []
                    photo_url = ""
                    if photos:
                        photo_url = photos[0].get("c246x328", "") or photos[0].get("big", "") or photos[0].get("hq", "")
                    nm_to_info[int(nm)] = {
                        "name": c.get("title", ""),
                        "brand": c.get("brand", ""),
                        "vendor_code": c.get("vendorCode", ""),
                        "photo": photo_url,
                    }

    for item in items:
        info = nm_to_info.get(item["nm_id"], {})
        item["name"] = info.get("name", "")
        item["brand"] = info.get("brand", "")
        item["vendor_code"] = info.get("vendor_code", "")
        item["photo"] = info.get("photo", "")

    totals = {
        "spent": round(sum(i["spent"] for i in items), 2),
        "views": sum(i["views"] for i in items),
        "clicks": sum(i["clicks"] for i in items),
        "orders": sum(i["orders"] for i in items),
        "ctr": round(sum(i["clicks"] for i in items) / max(sum(i["views"] for i in items), 1) * 100, 2),
        "cpc": round(sum(i["spent"] for i in items) / max(sum(i["clicks"] for i in items), 1), 2),
        "cr": round(sum(i["orders"] for i in items) / max(sum(i["clicks"] for i in items), 1) * 100, 2),
        "items_count": len(items),
        "campaigns_count": sum(i["campaigns_count"] for i in items),
        "total_orders": sum(i["total_orders"] for i in items),
        "total_revenue": round(sum(i["total_revenue"] for i in items), 2),
        "drr": round(sum(i["spent"] for i in items) / max(sum(c.get("sum_price", 0) for i in items for c in i.get("campaigns", [])), 1) * 100, 1),
    }

    return {"items": items, "totals": totals}
