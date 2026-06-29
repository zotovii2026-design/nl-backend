"""Celery задача: синхронизация рекламной статистики WB

Логика (v3 от 2026-06-07):
1. /adv/v1/promotion/count → список ID + changeTime по группам
2. ФИЛЬТР: оставляем только кампании с changeTime за последние 30 дней
3. /adv/v1/upd → названия за 30 дней
4. Upsert в БД только живых кампаний (групповой статус — др. варианта нет)
5. /adv/v3/fullstats → статистика за N дней
6. /adv/v1/balance → баланс кабинета
"""

import json
import asyncio
import logging
import httpx
from services.wb_api.keys import get_all_wb_keys as _get_all_keys_imported
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, List, Tuple

from celery import shared_task
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.config import settings
from core.security import decrypt_data
from models.organization import WbApiKey
from models.raw_data import RawApiData

logger = logging.getLogger(__name__)

ADVERT_API = "https://advert-api.wildberries.ru"

# Кампании старше этого количества дней — игнорируются (мусор/архив)
FRESH_DAYS = 30


def _make_session():
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False, future=True,
        pool_pre_ping=True, pool_recycle=300,
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _run(coro):
    async def wrapper():
        engine, sf = _make_session()
        try:
            return await coro(sf)
        finally:
            await engine.dispose()
    return asyncio.run(wrapper())


async def _get_all_keys(sf):
    return await _get_all_keys_imported(sf)


def _parse_change_time(raw: str) -> Optional[datetime]:
    """Parse changeTime from WB API — handles various ISO formats."""
    if not raw or not isinstance(raw, str):
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


@shared_task(name="wb.sched.ad_stats")
def sched_ad_stats(days_back: int = 1):
    async def _main():
        engine, sf = _make_session()
        try:
            return await _do_ad_stats(sf, days_back)
        finally:
            await engine.dispose()
    return asyncio.run(_main())


async def _do_ad_stats(sf, days_back=1):
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}

    total_result = {"status": "ok", "orgs": {}, "total_stats": 0, "total_campaigns": 0}

    for idx, (org_id, api_key) in enumerate(all_keys):
        logger.info("[ad_stats] Processing org %s (%d/%d)", org_id, idx + 1, len(all_keys))
        try:
            org_result = await _sync_org_ads(sf, org_id, api_key, days_back)
            total_result["orgs"][org_id] = org_result
            total_result["total_stats"] += org_result.get("stats_saved", 0)
            total_result["total_campaigns"] += org_result.get("campaigns", 0)
        except Exception as e:
            logger.error("[ad_stats] Org %s failed: %s", org_id, e)
            total_result["orgs"][org_id] = {"status": "error", "error": str(e)}

        if idx < len(all_keys) - 1:
            logger.info("[ad_stats] Waiting 5s before next org...")
            await asyncio.sleep(5)

    logger.info("[ad_stats] All done: %d orgs, %d stats, %d campaigns",
                len(all_keys), total_result["total_stats"], total_result["total_campaigns"])
    return total_result


async def _sync_org_ads(sf, org_id: str, api_key: str, days_back: int):
    async with httpx.AsyncClient(
        base_url=ADVERT_API,
        headers={"Authorization": "Bearer " + api_key},
        timeout=30.0,
    ) as client:

        # ═══ ШАГ 1: Список ID кампаний из /adv/v1/promotion/count ═══
        # Статус на уровне группы: 7=active, 9=finished, 11=paused
        # Статус группы ≠ статус конкретной кампании. Реальный статус
        # кампании берём из /adv/v1/upd (advertStatus) на шаге 2.
        resp = await client.get("/adv/v1/promotion/count")
        if resp.status_code in (401, 403):
            logger.warning("[ad_stats] Org %s: no access (%d)", org_id, resp.status_code)
            return {"status": "no_access", "http_code": resp.status_code}
        resp.raise_for_status()
        campaigns_data = resp.json()

        now = datetime.now(ZoneInfo("UTC"))
        cutoff = now - timedelta(days=FRESH_DAYS)

        all_campaigns = []   # все кампании с метаданными группы
        total_ids = 0

        for group in campaigns_data.get("adverts", []):
            g_type = group.get("type")
            g_status = str(group.get("status"))  # групповой статус — запасной
            for ad in group.get("advert_list", []):
                total_ids += 1
                ct = _parse_change_time(ad.get("changeTime", ""))
                all_campaigns.append({
                    "advertId": ad["advertId"],
                    "type": str(g_type),
                    "group_status": g_status,  # запасной, если нет в upd
                    "changeTime": ct,
                })

        logger.info("[ad_stats] Org %s: %d total IDs from promotion/count",
                     org_id, total_ids)

        if not all_campaigns:
            return {"status": "ok", "stats_saved": 0, "campaigns": 0}

        # ═══ ШАГ 2: Названия + РЕАЛЬНЫЙ статус кампаний через /adv/v1/upd ═══
        campaign_meta = {}  # advertId → {name, status, payment_type}
        try:
            today = datetime.now(ZoneInfo("Europe/Moscow")).date()
            resp_names = await client.get(
                "/adv/v1/upd",
                params={
                    "from": (today - timedelta(days=30)).isoformat(),
                    "to": today.isoformat(),
                },
            )
            if resp_names.status_code == 200:
                upd_data = resp_names.json()
                if isinstance(upd_data, list):
                    for item in upd_data:
                        aid = item.get("advertId")
                        if aid:
                            campaign_meta[aid] = {
                                "name": item.get("campName", ""),
                                "status": str(item.get("advertStatus", "")),
                                "payment_type": item.get("paymentType", ""),
                            }
                logger.info("[ad_stats] Org %s: %d items from /upd (names+status)", org_id, len(campaign_meta))
            else:
                logger.warning("[ad_stats] Org %s: /upd status %d", org_id, resp_names.status_code)
        except Exception as e:
            logger.warning("[ad_stats] Org %s: /upd error: %s", org_id, e)

        # ═══ ШАГ 2b: Фильтр — какие кампании сохранять в БД ═══
        # Сохраняем кампанию если:
        #   - её статус есть в upd (любой, включая 7=active), ИЛИ
        #   - changeTime свежая (< FRESH_DAYS), ИЛИ
        #   - group_status=7 (активная группа — подстраховка)
        fresh_campaigns = []
        for camp in all_campaigns:
            aid = camp["advertId"]
            meta = campaign_meta.get(aid)
            real_status = (meta["status"] if meta and meta["status"] else None) or camp["group_status"]
            ct = camp["changeTime"]
            is_fresh = ct is not None and ct >= cutoff
            is_active = real_status == "7"
            has_meta = meta is not None

            if is_active or is_fresh or has_meta:
                camp["status"] = real_status
                camp["name"] = meta["name"] if meta else ""
                camp["payment_type"] = meta["payment_type"] if meta else ""
                fresh_campaigns.append(camp)

        skipped_old = total_ids - len(fresh_campaigns)
        logger.info("[ad_stats] Org %s: %d campaigns to upsert (%d skipped as old/inactive), statuses: %s",
                     org_id, len(fresh_campaigns), skipped_old,
                     {s: sum(1 for c in fresh_campaigns if c["status"] == s) for s in sorted(set(c["status"] for c in fresh_campaigns))})

        if not fresh_campaigns:
            return {"status": "ok", "stats_saved": 0, "campaigns": 0}

        # ═══ ШАГ 3: Upsert в БД — статус кампании из advertStatus ═══
        async with sf() as db:
            for camp in fresh_campaigns:
                aid = camp["advertId"]
                await db.execute(text("""
                    INSERT INTO ad_campaigns (
                        id, organization_id, wb_campaign_id, name, type, status, wb_change_time
                    ) VALUES (
                        gen_random_uuid(), :org, :cid, :name, :ctype, :cstatus, :ctime
                    )
                    ON CONFLICT (organization_id, wb_campaign_id) DO UPDATE SET
                        name = EXCLUDED.name, type = EXCLUDED.type, status = EXCLUDED.status,
                        wb_change_time = EXCLUDED.wb_change_time, updated_at = now()
                """), {
                    "org": org_id, "cid": aid,
                    "name": camp.get("name", ""),
                    "ctype": camp["type"],
                    "cstatus": camp["status"],
                    "ctime": camp["changeTime"],
                })
            await db.commit()
            logger.info("[ad_stats] Org %s: upserted %d campaigns", org_id, len(fresh_campaigns))

        # ═══ ШАГ 4: Статистика /adv/v3/fullstats ═══
        stat_ids = [str(c["advertId"]) for c in fresh_campaigns]
        total_saved = 0

        for day_offset in range(days_back):
            target_date = (datetime.now(ZoneInfo("Europe/Moscow")).date() - timedelta(days=day_offset + 1)).isoformat()

            for batch_start in range(0, len(stat_ids), 50):
                batch = stat_ids[batch_start:batch_start + 50]
                ids_str = ",".join(batch)

                retries = 0
                stats = []
                while retries < 5:
                    try:
                        resp3 = await client.get(
                            "/adv/v3/fullstats",
                            params={"ids": ids_str, "beginDate": target_date, "endDate": target_date},
                        )
                        if resp3.status_code == 429:
                            logger.info("[ad_stats] Org %s rate limited, wait 65s", org_id)
                            await asyncio.sleep(65)
                            retries += 1
                            continue
                        if resp3.status_code == 500:
                            logger.warning("[ad_stats] Org %s WB 500, retry %d/3 in 30s", org_id, retries + 1)
                            if retries < 3:
                                await asyncio.sleep(30)
                                retries += 1
                                continue
                            else:
                                logger.error("[ad_stats] Org %s WB 500 after 3 retries, skipping", org_id)
                                break
                        resp3.raise_for_status()
                        stats = resp3.json()
                        if not isinstance(stats, list):
                            stats = []
                        break
                    except Exception as e:
                        logger.error("[ad_stats] Org %s fullstats error: %s", org_id, e)
                        break

                if not stats:
                    continue

                async with sf() as db:
                    for s in stats:
                        await db.execute(text("""
                            INSERT INTO ad_stats (
                                id, organization_id, wb_campaign_id, stat_date,
                                views, clicks, spent, ctr, cpc, orders, atbs, cr,
                                canceled, shks, sum_price, currency
                            ) VALUES (
                                gen_random_uuid(), :org, :cid, :sdate,
                                :views, :clicks, :spent, :ctr, :cpc, :orders, :atbs, :cr,
                                :canceled, :shks, :sum_price, :currency
                            )
                            ON CONFLICT (organization_id, wb_campaign_id, stat_date) DO UPDATE SET
                                views=EXCLUDED.views, clicks=EXCLUDED.clicks, spent=EXCLUDED.spent,
                                ctr=EXCLUDED.ctr, cpc=EXCLUDED.cpc, orders=EXCLUDED.orders,
                                atbs=EXCLUDED.atbs, cr=EXCLUDED.cr, canceled=EXCLUDED.canceled,
                                shks=EXCLUDED.shks, sum_price=EXCLUDED.sum_price, currency=EXCLUDED.currency
                        """), {
                            "org": org_id, "cid": s.get("advertId"), "sdate": date.fromisoformat(target_date),
                            "views": s.get("views", 0), "clicks": s.get("clicks", 0),
                            "spent": s.get("sum", 0), "ctr": s.get("ctr", 0),
                            "cpc": s.get("cpc", 0), "orders": s.get("orders", 0),
                            "atbs": s.get("atbs", 0), "cr": s.get("cr", 0),
                            "canceled": s.get("canceled", 0), "shks": s.get("shks", 0),
                            "sum_price": s.get("sum_price", 0), "currency": s.get("currency", "RUB"),
                        })
                    # Детализация по nm_id
                    for s in stats:
                        for day in (s.get("days") or []):
                            dt_raw = day.get("date", "")
                            if not dt_raw:
                                continue
                            try:
                                dt_str = dt_raw[:10]
                            except:
                                continue
                            for app in (day.get("apps") or []):
                                app_type = app.get("appType", 1)
                                for nm in (app.get("nms") or []):
                                    nm_id = nm.get("nmId")
                                    if not nm_id:
                                        continue
                                    try:
                                        await db.execute(text("""
                                            INSERT INTO ad_stats_nm (
                                                organization_id, wb_campaign_id, nm_id, stat_date, app_type,
                                                views, clicks, spent, ctr, cpc, orders, atbs, cr,
                                                canceled, shks, sum_price
                                            ) VALUES (
                                                :org, :cid, :nm, :sdate, :apptype,
                                                :views, :clicks, :spent, :ctr, :cpc, :orders, :atbs, :cr,
                                                :canceled, :shks, :sum_price
                                            )
                                            ON CONFLICT (organization_id, wb_campaign_id, nm_id, stat_date, app_type) DO UPDATE SET
                                                views=EXCLUDED.views, clicks=EXCLUDED.clicks, spent=EXCLUDED.spent,
                                                ctr=EXCLUDED.ctr, cpc=EXCLUDED.cpc, orders=EXCLUDED.orders,
                                                atbs=EXCLUDED.atbs, cr=EXCLUDED.cr, canceled=EXCLUDED.canceled,
                                                shks=EXCLUDED.shks, sum_price=EXCLUDED.sum_price
                                        """), {
                                            "org": org_id, "cid": s.get("advertId"), "nm": int(nm_id),
                                            "sdate": date.fromisoformat(dt_str), "apptype": app_type,
                                            "views": int(nm.get("views", 0)), "clicks": int(nm.get("clicks", 0)),
                                            "spent": float(nm.get("sum", 0)), "ctr": float(nm.get("ctr", 0)),
                                            "cpc": float(nm.get("cpc", 0)), "orders": int(nm.get("orders", 0)),
                                            "atbs": int(nm.get("atbs", 0)), "cr": float(nm.get("cr", 0)),
                                            "canceled": int(nm.get("canceled", 0)), "shks": int(nm.get("shks", 0)),
                                            "sum_price": float(nm.get("sum_price", 0)),
                                        })
                                    except Exception as e:
                                        logger.warning("[ad_stats] nm save error camp=%s nm=%s: %s", s.get("advertId"), nm_id, e)

                    await db.commit()
                    total_saved += len(stats)

                if batch_start + 50 < len(stat_ids):
                    logger.info("[ad_stats] Org %s: wait 65s for next batch", org_id)
                    await asyncio.sleep(65)

            if day_offset < days_back - 1:
                await asyncio.sleep(65)

        # ═══ ШАГ 4b: Обновить nm_ids в ad_campaigns из ad_stats_nm ═══
        # nm_id для каждой кампании берём из ad_stats_nm (источник: /adv/v3/fullstats)
        try:
            async with sf() as db:
                result = await db.execute(text("""
                    SELECT wb_campaign_id,
                           jsonb_agg(DISTINCT nm_id ORDER BY nm_id) AS nm_ids
                    FROM ad_stats_nm
                    WHERE organization_id = :org
                    GROUP BY wb_campaign_id
                """), {"org": org_id})
                rows = result.fetchall()
                updated = 0
                for row in rows:
                    cid = row[0]
                    nm_ids_json = row[1]
                    await db.execute(text("""
                        UPDATE ad_campaigns
                        SET nm_ids = CAST(:nm_ids AS jsonb), updated_at = now()
                        WHERE organization_id = :org AND wb_campaign_id = :cid
                    """), {"org": org_id, "cid": cid, "nm_ids": json.dumps(nm_ids_json) if isinstance(nm_ids_json, list) else nm_ids_json})
                    updated += 1
                await db.commit()
                logger.info("[ad_stats] Org %s: updated nm_ids for %d campaigns", org_id, updated)
        except Exception as e:
            logger.warning("[ad_stats] Org %s: nm_ids update error: %s", org_id, e)

        # ═══ ШАГ 5: Баланс ═══
        try:
            resp4 = await client.get("/adv/v1/balance")
            if resp4.status_code == 200:
                bal = resp4.json()
                async with sf() as db:
                    ins = pg_insert(RawApiData)
                    await db.execute(ins.values(
                        organization_id=org_id, api_method="ad_balance",
                        target_date=datetime.now(ZoneInfo("Europe/Moscow")).date(), raw_response=bal,
                        status="ok", fetched_at=datetime.utcnow(),
                    ).on_conflict_do_update(
                        constraint="raw_api_data_organization_id_api_method_target_date_key",
                        set_={"raw_response": ins.excluded.raw_response, "fetched_at": datetime.utcnow()},
                    ))
                    await db.commit()
        except Exception as e:
            logger.warning("[ad_stats] Org %s balance error: %s", org_id, e)

    return {"status": "ok", "stats_saved": total_saved, "campaigns": len(fresh_campaigns), "skipped_old": skipped_old}
