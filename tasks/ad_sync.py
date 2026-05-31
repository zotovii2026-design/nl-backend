"""Celery задача: синхронизация рекламной статистики WB"""

import json
import asyncio
import logging
import httpx
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

def _parse_dt(v):
    """Parse datetime string from WB API"""
    if not v or not isinstance(v, str):
        return None
    try:
        from datetime import datetime as dt
        return dt.fromisoformat(v)
    except:
        return None


ADVERT_API = "https://advert-api.wildberries.ru"


def _make_session():
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False, future=True,
        pool_pre_ping=True, pool_recycle=300,
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _run(coro):
    """Запуск async из Celery"""
    async def wrapper():
        engine, sf = _make_session()
        try:
            return await coro(sf)
        finally:
            await engine.dispose()
    return asyncio.run(wrapper())


async def _get_all_keys(sf) -> List[Tuple[str, str]]:
    """Получить все API-ключи (org_id, token) для всех организаций"""
    async with sf() as db:
        result = await db.execute(select(WbApiKey))
        keys = result.scalars().all()
        pairs = []
        for k in keys:
            token = None
            if k.personal_token:
                token = decrypt_data(k.personal_token)
            elif k.api_key:
                token = decrypt_data(k.api_key)
            if token:
                pairs.append((str(k.organization_id), token))
        return pairs


@shared_task(name="wb.sched.ad_stats")
def sched_ad_stats(days_back: int = 1):
    """Синхронизация рекламной статистики за последние N дней — по ВСЕМ кабинетам"""

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

        # Пауза между кабинетами (rate limit WB)
        if idx < len(all_keys) - 1:
            logger.info("[ad_stats] Waiting 5s before next org...")
            await asyncio.sleep(5)

    logger.info("[ad_stats] All done: %d orgs, %d stats, %d campaigns",
                len(all_keys), total_result["total_stats"], total_result["total_campaigns"])
    return total_result


async def _sync_org_ads(sf, org_id: str, api_key: str, days_back: int):
    """Синхронизация рекламы для одного кабинета"""

    async with httpx.AsyncClient(
        base_url=ADVERT_API,
        headers={"Authorization": "Bearer " + api_key},
        timeout=30.0,
    ) as client:

        # 1) Список кампаний
        resp = await client.get("/adv/v1/promotion/count")
        if resp.status_code in (401, 403):
            logger.warning("[ad_stats] Org %s: no access to advert API (%d)", org_id, resp.status_code)
            return {"status": "no_access", "http_code": resp.status_code}
        resp.raise_for_status()
        campaigns_data = resp.json()

        all_campaigns = []
        for group in campaigns_data.get("adverts", []):
            camp_type = group.get("type")
            camp_status = group.get("status")
            for ad in group.get("advert_list", []):
                all_campaigns.append({
                    "advertId": ad["advertId"],
                    "type": camp_type,
                    "status": camp_status,
                    "changeTime": ad.get("changeTime"),
                })

        logger.info("[ad_stats] Org %s: %d campaigns", org_id, len(all_campaigns))

        # 2) Имена кампаний через /adv/v1/upd
        campaign_names = {}
        try:
            today = datetime.now(ZoneInfo("Europe/Moscow")).date()
            from_date = (today - timedelta(days=30)).isoformat()
            to_date = today.isoformat()
            resp_names = await client.get(
                "/adv/v1/upd",
                params={"from": from_date, "to": to_date},
            )
            if resp_names.status_code == 200:
                upd_data = resp_names.json()
                if isinstance(upd_data, list):
                    for item in upd_data:
                        aid = item.get("advertId")
                        name = item.get("campName", "")
                        if aid and name:
                            campaign_names[aid] = name
                logger.info("[ad_stats] Org %s: %d campaign names from /adv/v1/upd", org_id, len(campaign_names))
            else:
                logger.warning("[ad_stats] Org %s: /adv/v1/upd status %d", org_id, resp_names.status_code)
        except Exception as e:
            logger.warning("[ad_stats] Org %s: upd names error: %s", org_id, e)

        # 3) Upsert кампаний
        async with sf() as db:
            for camp in all_campaigns:
                aid = camp["advertId"]
                await db.execute(text("""
                    INSERT INTO ad_campaigns (id, organization_id, wb_campaign_id, name, type, status, wb_change_time)
                    VALUES (gen_random_uuid(), :org, :cid, :name, :ctype, :cstatus, :ctime)
                    ON CONFLICT (organization_id, wb_campaign_id) DO UPDATE SET
                        name = EXCLUDED.name, type = EXCLUDED.type, status = EXCLUDED.status,
                        wb_change_time = EXCLUDED.wb_change_time, updated_at = now()
                """), {
                    "org": org_id, "cid": aid,
                    "name": campaign_names.get(aid, ""),
                    "ctype": str(camp["type"]),
                    "cstatus": str(camp["status"]),
                    "ctime": _parse_dt(camp.get("changeTime")),
                })
            await db.commit()

        # 5) Статистика по дням
        stat_ids = [str(c["advertId"]) for c in all_campaigns if c["status"] in (7, 9, 11)]

        total_saved = 0
        for day_offset in range(days_back):
            target_date = (datetime.now(ZoneInfo("Europe/Moscow")).date() - timedelta(days=day_offset + 1)).isoformat()

            for batch_start in range(0, len(stat_ids), 50):
                batch = stat_ids[batch_start:batch_start+50]
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
                    # Сохраняем статистику по каждому nm_id внутри кампании
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

                    # Обновляем nm_ids для кампаний (уникальные nm из fullstats)
                    for s in stats:
                        aid = s.get("advertId")
                        nm_set = set()
                        for day in (s.get("days") or []):
                            for app in (day.get("apps") or []):
                                for nm in (app.get("nms") or []):
                                    nm_id = nm.get("nmId")
                                    if nm_id:
                                        nm_set.add(int(nm_id))
                        if nm_set:
                            await db.execute(text("""
                                UPDATE ad_campaigns SET nm_ids = CAST(:nms AS jsonb), updated_at = now()
                                WHERE organization_id = :org AND wb_campaign_id = :cid
                            """), {"org": org_id, "cid": aid, "nms": json.dumps(sorted(nm_set))})

                    await db.commit()
                    total_saved += len(stats)

                # Rate limit между батчами
                if batch_start + 50 < len(stat_ids):
                    logger.info("[ad_stats] Org %s: wait 65s for next batch", org_id)
                    await asyncio.sleep(65)

            # Между днями
            if day_offset < days_back - 1:
                await asyncio.sleep(65)

        # 6) Баланс
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

    return {"status": "ok", "stats_saved": total_saved, "campaigns": len(all_campaigns)}
