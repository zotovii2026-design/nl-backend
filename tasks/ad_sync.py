"""Celery задача: синхронизация рекламной статистики WB"""

import asyncio
import logging
import httpx
from datetime import date, datetime, timedelta
from typing import Optional

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


async def _get_first_key(sf):
    async with sf() as db:
        result = await db.execute(select(WbApiKey).limit(1))
        key_rec = result.scalar_one_or_none()
        if not key_rec:
            return None
        if key_rec.personal_token:
            decrypted = decrypt_data(key_rec.personal_token)
            return (str(key_rec.organization_id), decrypted)
        decrypted = decrypt_data(key_rec.api_key)
        return (str(key_rec.organization_id), decrypted)


@shared_task(name="wb.sched.ad_stats")
def sched_ad_stats(days_back: int = 1):
    """Синхронизация рекламной статистики за последние N дней"""

    async def _main():
        engine, sf = _make_session()
        try:
            return await _do_ad_stats(sf, days_back)
        finally:
            await engine.dispose()

    return asyncio.run(_main())


async def _do_ad_stats(sf, days_back=1):
    info = await _get_first_key(sf)
    if not info:
        return {"status": "skipped", "reason": "no_keys"}
    org_id, api_key = info

    async with httpx.AsyncClient(
        base_url=ADVERT_API,
        headers={"Authorization": "Bearer " + api_key},
        timeout=30.0,
    ) as client:

        # 1) Список кампаний
        resp = await client.get("/adv/v1/promotion/count")
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

        logger.info("[ad_stats] Total campaigns: %d", len(all_campaigns))

        # 2) Имена кампаний через /adv/v1/upd (история расходов содержит campName)
        campaign_names = {}
        try:
            # Запрашиваем расходы за последние 30 дней — этого достаточно чтобы покрыть
            # все активные и недавно завершённые кампании
            today = date.today()
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
                logger.info("[ad_stats] Got %d campaign names from /adv/v1/upd", len(campaign_names))
            else:
                logger.warning("[ad_stats] /adv/v1/upd status %d: %s", resp_names.status_code, resp_names.text[:200])
        except Exception as e:
            logger.warning("[ad_stats] upd names error: %s", e)

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

        # 4) Статистика по дням — только активные/на паузе/завершённые
        stat_ids = [str(c["advertId"]) for c in all_campaigns if c["status"] in (7, 9, 11)]

        total_saved = 0
        for day_offset in range(days_back):
            target_date = (date.today() - timedelta(days=day_offset + 1)).isoformat()

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
                            logger.info("[ad_stats] Rate limited, wait 65s")
                            await asyncio.sleep(65)
                            retries += 1
                            continue
                        resp3.raise_for_status()
                        stats = resp3.json()
                        if not isinstance(stats, list):
                            stats = []
                        break
                    except Exception as e:
                        logger.error("[ad_stats] fullstats error: %s", e)
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
                    await db.commit()
                    total_saved += len(stats)

                # Rate limit: 65 сек между батчами
                if batch_start + 50 < len(stat_ids):
                    logger.info("[ad_stats] Wait 65s for next batch")
                    await asyncio.sleep(65)

            # Между днями
            if day_offset < days_back - 1:
                await asyncio.sleep(65)

        # 5) Баланс
        try:
            resp4 = await client.get("/adv/v1/balance")
            if resp4.status_code == 200:
                bal = resp4.json()
                async with sf() as db:
                    ins = pg_insert(RawApiData)
                    await db.execute(ins.values(
                        organization_id=org_id, api_method="ad_balance",
                        target_date=date.today(), raw_response=bal,
                        status="ok", fetched_at=datetime.utcnow(),
                    ).on_conflict_do_update(
                        constraint="raw_api_data_organization_id_api_method_target_date_key",
                        set_={"raw_response": ins.excluded.raw_response, "fetched_at": datetime.utcnow()},
                    ))
                    await db.commit()
        except Exception as e:
            logger.warning("[ad_stats] balance error: %s", e)

    logger.info("[ad_stats] Done: %d stats saved", total_saved)
    return {"status": "ok", "stats_saved": total_saved, "campaigns": len(all_campaigns)}
