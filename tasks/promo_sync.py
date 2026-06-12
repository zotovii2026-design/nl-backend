"""
Синхронизация акций WB через Calendar API
"""
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from celery import shared_task
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.config import settings
from services.wb_api.keys import get_all_wb_keys as _get_all_keys_imported
from core.security import decrypt_data
from models.organization import WbApiKey
from models.promotion import WbPromotion, WbPromotionProduct
from services.wb_api.client import WBApiClient

logger = logging.getLogger(__name__)


def _make_session():
    engine = create_async_engine(settings.DATABASE_URL, echo=False, future=True, pool_pre_ping=True, pool_recycle=300)
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
    """Delegate to services.wb_api.keys"""
    return await _get_all_keys_imported(sf)

@shared_task(name="wb.sched.promo_sync")
def do_promo_sync():
    """Синхронизация акций WB — раз в 2 часа"""
    return _run(_do_promo_sync)


def _parse_dt(dt_str):
    """Parse datetime string from WB API"""
    if not dt_str:
        return None
    try:
        dt_str = str(dt_str)
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None


def _is_nomenclature_sync_eligible(promotion, detail, now):
    """Nomenclatures are available only for regular, non-expired promotions."""
    promo_type = detail.get("type") or promotion.get("type")
    if promo_type != "regular":
        return False

    end_date = _parse_dt(
        detail.get("endDateTime") or promotion.get("endDateTime")
    )
    if end_date is None:
        return True
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)
    return end_date >= now


async def _do_promo_sync(sf):
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}

    now = datetime.now(timezone.utc)
    start_date = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = (now + timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")

    results = {}
    for org_id, api_key in all_keys:
        try:
            async with WBApiClient(api_key) as client:
                # 1. Get promotions list
                logger.info(f"[promo_sync] org={org_id[:8]}: fetching promotions")
                promo_resp = await client.get_calendar_promotions(
                    start_date=start_date, end_date=end_date, all_promo=False
                )
                promotions = promo_resp.get("data", {}).get("promotions", [])
                logger.info(f"[promo_sync] org={org_id[:8]}: got {len(promotions)} promotions")

                if not promotions:
                    results[org_id[:8]] = {"status": "ok", "promotions": 0}
                    continue

                # 2. Get details for each promotion individually
                all_details = {}
                for p in promotions:
                    pid = p["id"]
                    try:
                        await asyncio.sleep(0.7)
                        det_resp = await client.get_promotion_details(pid)
                        det_promos = det_resp.get("data", {}).get("promotions", [])
                        if det_promos:
                            all_details[pid] = det_promos[0]
                    except Exception as e:
                        logger.error(f"[promo_sync] details error promo={pid}: {e}")

                # 3. Upsert promotions
                promo_count = 0
                for p in promotions:
                    pid = p["id"]
                    detail = all_details.get(pid, {})
                    ptype = p.get("type") or detail.get("type", "regular")

                    sd = p.get("startDateTime") or detail.get("startDateTime")
                    ed = p.get("endDateTime") or detail.get("endDateTime")

                    ranging = detail.get("ranging", [])
                    has_boost = False
                    boost_value = None
                    for r in ranging:
                        if r.get("boost"):
                            has_boost = True
                            boost_value = r["boost"]
                            break

                    async with sf() as db:
                        ins = pg_insert(WbPromotion)
                        stmt = ins.values(
                            organization_id=org_id,
                            promotion_id=pid,
                            title=p.get("name") or detail.get("name", ""),
                            promo_type=ptype,
                            start_date=_parse_dt(sd),
                            end_date=_parse_dt(ed),
                            has_boost=has_boost,
                            boost_value=boost_value,
                            is_active=True,
                            raw_data={**p, "details": detail} if detail else p,
                            source="api",
                        ).on_conflict_do_update(
                            constraint="wb_promotions_org_promo_id_key",
                            set_={
                                "title": ins.excluded.title,
                                "promo_type": ins.excluded.promo_type,
                                "start_date": ins.excluded.start_date,
                                "end_date": ins.excluded.end_date,
                                "has_boost": ins.excluded.has_boost,
                                "boost_value": ins.excluded.boost_value,
                                "raw_data": ins.excluded.raw_data,
                                "source": ins.excluded.source,
                                "updated_at": datetime.now(timezone.utc),
                            }
                        )
                        await db.execute(stmt)
                        await db.commit()
                        promo_count += 1

                logger.info(f"[promo_sync] org={org_id[:8]}: upserted {promo_count} promotions")

                # 4. Get nomenclatures for regular promotions
                product_count = 0
                eligible_promos = [
                    p
                    for p in promotions
                    if _is_nomenclature_sync_eligible(
                        p,
                        all_details.get(p["id"], {}),
                        now,
                    )
                ]
                skipped_promos = len(promotions) - len(eligible_promos)

                for p in eligible_promos:
                    pid = p["id"]
                    for in_action_val in [True, False]:
                        try:
                            await asyncio.sleep(0.7)
                            nom_resp = await client.get_promotion_nomenclatures(
                                promotion_id=pid, in_action=in_action_val
                            )
                            noms = nom_resp.get("data", {}).get("nomenclatures", [])

                            for nom in noms:
                                nm_id = nom.get("id")  # WB returns 'id' as nmId
                                if not nm_id:
                                    continue

                                price = nom.get("price")
                                plan_price = nom.get("planPrice")
                                discount = nom.get("discount")
                                plan_discount = nom.get("planDiscount")

                                async with sf() as db:
                                    ins = pg_insert(WbPromotionProduct)
                                    stmt = ins.values(
                                        organization_id=org_id,
                                        wb_promotion_ext_id=pid,
                                        nm_id=int(nm_id),
                                        in_action=in_action_val,
                                        current_price=price,
                                        required_price=plan_price,
                                        synced_at=datetime.now(timezone.utc),
                                    ).on_conflict_do_update(
                                        constraint="wb_promo_products_org_ext_nm_key",
                                        set_={
                                            "in_action": ins.excluded.in_action,
                                            "current_price": ins.excluded.current_price,
                                            "required_price": ins.excluded.required_price,
                                            "synced_at": ins.excluded.synced_at,
                                        }
                                    )
                                    await db.execute(stmt)
                                    await db.commit()
                                    product_count += 1

                        except Exception as e:
                            logger.warning(f"[promo_sync] nomenclatures error promo={pid} in_action={in_action_val}: {e}")

                # 5. Link promotion_id_col (FK to wb_promotions)
                async with sf() as db:
                    await db.execute(text("""
                        UPDATE wb_promotion_products pp
                        SET promotion_id_col = wp.id
                        FROM wb_promotions wp
                        WHERE pp.organization_id = wp.organization_id
                        AND pp.wb_promotion_ext_id = wp.promotion_id
                        AND pp.promotion_id_col IS NULL
                    """))
                    await db.commit()

                results[org_id[:8]] = {
                    "status": "ok",
                    "promotions": promo_count,
                    "products": product_count,
                    "nomenclature_skipped": skipped_promos,
                }

        except Exception as e:
            logger.error(f"[promo_sync] error org={org_id[:8]}: {e}", exc_info=True)
            results[org_id[:8]] = {"status": "error", "error": str(e)}

    return results
