"""
Синхронизация акций WB через Calendar API + promo snapshot через card.wb.ru
"""
import asyncio
import logging
import httpx
from datetime import datetime, timedelta, timezone, date as date_type
from typing import Optional

from celery import shared_task
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.config import settings
from services.wb_api.keys import get_all_wb_keys as _get_all_keys_imported
from core.security import decrypt_data
from models.organization import WbApiKey
from models.promotion import WbPromotion, WbPromotionProduct, WbPromotionSnapshot
from services.wb_api.client import WBApiClient

logger = logging.getLogger(__name__)


def _is_usable_wb_key(api_key: str | None) -> bool:
    return bool(
        api_key
        and len(api_key) >= 50
        and not api_key.startswith(("fake", "test", "unused"))
    )


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

    # Группируем ключи по org, оставляем только один валидный JWT-ключ на org
    seen_orgs = set()
    deduped_keys = []
    for org_id, api_key in all_keys:
        if org_id in seen_orgs:
            continue
        # Пропускаем тестовые/фейские ключи
        if not _is_usable_wb_key(api_key):
            continue
        deduped_keys.append((org_id, api_key))
        seen_orgs.add(org_id)

    results = {}
    for org_id, api_key in deduped_keys:
        try:
            async with WBApiClient(api_key) as client:
                # 1. Get promotions list
                logger.info(f"[promo_sync] org={org_id[:8]}: fetching promotions")
                promo_resp = await client.get_calendar_promotions(
                    start_date=start_date, end_date=end_date, all_promo=True
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
                                    # Lookup entity_id from product_entities
                                    entity_result = await db.execute(
                                        text("SELECT id FROM product_entities WHERE organization_id = :org_id AND nm_id = :nm_id LIMIT 1"),
                                        {"org_id": org_id, "nm_id": int(nm_id)}
                                    )
                                    entity_row = entity_result.fetchone()
                                    entity_id = entity_row[0] if entity_row else None

                                    ins = pg_insert(WbPromotionProduct)
                                    stmt = ins.values(
                                        organization_id=org_id,
                                        wb_promotion_ext_id=pid,
                                        nm_id=int(nm_id),
                                        in_action=in_action_val,
                                        current_price=price,
                                        required_price=plan_price,
                                        entity_id=entity_id,
                                        synced_at=datetime.now(timezone.utc),
                                    ).on_conflict_do_update(
                                        constraint="wb_promo_products_org_ext_nm_key",
                                        set_={
                                            "in_action": ins.excluded.in_action,
                                            "entity_id": entity_id,
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

                # 4b. Auto акции — пропускаем, заполняются из wb_promotion_snapshots через card.wb.ru
                auto_promos = [p for p in promotions if (p.get("type") or all_details.get(p["id"], {}).get("type")) == "auto"]
                if auto_promos:
                    logger.info(f"[promo_sync] org={org_id[:8]}: skipping {len(auto_promos)} auto promotions (see wb_promotion_snapshots)")

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


@shared_task(name="wb.sched.promo_snapshot")
def do_promo_snapshot():
    """Снимок промо через card.wb.ru для всех товаров — раз в сутки"""
    return _run(_do_promo_snapshot)


async def _do_promo_snapshot(sf):
    """
    Получить все nm_id из product_entities, батчами запросить card.wb.ru,
    сохранить промо и цены в wb_promotion_snapshots.
    """
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}

    today = date_type.today()
    results = {}

    seen_orgs = set()
    deduped_keys = []
    for org_id, api_key in all_keys:
        if org_id in seen_orgs or not _is_usable_wb_key(api_key):
            continue
        deduped_keys.append((org_id, api_key))
        seen_orgs.add(org_id)

    for org_id, api_key in deduped_keys:
        try:
            # Получить все nm_id для организации
            async with sf() as db:
                nm_result = await db.execute(
                    text("SELECT nm_id, id FROM product_entities WHERE organization_id = :org_id AND nm_id IS NOT NULL"),
                    {"org_id": org_id}
                )
                nm_rows = nm_result.fetchall()
                nm_to_entity = {row[0]: row[1] for row in nm_rows}
                all_nm_ids = list(nm_to_entity.keys())

            if not all_nm_ids:
                results[org_id[:8]] = {"status": "skipped", "reason": "no_products"}
                continue

            logger.info(f"[promo_snapshot] org={org_id[:8]}: {len(all_nm_ids)} products")

            processed = 0
            with_promotions = 0
            no_promotions = 0

            try:
                    goods_by_nm = await _fetch_goods_price_map(api_key)
                    card_products = await _fetch_card_products(all_nm_ids)
                    if not card_products:
                        raise RuntimeError("card.wb.ru returned no products; skip snapshot to avoid false negatives")

                    async with sf() as db:
                        for nm_id in all_nm_ids:
                            goods_item = goods_by_nm.get(nm_id, {})
                            card_item = card_products.get(nm_id, {})
                            snapshot_payload = _build_snapshot_payload(goods_item, card_item)

                            ins = pg_insert(WbPromotionSnapshot)
                            stmt = ins.values(
                                organization_id=org_id,
                                nm_id=nm_id,
                                entity_id=nm_to_entity.get(nm_id),
                                snapshot_date=today,
                                promotions=snapshot_payload["promotions"],
                                sale_conditions=snapshot_payload["sale_conditions"],
                                price_basic=snapshot_payload["price_basic"],
                                price_product=snapshot_payload["price_product"],
                                fetched_at=datetime.now(timezone.utc),
                            ).on_conflict_do_update(
                                index_elements=["organization_id", "nm_id", "snapshot_date"],
                                set_={
                                    "promotions": ins.excluded.promotions,
                                    "sale_conditions": ins.excluded.sale_conditions,
                                    "price_basic": ins.excluded.price_basic,
                                    "price_product": ins.excluded.price_product,
                                    "fetched_at": ins.excluded.fetched_at,
                                    "entity_id": nm_to_entity.get(nm_id),
                                }
                            )
                            await db.execute(stmt)

                            processed += 1
                            if snapshot_payload["promotions"]:
                                with_promotions += 1
                            else:
                                no_promotions += 1
                        await db.commit()

            except Exception as e:
                logger.error(f"[promo_snapshot] goods error org={org_id[:8]}: {e}")
                continue

            logger.info(f"[promo_snapshot] org={org_id[:8]}: processed {processed}, with_promo={with_promotions}, no_promo={no_promotions}")
            results[org_id[:8]] = {
                "status": "ok",
                "processed": processed,
                "with_promotions": with_promotions,
                "no_promotions": no_promotions,
            }

        except Exception as e:
            logger.error(f"[promo_snapshot] error org={org_id[:8]}: {e}", exc_info=True)
            results[org_id[:8]] = {"status": "error", "error": str(e)}

    return results


async def _fetch_goods_price_map(api_key):
    """Official prices API: nmID -> seller price/discount data."""
    goods_by_nm = {}
    try:
        async with httpx.AsyncClient(timeout=30) as hc:
            offset = 0
            while True:
                response = await hc.get(
                    "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter",
                    params={"limit": 1000, "offset": offset},
                    headers={"Authorization": api_key},
                )
                response.raise_for_status()
                data = response.json().get("data", {})
                goods = data.get("listGoods", [])
                for item in goods:
                    nm_id = item.get("nmID")
                    if nm_id:
                        goods_by_nm[int(nm_id)] = item
                if len(goods) < 1000:
                    break
                offset += 1000
                await asyncio.sleep(1.0)
    except Exception as exc:
        logger.warning(f"[promo_snapshot] prices API skipped: {exc}")
    return goods_by_nm


async def _fetch_card_products(nm_ids):
    """
    Public card.wb.ru snapshot: nmID -> raw product card.

    card.wb.ru has been more reliable from the Russian host through curl than
    through httpx inside Docker, so keep the existing subprocess curl path.
    """
    import json as _json
    import subprocess

    products_by_nm = {}
    batch_size = 50
    for i in range(0, len(nm_ids), batch_size):
        batch = nm_ids[i:i + batch_size]
        nm_str = ";".join(str(n) for n in batch)
        try:
            result = subprocess.run(
                [
                    "curl", "-s", "--max-time", "15",
                    "https://card.wb.ru/cards/v4/detail",
                    "-G",
                    "-d", "curr=rub",
                    "-d", "dest=-1257786",
                    "-d", "spp=27",
                    "-d", "pricemarginPct=1",
                    "-d", "nm=" + nm_str,
                    "-H", (
                        "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36"
                    ),
                ],
                capture_output=True,
                text=True,
                timeout=20,
            )
            if result.returncode == 0 and result.stdout:
                data = _json.loads(result.stdout)
                for product in data.get("products", []):
                    nm_id = product.get("id")
                    if nm_id:
                        products_by_nm[int(nm_id)] = product
        except Exception as exc:
            logger.warning(f"[promo_snapshot] card.wb.ru batch error: {exc}")
        await asyncio.sleep(0.3)

    logger.info(f"[promo_snapshot] card.wb.ru returned {len(products_by_nm)} products")
    return products_by_nm


def _normalise_promotions(promotions):
    normalised = []
    for promo in promotions or []:
        if isinstance(promo, int):
            normalised.append({"id": promo, "source": "card"})
        elif isinstance(promo, dict):
            normalised.append(
                {
                    "id": promo.get("id"),
                    "title": promo.get("title", ""),
                    "active": promo.get("active", True),
                    "start_date": promo.get("startDateTime") or promo.get("start_date", ""),
                    "end_date": promo.get("endDateTime") or promo.get("end_date", ""),
                    "source": promo.get("source", "card"),
                }
            )
    return normalised or None


def _price_from_card(card_item, key):
    sizes = card_item.get("sizes") or []
    if not sizes:
        return None
    price = (sizes[0].get("price") or {}).get(key)
    return float(price) if price else None


def _price_from_goods(goods_item, key):
    sizes = goods_item.get("sizes") or []
    if not sizes:
        return None
    price = sizes[0].get(key)
    return float(price) / 100.0 if price else None


def _build_snapshot_payload(goods_item, card_item):
    goods_item = goods_item or {}
    card_item = card_item or {}
    card_sale_conditions = []
    for size in card_item.get("sizes") or []:
        card_sale_conditions.extend(size.get("saleConditions") or [])

    sale_conditions = {
        "seller_discount": goods_item.get("discount", 0),
        "card_sale_conditions": card_sale_conditions,
        "card_returned": bool(card_item),
    }

    return {
        "promotions": _normalise_promotions(card_item.get("promotions")),
        "sale_conditions": sale_conditions,
        "price_basic": _price_from_goods(goods_item, "price") or _price_from_card(card_item, "basic"),
        "price_product": (
            _price_from_goods(goods_item, "discountedPrice")
            or _price_from_card(card_item, "product")
        ),
    }
