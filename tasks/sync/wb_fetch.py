"""
WB API fetch-таски (сбор сырых данных).
Извлечено из scheduled_sync.py без изменения логики.
"""

import asyncio
import httpx
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from celery import shared_task
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tasks.ue_precompute import run_precompute
from tasks.sync.utils import (
    PAUSE_SEC,
    _run,
    _get_all_keys,
    _save_raw,
    _fetch_with_retry,
)
from models.raw_data import TechStatus, WarehouseRef
from models.product_entity import ProductEntity
from models.wb_tariff_snapshot import WbTariffSnapshot
from services.wb_api.client import WBApiClient
from services.entity_sync import sync_entities_from_raw

logger = logging.getLogger(__name__)




# ─── МЕЛКИЕ ЗАДАЧИ ────────────────────────────────────────

@shared_task(name="wb.sched.products")
def sched_products():
    """Карточки товаров — 1 раз/сут"""
    return _run(_do_products)


async def _do_products(sf):
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}
    results = {}
    for org_id, api_key in all_keys:
        try:
            async with WBApiClient(api_key) as client:
                cards = await client.get_all_cards()
                count = len(cards)
                today = datetime.now(ZoneInfo("Europe/Moscow")).date()  # МСК

                async with sf() as db:
                    await _save_raw(db, org_id, "products", today, cards, count=count)

                archive = sum(1 for c in cards if c.get("isArchive", False))
                active = count - archive
                async with sf() as db:
                    await _save_raw(db, org_id, "products_stats", today,
                                    {"total": count, "archive": archive, "active": active}, count=count)

            # Синхронизация сущностей из карточек
            entity_result = None
            async with sf() as db:
                try:
                    entity_result = await sync_entities_from_raw(db, org_id, today)
                except Exception as e:
                    logger.error(f"[sched] entity_sync error: {e}")

            logger.info(f"[sched] products: {count} cards, entities: {entity_result}")
            results[org_id[:8]] = {"status": "ok", "cards": count, "entities": entity_result}


        except Exception as e:
            logger.error(f"[sched] products error org={org_id}: {e}")
            results[org_id[:8]] = {"status": "error", "error": str(e)}

    return results
@shared_task(name="wb.sched.sales")
def sched_sales():
    """Продажи за вчера и сегодня"""
    return _run(_do_sales)


async def _do_sales(sf):
    """Продажи за 3 дня по МСК времени, с пагинацией"""
    today_msk = datetime.now(ZoneInfo("Europe/Moscow")).date()

    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}
    results = {}
    for org_id, api_key in all_keys:
        try:
            async with WBApiClient(api_key) as client:
                for i in range(3):  # сегодня, вчера, позавчера
                    target = today_msk - timedelta(days=i)
                    try:
                        sales = await _fetch_with_retry(
                            lambda t=target: client.get_all_sales(date_from=t.isoformat()),
                            label=f"sales/{target}"
                        )
                        count = len(sales) if isinstance(sales, list) else 0

                        async with sf() as db:
                            await _save_raw(db, org_id, "sales", target, sales, count=count)

                        results[str(target)] = count
                        if i < 2:
                            await asyncio.sleep(PAUSE_SEC)
                    except Exception as e:
                        logger.error(f"[sched] sales {target}: {e}")
                        results[str(target)] = f"error: {e}"

            logger.info(f"[sched] sales org={org_id}: {results}")


        except Exception as e:
            logger.error(f"[sched] sales error org={org_id}: {e}")
            results[org_id[:8]] = {"status": "error", "error": str(e)}

    return results
@shared_task(name="wb.sched.orders")
def sched_orders():
    """Заказы за вчера и сегодня"""
    return _run(_do_orders)


async def _do_orders(sf):
    """Заказы за 3 дня (позавчера, вчера, сегодня) по МСК времени, с пагинацией"""
    msk = ZoneInfo("Europe/Moscow")
    # Правильный способ:
    today_msk = datetime.now(ZoneInfo("Europe/Moscow")).date()

    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}
    results = {}
    for org_id, api_key in all_keys:
        try:
            async with WBApiClient(api_key) as client:
                for i in range(3):  # сегодня, вчера, позавчера
                    target = today_msk - timedelta(days=i)
                    try:
                        orders = await _fetch_with_retry(
                            lambda t=target: client.get_all_orders(date_from=t.isoformat()),
                            label=f"orders/{target}"
                        )
                        count = len(orders) if isinstance(orders, list) else 0

                        async with sf() as db:
                            await _save_raw(db, org_id, "orders", target, orders, count=count)

                        results[str(target)] = count
                        if i < 2:
                            await asyncio.sleep(PAUSE_SEC)
                    except Exception as e:
                        logger.error(f"[sched] orders {target}: {e}")
                        results[str(target)] = f"error: {e}"

            logger.info(f"[sched] orders org={org_id}: {results}")


        except Exception as e:
            logger.error(f"[sched] orders error org={org_id}: {e}")
            results[org_id[:8]] = {"status": "error", "error": str(e)}

    return results
@shared_task(name="wb.sched.stocks_fbo")
def sched_stocks_fbo():
    """FBO остатки со складов WB"""
    return _run(_do_stocks_fbo)


async def _do_stocks_fbo(sf):
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}
    results = {}
    for org_id, api_key in all_keys:
        today = datetime.now(ZoneInfo("Europe/Moscow")).date()
        try:
            async with WBApiClient(api_key) as client:
                try:
                    stocks = await _fetch_with_retry(
                        lambda: client.get_stocks_warehouses(is_archive=False),
                        label=f"stocks_fbo org={org_id[:8]}",
                    )
                    count = len(stocks) if isinstance(stocks, list) else 0
                    async with sf() as db:
                        await _save_raw(db, org_id, "stocks_fbo", today, stocks, count=count)
                    logger.info(f"[sched] stocks_fbo org={org_id[:8]}: {count} records")
                    results[org_id[:8]] = {"status": "ok", "count": count}
                except Exception as e:
                    logger.error(f"[sched] stocks_fbo error org={org_id}: {e}")
                    results[org_id[:8]] = {"status": "error", "error": str(e)}
        except Exception as e:
            logger.error(f"[sched] stocks_fbo error org={org_id}: {e}")
            results[org_id[:8]] = {"status": "error", "error": str(e)}
    return results

@shared_task(name="wb.sched.tariffs")
def sched_tariffs():
    """Тарифы складов на сегодня"""
    return _run(_do_tariffs)


async def _do_tariffs(sf):
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}
    results = {}
    today = datetime.now(ZoneInfo("Europe/Moscow")).date()  # МСК
    for org_id, api_key in all_keys:
        try:
            async with WBApiClient(api_key) as client:
                for tariff_type in ["box", "pallet"]:
                    try:
                        resp = await client.client.get(
                            f"https://common-api.wildberries.ru/api/v1/tariffs/{tariff_type}",
                            params={"date": today.isoformat()}
                        )
                        resp.raise_for_status()
                        data = resp.json()

                        async with sf() as db:
                            await _save_raw(db, org_id, f"tariffs_{tariff_type}", today, data)

                        logger.info(f"[sched] tariffs_{tariff_type} org={org_id[:8]}: ok")
                    except Exception as e:
                        logger.error(f"[sched] tariffs_{tariff_type} org={org_id[:8]}: {e}")
            results[org_id[:8]] = {"status": "ok"}
        except Exception as e:
            logger.error(f"[sched] tariffs error org={org_id[:8]}: {e}")
            results[org_id[:8]] = {"status": "error", "error": str(e)}
    return results


@shared_task(name="wb.sched.adverts")
def sched_adverts():
    """Рекламные кампании"""
    return _run(_do_adverts)


async def _do_adverts(sf):
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}
    results = {}
    for org_id, api_key in all_keys:
        today = datetime.now(ZoneInfo("Europe/Moscow")).date()  # МСК
        async with WBApiClient(api_key) as client:
            try:
                resp = await client.client.get(
                    "https://advert-api.wildberries.ru/adv/v1/promotion/count"
                )
                resp.raise_for_status()
                data = resp.json()

                async with sf() as db:
                    await _save_raw(db, org_id, "adverts", today, data)

                logger.info(f"[sched] adverts org={org_id}: ok")
                results[org_id[:8]] = {"status": "ok"}
            except Exception as e:
                logger.error(f"[sched] adverts error org={org_id}: {e}")
                results[org_id[:8]] = {"status": "error", "error": str(e)}
    return results


@shared_task(name="wb.sched.prices")
def sched_prices():
    """Синхронизация цен товаров из WB Marketplace API"""
    result = _run(_do_prices)
    try:
        run_precompute()
    except Exception as e:
        logging.getLogger(__name__).warning(f"[prices] ue_precompute skipped: {e}")
    return result


async def _do_prices(sf):
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}
    results = {}
    for org_id, api_key in all_keys:
        try:
            async with WBApiClient(api_key) as client:
                prices_data = await client.get_all_prices()
                items = prices_data if isinstance(prices_data, list) else prices_data.get("items", [])
                count = len(items)

                # Сохраняем в raw_api_data
                async with sf() as db:
                    await _save_raw(db, org_id, "prices", datetime.now(ZoneInfo("Europe/Moscow")).date(), items, count=count)  # МСК

                # Обновляем цены в tech_status + reference_book (WB API возвращает в рублях)
                async with sf() as db:
                    updated_ts = 0
                    updated_rb = 0
                    from datetime import datetime as _dt, timezone as _tz
                    now_utc = _dt.now(_tz.utc)
                    for item in items:
                        nm_id = item.get("nmID") or item.get("nmId") or item.get("nm_id")
                        if not nm_id:
                            continue
                        sizes = item.get("sizes") or []
                        if not sizes:
                            continue
                        sz = sizes[0]
                        discount = item.get("discount", 0)
# WB Prices API возвращает цены в копейках — делим на 100
                        price_retail = float(sz.get("price") or 0) / 100
                        price_discounted = float(sz.get("discountedPrice") or 0) / 100
                        price_club = float(sz.get("clubDiscountedPrice") or 0) / 100
                        if not price_retail:
                            continue
                        # tech_status
                        r1 = await db.execute(text("""
                            UPDATE tech_status 
                            SET price = :price, price_discount = :price_disc, price_spp = :price_spp
                            WHERE organization_id = :org AND nm_id = :nm 
                            AND target_date = CURRENT_DATE
                        """), {"price": price_retail, "price_disc": price_discounted, "price_spp": price_club, "org": org_id, "nm": int(nm_id)})
                        updated_ts += r1.rowcount
                        # reference_book — цена факта из WB API
                        r2 = await db.execute(text("""
                            UPDATE reference_book 
                            SET wb_price_fact = :pf, wb_price_retail = :pr, wb_discount_pct = :disc, wb_prices_updated_at = :now
                            WHERE organization_id = :org AND nm_id = :nm 
                            AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)
                        """), {"pf": price_discounted, "pr": price_retail, "disc": discount, "now": now_utc, "org": org_id, "nm": int(nm_id)})
                        updated_rb += r2.rowcount
                    await db.commit()
                    logger.info(f"[sched] prices updated {updated_ts} tech_status, {updated_rb} reference_book")

                logger.info(f"[sched] prices org={org_id}: {count} items")
                results[org_id[:8]] = {"status": "ok", "count": count}
        except Exception as e:
            logger.error(f"[sched] prices error org={org_id}: {e}")
            results[org_id[:8]] = {"status": "error", "error": str(e)}
    return results


@shared_task(name="wb.sched.warehouses")
def sched_warehouses():
    """Справочник складов"""
    return _run(_do_warehouses)


async def _do_warehouses(sf):
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}

    from models.raw_data import WarehouseRef
    results = {}
    for org_id, api_key in all_keys:
        async with WBApiClient(api_key) as client:
            try:
                resp = await client.client.get(
                    "https://marketplace-api.wildberries.ru/api/v3/warehouses"
                )
                resp.raise_for_status()
                warehouses = resp.json()
                if not isinstance(warehouses, list):
                    results[org_id[:8]] = {"status": "ok", "count": 0}
                    continue

                async with sf() as db:
                    for wh in warehouses:
                        wh_id = wh.get("id") or wh.get("warehouseId")
                        wh_name = wh.get("name") or wh.get("warehouseName", "")
                        if wh_id:
                            ins = pg_insert(WarehouseRef)
                            stmt = ins.values(
                                organization_id=org_id,
                                wb_warehouse_id=wh_id,
                                name=wh_name,
                            ).on_conflict_do_update(
                                constraint="warehouse_refs_wb_warehouse_id_key",
                                set_={"name": ins.excluded.name}
                            )
                            await db.execute(stmt)
                    await db.commit()

                logger.info(f"[sched] warehouses org={org_id[:8]}: {len(warehouses)}")
                results[org_id[:8]] = {"status": "ok", "count": len(warehouses)}
            except Exception as e:
                logger.error(f"[sched] warehouses org={org_id[:8]}: {e}")
                results[org_id[:8]] = {"status": "error", "error": str(e)}
    return results


@shared_task(name="wb.sched.fetch_photos")
def sched_fetch_photos():
    """Подтянуть фото для товаров без фото через публичный API WB"""
    return _run(_do_fetch_photos)


async def _do_fetch_photos(sf):
    from services.photo_fetch import fetch_photos_batch
    from models.product_entity import ProductEntity
    from models.raw_data import TechStatus

    # 1. Найти nm_id без фото в product_entities
    async with sf() as db:
        result = await db.execute(text("""
            SELECT DISTINCT nm_id FROM (
                SELECT DISTINCT nm_id FROM product_entities 
                WHERE (photo_main IS NULL OR photo_main = '') AND organization_id IS NOT NULL
                UNION
                SELECT DISTINCT nm_id FROM tech_status 
                WHERE (photo_main IS NULL OR photo_main = '') AND organization_id IS NOT NULL
            ) sub
        """))
        nm_ids = [r[0] for r in result.all()]

    if not nm_ids:
        logger.info("[sched] fetch_photos: все сущности с фото")
        return {"status": "ok", "fetched": 0, "reason": "all_have_photos"}

    logger.info(f"[sched] fetch_photos: {len(nm_ids)} товаров без фото")

    # 2. Батчевая подтяжка (по 30 штук — WB лимит)
    all_photos = {}
    batch_size = 30
    for i in range(0, len(nm_ids), batch_size):
        batch = nm_ids[i:i+batch_size]
        photos = await fetch_photos_batch(batch)
        all_photos.update(photos)
        if i + batch_size < len(nm_ids):
            import asyncio
            await asyncio.sleep(1)  # пауза между батчами

    if not all_photos:
        logger.info("[sched] fetch_photos: фото не найдены")
        return {"status": "ok", "fetched": 0, "reason": "no_photos_found"}

    # 3. Обновить product_entities
    updated_entities = 0
    async with sf() as db:
        for nm_id, photo_url in all_photos.items():
            result = await db.execute(
                ProductEntity.__table__.update()
                .where(ProductEntity.nm_id == nm_id)
                .where((ProductEntity.photo_main == None) | (ProductEntity.photo_main == ""))
                .values(photo_main=photo_url, updated_at=datetime.utcnow())
            )
            updated_entities += result.rowcount
        await db.commit()

    # 4. Обновить tech_status
    updated_ts = 0
    async with sf() as db:
        for nm_id, photo_url in all_photos.items():
            result = await db.execute(
                TechStatus.__table__.update()
                .where(TechStatus.nm_id == nm_id)
                .where((TechStatus.photo_main == None) | (TechStatus.photo_main == ""))
                .values(photo_main=photo_url, updated_at=datetime.utcnow())
            )
            updated_ts += result.rowcount
        await db.commit()

    logger.info(f"[sched] fetch_photos: {len(all_photos)} photos, {updated_entities} entities, {updated_ts} ts rows updated")
    return {"status": "ok", "photos_found": len(all_photos), "entities_updated": updated_entities, "ts_updated": updated_ts}

# ==================== WB TARIFF SNAPSHOT SYNC ====================



@shared_task(name="wb.sched.commission")
def sched_commission():
    """Подтягивает комиссии МП по предметам"""
    return _run(_do_commission)


async def _do_commission(sf):
    """Сохраняет маппинг subjectID -> commission в raw_api_data для каждой организации"""
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}
    results = {}
    today = datetime.now(ZoneInfo("Europe/Moscow")).date()  # МСК

    import httpx
    async with httpx.AsyncClient() as http:
        for org_id, api_key in all_keys:
            try:
                resp = await http.get(
                    "https://common-api.wildberries.ru/api/v1/tariffs/commission",
                    headers={"Authorization": api_key},
                    timeout=30
                )
                if resp.status_code != 200:
                    logger.error(f"[commission] API error org={org_id[:8]}: {resp.status_code} {resp.text[:200]}")
                    results[org_id[:8]] = {"status": "error", "code": resp.status_code}
                    continue

                data = resp.json()
                report = data.get("report", [])
                logger.info(f"[commission] org={org_id[:8]} got {len(report)} subjects")

                async with sf() as db:
                    await _save_raw(db, org_id, "tariffs_commission", today, data, count=len(report))

                results[org_id[:8]] = {"status": "ok", "subjects": len(report)}
            except Exception as e:
                logger.error(f"[commission] error org={org_id[:8]}: {e}")
                results[org_id[:8]] = {"status": "error", "error": str(e)}
    return results

@shared_task(name="wb.sched.tariff_snapshot")
def sched_tariff_snapshot():
    """Собирает снимок WB-данных (тарифы, цены, комиссия, реклама, % выкупа)"""
    return _run(_do_tariff_snapshot)


async def _do_tariff_snapshot(sf):
    """Заполняет wb_tariff_snapshot из уже собранных raw_api_data + tech_status"""
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}
    results = {}
    for org_id, api_key in all_keys:
        today = datetime.now(ZoneInfo("Europe/Moscow")).date()  # МСК
        try:
            import json as _json
            from models.wb_tariff_snapshot import WbTariffSnapshot

            org_results = {"tariffs": 0, "adverts": 0, "buyout": 0, "total": 0}

            # 0b. Загружаем цены из prices API (discounts-prices-api v2)
            prices_by_nm = {}  # nm_id -> {price_retail, price_with_spp}

            async with sf() as db:
                prices_result = await db.execute(
                    text("SELECT raw_response FROM raw_api_data "
                         "WHERE organization_id = :org AND api_method = 'prices' "
                         "ORDER BY target_date DESC LIMIT 1"),
                    {"org": org_id}
                )
                prices_row = prices_result.first()
                if prices_row and prices_row[0]:
                    try:
                        pdata = prices_row[0] if isinstance(prices_row[0], list) else _json.loads(prices_row[0])
                        items = pdata if isinstance(pdata, list) else (pdata if isinstance(pdata, dict) else [])
                        if isinstance(items, dict):
                            items = items.get("items", [])
                        if isinstance(items, list):
                            for item in items:
                                nm = int(item.get("nmID") or item.get("nmId") or 0)
                                if not nm:
                                    continue
                                sizes = item.get("sizes") or []
                                if not sizes:
                                    continue
                                sz = sizes[0]
                                # Цены из discounts-prices-api в копейках
                                price = float(sz.get("price") or 0) / 100
                                price_disc = float(sz.get("discountedPrice") or 0) / 100
                                price_club = float(sz.get("clubDiscountedPrice") or 0) / 100
                                if nm and price:
                                    price_with_spp = price_disc if price_disc > 0 else price
                                    if nm not in prices_by_nm or price > prices_by_nm[nm]["price_retail"]:
                                        prices_by_nm[nm] = {"price_retail": price, "price_with_spp": price_with_spp}
                        logger.info(f"[tariff_snapshot] loaded {len(prices_by_nm)} prices from prices API")
                    except Exception as e:
                        logger.error(f"[tariff_snapshot] prices parse error: {e}")

            # 1. Извлекаем тарифы (логистика + хранение) из raw_api_data
            logistics_avg = 0
            storage_avg = 0
            # 0. Загружаем комиссии по subjectID из raw_api_data
            commission_rates = {}  # subjectID -> {fbo: paidStorageKgvp, fbs: kgvpMarketplace}
            products_subjects = {}  # nm_id -> subjectID

            async with sf() as db:
                comm_result = await db.execute(
                    text("SELECT raw_response FROM raw_api_data "
                         "WHERE organization_id = :org AND api_method = 'tariffs_commission' "
                         "ORDER BY target_date DESC LIMIT 1"),
                    {"org": org_id}
                )
                comm_row = comm_result.first()
                if comm_row and comm_row[0]:
                    try:
                        cdata = comm_row[0] if isinstance(comm_row[0], dict) else _json.loads(comm_row[0])
                        for item in cdata.get("report", []):
                            sid = item.get("subjectID")
                            fbo_pct = item.get("paidStorageKgvp")   # ФБО (Склад WB)
                            fbs_pct = item.get("kgvpMarketplace")   # ФБС (Маркетплейс)
                            if sid:
                                commission_rates[sid] = {"fbo": float(fbo_pct) if fbo_pct else None, "fbs": float(fbs_pct) if fbs_pct else None}
                        logger.info(f"[tariff_snapshot] loaded {len(commission_rates)} commission rates (FBO+FBS)")
                    except Exception as e:
                        logger.error(f"[tariff_snapshot] commission parse error: {e}")

                # Загружаем subjectID из product_entities (надёжнее, чем из raw кэша)
                subj_result = await db.execute(
                    text("SELECT DISTINCT nm_id, subject_id FROM product_entities "
                         "WHERE organization_id = :org AND subject_id IS NOT NULL"),
                    {"org": org_id}
                )
                for r in subj_result.all():
                    products_subjects[int(r[0])] = int(r[1])
                logger.info(f"[tariff_snapshot] loaded {len(products_subjects)} product subjects from product_entities")

            async with sf() as db:
                box_result = await db.execute(
                    text("SELECT raw_response FROM raw_api_data "
                         "WHERE organization_id = :org AND api_method IN ('tariffs', 'tariffs_box') "
                         "ORDER BY target_date DESC LIMIT 1"),
                    {"org": org_id}
                )
                box_row = box_result.first()
                if box_row and box_row[0]:
                    try:
                        tdata = box_row[0] if isinstance(box_row[0], dict) else _json.loads(box_row[0])
                        warehouses = tdata.get("response", {}).get("data", {}).get("warehouseList", [])
                        target_wh = ["Коледино", "Краснодар", "Казань"]
                        delivery_vals = []
                        storage_vals = []
                        for wh in warehouses:
                            name = wh.get("warehouseName", "")
                            if any(t in name for t in target_wh):
                                db_val = wh.get("boxDeliveryBase", "0")
                                sb_val = wh.get("boxStorageBase", "0")
                                try: delivery_vals.append(float(str(db_val).replace(",", ".")))
                                except: pass
                                try: storage_vals.append(float(str(sb_val).replace(",", ".")))
                                except: pass
                        logistics_avg = sum(delivery_vals) / len(delivery_vals) if delivery_vals else 0
                        storage_avg = sum(storage_vals) / len(storage_vals) if storage_vals else 0
                        org_results["tariffs"] = len(delivery_vals)
                    except Exception as e:
                        logger.error(f"[tariff_snapshot] tariffs parse error: {e}")

            # 2. Рекламные расходы по nm_id из ad_stats + ad_campaigns
            ad_by_nm = {}
            async with sf() as db:
                ad_result = await db.execute(
                    text(r"""
                        WITH camp_nm AS (
                            SELECT wb_campaign_id,
                                   (regexp_match(name, '^\s*(\d+)'))[1]::int as nm_id
                            FROM ad_campaigns
                            WHERE organization_id = :org AND name ~ '^\s*\d+'
                        )
                        SELECT c.nm_id, SUM(a.spent)::numeric(10,2) as total_spent
                        FROM camp_nm c
                        JOIN ad_stats a ON a.wb_campaign_id = c.wb_campaign_id
                            AND a.organization_id = :org
                            AND a.stat_date >= CURRENT_DATE - INTERVAL '30 days'
                        GROUP BY c.nm_id
                    """),
                    {"org": org_id}
                )
                for r in ad_result.all():
                    ad_by_nm[int(r[0])] = float(r[1] or 0)
                org_results["adverts"] = len(ad_by_nm)

            # 3. % выкупа из tech_status (последние 30 дней)
            buyout_map = {}
            async with sf() as db:
                buyout_result = await db.execute(
                    text("""
                        SELECT nm_id,
                               SUM(COALESCE(buyouts_count, 0)) as buyouts,
                               SUM(COALESCE(orders_count, 0)) as orders
                        FROM tech_status
                        WHERE organization_id = :org AND target_date >= CURRENT_DATE - INTERVAL '30 days'
                        GROUP BY nm_id
                    """),
                    {"org": org_id}
                )
                for r in buyout_result.all():
                    b = float(r[1] or 0)
                    o = float(r[2] or 0)
                    buyout_map[r[0]] = round(b / o * 100, 1) if o > 0 else 0
                org_results["buyout"] = len(buyout_map)

            # 4. Список nm_id из product_entities
            entities = {}
            async with sf() as db:
                ent_result = await db.execute(
                    text("SELECT id, nm_id FROM product_entities WHERE organization_id = :org"),
                    {"org": org_id}
                )
                for r in ent_result.all():
                    nm = r[1]
                    if nm not in entities:
                        entities[nm] = []
                    entities[nm].append(r[0])

            # 4b. Маппинг nm_id -> commission_pct
            commission_pct_map = {}
            for nm_id in entities:
                sid = products_subjects.get(nm_id)
                if sid and sid in commission_rates:
                    commission_pct_map[nm_id] = commission_rates[sid]

            # 5. Записываем в wb_tariff_snapshot
            for nm_id, entity_ids in entities.items():
                eid = entity_ids[0] if entity_ids else None

                async with sf() as db:
                    ins = pg_insert(WbTariffSnapshot).values(
                        organization_id=org_id,
                        entity_id=eid,
                        nm_id=nm_id,
                        target_date=today,
                        logistics_tariff=round(logistics_avg, 2) if logistics_avg else None,
                        logistics_base=round(logistics_avg, 2) if logistics_avg else None,
                        storage_tariff=round(storage_avg, 4) if storage_avg else None,
                        storage_base=round(storage_avg, 4) if storage_avg else None,
                        commission_pct=commission_pct_map.get(nm_id, {}).get("fbo") if isinstance(commission_pct_map.get(nm_id), dict) else commission_pct_map.get(nm_id),
                        commission_fbs_pct=commission_pct_map.get(nm_id, {}).get("fbs") if isinstance(commission_pct_map.get(nm_id), dict) else None,
                        price_retail=prices_by_nm.get(nm_id, {}).get("price_retail"),
                        price_with_spp=prices_by_nm.get(nm_id, {}).get("price_with_spp"),
                        ad_cost_fact=ad_by_nm.get(nm_id, 0) if ad_by_nm.get(nm_id, 0) > 0 else None,
                        buyout_pct_fact=buyout_map.get(nm_id),
                    )
                    stmt = ins.on_conflict_do_update(
                        constraint="wb_tariff_snapshot_org_nm_date_key",
                        set_={
                            "logistics_tariff": ins.excluded.logistics_tariff,
                            "logistics_base": ins.excluded.logistics_base,
                            "storage_tariff": ins.excluded.storage_tariff,
                            "storage_base": ins.excluded.storage_base,
                            "ad_cost_fact": ins.excluded.ad_cost_fact,
                            "buyout_pct_fact": ins.excluded.buyout_pct_fact,
                            "commission_pct": ins.excluded.commission_pct,
                            "commission_fbs_pct": ins.excluded.commission_fbs_pct,
                            "price_retail": ins.excluded.price_retail,
                            "price_with_spp": ins.excluded.price_with_spp,
                            "fetched_at": datetime.utcnow(),
                        }
                    )
                    try:
                        await db.execute(stmt)
                        await db.commit()
                        org_results["total"] += 1
                    except Exception as e:
                        await db.rollback()
                        logger.error(f"[tariff_snapshot] upsert error nm={nm_id}: {e}")

            logger.info(f"[tariff_snapshot] org={org_id[:8]}: {org_results}")
            results[org_id[:8]] = {"status": "ok", "total": org_results.get("total", 0)}

        except Exception as e:
            logger.error(f"[sched] error org={org_id}: {e}")
            results[org_id[:8]] = {"status": "error", "error": str(e)}

    return results


# ─── SALES FUNNEL — Показы/клики по товарам ─────────────

@shared_task(name="wb.sched.sales_funnel")
def sched_sales_funnel():
    """Показы/клики/корзина/заказы по товарам из sales-funnel API"""
    return _run(_do_sales_funnel)


async def _do_sales_funnel(sf):
    """Собирает sales-funnel/products за текущий день и сохраняет в raw_api_data."""
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}

    msk = ZoneInfo("Europe/Moscow")
    today_msk = datetime.now(msk).date()
    from_date = today_msk.isoformat()
    to_date = today_msk.isoformat()

    results = {}
    for org_id, api_key in all_keys:
        try:
            async with WBApiClient(api_key) as client:
                funnel = await client.get_sales_funnel_products(
                    date_from=from_date,
                    date_to=to_date,
                )
                count = len(funnel) if isinstance(funnel, list) else 0

                async with sf() as db:
                    await _save_raw(db, org_id, "sales_funnel", today_msk, funnel, count=count)

                logger.info(f"[sales_funnel] org={org_id[:8]}: {count} products")
                results[org_id[:8]] = {"status": "ok", "count": count}
        except Exception as e:
            logger.error(f"[sales_funnel] error org={org_id[:8]}: {e}")
            results[org_id[:8]] = {"status": "error", "error": str(e)}

    return results
