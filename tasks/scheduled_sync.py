"""
Пошаговая автосинхронизация WB API — мелкие задачи с лимитами
Вместо одного мега-прогона — серия коротких задач по расписанию
"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Optional

from celery import shared_task
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from core.config import settings
from core.security import decrypt_data
from models.organization import WbApiKey
from models.raw_data import RawApiData, TechStatus
from services.entity_sync import sync_entities_from_raw, find_entity_by_barcode, find_entity_by_nm_and_size, add_unmatched
from models.product_entity import ProductEntity, EntityBarcode
from services.wb_api.client import WBApiClient
from sqlalchemy.dialects.postgresql import insert as pg_insert

import uuid
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

PAUSE_SEC = 30
RETRY_DELAYS = [30, 60, 120]  # base delays for exponential backoff


def _get_retry_delay(attempt: int, response=None) -> float:
    """Вычислить задержку с учётом Retry-After + jitter."""
    import random
    base = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
    # Проверяем Retry-After заголовок от WB
    if response is not None:
        ra = response.headers.get("retry-after") or response.headers.get("Retry-After")
        if ra:
            try:
                server_delay = float(ra)
                base = max(base, server_delay)
            except ValueError:
                pass
    # Jitter ±20% чтобы параллельные задачи не стучали одновременно
    jitter = base * 0.2 * (random.random() * 2 - 1)
    return max(1.0, base + jitter)


async def _fetch_with_retry(coro_factory, label="", max_retries=3):
    """Retry async call on 429 с Retry-After + jitter."""
    import httpx
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            result = coro_factory()
            if asyncio.iscoroutine(result):
                result = await result
            return result
        except httpx.HTTPStatusError as e:
            last_exc = e
            if e.response.status_code == 429 and attempt < max_retries:
                delay = _get_retry_delay(attempt, e.response)
                logger.warning(f"[retry] {label} 429 (attempt {attempt+1}/{max_retries}), waiting {delay:.1f}s [Retry-After: {e.response.headers.get('retry-after', 'none')}]")
                await asyncio.sleep(delay)
            else:
                raise
        except Exception as e:
            resp = getattr(e, 'response', None)
            if resp is not None and getattr(resp, 'status_code', None) == 429:
                last_exc = e
                if attempt < max_retries:
                    delay = _get_retry_delay(attempt, resp)
                    logger.warning(f"[retry] {label} 429 wrapped (attempt {attempt+1}/{max_retries}), waiting {delay:.1f}s")
                    await asyncio.sleep(delay)
                    continue
            raise
    raise last_exc


def _make_session():
    """Создаёт свежий engine + sessionmaker для текущего event loop"""
    engine = create_async_engine(
        settings.DATABASE_URL,
        echo=False,
        future=True,
        pool_pre_ping=True,
        pool_recycle=300,
    )
    return engine, async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def _run(coro):
    """Запуск async из Celery — каждый раз чистый loop"""
    async def wrapper():
        engine, session_factory = _make_session()
        try:
            return await coro(session_factory)
        finally:
            await engine.dispose()

    return asyncio.run(wrapper())


async def _get_all_keys(sf) -> list:
    """Получить все org_id + рабочие API ключи"""
    async with sf() as db:
        result = await db.execute(select(WbApiKey))
        key_recs = result.scalars().all()
        if not key_recs:
            return []
        keys = []
        for key_rec in key_recs:
            if key_rec.personal_token:
                decrypted = decrypt_data(key_rec.personal_token)
            else:
                decrypted = decrypt_data(key_rec.api_key)
            keys.append((str(key_rec.organization_id), decrypted))
        return keys


async def _get_first_key(sf) -> Optional[tuple]:
    """Backward compat — returns first key"""
    keys = await _get_all_keys(sf)
    return keys[0] if keys else None


async def _save_raw(db, org_id, method, target, response, count=None, status="ok", error=None):
    """Upsert сырых данных"""
    stmt = pg_insert(RawApiData).values(
        organization_id=org_id,
        api_method=method,
        target_date=target,
        raw_response=response,
        status=status,
        error_message=error,
        records_count=count,
        fetched_at=datetime.utcnow(),
    ).on_conflict_do_update(
        constraint="raw_api_data_organization_id_api_method_target_date_key",
        set_={
            "raw_response": pg_insert(RawApiData).excluded.raw_response,
            "status": status,
            "records_count": count,
            "fetched_at": datetime.utcnow(),
        }
    )
    await db.execute(stmt)
    await db.commit()


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
@shared_task(name="wb.sched.stocks")
def sched_stocks():
    """Остатки на сегодня"""
    return _run(_do_stocks)


async def _do_stocks(sf):
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}
    results = {}
    for org_id, api_key in all_keys:
        today = datetime.now(ZoneInfo("Europe/Moscow")).date()  # МСК
        try:
            async with WBApiClient(api_key) as client:
                try:
                    stocks = await client.get_stocks_api(date_from=today.isoformat())
                    data = stocks if isinstance(stocks, list) else {"response": stocks}
                    count = len(stocks) if isinstance(stocks, list) else 0

                    async with sf() as db:
                        await _save_raw(db, org_id, "stocks", today, data, count=count)

                    logger.info(f"[sched] stocks org={org_id}: {count} records")
                    results[org_id[:8]] = {"status": "ok", "stocks": count}
                except Exception as e:
                    logger.error(f"[sched] stocks error org={org_id}: {e}")
                    results[org_id[:8]] = {"status": "error", "error": str(e)}


        except Exception as e:
            logger.error(f"[sched] stocks error org={org_id}: {e}")
            results[org_id[:8]] = {"status": "error", "error": str(e)}

    return results
@shared_task(name="wb.sched.tariffs")
def sched_tariffs():
    """Тарифы складов на сегодня"""
    return _run(_do_tariffs)


async def _do_tariffs(sf):
    info = await _get_first_key(sf)
    if not info:
        return {"status": "skipped", "reason": "no_keys"}
    org_id, api_key = info

    today = datetime.now(ZoneInfo("Europe/Moscow")).date()  # МСК
    async with WBApiClient(api_key) as client:
        results = {}
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

                results[tariff_type] = "ok"
                logger.info(f"[sched] tariffs_{tariff_type}: ok")
            except Exception as e:
                logger.error(f"[sched] tariffs_{tariff_type}: {e}")
                results[tariff_type] = f"error: {e}"
        results[org_id[:8]] = {"status": results}


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
    return _run(_do_prices)


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

                # Обновляем цены в tech_status (цены в копейках, переводим в рубли)
                async with sf() as db:
                    updated = 0
                    for item in items:
                        nm_id = item.get("nmID") or item.get("nmId") or item.get("nm_id")
                        if not nm_id:
                            continue
                        # Цена на уровне nm (может быть 0 если размерные цены)
                        nm_price = float(item.get("price") or 0) / 100
                        discount = float(item.get("discount") or 0)
                        club_discount = float(item.get("clubDiscount") or 0)
                        
                        # Если есть размеры — берём цену из первого размера
                        sizes = item.get("sizes") or []
                        if sizes:
                            size_price = float(sizes[0].get("price") or 0) / 100
                            size_discounted = float(sizes[0].get("discountedPrice") or 0) / 100
                            price = size_discounted if size_discounted > 0 else (size_price if size_price > 0 else nm_price)
                        else:
                            price = nm_price
                        
                        if not price:
                            continue
                        
                        price_with_spp = round(price * (1 - discount / 100), 2) if discount > 0 else price
                        
                        await db.execute(text("""
                            UPDATE tech_status 
                            SET price = :price, price_spp = :spp, price_discount = :spp
                            WHERE organization_id = :org AND nm_id = :nm 
                            AND target_date = CURRENT_DATE
                        """), {"price": price, "spp": price_with_spp, "org": org_id, "nm": int(nm_id)})
                        updated += 1
                    await db.commit()
                    logger.info(f"[sched] prices updated {updated} records in tech_status")

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
    info = await _get_first_key(sf)
    if not info:
        return {"status": "skipped", "reason": "no_keys"}
    org_id, api_key = info

    from models.raw_data import WarehouseRef
    results = {}
    async with WBApiClient(api_key) as client:
        try:
            resp = await client.client.get(
                "https://marketplace-api.wildberries.ru/api/v3/warehouses"
            )
            resp.raise_for_status()
            warehouses = resp.json()
            if not isinstance(warehouses, list):
                return {"status": "ok", "count": 0}

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

            logger.info(f"[sched] warehouses: {len(warehouses)}")
            results[org_id[:8]] = {"status": "ok", "count": len(warehouses)}
            return results
        except Exception as e:
            logger.error(f"[sched] warehouses: {e}")
            return {"status": "error", "error": str(e)}

@shared_task(name="wb.sched.parse_raw")
def sched_parse_raw():
    """Парсинг raw_api_data → tech_status после всех сборов"""
    return _run(_do_parse_raw)


async def _do_parse_raw(sf):
    """
    Парсер raw → tech_status по entity_id (слот размера).
    
    Логика:
    - 9-дневное окно: обновляем сегодня + 8 предыдущих дней
    - WB дописывает данные задним числом → полная замена, не инкремент
    - Сток — моментальный срез за конкретный день, НЕ суммируется
    - Все товары на каждый день (entity gap-fill)
    - Все даты по МСК
    """

    from services.entity_sync import find_entity_by_barcode, add_unmatched

    msk = ZoneInfo("Europe/Moscow")
    today_msk = datetime.now(msk).date()
    # 9-дневное окно: сегодня + 8 дней назад
    window_dates = [today_msk - timedelta(days=i) for i in range(9)]
    window_dates_set = set(window_dates)

    # Получаем org_ids из raw_api_data
    async with sf() as db:
        result = await db.execute(
            text("SELECT DISTINCT organization_id FROM raw_api_data WHERE status = 'ok'")
        )
        org_ids = [str(r[0]) for r in result.all()]

    total = 0
    for org_id in org_ids:
        logger.info(f"[parse_raw] processing org={org_id[:8]}, window={window_dates[-1]}..{window_dates[0]}")

        # --- Маппинг entity_id по (nm_id, size_name) ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT id, nm_id, size_name FROM product_entities WHERE organization_id = :org
            """), {"org": org_id})
            entity_by_nm_size = {}
            nm_to_first_entity = {}
            for row in result.all():
                eid = str(row[0])
                nm = int(row[1])
                sz = str(row[2])
                entity_by_nm_size[(nm, sz)] = eid
                if nm not in nm_to_first_entity:
                    nm_to_first_entity[nm] = eid

        # --- Загружаем маппинг entity_id по barcode ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT eb.barcode, eb.entity_id FROM entity_barcodes eb
                WHERE eb.organization_id = :org
            """), {"org": org_id})
            entity_by_barcode = {}
            for row in result.all():
                entity_by_barcode[str(row[0])] = str(row[1])

        # --- Products (карточки) — один раз ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT raw_response FROM raw_api_data 
                WHERE api_method = 'products' AND status = 'ok' AND organization_id = :org
                ORDER BY fetched_at DESC LIMIT 1
            """), {"org": org_id})
            prod_row = result.first()

        product_map = {}
        if prod_row and prod_row[0]:
            cards = prod_row[0] if isinstance(prod_row[0], list) else (prod_row[0].get("cards", []) if isinstance(prod_row[0], dict) else [])
            logger.info(f"[parse_raw] org={org_id[:8]}: products raw type={type(prod_row[0]).__name__}, cards_count={len(cards)}")
            for c in cards:
                if not isinstance(c, dict):
                    continue
                nm = c.get("nmID")
                if not nm:
                    continue
                photos = c.get("photos") or []
                for sz in (c.get("sizes") or []):
                    size_name = sz.get("techSizeName") or sz.get("techSize") or "ONE SIZE"
                    entity_id = entity_by_nm_size.get((int(nm), size_name))
                    key = entity_id or int(nm)
                    if key not in product_map:
                        product_map[key] = {
                            "name": c.get("title", ""),
                            "brand": c.get("brand", ""),
                            "photo": photos[0].get("hq", photos[0].get("tm", photos[0].get("big", ""))) if photos else "",
                            "nm_id": int(nm),
                            "entity_id": entity_id,
                            "vendor_code": c.get("vendorCode", "") or "",
                            "barcodes": [bc for sz_inner in (c.get("sizes") or []) for bc in (sz_inner.get("skus") or [])],
                            "rating": float(c.get("reviewRating", 0) or 0),
                        }

        logger.info(f"[parse_raw] org={org_id[:8]}: product_map built with {len(product_map)} entries")

        # --- Fallback: подтянуть vendor_code из product_entities ---
        for k, v in product_map.items():
            if not v.get('vendor_code'):
                eid = v.get('entity_id')
                nm = v.get('nm_id')
                async with sf() as db:
                    if eid:
                        result = await db.execute(text(
                            'SELECT vendor_code FROM product_entities WHERE id = :val LIMIT 1'
                        ), {'val': eid})
                    elif nm:
                        result = await db.execute(text(
                            'SELECT vendor_code FROM product_entities WHERE nm_id = :val AND organization_id = :org LIMIT 1'
                        ), {'val': nm, 'org': org_id})
                    else:
                        continue
                    row = result.first()
                    if row and row[0]:
                        v['vendor_code'] = row[0]

        # --- Fallback: подтянуть barcode из entity_barcodes ---
        for k, v in product_map.items():
            if not v.get('barcodes'):
                eid = v.get('entity_id')
                if eid:
                    async with sf() as db:
                        result = await db.execute(text(
                            'SELECT barcode FROM entity_barcodes WHERE entity_id = :eid AND is_active = true LIMIT 1'
                        ), {'eid': eid})
                        row = result.first()
                        if row and row[0]:
                            v['barcodes'] = [row[0]]

        # --- Fallback: подтянуть фото из product_entities ---
        for k, v in product_map.items():
            if not v.get('photo'):
                eid = v.get('entity_id')
                nm = v.get('nm_id')
                async with sf() as db:
                    if eid:
                        result = await db.execute(text(
                            'SELECT photo_main FROM product_entities WHERE id = :val LIMIT 1'
                        ), {'val': eid})
                    elif nm:
                        result = await db.execute(text(
                            'SELECT photo_main FROM product_entities WHERE nm_id = :val AND organization_id = :org LIMIT 1'
                        ), {'val': nm, 'org': org_id})
                    else:
                        continue
                    row = result.first()
                    if row and row[0]:
                        product_map[k]['photo'] = row[0]

        # --- Orders — за все дни из окна ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT target_date, raw_response FROM raw_api_data 
                WHERE api_method = 'orders' AND status = 'ok' AND organization_id = :org
                AND target_date >= :start_date
            """), {"org": org_id, "start_date": window_dates[-1]})
            orders_rows = result.all()

        orders_map = {}  # key = (date, entity_id)
        seen_srids = set()  # дедупликация по srid (WB дублирует заказы в разных raw)
        from datetime import datetime as _dt_parse
        for orow in orders_rows:
            td, resp = orow
            ords = resp if isinstance(resp, list) else []
            for o in ords:
                if not isinstance(o, dict):
                    continue
                nm = o.get("nmId") or o.get("nm_id")
                barcode = str(o.get("barcode", "") or "")
                tech_size = str(o.get("techSize", "") or "")
                if not nm:
                    continue
                nm = int(nm)
                entity_id = entity_by_barcode.get(barcode) if barcode else None
                if not entity_id and tech_size:
                    entity_id = entity_by_nm_size.get((nm, tech_size))
                if not entity_id:
                    entity_id = nm_to_first_entity.get(nm)

                # Дедупликация по srid (WB дублирует один заказ в разных raw ответах)
                srid = str(o.get("srid", "") or "")
                if srid and srid in seen_srids:
                    continue
                if srid:
                    seen_srids.add(srid)

                # Используем РЕАЛЬНУЮ дату заказа, а не target_date из raw_api_data
                order_date_str = o.get("date", "")[:10]  # "2026-05-25T09:18:58" -> "2026-05-25"
                try:
                    order_date = date.fromisoformat(order_date_str) if order_date_str else td
                except ValueError:
                    order_date = td
                # Только если дата в окне
                if order_date not in window_dates_set:
                    continue

                key = (order_date, entity_id or nm)
                if key not in orders_map:
                    orders_map[key] = {"count": 0, "revenue": 0, "vendor_code": "", "barcode": barcode, "entity_id": entity_id, "nm_id": nm, "price": 0, "price_discount": 0}
                orders_map[key]["count"] += 1
                orders_map[key]["revenue"] += float(o.get("totalPrice") or o.get("price") or 0)
                tp = float(o.get("totalPrice") or 0)
                pd = float(o.get("priceWithDisc") or 0)
                if tp > 0:
                    orders_map[key]["price"] = tp
                if pd > 0:
                    orders_map[key]["price_discount"] = pd
                if not orders_map[key]["vendor_code"]:
                    orders_map[key]["vendor_code"] = str(o.get("supplierArticle", "") or "")
                if entity_id and not orders_map[key]["entity_id"]:
                    orders_map[key]["entity_id"] = entity_id

        # --- Sales — за все дни из окна ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT target_date, raw_response FROM raw_api_data 
                WHERE api_method = 'sales' AND status = 'ok' AND organization_id = :org
                AND target_date >= :start_date
            """), {"org": org_id, "start_date": window_dates[-1]})
            sales_rows = result.all()

        sales_map = {}  # key = (date, entity_id)
        seen_sale_ids = set()  # дедупликация по saleID
        for srow in sales_rows:
            td, resp = srow
            sls = resp if isinstance(resp, list) else []
            for s in sls:
                if not isinstance(s, dict):
                    continue
                nm = s.get("nmId") or s.get("nm_id")
                barcode = str(s.get("barcode", "") or "")
                tech_size = str(s.get("techSize", "") or "")  # баг: было o.get вместо s.get
                if not nm:
                    continue
                nm = int(nm)
                entity_id = entity_by_barcode.get(barcode) if barcode else None
                if not entity_id and tech_size:
                    entity_id = entity_by_nm_size.get((nm, tech_size))
                if not entity_id:
                    entity_id = nm_to_first_entity.get(nm)

                # Дедупликация по saleID
                sale_id = str(s.get("saleID", "") or "")
                if sale_id and sale_id in seen_sale_ids:
                    continue
                if sale_id:
                    seen_sale_ids.add(sale_id)

                # Используем РЕАЛЬНУЮ дату продажи
                sale_date_str = s.get("date", "")[:10]
                try:
                    sale_date = date.fromisoformat(sale_date_str) if sale_date_str else td
                except ValueError:
                    sale_date = td
                if sale_date not in window_dates_set:
                    continue

                key = (sale_date, entity_id or nm)
                if key not in sales_map:
                    sales_map[key] = {"buyouts": 0, "returns": 0, "revenue": 0, "entity_id": entity_id, "nm_id": nm, "price": 0, "price_discount": 0}
                sale_id = str(s.get("saleID", "") or "")
                price = float(s.get("forPay") or s.get("totalPrice") or 0)
                tp = float(s.get("totalPrice") or 0)
                pd = float(s.get("priceWithDisc") or 0)
                if tp > 0:
                    sales_map[key]["price"] = tp
                if pd > 0:
                    sales_map[key]["price_discount"] = pd
                if "R" in sale_id and not sale_id.startswith("S"):
                    sales_map[key]["returns"] += 1
                    sales_map[key]["revenue"] -= price
                else:
                    sales_map[key]["buyouts"] += 1
                    sales_map[key]["revenue"] += price
                if entity_id and not sales_map[key]["entity_id"]:
                    sales_map[key]["entity_id"] = entity_id

        # --- Stocks — ПО КАЖДЫЙ ДЕНЬ отдельно! ---
        stocks_by_date = {}  # key = date -> {entity_id: {qty, warehouses}}
        async with sf() as db:
            result = await db.execute(text("""
                SELECT target_date, raw_response FROM raw_api_data 
                WHERE api_method = 'stocks' AND status = 'ok' AND organization_id = :org
                AND target_date >= :start_date
                ORDER BY target_date
            """), {"org": org_id, "start_date": window_dates[-1]})
            stocks_rows = result.all()

        for srow in stocks_rows:
            td, resp = srow
            stock_map = {}
            stks = resp if isinstance(resp, list) else []
            for st in stks:
                if not isinstance(st, dict):
                    continue
                nm = st.get("nmId") or st.get("nm_id")
                barcode = str(st.get("barcode", "") or "")
                if not nm:
                    continue
                nm = int(nm)
                entity_id = entity_by_barcode.get(barcode) if barcode else None
                if not entity_id:
                    entity_id = nm_to_first_entity.get(nm)

                key = entity_id or nm
                if key not in stock_map:
                    stock_map[key] = {"qty": 0, "warehouses": set(), "entity_id": entity_id, "nm_id": nm}
                stock_map[key]["qty"] += int(st.get("quantity", st.get("qty", 0)) or 0)
                wh = st.get("warehouseName", st.get("warehouse_name", ""))
                if wh:
                    stock_map[key]["warehouses"].add(wh)
            stocks_by_date[td] = stock_map

        # --- Цены из tariff_snapshot (fallback) ---
        async with sf() as db:
            price_result = await db.execute(text("""
                SELECT DISTINCT ON (nm_id) nm_id, price_retail, price_with_spp
                FROM wb_tariff_snapshot
                WHERE organization_id = :org
                ORDER BY nm_id, target_date DESC
            """), {"org": org_id})
            tariff_prices = {r[0]: {"price": float(r[1]) if r[1] else 0, "price_spp": float(r[2]) if r[2] else 0} for r in price_result.all()}

        # --- Цены из prices raw (последний синк, точнее tariff_snapshot) ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT raw_response FROM raw_api_data 
                WHERE api_method = 'prices' AND status = 'ok' AND organization_id = :org
                ORDER BY fetched_at DESC LIMIT 1
            """), {"org": org_id})
            prices_row = result.first()
        
        prices_by_nm = {}
        if prices_row and prices_row[0]:
            items = prices_row[0] if isinstance(prices_row[0], list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                nm = item.get("nmID") or item.get("nmId") or item.get("nm_id")
                if not nm:
                    continue
                for sz in (item.get("sizes") or []):
                    prices_by_nm[int(nm)] = {
                        "price": float(sz.get("price", 0) or 0) / 100,  # копейки → рубли
                        "price_discount": float(sz.get("discountedPrice", 0) or 0) / 100,
                        "price_spp": float(sz.get("clubDiscountedPrice", 0) or 0) / 100,
                    }

        # --- Sales Funnel: показы/клики по nm_id за каждый день ---
        # Формат WB: [{product: {nmId, title, ...}, statistic: {past: {openCount, cartCount, ...}}}]
        funnel_map = {}  # key = (date, nm_id) -> {impressions, clicks}
        async with sf() as db:
            result = await db.execute(text("""
                SELECT raw_response FROM raw_api_data
                WHERE api_method = 'sales_funnel' AND status = 'ok' AND organization_id = :org
                ORDER BY fetched_at DESC LIMIT 1
            """), {"org": org_id})
            funnel_row = result.first()

        if funnel_row and funnel_row[0]:
            funnel_data = funnel_row[0] if isinstance(funnel_row[0], list) else []
            n_days = len(window_dates)
            for fp in funnel_data:
                if not isinstance(fp, dict):
                    continue
                # nm_id внутри product.nmId
                prod = fp.get("product") or {}
                nm = prod.get("nmId") or prod.get("nmID") or prod.get("nm_id")
                if not nm:
                    continue
                # Статистика внутри statistic.past
                stat = (fp.get("statistic") or {}).get("past") or {}
                impressions_total = int(stat.get("openCount", 0) or 0)   # показы карточки
                clicks_total = int(stat.get("cartCount", 0) or 0)        # добавления в корзину ~ клики
                for td in window_dates:
                    funnel_map[(td, int(nm))] = {
                        "impressions": impressions_total // n_days,
                        "clicks": clicks_total // n_days,
                    }
            logger.info(f"[parse_raw] org={org_id[:8]}: sales_funnel products={len(funnel_data)}, funnel_map_keys={len(funnel_map)}")

        # --- Ad Stats: расходы по кампаниям за каждый день ---
        # Распределяем расходы пропорционально заказам (из tech_status)
        ad_cost_by_date = {}  # date -> total spent
        async with sf() as db:
            result = await db.execute(text("""
                SELECT stat_date, SUM(spent) as total_spent
                FROM ad_stats
                WHERE organization_id = :org AND stat_date >= :start_date
                GROUP BY stat_date
            """), {"org": org_id, "start_date": window_dates[-1]})
            for r in result.all():
                ad_cost_by_date[r[0]] = float(r[1]) if r[1] else 0

        logger.info(f"[parse_raw] org={org_id[:8]}: ad_cost_by_date={len(ad_cost_by_date)}")

        # ============================================================
        # --- Upsert: перебираем ВСЕ entity × ВСЕ даты из окна ---
        # ============================================================
        
        # Собираем все entity_id
        all_entities = set()
        for (nm, sz), eid in entity_by_nm_size.items():
            all_entities.add(eid)

        # Pre-pass: суммарные заказы за каждый день (для пропорционального распределения ad_cost)
        total_orders_by_date = {}
        for td in window_dates:
            day_total = 0
            for ek in all_entities:
                day_total += orders_map.get((td, ek), {}).get("count", 0)
            total_orders_by_date[td] = day_total

        logger.info(f"[parse_raw] org={org_id[:8]}: dates={len(window_dates)}, entities={len(all_entities)}, orders_keys={len(orders_map)}, sales_keys={len(sales_map)}, stocks_dates={len(stocks_by_date)}")

        for target_date in window_dates:
            date_stock = stocks_by_date.get(target_date, {})
            
            for entity_key in all_entities:
                pinfo = product_map.get(entity_key, {})
                nm_from_pinfo = pinfo.get("nm_id", None)
                entity_from_pinfo = pinfo.get("entity_id", None)

                e_id = entity_from_pinfo or entity_key
                n_id = nm_from_pinfo
                if not n_id:
                    for (nm, sz), eid in entity_by_nm_size.items():
                        if eid == entity_key:
                            n_id = nm
                            break

                # Данные за конкретную дату
                oinfo = orders_map.get((target_date, entity_key), {})
                sinfo = sales_map.get((target_date, entity_key), {})
                skinfo = date_stock.get(entity_key, {})

                # Цены: приоритет sales > orders > prices raw > tariff_snapshot
                _tp = tariff_prices.get(n_id, {}) if n_id else {}
                _wp = prices_by_nm.get(n_id, {}) if n_id else {}
                _price = sinfo.get("price", 0) or oinfo.get("price", 0) or _wp.get("price", 0) or _tp.get("price", 0)
                _price_discount = sinfo.get("price_discount", 0) or oinfo.get("price_discount", 0) or _wp.get("price_discount", 0) or _tp.get("price_spp", 0)
                _price_spp = _wp.get("price_spp", 0) or _tp.get("price_spp", 0)

                # Impressions/clicks из sales_funnel (по nm_id)
                _funnel = funnel_map.get((target_date, n_id), {}) if n_id else {}
                _impressions = _funnel.get("impressions", 0)
                _clicks = _funnel.get("clicks", 0)

                # Расходы: пропорционально заказам товара относительно всех заказов за день
                _date_ad_total = ad_cost_by_date.get(target_date, 0)
                _day_total_orders = total_orders_by_date.get(target_date, 0)
                _entity_orders = oinfo.get("count", 0)
                if _date_ad_total > 0 and _day_total_orders > 0 and _entity_orders > 0:
                    _ad_cost = round(_date_ad_total * _entity_orders / _day_total_orders, 2)
                else:
                    _ad_cost = 0

                async with sf() as db:
                    ins = pg_insert(TechStatus)
                    stmt = ins.values(
                        id=uuid.uuid4(),
                        organization_id=org_id,
                        target_date=target_date,
                        nm_id=n_id,
                        entity_id=e_id,
                        product_name=pinfo.get("name", ""),
                        vendor_code=oinfo.get("vendor_code", "") or pinfo.get("vendor_code", ""),
                        barcode=oinfo.get("barcode", "") or (pinfo.get("barcodes", [""])[0] if pinfo.get("barcodes") else ""),
                        photo_main=pinfo.get("photo", ""),
                        orders_count=oinfo.get("count", 0),
                        buyouts_count=sinfo.get("buyouts", 0),
                        returns_count=sinfo.get("returns", 0),
                        stock_qty=skinfo.get("qty", 0),
                        warehouse_name=", ".join(skinfo.get("warehouses", set())) if skinfo.get("warehouses") else None,
                        price=_price if _price else None,
                        price_discount=_price_discount if _price_discount else None,
                        price_spp=_price_spp if _price_spp else None,
                        rating=pinfo.get("rating", None),
                        impressions=_impressions if _impressions else None,
                        clicks=_clicks if _clicks else None,
                        ad_cost=_ad_cost if _ad_cost else None,
                        row_status="active",
                        is_final="no",
                        last_sync_at=datetime.utcnow(),
                        updated_at=datetime.utcnow(),
                    ).on_conflict_do_update(
                        constraint="tech_status_org_date_entity_key",
                        set_={
                            "product_name": ins.excluded.product_name,
                            "vendor_code": ins.excluded.vendor_code,
                            "barcode": ins.excluded.barcode,
                            "photo_main": ins.excluded.photo_main,
                            "orders_count": ins.excluded.orders_count,
                            "buyouts_count": ins.excluded.buyouts_count,
                            "returns_count": ins.excluded.returns_count,
                            "stock_qty": ins.excluded.stock_qty,
                            "warehouse_name": ins.excluded.warehouse_name,
                            "nm_id": ins.excluded.nm_id,
                            "price": ins.excluded.price,
                            "price_discount": ins.excluded.price_discount,
                            "price_spp": ins.excluded.price_spp,
                            "rating": ins.excluded.rating,
                            "impressions": ins.excluded.impressions,
                            "clicks": ins.excluded.clicks,
                            "ad_cost": ins.excluded.ad_cost,
                            "last_sync_at": datetime.utcnow(),
                            "updated_at": datetime.utcnow(),
                        }
                    )
                    try:
                        await db.execute(stmt)
                        await db.commit()
                        total += 1
                    except Exception as exc:
                        await db.rollback()
                        logger.error(f"[parse_raw] upsert error entity={e_id}, nm={n_id}, date={target_date}: {exc}")

    logger.info(f"[sched] parse_raw: {total} records upserted across {len(window_dates)} dates for {len(org_ids)} orgs")
    return {"parsed": total}


# ─── ПОДТЯЖКА ФОТО ────────────────────────────────────────

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
    """Сохраняет маппинг subjectID -> commission в raw_api_data"""
    results = {}
    info = await _get_first_key(sf)
    if not info:
        return {"status": "skipped", "reason": "no_keys"}
    org_id, api_key = info

    today = datetime.now(ZoneInfo("Europe/Moscow")).date()  # МСК

    import httpx
    async with httpx.AsyncClient() as http:
        resp = await http.get(
            "https://common-api.wildberries.ru/api/v1/tariffs/commission",
            headers={"Authorization": api_key},
            timeout=30
        )
        if resp.status_code != 200:
            logger.error(f"[commission] API error: {resp.status_code} {resp.text[:200]}")
            return {"status": "error", "code": resp.status_code}

        data = resp.json()
        report = data.get("report", [])
        logger.info(f"[commission] got {len(report)} subjects")

        async with sf() as db:
            await _save_raw(db, org_id, "tariffs_commission", today, data, count=len(report))

        results[org_id[:8]] = {"status": "ok", "subjects": len(report)}

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

            results = {"tariffs": 0, "adverts": 0, "buyout": 0, "total": 0}

            # 0b. Загружаем цены из stocks (Price + Discount -> price_retail, price_with_spp)
            prices_by_nm = {}  # nm_id -> {price_retail, price_with_spp}

            async with sf() as db:
                stocks_result = await db.execute(
                    text("SELECT raw_response FROM raw_api_data "
                         "WHERE organization_id = :org AND api_method = 'stocks' "
                         "ORDER BY target_date DESC LIMIT 1"),
                    {"org": org_id}
                )
                stocks_row = stocks_result.first()
                if stocks_row and stocks_row[0]:
                    try:
                        sdata = stocks_row[0] if isinstance(stocks_row[0], list) else _json.loads(stocks_row[0])
                        items = sdata if isinstance(sdata, list) else sdata.get("response", sdata.get("data", []))
                        if isinstance(items, list):
                            for s in items:
                                nm = int(s.get("nmId", 0))
                                price = float(s.get("Price", 0) or 0)
                                discount = float(s.get("Discount", 0) or 0)
                                if nm and price:
                                    price_with_spp = round(price * (1 - discount / 100), 2)
                                    # Берём максимальную цену (может быть несколько записей для одного nm)
                                    if nm not in prices_by_nm or price > prices_by_nm[nm]["price_retail"]:
                                        prices_by_nm[nm] = {"price_retail": price, "price_with_spp": price_with_spp}
                        logger.info(f"[tariff_snapshot] loaded {len(prices_by_nm)} prices from stocks")
                    except Exception as e:
                        logger.error(f"[tariff_snapshot] stocks prices parse error: {e}")

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
                        results["tariffs"] = len(delivery_vals)
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
                results["adverts"] = len(ad_by_nm)

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
                results["buyout"] = len(buyout_map)

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
                        results["total"] += 1
                    except Exception as e:
                        await db.rollback()
                        logger.error(f"[tariff_snapshot] upsert error nm={nm_id}: {e}")

            logger.info(f"[tariff_snapshot] done: {results}")
            results[org_id[:8]] = {"status": "ok", "total": results.get("total", 0)}

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
    """Собирает sales-funnel/products за 9 дней и сохраняет в raw_api_data"""
    all_keys = await _get_all_keys(sf)
    if not all_keys:
        return {"status": "skipped", "reason": "no_keys"}

    msk = ZoneInfo("Europe/Moscow")
    today_msk = datetime.now(msk).date()
    from_date = (today_msk - timedelta(days=8)).isoformat()  # 9 дней
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
