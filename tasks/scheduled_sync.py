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
from services.wb_api.client import WBApiClient
from sqlalchemy.dialects.postgresql import insert as pg_insert

import uuid

logger = logging.getLogger(__name__)

PAUSE_SEC = 30


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


async def _get_first_key(sf) -> Optional[tuple]:
    """Получить org_id + рабочий API ключ (personal_token приоритет)"""
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
    info = await _get_first_key(sf)
    if not info:
        return {"status": "skipped", "reason": "no_keys"}
    org_id, api_key = info

    async with WBApiClient(api_key) as client:
        cards = await client.get_all_cards()
        count = len(cards)
        today = date.today()

        async with sf() as db:
            await _save_raw(db, org_id, "products", today, cards, count=count)

        archive = sum(1 for c in cards if c.get("isArchive", False))
        active = count - archive
        async with sf() as db:
            await _save_raw(db, org_id, "products_stats", today,
                            {"total": count, "archive": archive, "active": active}, count=count)

    logger.info(f"[sched] products: {count} cards")
    return {"status": "ok", "cards": count}


@shared_task(name="wb.sched.sales")
def sched_sales():
    """Продажи за вчера и сегодня"""
    return _run(_do_sales)


async def _do_sales(sf):
    info = await _get_first_key(sf)
    if not info:
        return {"status": "skipped", "reason": "no_keys"}
    org_id, api_key = info

    results = {}
    async with WBApiClient(api_key) as client:
        for i in range(2):
            target = date.today() - timedelta(days=i)
            try:
                sales = await client.get_sales(date_from=target.isoformat())
                data = sales if isinstance(sales, list) else {"response": sales}
                count = len(sales) if isinstance(sales, list) else 0

                async with sf() as db:
                    await _save_raw(db, org_id, "sales", target, data, count=count)

                results[str(target)] = count
                if i == 0:
                    await asyncio.sleep(PAUSE_SEC)
            except Exception as e:
                logger.error(f"[sched] sales {target}: {e}")
                results[str(target)] = f"error: {e}"

    logger.info(f"[sched] sales: {results}")
    return {"status": "ok", "results": results}


@shared_task(name="wb.sched.orders")
def sched_orders():
    """Заказы за вчера и сегодня"""
    return _run(_do_orders)


async def _do_orders(sf):
    info = await _get_first_key(sf)
    if not info:
        return {"status": "skipped", "reason": "no_keys"}
    org_id, api_key = info

    results = {}
    async with WBApiClient(api_key) as client:
        for i in range(2):
            target = date.today() - timedelta(days=i)
            try:
                orders = await client.get_orders(date_from=target.isoformat())
                data = orders if isinstance(orders, list) else {"response": orders}
                count = len(orders) if isinstance(orders, list) else 0

                async with sf() as db:
                    await _save_raw(db, org_id, "orders", target, data, count=count)

                results[str(target)] = count
                if i == 0:
                    await asyncio.sleep(PAUSE_SEC)
            except Exception as e:
                logger.error(f"[sched] orders {target}: {e}")
                results[str(target)] = f"error: {e}"

    logger.info(f"[sched] orders: {results}")
    return {"status": "ok", "results": results}


@shared_task(name="wb.sched.stocks")
def sched_stocks():
    """Остатки на сегодня"""
    return _run(_do_stocks)


async def _do_stocks(sf):
    info = await _get_first_key(sf)
    if not info:
        return {"status": "skipped", "reason": "no_keys"}
    org_id, api_key = info

    today = date.today()
    async with WBApiClient(api_key) as client:
        try:
            stocks = await client.get_stocks_api(date_from=today.isoformat())
            data = stocks if isinstance(stocks, list) else {"response": stocks}
            count = len(stocks) if isinstance(stocks, list) else 0

            async with sf() as db:
                await _save_raw(db, org_id, "stocks", today, data, count=count)

            logger.info(f"[sched] stocks: {count} records")
            return {"status": "ok", "stocks": count}
        except Exception as e:
            logger.error(f"[sched] stocks: {e}")
            return {"status": "error", "error": str(e)}


@shared_task(name="wb.sched.tariffs")
def sched_tariffs():
    """Тарифы складов на сегодня"""
    return _run(_do_tariffs)


async def _do_tariffs(sf):
    info = await _get_first_key(sf)
    if not info:
        return {"status": "skipped", "reason": "no_keys"}
    org_id, api_key = info

    today = date.today()
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
        return {"status": results}


@shared_task(name="wb.sched.adverts")
def sched_adverts():
    """Рекламные кампании"""
    return _run(_do_adverts)


async def _do_adverts(sf):
    info = await _get_first_key(sf)
    if not info:
        return {"status": "skipped", "reason": "no_keys"}
    org_id, api_key = info

    today = date.today()
    async with WBApiClient(api_key) as client:
        try:
            resp = await client.client.get(
                "https://advert-api.wildberries.ru/adv/v1/promotion/count"
            )
            resp.raise_for_status()
            data = resp.json()

            async with sf() as db:
                await _save_raw(db, org_id, "adverts", today, data)

            logger.info("[sched] adverts: ok")
            return {"status": "ok"}
        except Exception as e:
            logger.error(f"[sched] adverts: {e}")
            return {"status": "error", "error": str(e)}


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
            return {"status": "ok", "count": len(warehouses)}
        except Exception as e:
            logger.error(f"[sched] warehouses: {e}")
            return {"status": "error", "error": str(e)}


@shared_task(name="wb.sched.parse_raw")
def sched_parse_raw():
    """Парсинг raw_api_data → tech_status после всех сборов"""
    return _run(_do_parse_raw)


async def _do_parse_raw(sf):
    """Парсер raw → tech_status с новым session factory"""

    # Получаем org_ids из raw_api_data
    async with sf() as db:
        result = await db.execute(
            text("SELECT DISTINCT organization_id FROM raw_api_data WHERE status = 'ok'")
        )
        org_ids = [str(r[0]) for r in result.all()]

    total = 0
    for org_id in org_ids:
        # --- products ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT raw_response FROM raw_api_data 
                WHERE api_method = 'products' AND status = 'ok' AND organization_id = :org
                ORDER BY fetched_at DESC LIMIT 1
            """), {"org": org_id})
            prod_row = result.first()

        product_map = {}
        if prod_row and prod_row[0]:
            cards = prod_row[0] if isinstance(prod_row[0], list) else []
            for c in cards:
                if not isinstance(c, dict):
                    continue
                nm = c.get("nmID")
                if not nm:
                    continue
                barcodes = []
                for sz in (c.get("sizes") or []):
                    barcodes.extend(sz.get("skus") or [])
                photos = c.get("photos") or []
                product_map[int(nm)] = {
                    "name": c.get("title", ""),
                    "brand": c.get("brand", ""),
                    "photo": photos[0].get("hq", photos[0].get("tm", photos[0].get("big", ""))) if photos else "",
                    "barcodes": barcodes,
                }

        # --- orders ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT target_date, raw_response FROM raw_api_data 
                WHERE api_method = 'orders' AND status = 'ok' AND organization_id = :org
            """), {"org": org_id})
            orders_rows = result.all()

        orders_map = {}
        for orow in orders_rows:
            td, resp = orow
            ords = resp if isinstance(resp, list) else []
            for o in ords:
                if not isinstance(o, dict):
                    continue
                nm = o.get("nmId") or o.get("nm_id")
                if not nm:
                    continue
                nm = int(nm)
                key = (td, nm)
                if key not in orders_map:
                    orders_map[key] = {"count": 0, "revenue": 0, "vendor_code": "", "barcode": ""}
                orders_map[key]["count"] += 1
                orders_map[key]["revenue"] += float(o.get("totalPrice") or o.get("price") or 0)
                if not orders_map[key]["vendor_code"]:
                    orders_map[key]["vendor_code"] = str(o.get("supplierArticle", "") or "")
                if not orders_map[key]["barcode"]:
                    orders_map[key]["barcode"] = str(o.get("barcode", "") or "")

        # --- sales ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT target_date, raw_response FROM raw_api_data 
                WHERE api_method = 'sales' AND status = 'ok' AND organization_id = :org
            """), {"org": org_id})
            sales_rows = result.all()

        sales_map = {}
        for srow in sales_rows:
            td, resp = srow
            sls = resp if isinstance(resp, list) else []
            for s in sls:
                if not isinstance(s, dict):
                    continue
                nm = s.get("nmId") or s.get("nm_id")
                if not nm:
                    continue
                nm = int(nm)
                key = (td, nm)
                if key not in sales_map:
                    sales_map[key] = {"buyouts": 0, "returns": 0, "revenue": 0}
                sale_id = str(s.get("saleID", "") or "")
                price = float(s.get("forPay") or s.get("totalPrice") or 0)
                if "R" in sale_id and not sale_id.startswith("S"):
                    sales_map[key]["returns"] += 1
                    sales_map[key]["revenue"] -= price
                else:
                    sales_map[key]["buyouts"] += 1
                    sales_map[key]["revenue"] += price

        # --- stocks ---
        async with sf() as db:
            result = await db.execute(text("""
                SELECT raw_response FROM raw_api_data 
                WHERE api_method = 'stocks' AND status = 'ok' AND organization_id = :org
                ORDER BY fetched_at DESC LIMIT 1
            """), {"org": org_id})
            stocks_row = result.first()

        stock_map = {}
        if stocks_row and stocks_row[0]:
            stks = stocks_row[0] if isinstance(stocks_row[0], list) else []
            for st in stks:
                if not isinstance(st, dict):
                    continue
                nm = st.get("nmId") or st.get("nm_id")
                if not nm:
                    continue
                nm = int(nm)
                if nm not in stock_map:
                    stock_map[nm] = {"qty": 0, "warehouses": set()}
                stock_map[nm]["qty"] += int(st.get("quantity", st.get("qty", 0)) or 0)
                wh = st.get("warehouseName", st.get("warehouse_name", ""))
                if wh:
                    stock_map[nm]["warehouses"].add(wh)

        # Собираем уникальные (date, nm) из всех источников
        all_keys = set(orders_map.keys()) | set(sales_map.keys())
        all_nms = set(product_map.keys()) | set(stock_map.keys())

        today = date.today()
        for nm in all_nms:
            if (today, nm) not in all_keys:
                all_keys.add((today, nm))

        # Upsert в tech_status
        for td, nm in all_keys:
            pinfo = product_map.get(nm, {})
            oinfo = orders_map.get((td, nm), {})
            sinfo = sales_map.get((td, nm), {})
            skinfo = stock_map.get(nm, {})

            async with sf() as db:
                ins = pg_insert(TechStatus)
                stmt = ins.values(
                    id=str(uuid.uuid4()),
                    organization_id=org_id,
                    target_date=td,
                    nm_id=nm,
                    product_name=pinfo.get("name", ""),
                    vendor_code=oinfo.get("vendor_code", ""),
                    barcode=oinfo.get("barcode", "") or (pinfo.get("barcodes") or [""])[0],
                    photo_main=pinfo.get("photo", ""),
                    orders_count=oinfo.get("count", 0),
                    buyouts_count=sinfo.get("buyouts", 0),
                    returns_count=sinfo.get("returns", 0),
                    stock_qty=skinfo.get("qty", 0),
                    warehouse_name=", ".join(skinfo.get("warehouses", set())) if skinfo.get("warehouses") else None,
                    row_status="active",
                    is_final="no",
                    last_sync_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                ).on_conflict_do_update(
                    constraint="tech_status_organization_id_target_date_nm_id_key",
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
                        "last_sync_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                    }
                )
                await db.execute(stmt)
            total += 1

        async with sf() as db:
            await db.commit()

    logger.info(f"[sched] parse_raw: {total} records")
    return {"parsed": total}
