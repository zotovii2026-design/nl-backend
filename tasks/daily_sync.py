import uuid
import json
"""Сборщик данных WB API — сохраняет сырые данные и заполняет ТС"""
import asyncio
import logging
from datetime import date, datetime, timedelta

from typing import List

from celery import shared_task
from sqlalchemy import select, delete
from sqlalchemy.dialects.postgresql import insert

from core.database import async_session
from core.security import decrypt_data
from models.organization import WbApiKey
from models.raw_data import RawApiData, TechStatus, WarehouseRef, RawBarcode
from services.wb_api.client import WBApiClient

logger = logging.getLogger(__name__)

# WB API endpoints которые собираем
API_METHODS = [
    "products_stats",  # Суммарные метрики
    "products",         # Карточки товаров
    "stocks",           # Остатки по складам
    "sales",            # Продажи
    "orders",           # Заказы
    "tariffs",          # Тарифы складов
    "adverts",          # Реклама
    "warehouses",       # Справочник складов
]

# Окно перезаписи (дней)
REWRITE_WINDOW = 15

# Паузы между запросами (секунд) — для rate-limited методов
RATE_LIMIT_PAUSE = 25


def run_async(coro):
    """Запуск async из Celery (sync контекст)"""
    import asyncio as _a
    loop = _a.new_event_loop()
    _a.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        # Закриваем все asyncpg connections привязанные к этому loop
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


async def get_api_keys(org_id: str) -> List[str]:
    """Получить все расшифрованные API ключи организации"""
    async with async_session() as db:
        result = await db.execute(
            select(WbApiKey).where(WbApiKey.organization_id == org_id)
        )
        keys = result.scalars().all()
        return [decrypt_data(k.api_key) for k in keys]


# ─── СОХРАНЕНИЕ СЫРЫХ ДАННЫХ ──────────────────────────────

async def save_raw(org_id: str, method: str, target_date: date, response, status: str = "ok", error: str = None, count: int = None):
    """Сохранить/обновить сырой ответ в raw_api_data (upsert)"""
    async with async_session() as db:
        stmt = insert(RawApiData).values(
            organization_id=org_id,
            api_method=method,
            target_date=target_date,
            raw_response=response,
            status=status,
            error_message=error,
            records_count=count,
            fetched_at=datetime.utcnow(),
        )
        stmt = stmt.on_conflict_do_update(
            constraint="raw_api_data_organization_id_api_method_target_date_key",
            set_={
                "raw_response": stmt.excluded.raw_response,
                "status": stmt.excluded.status,
                "error_message": stmt.excluded.error_message,
                "records_count": stmt.excluded.records_count,
                "fetched_at": stmt.excluded.fetched_at,
            }
        )
        await db.execute(stmt)
        await db.commit()


# ─── СБОРЩИКИ ПО МЕТОДАМ ──────────────────────────────────

async def fetch_products_stats(client: WBApiClient, org_id: str, target_date: date):
    """Суммарные метрики карточек"""
    try:
        cards = await client.get_all_cards()
        total = len(cards)
        archive = sum(1 for c in cards if c.get("isArchive", False))
        draft = sum(1 for c in cards if not c.get("isArchive") and not c.get("nmID"))
        active = total - archive - draft

        await save_raw(org_id, "products_stats", target_date, {
            "total": total, "archive": archive, "draft": draft, "active": active
        }, count=total)
        return {"total": total, "archive": archive, "draft": draft, "active": active}
    except Exception as e:
        await save_raw(org_id, "products_stats", target_date, None, status="error", error=str(e))
        logger.error(f"products_stats error: {e}")
        return None


async def fetch_products(client: WBApiClient, org_id: str, target_date: date):
    """Карточки товаров детально"""
    try:
        cards = await client.get_all_cards()
        await save_raw(org_id, "products", target_date, cards, count=len(cards))
        return cards
    except Exception as e:
        await save_raw(org_id, "products", target_date, None, status="error", error=str(e))
        logger.error(f"products error: {e}")
        return None


async def fetch_with_retry(request_fn, max_retries=3):
    """HTTP запрос с retry при 429"""
    for attempt in range(max_retries):
        try:
            return await request_fn()
        except Exception as e:
            if "429" in str(e) and attempt < max_retries - 1:
                wait = 15 * (attempt + 1)
                logger.warning(f"429 rate limit, waiting {wait}s (attempt {attempt+1}/{max_retries})")
                await asyncio.sleep(wait)
            else:
                raise


async def fetch_sales(client: WBApiClient, org_id: str, target_date: date):
    """Продажи за дату"""
    try:
        sales = await fetch_with_retry(lambda: client.get_sales(date_from=target_date.isoformat()))
        if isinstance(sales, list):
            await save_raw(org_id, "sales", target_date, sales, count=len(sales))
        else:
            await save_raw(org_id, "sales", target_date, {"response": sales})
        return sales
    except Exception as e:
        await save_raw(org_id, "sales", target_date, None, status="error", error=str(e))
        logger.error(f"sales error: {e}")
        return None


async def fetch_orders(client: WBApiClient, org_id: str, target_date: date):
    """Заказы за дату"""
    try:
        date_str = target_date.isoformat()
        orders = await client.get_orders(date_from=date_str)
        if isinstance(orders, list):
            await save_raw(org_id, "orders", target_date, orders, count=len(orders))
        else:
            await save_raw(org_id, "orders", target_date, {"response": orders})
        return orders
    except Exception as e:
        await save_raw(org_id, "orders", target_date, None, status="error", error=str(e))
        logger.error(f"orders error: {e}")
        return None


async def fetch_stocks(client: WBApiClient, org_id: str, target_date: date):
    """Остатки со складов через statistics API"""
    try:
        date_str = target_date.isoformat()
        stocks = await client.get_stocks_api(date_from=date_str)
        if isinstance(stocks, list):
            await save_raw(org_id, "stocks", target_date, stocks, count=len(stocks))
        else:
            await save_raw(org_id, "stocks", target_date, {"response": stocks})
        return stocks
    except Exception as e:
        await save_raw(org_id, "stocks", target_date, None, status="error", error=str(e))
        logger.error(f"stocks error: {e}")
        return None


async def fetch_tariffs(client: WBApiClient, org_id: str, target_date: date):
    """Тарифы складов"""
    try:
        async def _do():
            resp = await client.client.get(
                "https://common-api.wildberries.ru/api/v1/tariffs/box",
                params={"date": target_date.isoformat()}
            )
            resp.raise_for_status()
            return resp.json()
        data = await fetch_with_retry(_do)
        await save_raw(org_id, "tariffs", target_date, data)
        return data
    except Exception as e:
        await save_raw(org_id, "tariffs", target_date, None, status="error", error=str(e))
        logger.error(f"tariffs error: {e}")
        return None


async def fetch_adverts(client: WBApiClient, org_id: str, target_date: date):
    """Рекламные кампании"""
    try:
        resp = await client.client.get(
            "https://advert-api.wildberries.ru/adv/v1/promotion/count"
        )
        resp.raise_for_status()
        data = resp.json()
        await save_raw(org_id, "adverts", target_date, data)
        return data
    except Exception as e:
        await save_raw(org_id, "adverts", target_date, None, status="error", error=str(e))
        logger.error(f"adverts error: {e}")
        return None


async def fetch_warehouses(client: WBApiClient, org_id: str):
    """Справочник складов WB — автозаполнение"""
    try:
        resp = await client.client.get(
            "https://marketplace-api.wildberries.ru/api/v3/warehouses"
        )
        resp.raise_for_status()
        warehouses = resp.json()
        if not isinstance(warehouses, list):
            logger.warning(f"Unexpected warehouses response: {type(warehouses)}")
            return warehouses

        async with async_session() as db:
            for wh in warehouses:
                wh_id = wh.get("id") or wh.get("warehouseId")
                wh_name = wh.get("name") or wh.get("warehouseName", "")
                if wh_id:
                    ins = insert(WarehouseRef)
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

        logger.info(f"Synced {len(warehouses)} warehouses for org {org_id}")
        return warehouses
    except Exception as e:
        logger.error(f"warehouses error: {e}")
        return None


# ─── АГРЕГАЦИЯ В ТС ────────────────────────────────────────

async def aggregate_to_tech_status(org_id: str, target_date: date):
    """Агрегировать сырые данные в tech_status"""
    async with async_session() as db:
        result = await db.execute(
            select(RawApiData).where(
                RawApiData.organization_id == org_id,
                RawApiData.target_date == target_date,
            )
        )
        raw_rows = result.scalars().all()

        # Собираем данные по методам
        data = {}
        cell_statuses = {}
        for row in raw_rows:
            data[row.api_method] = row
            cell_statuses[row.api_method] = "green" if row.status == "ok" else "red"

        # Суммарные метрики
        stats = data.get("products_stats")
        cards_total = stats.raw_response.get("total") if stats and stats.raw_response and stats.status == "ok" else None
        cards_archive = stats.raw_response.get("archive") if stats and stats.raw_response and stats.status == "ok" else None
        cards_draft = stats.raw_response.get("draft") if stats and stats.raw_response and stats.status == "ok" else None
        cards_active = stats.raw_response.get("active") if stats and stats.raw_response and stats.status == "ok" else None

        # Остатки
        stocks_raw = data.get("stocks")
        stock_by_nm = {}
        if stocks_raw and stocks_raw.raw_response and stocks_raw.status == "ok":
            stocks_list = stocks_raw.raw_response if isinstance(stocks_raw.raw_response, list) else []
            for s in stocks_list:
                nm = s.get("nmId") or s.get("nm_id")
                if nm and nm not in stock_by_nm:
                    stock_by_nm[nm] = {
                        "warehouse": s.get("warehouseName") or s.get("lastChangeDate", ""),
                        "qty": s.get("quantity") or s.get("quantityFull", 0),
                    }

        # Заказы/продажи по nm_id
        orders_raw = data.get("orders")
        sales_raw = data.get("sales")
        order_counts = {}
        sale_counts = {}
        if orders_raw and orders_raw.raw_response and orders_raw.status == "ok":
            orders_list = orders_raw.raw_response if isinstance(orders_raw.raw_response, list) else []
            for o in orders_list:
                nm = o.get("nmId") or o.get("nm_id")
                if nm:
                    order_counts[nm] = order_counts.get(nm, 0) + 1
        if sales_raw and sales_raw.raw_response and sales_raw.status == "ok":
            sales_list = sales_raw.raw_response if isinstance(sales_raw.raw_response, list) else []
            for s in sales_list:
                nm = s.get("nmId") or s.get("nm_id")
                if nm:
                    sale_counts[nm] = sale_counts.get(nm, 0) + 1

        # row_status
        today = date.today()
        age = (today - target_date).days
        has_errors = any(r.status == "error" for r in raw_rows)

        if has_errors:
            row_status = "error"
        elif age >= REWRITE_WINDOW:
            row_status = "closed"
        else:
            row_status = "active"

        is_final = "yes" if age >= REWRITE_WINDOW else "no"

        # Если есть products — строки по карточкам
        products_raw = data.get("products")
        if products_raw and products_raw.raw_response and products_raw.status == "ok":
            cards = products_raw.raw_response if isinstance(products_raw.raw_response, list) else []

            for card in cards:
                nm_id = card.get("nmID")
                vendor_code = card.get("vendorCode")
                name = card.get("title", "")
                photo = ""
                if card.get("photos") and len(card["photos"]) > 0:
                    photo = card["photos"][0].get("big", "")
                photo_count = len(card.get("photos", []))
                has_video = "yes" if card.get("video") else "no"
                desc = card.get("description", "") or ""
                desc_chars = len(desc)

                sizes = card.get("sizes", [])

                for size_info in sizes:
                    barcode = ""
                    if size_info.get("skus") and len(size_info["skus"]) > 0:
                        barcode = size_info["skus"][0]
                    size_name = size_info.get("techSizeName", "")

                    ins = insert(TechStatus)
                    stmt = ins.values(
                        organization_id=org_id,
                        target_date=target_date,
                        cards_total=cards_total,
                        cards_archive=cards_archive,
                        cards_draft=cards_draft,
                        cards_active=cards_active,
                        nm_id=nm_id,
                        vendor_code=vendor_code,
                        barcode=barcode,
                        product_name=name,
                        photo_main=photo,
                        photo_count=photo_count,
                        has_video=has_video,
                        description_chars=desc_chars,
                        orders_count=order_counts.get(nm_id, 0),
                        buyouts_count=sale_counts.get(nm_id, 0),
                        warehouse_name=stock_by_nm.get(nm_id, {}).get("warehouse", None),
                        stock_qty=stock_by_nm.get(nm_id, {}).get("qty", None),
                        row_status=row_status,
                        cell_statuses=cell_statuses,
                        last_sync_at=datetime.utcnow(),
                        is_final=is_final,
                    ).on_conflict_do_update(
                        constraint="tech_status_organization_id_target_date_nm_id_key",
                        set_={
                            "cards_total": ins.excluded.cards_total,
                            "cards_archive": ins.excluded.cards_archive,
                            "cards_draft": ins.excluded.cards_draft,
                            "cards_active": ins.excluded.cards_active,
                            "vendor_code": ins.excluded.vendor_code,
                            "barcode": ins.excluded.barcode,
                            "product_name": ins.excluded.product_name,
                            "photo_main": ins.excluded.photo_main,
                            "photo_count": ins.excluded.photo_count,
                            "has_video": ins.excluded.has_video,
                            "description_chars": ins.excluded.description_chars,
                            "orders_count": ins.excluded.orders_count,
                            "buyouts_count": ins.excluded.buyouts_count,
                            "warehouse_name": ins.excluded.warehouse_name,
                            "stock_qty": ins.excluded.stock_qty,
                            "row_status": ins.excluded.row_status,
                            "cell_statuses": ins.excluded.cell_statuses,
                            "last_sync_at": ins.excluded.last_sync_at,
                            "is_final": ins.excluded.is_final,
                        }
                    )
                    await db.execute(stmt)

                    # Штрихкод в справочник
                    if barcode:
                        bc_ins = insert(RawBarcode)
                        bc_stmt = bc_ins.values(
                            organization_id=org_id,
                            nm_id=nm_id,
                            vendor_code=vendor_code,
                            barcode=barcode,
                            size_name=size_name,
                        ).on_conflict_do_update(
                            constraint="raw_barcodes_organization_id_barcode_key",
                            set_={
                                "nm_id": bc_ins.excluded.nm_id,
                                "vendor_code": bc_ins.excluded.vendor_code,
                                "size_name": bc_ins.excluded.size_name,
                                "updated_at": datetime.utcnow(),
                            }
                        )
                        await db.execute(bc_stmt)

            await db.commit()
        else:
            stmt = insert(TechStatus).values(
                organization_id=org_id,
                target_date=target_date,
                cards_total=cards_total,
                cards_archive=cards_archive,
                cards_draft=cards_draft,
                cards_active=cards_active,
                row_status=row_status,
                cell_statuses=cell_statuses,
                last_sync_at=datetime.utcnow(),
                is_final=is_final,
            ).on_conflict_do_nothing()
            await db.execute(stmt)
            await db.commit()


# ─── MARK OLD ROWS AS FINAL ────────────────────────────────

async def mark_final_rows(org_id: str):
    """Отметить строки старше 15 дней как финальные"""
    async with async_session() as db:
        cutoff = date.today() - timedelta(days=REWRITE_WINDOW)
        await db.execute(
            TechStatus.__table__.update()
            .where(TechStatus.organization_id == org_id)
            .where(TechStatus.target_date < cutoff)
            .where(TechStatus.is_final == "no")
            .values(is_final="yes", row_status="closed")
        )
        await db.execute(
            RawApiData.__table__.update()
            .where(RawApiData.organization_id == org_id)
            .where(RawApiData.target_date < cutoff)
            .where(RawApiData.is_final == "no")
            .values(is_final="yes")
        )
        await db.commit()


# ─── CELERY TASK ────────────────────────────────────────────

@shared_task(name="wb.daily_sync")
def daily_sync_task(organization_id: str = None):
    """Ежедневный сбор данных WB API для всех организаций (или одной)"""
    return run_async(_daily_sync(organization_id))


async def _daily_sync(organization_id: str = None):
    async with async_session() as db:
        if organization_id:
            result = await db.execute(
                select(WbApiKey.organization_id).where(
                    WbApiKey.organization_id == organization_id
                ).distinct()
            )
        else:
            result = await db.execute(
                select(WbApiKey.organization_id).distinct()
            )
        org_ids = [str(r[0]) for r in result.all()]

    if not org_ids:
        logger.warning("No organizations with WB API keys found")
        return {"status": "skipped", "reason": "no_api_keys"}

    results = {}
    dates_to_sync = [date.today() - timedelta(days=i) for i in range(REWRITE_WINDOW)]

    for org_id in org_ids:
        org_result = {"synced": 0, "errors": 0}
        api_keys = await get_api_keys(org_id)

        if not api_keys:
            continue

        api_key = api_keys[0]

        async with WBApiClient(api_key) as client:
            # Сначала — справочник складов (один раз за синк)
            await fetch_warehouses(client, org_id)
            await asyncio.sleep(RATE_LIMIT_PAUSE)

            for target_date in dates_to_sync:
                logger.info(f"Syncing {org_id} for {target_date}")

                # Продуктовые методы (быстрые)
                await fetch_products_stats(client, org_id, target_date)
                await asyncio.sleep(3)
                await fetch_products(client, org_id, target_date)
                await asyncio.sleep(3)

                # Sales/orders — за ВСЕ 15 дней с rate-limit паузой
                await fetch_sales(client, org_id, target_date)
                await asyncio.sleep(RATE_LIMIT_PAUSE)
                await fetch_orders(client, org_id, target_date)
                await asyncio.sleep(RATE_LIMIT_PAUSE)

                # Остатки, тарифы, реклама
                await fetch_stocks(client, org_id, target_date)
                await asyncio.sleep(3)
                await fetch_tariffs(client, org_id, target_date)
                await asyncio.sleep(3)
                await fetch_adverts(client, org_id, target_date)

                # Агрегация
                await aggregate_to_tech_status(org_id, target_date)
                org_result["synced"] += 1

        await mark_final_rows(org_id)
        results[org_id] = org_result

    return {"status": "completed", "results": results}



async def parse_raw_to_tech_status(org_id: str = None):
    """Парсинг raw_api_data → tech_status по реальному формату WB"""
    async with async_session() as db:
        if org_id:
            org_ids = [org_id]
        else:
            result = await db.execute(select(RawApiData.organization_id).distinct())
            org_ids = [str(r[0]) for r in result.all()]

        total_parsed = 0
        for oid in org_ids:
            result = await db.execute(
                select(RawApiData.target_date).where(
                    RawApiData.organization_id == oid, RawApiData.status == "ok"
                ).distinct().order_by(RawApiData.target_date.desc())
            )
            dates = [r[0] for r in result.all()]

            for td in dates:
                # --- products: названия, бренд, фото ---
                result = await db.execute(
                    select(RawApiData.raw_response).where(
                        RawApiData.organization_id == oid,
                        RawApiData.api_method == "products",
                        RawApiData.target_date == td,
                        RawApiData.status == "ok"
                    ).limit(1)
                )
                prod_row = result.first()
                product_map = {}  # nm_id → {name, brand, photo, barcodes}
                if prod_row and prod_row[0]:
                    cards = prod_row[0] if isinstance(prod_row[0], list) else prod_row[0].get("cards", prod_row[0].get("items", []))
                    if isinstance(cards, dict):
                        cards = cards.get("cards", cards.get("items", []))
                    for c in (cards if isinstance(cards, list) else []):
                        if not isinstance(c, dict): continue
                        nm = c.get("nmID", c.get("nm_id"))
                        if not nm: continue
                        barcodes = []
                        for sz in (c.get("sizes") or []):
                            barcodes.extend(sz.get("skus") or [])
                        photos = c.get("photos") or []
                        product_map[int(nm)] = {
                            "name": c.get("title", c.get("name", "")),
                            "brand": c.get("brand", ""),
                            "photo": photos[0].get("hq", photos[0].get("tm", "")) if photos else "",
                            "barcodes": barcodes,
                        }

                # --- orders: продажи/заказы по nmId ---
                result = await db.execute(
                    select(RawApiData.raw_response).where(
                        RawApiData.organization_id == oid,
                        RawApiData.api_method == "orders",
                        RawApiData.target_date == td,
                        RawApiData.status == "ok"
                    ).limit(1)
                )
                orders_row = result.first()
                orders_map = {}  # nmId → {count, revenue}
                if orders_row and orders_row[0]:
                    ords = orders_row[0] if isinstance(orders_row[0], list) else []
                    for o in (ords if isinstance(ords, list) else []):
                        if not isinstance(o, dict): continue
                        nm = o.get("nmId", o.get("nm_id"))
                        if not nm: continue
                        nm = int(nm)
                        if nm not in orders_map:
                            orders_map[nm] = {"count": 0, "revenue": 0, "vendor_code": "", "barcode": "", "subject": ""}
                        orders_map[nm]["count"] += 1
                        price = o.get("totalPrice") or o.get("price") or 0
                        orders_map[nm]["revenue"] += float(price)
                        if not orders_map[nm]["vendor_code"]:
                            orders_map[nm]["vendor_code"] = str(o.get("supplierArticle", o.get("vendor_code", "")) or "")
                        if not orders_map[nm]["barcode"]:
                            orders_map[nm]["barcode"] = str(o.get("barcode", "") or "")
                        if not orders_map[nm]["subject"]:
                            orders_map[nm]["subject"] = str(o.get("subject", "") or "")

                # --- sales: выкупы/возвраты ---
                result = await db.execute(
                    select(RawApiData.raw_response).where(
                        RawApiData.organization_id == oid,
                        RawApiData.api_method == "sales",
                        RawApiData.target_date == td,
                        RawApiData.status == "ok"
                    ).limit(1)
                )
                sales_row = result.first()
                sales_map = {}  # nmId → {buyouts, returns, sales_revenue}
                if sales_row and sales_row[0]:
                    sls = sales_row[0] if isinstance(sales_row[0], list) else []
                    for s in (sls if isinstance(sls, list) else []):
                        if not isinstance(s, dict): continue
                        nm = s.get("nmId", s.get("nm_id"))
                        if not nm: continue
                        nm = int(nm)
                        if nm not in sales_map:
                            sales_map[nm] = {"buyouts": 0, "returns": 0, "revenue": 0}
                        sale_type = str(s.get("saleID", "") or "")
                        price = float(s.get("forPay") or s.get("totalPrice") or 0)
                        # WB sales: "S" = продажа, "R" = возврат
                        if "R" in sale_type and not sale_type.startswith("S"):
                            sales_map[nm]["returns"] += 1
                            sales_map[nm]["revenue"] -= price
                        else:
                            sales_map[nm]["buyouts"] += 1
                            sales_map[nm]["revenue"] += price

                # --- stocks: остатки ---
                result = await db.execute(
                    select(RawApiData.raw_response).where(
                        RawApiData.organization_id == oid,
                        RawApiData.api_method == "stocks",
                        RawApiData.target_date == td,
                        RawApiData.status == "ok"
                    ).limit(1)
                )
                stocks_row = result.first()
                stock_map = {}
                if stocks_row and stocks_row[0]:
                    stks = stocks_row[0] if isinstance(stocks_row[0], list) else stocks_row[0].get("stocks", [])
                    for st in (stks if isinstance(stks, list) else []):
                        if not isinstance(st, dict): continue
                        nm = st.get("nmId", st.get("nm_id"))
                        if not nm: continue
                        nm = int(nm)
                        if nm not in stock_map:
                            stock_map[nm] = {"qty": 0, "warehouses": set()}
                        stock_map[nm]["qty"] += int(st.get("quantity", st.get("qty", 0)) or 0)
                        wh = st.get("warehouseName", st.get("warehouse_name", ""))
                        if wh: stock_map[nm]["warehouses"].add(wh)

                # --- adverts: расходы ---
                result = await db.execute(
                    select(RawApiData.raw_response).where(
                        RawApiData.organization_id == oid,
                        RawApiData.api_method == "adverts",
                        RawApiData.target_date == td,
                        RawApiData.status == "ok"
                    ).limit(1)
                )
                ad_row = result.first()
                ad_cost_map = {}
                if ad_row and ad_row[0]:
                    adata = ad_row[0]
                    if isinstance(adata, dict):
                        adata = adata.get("response", adata)
                    if isinstance(adata, list):
                        for a in adata:
                            if not isinstance(a, dict): continue
                            for nm_id in (a.get("nm_ids") or a.get("nms") or []):
                                ad_cost_map[int(nm_id)] = ad_cost_map.get(int(nm_id), 0) + float(a.get("sum", a.get("total", 0)) or 0)

                # Собираем все уникальные nm_id
                all_nms = set(product_map.keys()) | set(orders_map.keys()) | set(sales_map.keys()) | set(stock_map.keys()) | set(ad_cost_map.keys())

                for nm_id in all_nms:
                    pinfo = product_map.get(nm_id, {})
                    oinfo = orders_map.get(nm_id, {})
                    sinfo = sales_map.get(nm_id, {})
                    skinfo = stock_map.get(nm_id, {})

                    # Ищем запись
                    from models.raw_data import TechStatus
                    result = await db.execute(
                        select(TechStatus).where(
                            TechStatus.organization_id == oid,
                            TechStatus.target_date == td,
                            TechStatus.nm_id == nm_id
                        )
                    )
                    ts = result.scalar_one_or_none()
                    if not ts:
                        ts = TechStatus(
                            id=str(uuid.uuid4()),
                            organization_id=oid,
                            target_date=td,
                            nm_id=nm_id,
                            row_status="active",
                            is_final="no"
                        )
                        db.add(ts)

                    ts.product_name = pinfo.get("name", "") or oinfo.get("subject", "")
                    ts.vendor_code = oinfo.get("vendor_code", "")
                    ts.barcode = oinfo.get("barcode", "") or (pinfo.get("barcodes") or [""])[0]
                    ts.photo_main = pinfo.get("photo", "")
                    ts.orders_count = oinfo.get("count", 0)
                    ts.buyouts_count = sinfo.get("buyouts", 0)
                    ts.returns_count = sinfo.get("returns", 0)
                    ts.price_discount = sinfo.get("revenue") / max(sinfo.get("buyouts", 1), 1) if sinfo.get("buyouts") else None
                    ts.stock_qty = skinfo.get("qty", 0)
                    ts.warehouse_name = ", ".join(skinfo.get("warehouses", set())) if skinfo.get("warehouses") else ""
                    ts.ad_cost = ad_cost_map.get(nm_id, 0)
                    ts.updated_at = datetime.now()
                    total_parsed += 1

            await db.commit()
        logger.info(f"Parsed {total_parsed} tech_status records for {len(org_ids)} orgs")
        return {"parsed": total_parsed}



async def fetch_and_apply_tariffs(org_id: str):
    """Получить тарифы складов и обновить tech_status"""
    import httpx
    from core.database import async_session
    from sqlalchemy import text
    
    api_keys = await get_api_keys(org_id)
    if not api_keys:
        return {"error": "no api keys"}
    
    async with httpx.AsyncClient(timeout=30) as c:
        h = {"Authorization": api_keys[0]}
        
        # Try box tariffs first, then pallet
        for endpoint in ["box", "pallet"]:
            r = await c.get(f"https://common-api.wildberries.ru/api/v1/tariffs/{endpoint}", headers=h, params={"date": date.today().isoformat()})
            if r.status_code == 200:
                data = r.json()
                whs = data.get("response", {}).get("data", {}).get("warehouseList", [])
                if whs:
                    break
        else:
            return {"error": "tariffs 429", "status": r.status_code}
        
        # Map warehouse name → delivery tariff
        wh_map = {}
        for w in whs:
            name = w.get("warehouseName", "")
            if endpoint == "box":
                base = w.get("boxDeliveryBase", "-")
                storage = w.get("boxStorageBase", "-")
            else:
                base = w.get("palletDelivery", "-")
                storage = w.get("palletStorage", "-")
            
            if base and base != "-":
                try:
                    wh_map[name] = {"delivery": float(base.replace(",", ".")), "storage": float(storage.replace(",", ".")) if storage != "-" else 0}
                except:
                    pass
        
        logger.info(f"Got {len(wh_map)} warehouse tariffs from {endpoint}")
        
        # Update tech_status for today
        async with async_session() as db:
            result = await db.execute(text("""
                UPDATE tech_status t SET
                    tariff = CASE 
                        WHEN t.warehouse_name LIKE '%' || w.warehouse_name || '%' THEN w.tariff
                        ELSE t.tariff
                    END,
                    updated_at = NOW()
                FROM (VALUES :wh_list) AS w(warehouse_name, tariff)
                WHERE t.organization_id = :org AND t.target_date = CURRENT_DATE AND t.tariff IS NULL
            """), {"org": org_id, "wh_list": json.dumps([(k, v["delivery"]) for k, v in wh_map.items()])})
            await db.commit()
        
        return {"warehouses": len(wh_map), "endpoint": endpoint}


@shared_task(name="wb.fetch_tariffs")
def fetch_tariffs_task(organization_id: str = None):
    """Celery task: получить тарифы"""
    return run_async(fetch_and_apply_tariffs(organization_id))


@shared_task(name="wb.parse_raw")
def parse_raw_task(organization_id: str = None):
    """Celery task: парсинг raw → tech_status"""
    return run_async(parse_raw_to_tech_status(organization_id))
