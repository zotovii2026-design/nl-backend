"""Сборщик данных WB API — сохраняет сырые данные и заполняет ТС"""
import asyncio
import logging
from datetime import date, datetime, timedelta
import time
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
]

# Окно перезаписи (дней)
REWRITE_WINDOW = 15


def run_async(coro):
    """Запуск async из Celery (sync контекст)"""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
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
        # ON CONFLICT — обновляем если запись за этот метод+дата уже есть
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
    """Суммарные метрики карточек — считаем из get_all_cards"""
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


async def fetch_sales(client: WBApiClient, org_id: str, target_date: date):
    """Продажи за дату"""
    try:
        date_str = target_date.isoformat()
        sales = await client.get_sales(date_from=date_str)
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
    """Остатки по складам — через карточки (sizes + stocks)"""
    try:
        cards = await client.get_all_cards()
        stocks_data = []
        for card in cards:
            nm_id = card.get("nmID")
            for size in card.get("sizes", []):
                for stock in size.get("stocks", []):
                    stocks_data.append({
                        "nmId": nm_id,
                        "techSizeName": size.get("techSizeName", ""),
                        "barcode": size.get("skus", [""])[0] if size.get("skus") else "",
                        "warehouse": stock.get("wh", ""),
                        "qty": stock.get("qty", 0),
                    })
        await save_raw(org_id, "stocks", target_date, stocks_data, count=len(stocks_data))
        return stocks_data
    except Exception as e:
        await save_raw(org_id, "stocks", target_date, None, status="error", error=str(e))
        logger.error(f"stocks error: {e}")
        return None


async def fetch_tariffs(client: WBApiClient, org_id: str, target_date: date):
    """Тарифы складов —wb bietet отдельный API"""
    try:
        # WB API: GET /api/v2/tariffs/box?date=YYYY-MM-DD
        resp = await client.client.get(
            "https://common-api.wildberries.ru/api/v1/tariffs/box",
            params={"date": target_date.isoformat()}
        )
        resp.raise_for_status()
        data = resp.json()
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

        # Определяем row_status
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

        # Если есть products — создаём строки по карточкам
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

                # Получаем размеры/штрихкоды
                sizes = card.get("sizes", [])

                for size_info in sizes:
                    barcode = ""
                    if size_info.get("skus") and len(size_info["skus"]) > 0:
                        barcode = size_info["skus"][0]
                    size_name = size_info.get("techSizeName", "")

                    # Ищем продажи/заказы для этой карточки
                    orders_count = 0
                    buyouts_count = 0
                    returns_count = 0

                    # TODO: доработать подсчёт из sales/orders когда будет реальный формат

                    # Upsert в tech_status
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
                            "row_status": ins.excluded.row_status,
                            "cell_statuses": ins.excluded.cell_statuses,
                            "last_sync_at": ins.excluded.last_sync_at,
                            "is_final": ins.excluded.is_final,
                        }
                    )
                    await db.execute(stmt)

                    # Сохраняем штрихкод в справочник
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
            # Нет products — создаём сводную строку без карточек
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
        # То же для raw_api_data
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
    """
    Ежедневный сбор данных WB API для всех организаций (или одной)
    Собирает за последние 15 дней, агрегирует в ТС
    """
    return run_async(_daily_sync(organization_id))


async def _daily_sync(organization_id: str = None):
    async with async_session() as db:
        # Получаем организации с API ключами
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

        # Используем первый ключ (можно ротировать)
        api_key = api_keys[0]

        async with WBApiClient(api_key) as client:
            for target_date in dates_to_sync:
                logger.info(f"Syncing {org_id} for {target_date}")

                # Собираем все методы (products/tariffs/adverts за все дни, sales/orders только за сегодня)
                await fetch_products_stats(client, org_id, target_date); time.sleep(1)
                await fetch_products(client, org_id, target_date); time.sleep(1)
                # Sales/orders — rate limited, только за последние 3 дня
                if (date.today() - target_date).days <= 3:
                    await fetch_sales(client, org_id, target_date); time.sleep(3)
                    await fetch_orders(client, org_id, target_date); time.sleep(3)
                await fetch_stocks(client, org_id, target_date); time.sleep(1)
                await fetch_tariffs(client, org_id, target_date); time.sleep(1)
                await fetch_adverts(client, org_id, target_date)

                # Агрегируем в ТС
                await aggregate_to_tech_status(org_id, target_date)
                org_result["synced"] += 1

        # Отмечаем старые строки финальными
        await mark_final_rows(org_id)
        results[org_id] = org_result

    return {"status": "completed", "results": results}
