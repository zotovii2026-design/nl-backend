"""
Синхронизация карточек WB → product_entities + entity_barcodes.
Вызывается после каждого сбора карточек.
"""
import uuid
import logging
from datetime import date, datetime
from typing import List, Dict, Any, Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models.product_entity import ProductEntity, EntityBarcode, UnmatchedBarcode
from models.raw_data import RawApiData

logger = logging.getLogger(__name__)


async def sync_entities_from_raw(db: AsyncSession, org_id: str, today: date = None):
    """
    Разбирает последний raw_api_data (method='products') и создаёт/обновляет
    product_entities и entity_barcodes.

    Шаг 1: Тянем карточки из raw_api_data
    Шаг 2: Для каждой карточки разбираем sizes
    Шаг 3: Upsert сущности по (org_id, nm_id, size_name)
    Шаг 4: Upsert ШК по (entity_id, barcode)
    Шаг 5: Разрешаем unmatched_barcodes
    """
    if today is None:
        today = date.today()

    # ─── 1. Получаем последние карточки ────────────────────
    result = await db.execute(text("""
        SELECT raw_response FROM raw_api_data
        WHERE api_method = 'products' AND status = 'ok' AND organization_id = :org
        ORDER BY fetched_at DESC LIMIT 1
    """), {"org": org_id})
    row = result.first()
    if not row or not row[0]:
        logger.warning(f"No products raw data for org {org_id}")
        return {"entities": 0, "barcodes": 0, "unmatched_resolved": 0}

    cards = row[0]
    if not isinstance(cards, list):
        logger.warning(f"Products raw data is not a list for org {org_id}")
        return {"entities": 0, "barcodes": 0, "unmatched_resolved": 0}

    # ─── 2-4. Разбираем карточки → сущности + ШК ───────────
    entities_created = 0
    entities_updated = 0
    barcodes_created = 0

    # Собираем все текущие entity_id → barcodes для отметки is_active
    all_active_entity_ids = set()

    for card in cards:
        nm_id = card.get("nmID")
        if not nm_id:
            continue

        vendor_code = card.get("vendorCode", "")
        title = card.get("title", "")
        # Главное фото
        photos = card.get("photos", [])
        photo_main = ""
        if photos:
            photo_main = photos[0].get("big", "") or photos[0].get("small", "")

        # Новые поля из WB Content API
        brand = card.get("brand", "") or ""
        subject_name = card.get("subjectName", "") or ""
        need_kiz = card.get("needKiz", False)
        kiz_marked = card.get("kizMarked", False)

        # Габариты
        dims = card.get("dimensions") or {}
        weight = dims.get("weightBrutto")
        width = dims.get("width")
        height = dims.get("height")
        length = dims.get("length")

        # Характеристики → ТНВЭД, Цвет
        characteristics = card.get("characteristics", [])
        tnved = ""
        color = ""
        for ch in characteristics:
            ch_name = ch.get("name", "")
            ch_values = ch.get("value", [])
            if ch_name == "ТНВЭД" and ch_values:
                tnved = ch_values[0]
            elif ch_name == "Цвет" and ch_values:
                color = ", ".join(ch_values)

        sizes = card.get("sizes", [])
        if not sizes:
            # Карточка без размеров — создаём одну сущность
            sizes = [{"techSize": "ONE SIZE", "skus": []}]

        for size_obj in sizes:
            size_name = (
                size_obj.get("techSizeName")
                or size_obj.get("techSize")
                or "ONE SIZE"
            )
            chrt_id = size_obj.get("chrtID")
            skus = size_obj.get("skus", [])

            # Upsert сущность
            ins = pg_insert(ProductEntity)
            stmt = ins.values(
                id=str(uuid.uuid4()),
                organization_id=org_id,
                nm_id=nm_id,
                vendor_code=vendor_code,
                size_name=size_name,
                product_name=title,
                photo_main=photo_main,
            ).on_conflict_do_update(
                constraint="product_entities_org_nm_size_key",
                set_={
                    "vendor_code": ins.excluded.vendor_code,
                    "product_name": ins.excluded.product_name,
                    "photo_main": ins.excluded.photo_main,
                    "updated_at": datetime.utcnow(),
                }
            )
            # Нужно получить id сущности — сначала пробуем найти
            result = await db.execute(
                select(ProductEntity).where(
                    ProductEntity.organization_id == org_id,
                    ProductEntity.nm_id == nm_id,
                    ProductEntity.size_name == size_name,
                )
            )
            entity = result.scalar_one_or_none()

            if not entity:
                # Создаём
                entity = ProductEntity(
                    organization_id=org_id,
                    nm_id=nm_id,
                    vendor_code=vendor_code,
                    size_name=size_name,
                    product_name=title,
                    photo_main=photo_main,
                    brand=brand or None,
                    subject_name=subject_name or None,
                    tnved=tnved or None,
                    color=color or None,
                    weight=weight,
                    width=width,
                    height=height,
                    length=length,
                    chrt_id=chrt_id,
                    need_kiz=need_kiz,
                    kiz_marked=kiz_marked,
                )
                db.add(entity)
                await db.flush()
                entities_created += 1
            else:
                # Обновляем
                entity.vendor_code = vendor_code
                entity.product_name = title
                entity.photo_main = photo_main
                entity.brand = brand or None
                entity.subject_name = subject_name or None
                entity.tnved = tnved or None
                entity.color = color or None
                entity.weight = weight
                entity.width = width
                entity.height = height
                entity.length = length
                entity.chrt_id = chrt_id
                entity.need_kiz = need_kiz
                entity.kiz_marked = kiz_marked
                entities_updated += 1

            all_active_entity_ids.add(str(entity.id))

            # ─── Upsert ШК ─────────────────────────────
            for barcode in skus:
                if not barcode:
                    continue

                # Проверяем есть ли уже
                result = await db.execute(
                    select(EntityBarcode).where(
                        EntityBarcode.entity_id == entity.id,
                        EntityBarcode.barcode == barcode,
                    )
                )
                existing_bc = result.scalar_one_or_none()

                if existing_bc:
                    existing_bc.last_seen = today
                    existing_bc.is_active = True
                else:
                    new_bc = EntityBarcode(
                        entity_id=entity.id,
                        organization_id=org_id,
                        barcode=barcode,
                        size_name=size_name,
                        first_seen=today,
                        last_seen=today,
                        is_active=True,
                    )
                    db.add(new_bc)
                    barcodes_created += 1

            # ШК которых больше нет в карточке → is_active = false
            if skus:
                active_barcodes = set(skus)
                result = await db.execute(
                    select(EntityBarcode).where(
                        EntityBarcode.entity_id == entity.id,
                        EntityBarcode.is_active == True,
                    )
                )
                for bc in result.scalars().all():
                    if bc.barcode not in active_barcodes:
                        bc.is_active = False

    await db.flush()

    # ─── 5. Разрешаем unmatched_barcodes ───────────────────
    unmatched_resolved = 0
    result = await db.execute(
        select(UnmatchedBarcode).where(
            UnmatchedBarcode.organization_id == org_id,
            UnmatchedBarcode.resolved == False,
        )
    )
    unmatched_list = result.scalars().all()

    for um in unmatched_list:
        # Ищем entity по barcode
        bc_result = await db.execute(
            select(EntityBarcode).where(
                EntityBarcode.organization_id == org_id,
                EntityBarcode.barcode == um.barcode,
            )
        )
        bc = bc_result.scalar_one_or_none()
        if bc:
            um.resolved = True
            unmatched_resolved += 1
            # TODO: парсинг raw_data в tech_status — будет добавлено на шаге 6

    await db.commit()

    logger.info(
        f"[entity_sync] org={org_id}: entities_created={entities_created}, "
        f"entities_updated={entities_updated}, barcodes_created={barcodes_created}, "
        f"unmatched_resolved={unmatched_resolved}"
    )
    return {
        "entities": entities_created + entities_updated,
        "entities_created": entities_created,
        "entities_updated": entities_updated,
        "barcodes_created": barcodes_created,
        "unmatched_resolved": unmatched_resolved,
    }


async def find_entity_by_barcode(db: AsyncSession, org_id: str, barcode: str) -> Optional[str]:
    """
    Найти entity_id по штрихкоду.
    Возвращает entity_id (str) или None.
    """
    result = await db.execute(
        select(EntityBarcode.entity_id).where(
            EntityBarcode.organization_id == org_id,
            EntityBarcode.barcode == barcode,
        )
    )
    row = result.first()
    if row:
        return str(row[0])

    # Пробуем по nm_id + size_name если barcode не найден
    return None


async def find_entity_by_nm_and_size(db: AsyncSession, org_id: str, nm_id: int, size_name: str) -> Optional[str]:
    """
    Найти entity_id по артикулу + размер.
    """
    result = await db.execute(
        select(ProductEntity.id).where(
            ProductEntity.organization_id == org_id,
            ProductEntity.nm_id == nm_id,
            ProductEntity.size_name == size_name,
        )
    )
    row = result.first()
    return str(row[0]) if row else None


async def add_unmatched(db: AsyncSession, org_id: str, barcode: str,
                         source: str, target_date: date,
                         nm_id: int = None, size_name: str = None,
                         raw_data: dict = None):
    """
    Добавить ШК в буфер unmatched_barcodes.
    """
    ins = pg_insert(UnmatchedBarcode)
    stmt = ins.values(
        id=str(uuid.uuid4()),
        organization_id=org_id,
        barcode=barcode,
        nm_id=nm_id,
        size_name=size_name,
        source=source,
        raw_data=raw_data,
        target_date=target_date,
        resolved=False,
    ).on_conflict_do_update(
        constraint="unmatched_barcodes_org_barcode_source_date_key",
        set_={
            "nm_id": ins.excluded.nm_id,
            "size_name": ins.excluded.size_name,
            "raw_data": ins.excluded.raw_data,
            "resolved": False,
        }
    )
    await db.execute(stmt)
