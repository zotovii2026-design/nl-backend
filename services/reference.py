"""Бизнес-логика Справочника: утилиты, валидация, нормализация"""
import uuid
import logging
from datetime import datetime, date
from typing import Optional, Any

from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

_log = logging.getLogger(__name__)


async def resolve_org_id(org_id: str, db: AsyncSession) -> str:
    """Если org_id — числовой (wb_seller_id), найти UUID организации"""
    try:
        uuid.UUID(org_id)
        return org_id  # Уже UUID
    except ValueError:
        pass
    result = await db.execute(
        text("SELECT id FROM organizations WHERE wb_seller_id = :sid"),
        {"sid": int(org_id)}
    )
    row = result.first()
    if row:
        return str(row[0])
    raise HTTPException(status_code=400, detail=f"Организация не найдена: {org_id}")


def pfloat(v: Any) -> Optional[float]:
    """Парсинг float: None для пустых/невалидных значений"""
    if v is not None and str(v).strip() not in ("", "None"):
        try:
            return float(v)
        except (ValueError, TypeError):
            pass
    return None


def pint(v: Any) -> Optional[int]:
    """Парсинг int: None для пустых/невалидных значений"""
    if v is not None and str(v).strip() not in ("", "None"):
        try:
            return int(float(str(v).replace(",", ".")))
        except (ValueError, TypeError):
            pass
    return None


def normalize_product_class(v: Optional[str]) -> Optional[str]:
    """Нормализация класса товара: только A/B/C"""
    if v and v.upper() in ("A", "B", "C"):
        return v.upper()
    return None


def normalize_fulfillment(ffm_raw: str) -> str:
    """Нормализация ФБО/ФБС"""
    return "fbs" if ffm_raw.lower() in ("fbs", "фбс", "фбс ") else "fbo"


def auto_calc_volume(length: Optional[float], width: Optional[float], height: Optional[float]) -> Optional[float]:
    """Авто-расчёт объёма из габаритов"""
    if length and length > 0 and width and width > 0 and height and height > 0:
        return round((length * width * height) / 1000, 3)
    return None


async def resolve_entity_id(db: AsyncSession, org_id: str, nm_id: int, entity_id: Optional[str] = None, size_name: str = "") -> Optional[str]:
    """Поиск entity_id по nm_id + size_name, если не передан напрямую"""
    if entity_id:
        return entity_id
    if size_name:
        result = await db.execute(text(
            "SELECT pe.id FROM product_entities pe "
            "WHERE pe.organization_id = :org AND pe.nm_id = :nm AND pe.size_name = :sz LIMIT 1"
        ), {"org": org_id, "nm": nm_id, "sz": size_name})
        row = result.first()
        return str(row[0]) if row else None
    # Без size_name — ищем единственную entity
    result = await db.execute(text(
        "SELECT pe.id, pe.size_name FROM product_entities pe "
        "WHERE pe.organization_id = :org AND pe.nm_id = :nm"
    ), {"org": org_id, "nm": nm_id})
    rows = result.all()
    if len(rows) == 1:
        return str(rows[0][0])
    return None  # Нельзя однозначно определить


async def ensure_reference_book_for_entities(
    db: AsyncSession,
    org_id: str,
    valid_from: Optional[date] = None,
) -> int:
    """Create empty reference_book rows for product entities missing active reference.

    This keeps the seller-managed layer entity-first without touching existing
    manual values. It only inserts rows for entities that do not have an active
    reference_book record.
    """
    effective_date = valid_from or date.today()
    result = await db.execute(
        text("""
            INSERT INTO reference_book (
                organization_id, entity_id, nm_id, barcode, vendor_code, size_name,
                subject_id, subject_name, brand, fulfillment_model,
                transport_pack_qty, valid_from, source, notes
            )
            SELECT
                pe.organization_id,
                pe.id,
                pe.nm_id,
                (
                    SELECT string_agg(DISTINCT eb.barcode, ', ' ORDER BY eb.barcode)
                    FROM entity_barcodes eb
                    WHERE eb.entity_id = pe.id
                      AND eb.is_active = true
                ) AS barcode,
                pe.vendor_code,
                pe.size_name,
                pe.subject_id,
                pe.subject_name,
                pe.brand,
                'fbo',
                1,
                :valid_from,
                'api',
                'auto-created from product_entities'
            FROM product_entities pe
            WHERE pe.organization_id = :org
              AND NOT EXISTS (
                  SELECT 1
                  FROM reference_book rb
                  WHERE rb.organization_id = pe.organization_id
                    AND rb.entity_id = pe.id
                    AND (rb.valid_to IS NULL OR rb.valid_to >= CURRENT_DATE)
              )
            ON CONFLICT ON CONSTRAINT reference_book_org_nm_eid_vf_key DO NOTHING
        """),
        {"org": org_id, "valid_from": effective_date},
    )
    return int(result.rowcount or 0)
