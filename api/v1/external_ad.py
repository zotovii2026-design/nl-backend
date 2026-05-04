"""API для внешней рекламы и самовыкупов"""

import uuid
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime

from core.database import get_db
from models.external_ad import ExternalAd
from models.product_entity import ProductEntity

router = APIRouter(tags=["external_ad"])


# ─── SCHEMAS ────────────────────────────────────────────────

class ExternalAdCreate(BaseModel):
    """Создание / обновление записи рекламы/самовыкупа"""
    nm_id: Optional[int] = None
    vendor_code: Optional[str] = None
    article: Optional[str] = None
    photo_url: Optional[str] = None
    card_url: Optional[str] = None
    substitution_url: Optional[str] = None
    utm_url: Optional[str] = None
    source: Optional[str] = None
    query: Optional[str] = None
    ad_date: Optional[str] = None  # YYYY-MM-DD
    reach: Optional[int] = None
    amount: Optional[float] = None
    orders_count: Optional[int] = None
    orders_avg_weekly: Optional[float] = None
    ad_type: Optional[str] = "ad"  # ad / buyout
    notes: Optional[str] = None


class ExternalAdUpdate(BaseModel):
    """Частичное обновление"""
    nm_id: Optional[int] = None
    vendor_code: Optional[str] = None
    article: Optional[str] = None
    photo_url: Optional[str] = None
    card_url: Optional[str] = None
    substitution_url: Optional[str] = None
    utm_url: Optional[str] = None
    source: Optional[str] = None
    query: Optional[str] = None
    ad_date: Optional[str] = None
    reach: Optional[int] = None
    amount: Optional[float] = None
    orders_count: Optional[int] = None
    orders_avg_weekly: Optional[float] = None
    ad_type: Optional[str] = None
    notes: Optional[str] = None


# ─── HELPERS ────────────────────────────────────────────────

async def auto_fill_from_db(db: AsyncSession, org_id: str, nm_id: int) -> dict:
    """Автозаполнение: photo_url, card_url, vendor_code из product_entities"""
    result = await db.execute(
        select(ProductEntity).where(
            ProductEntity.organization_id == org_id,
            ProductEntity.nm_id == nm_id,
        ).limit(1)
    )
    entity = result.scalar_one_or_none()
    if entity:
        return {
            "entity_id": entity.id,
            "vendor_code": entity.vendor_code,
            "photo_url": entity.photo_main,
            "card_url": f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx",
        }
    return {
        "entity_id": None,
        "card_url": f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx",
    }


# ─── ENDPOINTS ──────────────────────────────────────────────

@router.get("/api/v1/nl/external-ads")
async def get_external_ads(
    org_id: str,
    ad_type: Optional[str] = None,
    source: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    search: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Список записей внешней рекламы/самовыкупов с фильтрами"""
    conditions = ["ea.organization_id = :org_id"]
    params = {"org_id": org_id}

    if ad_type:
        conditions.append("ea.ad_type = :ad_type")
        params["ad_type"] = ad_type
    if source:
        conditions.append("ea.source ILIKE :source")
        params["source"] = f"%{source}%"
    if date_from:
        conditions.append("ea.ad_date >= :date_from")
        params["date_from"] = date_from
    if date_to:
        conditions.append("ea.ad_date <= :date_to")
        params["date_to"] = date_to
    if search:
        conditions.append("(ea.vendor_code ILIKE :search OR ea.article ILIKE :search OR ea.source ILIKE :search OR ea.query ILIKE :search)")
        params["search"] = f"%{search}%"

    where = " AND ".join(conditions)

    sql = f"""
        SELECT ea.id, ea.entity_id, ea.nm_id, ea.vendor_code, ea.article,
               ea.photo_url, ea.card_url, ea.substitution_url, ea.utm_url,
               ea.source, ea.query, ea.ad_date, ea.reach, ea.amount,
               ea.orders_count, ea.orders_avg_weekly, ea.ad_type, ea.notes,
               ea.created_at, ea.updated_at,
               pe.product_name, pe.size_name
        FROM external_ads ea
        LEFT JOIN product_entities pe ON pe.id = ea.entity_id
        WHERE {where}
        ORDER BY ea.ad_date DESC NULLS LAST, ea.created_at DESC
    """
    result = await db.execute(text(sql), params)
    rows = result.all()

    return [{
        "id": str(r[0]),
        "entity_id": str(r[1]) if r[1] else None,
        "nm_id": r[2],
        "vendor_code": r[3],
        "article": r[4],
        "photo_url": r[5],
        "card_url": r[6],
        "substitution_url": r[7],
        "utm_url": r[8],
        "source": r[9],
        "query": r[10],
        "ad_date": str(r[11]) if r[11] else None,
        "reach": r[12],
        "amount": float(r[13]) if r[13] else None,
        "orders_count": r[14],
        "orders_avg_weekly": float(r[15]) if r[15] else None,
        "ad_type": r[16] or "ad",
        "notes": r[17],
        "created_at": str(r[18]) if r[18] else None,
        "updated_at": str(r[19]) if r[19] else None,
        "product_name": r[20],
        "size_name": r[21],
    } for r in rows]


@router.post("/api/v1/nl/external-ads")
async def create_external_ad(
    data: ExternalAdCreate,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Создать запись рекламы/самовыкупа с автозаполнением из БД"""
    ad = ExternalAd(
        organization_id=uuid.UUID(org_id),
        vendor_code=data.vendor_code,
        article=data.article,
        photo_url=data.photo_url,
        substitution_url=data.substitution_url,
        utm_url=data.utm_url,
        source=data.source,
        query=data.query,
        reach=data.reach,
        amount=data.amount,
        orders_count=data.orders_count,
        orders_avg_weekly=data.orders_avg_weekly,
        ad_type=data.ad_type or "ad",
        notes=data.notes,
    )

    # Автозаполнение по nm_id
    if data.nm_id:
        ad.nm_id = data.nm_id
        autofill = await auto_fill_from_db(db, org_id, data.nm_id)
        ad.entity_id = autofill.get("entity_id")
        if not ad.vendor_code and autofill.get("vendor_code"):
            ad.vendor_code = autofill["vendor_code"]
        if not ad.photo_url and autofill.get("photo_url"):
            ad.photo_url = autofill["photo_url"]
        if not ad.card_url and autofill.get("card_url"):
            ad.card_url = autofill["card_url"]

    # Дата
    if data.ad_date:
        ad.ad_date = datetime.strptime(data.ad_date, "%Y-%m-%d").date()

    db.add(ad)
    await db.commit()
    await db.refresh(ad)

    return {
        "status": "ok",
        "id": str(ad.id),
        "nm_id": ad.nm_id,
        "photo_url": ad.photo_url,
        "card_url": ad.card_url,
        "vendor_code": ad.vendor_code,
    }


@router.put("/api/v1/nl/external-ads/{ad_id}")
async def update_external_ad(
    ad_id: str,
    data: ExternalAdUpdate,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Обновить запись рекламы/самовыкупа"""
    result = await db.execute(
        select(ExternalAd).where(
            ExternalAd.id == uuid.UUID(ad_id),
            ExternalAd.organization_id == uuid.UUID(org_id),
        )
    )
    ad = result.scalar_one_or_none()
    if not ad:
        raise HTTPException(404, "Запись не найдена")

    update_fields = data.model_dump(exclude_unset=True)

    # Обработка даты
    if "ad_date" in update_fields and update_fields["ad_date"]:
        update_fields["ad_date"] = datetime.strptime(update_fields["ad_date"], "%Y-%m-%d").date()

    # Автозаполнение при смене nm_id
    if "nm_id" in update_fields and update_fields["nm_id"]:
        autofill = await auto_fill_from_db(db, org_id, update_fields["nm_id"])
        if autofill.get("entity_id"):
            ad.entity_id = autofill["entity_id"]
        if not update_fields.get("vendor_code") and autofill.get("vendor_code"):
            ad.vendor_code = autofill["vendor_code"]
        if not update_fields.get("photo_url") and autofill.get("photo_url"):
            ad.photo_url = autofill["photo_url"]
        if autofill.get("card_url"):
            ad.card_url = autofill["card_url"]

    for field, value in update_fields.items():
        if hasattr(ad, field):
            setattr(ad, field, value)

    ad.updated_at = datetime.utcnow()
    await db.commit()

    return {"status": "ok", "id": str(ad.id)}


@router.delete("/api/v1/nl/external-ads/{ad_id}")
async def delete_external_ad(
    ad_id: str,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Удалить запись рекламы/самовыкупа"""
    result = await db.execute(
        select(ExternalAd).where(
            ExternalAd.id == uuid.UUID(ad_id),
            ExternalAd.organization_id == uuid.UUID(org_id),
        )
    )
    ad = result.scalar_one_or_none()
    if not ad:
        raise HTTPException(404, "Запись не найдена")

    await db.delete(ad)
    await db.commit()
    return {"status": "ok", "deleted": str(ad.id)}


@router.get("/api/v1/nl/external-ads/{ad_id}")
async def get_external_ad_detail(
    ad_id: str,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Получить одну запись по ID"""
    result = await db.execute(
        select(ExternalAd).where(
            ExternalAd.id == uuid.UUID(ad_id),
            ExternalAd.organization_id == uuid.UUID(org_id),
        )
    )
    ad = result.scalar_one_or_none()
    if not ad:
        raise HTTPException(404, "Запись не найдена")

    return {
        "id": str(ad.id),
        "entity_id": str(ad.entity_id) if ad.entity_id else None,
        "nm_id": ad.nm_id,
        "vendor_code": ad.vendor_code,
        "article": ad.article,
        "photo_url": ad.photo_url,
        "card_url": ad.card_url,
        "substitution_url": ad.substitution_url,
        "utm_url": ad.utm_url,
        "source": ad.source,
        "query": ad.query,
        "ad_date": str(ad.ad_date) if ad.ad_date else None,
        "reach": ad.reach,
        "amount": float(ad.amount) if ad.amount else None,
        "orders_count": ad.orders_count,
        "orders_avg_weekly": float(ad.orders_avg_weekly) if ad.orders_avg_weekly else None,
        "ad_type": ad.ad_type,
        "notes": ad.notes,
        "created_at": str(ad.created_at) if ad.created_at else None,
        "updated_at": str(ad.updated_at) if ad.updated_at else None,
    }


@router.post("/api/v1/nl/external-ads/bulk-update")
async def bulk_update_external_ads(
    data: dict,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Массовое обновление записей"""
    ids = data.get("ids", [])
    updates = data.get("updates", {})
    if not ids or not updates:
        raise HTTPException(400, "Нужны ids и updates")

    # Дата
    if "ad_date" in updates and isinstance(updates["ad_date"], str):
        updates["ad_date"] = datetime.strptime(updates["ad_date"], "%Y-%m-%d").date()

    count = 0
    for ad_id_str in ids:
        result = await db.execute(
            select(ExternalAd).where(
                ExternalAd.id == uuid.UUID(ad_id_str),
                ExternalAd.organization_id == uuid.UUID(org_id),
            )
        )
        ad = result.scalar_one_or_none()
        if ad:
            for field, value in updates.items():
                if hasattr(ad, field):
                    setattr(ad, field, value)
            ad.updated_at = datetime.utcnow()
            count += 1

    await db.commit()
    return {"status": "ok", "updated": count}


@router.get("/api/v1/nl/external-ads/sources/list")
async def get_ad_sources(
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    """Список уникальных источников для фильтра"""
    result = await db.execute(
        select(ExternalAd.source).where(
            ExternalAd.organization_id == uuid.UUID(org_id),
            ExternalAd.source.isnot(None),
            ExternalAd.source != "",
        ).distinct().order_by(ExternalAd.source)
    )
    return [r[0] for r in result.all()]
