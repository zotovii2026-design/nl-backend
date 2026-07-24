"""Prices refresh API routes."""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import Optional
from datetime import datetime, timezone

from core.tenant_auth import require_query_organization_access
from core.database import get_db
from services.reference import resolve_org_id

router = APIRouter(
    tags=["nl"],
    dependencies=[Depends(require_query_organization_access)],
)

PRICES_REFRESH_COOLDOWN = 300  # 5 минут

# ─── ОБНОВЛЕНИЕ ЦЕН ИЗ WB API ─────────────────────────────

# Кулдаун: минимальный интервал между обновлениями цен (секунды)
PRICES_REFRESH_COOLDOWN = 15 * 60  # 15 минут


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@router.post("/api/v1/nl/prices/refresh")
async def refresh_prices_from_wb(org_id: str, db: AsyncSession = Depends(get_db)):
    """
    Обновить цены из WB Prices API и сохранить в reference_book.
    
    Тянет discountedPrice (цена со скидкой, реально на витрине),
    price (цена до скидки), discount (скидка %).
    
    Кулдаун 15 мин — защита от бана WB API.
    """
    from services.wb_api.keys import get_all_wb_keys as _get_keys
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    from core.config import settings
    from datetime import datetime as _dt, timezone as _tz
    
    org_id = await resolve_org_id(org_id, db)
    
    # Проверяем кулдаун — когда последний раз обновляли цены
    cooldown_sql = "SELECT MAX(wb_prices_updated_at) FROM reference_book WHERE organization_id = :org AND wb_prices_updated_at IS NOT NULL"
    cooldown_result = await db.execute(text(cooldown_sql), {"org": org_id})
    last_update_row = cooldown_result.first()
    last_update = last_update_row[0] if last_update_row else None
    
    if last_update:
        now_utc = _dt.now(_tz.utc)
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=_tz.utc)
        elapsed = (now_utc - last_update).total_seconds()
        remaining = PRICES_REFRESH_COOLDOWN - elapsed
        if remaining > 0:
            mins = int(remaining // 60)
            secs = int(remaining % 60)
            raise HTTPException(429, f"Кулдаун. Доступно через {mins}:{secs:02d}")
    
    # Получаем API ключи
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    try:
        all_keys = await _get_keys(sf)
    finally:
        await engine.dispose()
    
    # Находим ключ для этой организации
    api_key = None
    for oid, key in all_keys:
        if oid == org_id:
            api_key = key
            break
    
    if not api_key:
        raise HTTPException(400, "Нет WB API ключа для этой организации")
    
    # Запрашиваем цены из WB API
    from services.wb_api.client import WBApiClient
    
    try:
        async with WBApiClient(api_key) as client:
            prices_data = await client.get_all_prices()
    except Exception as e:
        raise HTTPException(502, f"Ошибка WB API: {str(e)}")
    
    items = prices_data if isinstance(prices_data, list) else prices_data.get("items", [])
    if not items:
        raise HTTPException(404, "WB API вернул пустой список товаров")
    
    entity_result = await db.execute(text("""
        SELECT id, nm_id, chrt_id
        FROM product_entities
        WHERE organization_id = :org
    """), {"org": org_id})
    entity_by_nm_chrt = {}
    for row in entity_result.all():
        nm_key = _safe_int(row[1])
        chrt_key = _safe_int(row[2])
        if nm_key is not None and chrt_key is not None:
            entity_by_nm_chrt[(nm_key, chrt_key)] = str(row[0])

    # Строим маппинг entity_id/nm_id -> цены
    price_map = {}
    for item in items:
        nm_id = item.get("nmID") or item.get("nmId") or item.get("nm_id")
        if not nm_id:
            continue
        nm_id = int(nm_id)
        discount = item.get("discount", 0)
        sizes = item.get("sizes", [])
        for sz in sizes:
            chrt_key = _safe_int(sz.get("chrtID") or sz.get("chrtId") or sz.get("sizeID"))
            entity_id = entity_by_nm_chrt.get((nm_id, chrt_key)) if chrt_key is not None else None
            price_retail = float(sz.get("price", 0))
            price_fact = float(sz.get("discountedPrice", 0))
            if price_retail > 0:
                price_map[entity_id or nm_id] = {
                    "entity_id": entity_id,
                    "nm_id": nm_id,
                    "price_retail": price_retail,
                    "price_fact": price_fact,
                    "discount": discount,
                }
    
    # Обновляем reference_book
    now = _dt.now(_tz.utc)
    updated_count = 0
    
    for prices in price_map.values():
        if prices["entity_id"]:
            update_sql = (
                "UPDATE reference_book "
                "SET wb_price_fact = :pf, "
                "    wb_price_retail = :pr, "
                "    wb_discount_pct = :disc, "
                "    wb_prices_updated_at = :now "
                "WHERE organization_id = :org "
                "  AND entity_id = :entity "
                "  AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)"
            )
            params = {"entity": prices["entity_id"]}
        else:
            update_sql = (
                "UPDATE reference_book "
                "SET wb_price_fact = :pf, "
                "    wb_price_retail = :pr, "
                "    wb_discount_pct = :disc, "
                "    wb_prices_updated_at = :now "
                "WHERE organization_id = :org "
                "  AND nm_id = :nm "
                "  AND (valid_to IS NULL OR valid_to >= CURRENT_DATE)"
            )
            params = {"nm": prices["nm_id"]}
        params.update({
            "pf": prices["price_fact"],
            "pr": prices["price_retail"],
            "disc": prices["discount"],
            "now": now,
            "org": org_id,
        })
        result = await db.execute(text(update_sql), params)
        updated_count += result.rowcount
    
    await db.commit()
    
    return {
        "ok": True,
        "updated": updated_count,
        "total_items": len(items),
        "total_with_prices": len(price_map),
        "updated_at": now.isoformat(),
        "cooldown_seconds": PRICES_REFRESH_COOLDOWN,
    }


@router.get("/api/v1/nl/prices/last-refresh")
async def get_last_prices_refresh(org_id: str, db: AsyncSession = Depends(get_db)):
    """Когда последний раз обновляли цены из WB API"""
    org_id = await resolve_org_id(org_id, db)
    last_sql = "SELECT MAX(wb_prices_updated_at) FROM reference_book WHERE organization_id = :org AND wb_prices_updated_at IS NOT NULL"
    result = await db.execute(text(last_sql), {"org": org_id})
    row = result.first()
    last_update = row[0] if row else None
    
    remaining = 0
    if last_update:
        from datetime import datetime as _dt2, timezone as _tz2
        now_utc = _dt2.now(_tz2.utc)
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=_tz2.utc)
        elapsed = (now_utc - last_update).total_seconds()
        if elapsed < PRICES_REFRESH_COOLDOWN:
            remaining = int(PRICES_REFRESH_COOLDOWN - elapsed)
    
    return {
        "last_update": last_update.isoformat() if last_update else None,
        "cooldown_remaining_seconds": remaining,
        "can_refresh": remaining == 0,
    }
