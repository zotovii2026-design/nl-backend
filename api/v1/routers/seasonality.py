import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc, text
from typing import Optional

from core.database import get_db
from core.tenant_auth import require_query_organization_access
from models.keyword_seasonality import WbKeywordSeasonality
from models.product_seasonality import WbProductSeasonality
from schemas.keyword_seasonality import (
    KeywordSeasonalityResponse, 
    KeywordSeasonalityListResponse,
    ProductSeasonalityResponse,
    SeasonalityCollectionStatus
)

_log = logging.getLogger(__name__)
router = APIRouter(dependencies=[Depends(require_query_organization_access)])


@router.get("/api/v1/nl/seasonality/status", response_model=SeasonalityCollectionStatus)
async def get_seasonality_status(org_id: str, db: AsyncSession = Depends(get_db)):
    """Статус сбора сезонности"""
    
    # Общее количество уникальных ключей в reference_book
    total_keywords_result = await db.execute(text("""
        SELECT COUNT(DISTINCT kw)
        FROM (
            SELECT top_query_1 as kw FROM reference_book WHERE organization_id = :org_id AND top_query_1 IS NOT NULL
            UNION SELECT top_query_2 FROM reference_book WHERE organization_id = :org_id AND top_query_2 IS NOT NULL
            UNION SELECT top_query_3 FROM reference_book WHERE organization_id = :org_id AND top_query_3 IS NOT NULL
        ) all_keywords
    """), {"org_id": org_id})
    total_keywords = total_keywords_result.scalar()
    
    # Собраны за последние 7 дней
    collected_result = await db.execute(text("""
        SELECT COUNT(DISTINCT keyword)
        FROM wb_keyword_seasonality
        WHERE organization_id = :org_id
          AND collected_at >= NOW() - INTERVAL '7 days'
    """), {"org_id": org_id})
    collected = collected_result.scalar()
    
    # Всего товаров
    products_total_result = await db.execute(text("""
        SELECT COUNT(DISTINCT nm_id)
        FROM reference_book
        WHERE organization_id = :org_id
    """), {"org_id": org_id})
    products_total = products_total_result.scalar()
    
    # Товаров с сезонностью
    products_with_seasonality_result = await db.execute(text("""
        SELECT COUNT(DISTINCT nm_id)
        FROM wb_product_seasonality
        WHERE organization_id = :org_id
    """), {"org_id": org_id})
    products_with_seasonality = products_with_seasonality_result.scalar()
    
    # Дата последнего сбора
    last_collection_result = await db.execute(text("""
        SELECT MAX(collected_at)
        FROM wb_keyword_seasonality
        WHERE organization_id = :org_id
    """), {"org_id": org_id})
    last_collection_date = last_collection_result.scalar()
    
    # pending = нет данных или старше 7 дней
    pending = total_keywords - collected
    
    # Для failed считаем записи с ошибкой (пока нет поля last_error, ставим 0)
    failed = 0
    
    return SeasonalityCollectionStatus(
        total_keywords=total_keywords,
        collected=collected,
        pending=pending,
        failed=failed,
        last_collection_date=last_collection_date,
        evirma_remaining=None,  # Нужно хранить в отдельной таблице
        evirma_limit=None,
        products_with_seasonality=products_with_seasonality,
        products_total=products_total
    )


@router.get("/api/v1/nl/seasonality/{keyword}", response_model=KeywordSeasonalityResponse)
async def get_keyword_seasonality(keyword: str, org_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(WbKeywordSeasonality)
        .where(WbKeywordSeasonality.keyword == keyword, WbKeywordSeasonality.organization_id == org_id)
        .order_by(desc(WbKeywordSeasonality.collected_at))
        .limit(1)
    )
    seasonality = result.scalar_one_or_none()
    if not seasonality:
        raise HTTPException(status_code=404, detail=f"Сезонность для ключа '{keyword}' не найдена")
    return KeywordSeasonalityResponse.model_validate(seasonality)


@router.get("/api/v1/nl/seasonality", response_model=KeywordSeasonalityListResponse)
async def list_keyword_seasonality(
    org_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db)
):
    offset = (page - 1) * page_size
    count_result = await db.execute(
        select(func.count()).where(WbKeywordSeasonality.organization_id == org_id)
    )
    total = count_result.scalar()
    result = await db.execute(
        select(WbKeywordSeasonality)
        .where(WbKeywordSeasonality.organization_id == org_id)
        .order_by(desc(WbKeywordSeasonality.collected_at))
        .offset(offset)
        .limit(page_size)
    )
    items = result.scalars().all()
    return KeywordSeasonalityListResponse(
        items=[KeywordSeasonalityResponse.model_validate(item) for item in items],
        total=total,
        page=page,
        page_size=page_size
    )


@router.get("/api/v1/nl/seasonality/product/{nm_id}", response_model=ProductSeasonalityResponse)
async def get_product_seasonality(nm_id: int, org_id: str, db: AsyncSession = Depends(get_db)):
    """Получить усреднённую сезонность товара"""
    result = await db.execute(
        select(WbProductSeasonality)
        .where(
            WbProductSeasonality.nm_id == nm_id, 
            WbProductSeasonality.organization_id == org_id
        )
        .order_by(desc(WbProductSeasonality.collected_at))
        .limit(1)
    )
    seasonality = result.scalar_one_or_none()
    if not seasonality:
        raise HTTPException(status_code=404, detail=f"Сезонность для товара nm_id={nm_id} не найдена")
    return ProductSeasonalityResponse.model_validate(seasonality)
