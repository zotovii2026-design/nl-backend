import logging
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, desc

from core.database import get_db
from core.tenant_auth import require_query_organization_access
from models.keyword_seasonality import WbKeywordSeasonality
from schemas.keyword_seasonality import KeywordSeasonalityResponse, KeywordSeasonalityListResponse

_log = logging.getLogger(__name__)
router = APIRouter(dependencies=[Depends(require_query_organization_access)])


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
