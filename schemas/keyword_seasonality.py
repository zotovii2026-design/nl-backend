"""Схемы для сезонности ключевых слов"""

from pydantic import BaseModel, Field
from typing import Optional, List, Any
from datetime import datetime
from uuid import UUID


class KeywordSeasonalityResponse(BaseModel):
    """Сезонность ключевого слова"""
    id: UUID
    organization_id: Optional[UUID] = None
    keyword: str
    normquery: Optional[str] = None
    freq365: Optional[int] = None
    freq_monthly: Optional[int] = None
    freq_weekly: Optional[int] = None
    weekly_trend: Optional[int] = None
    growth_rate: Optional[float] = None
    product_count: Optional[int] = None
    wb_subject_id: Optional[int] = None
    wb_subject_name: Optional[str] = None
    freq_history_monthly: Optional[List[List[Any]]] = None
    freq_history_weekly: Optional[List[List[Any]]] = None
    source: str = 'evirma'
    collected_at: datetime

    class Config:
        from_attributes = True


class KeywordSeasonalityListResponse(BaseModel):
    """Список сезонности с пагинацией"""
    items: List[KeywordSeasonalityResponse]
    total: int
    page: int
    page_size: int
