"""Схемы для сезонности ключевых слов и товаров"""

from pydantic import BaseModel, Field
from typing import Optional, List, Any, Dict
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
    seasonality_coefficients: Optional[Dict[str, float]] = Field(None, description="Коэффициенты сезонности по месяцам (1-12)")
    source: str = "evirma"
    collected_at: datetime

    class Config:
        from_attributes = True


class KeywordSeasonalityListResponse(BaseModel):
    """Список сезонности с пагинацией"""
    items: List[KeywordSeasonalityResponse]
    total: int
    page: int
    page_size: int


class ProductSeasonalityResponse(BaseModel):
    """Сезонность товара (усреднённая по ключевым словам)"""
    id: UUID
    organization_id: Optional[UUID] = None
    nm_id: int
    vendor_code: Optional[str] = None
    seasonality_coefficients: Dict[str, float] = Field(..., description="Коэффициенты сезонности по месяцам (1-12)")
    source_keywords: Optional[List[str]] = Field(None, description="Ключевые слова, по которым считали профиль")
    collected_at: datetime

    class Config:
        from_attributes = True


class SeasonalityCollectionStatus(BaseModel):
    """Статус сбора сезонности"""
    total_keywords: int
    collected: int
    pending: int
    failed: int
    last_collection_date: Optional[datetime] = None
    evirma_remaining: Optional[int] = None
    evirma_limit: Optional[int] = None
    products_with_seasonality: int
    products_total: int
