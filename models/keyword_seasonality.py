"""Модель сезонности ключевых слов WB (Evirma API)"""

import uuid
from datetime import datetime
from sqlalchemy import Column, String, Integer, Numeric, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from core.database import Base


class WbKeywordSeasonality(Base):
    """Сезонность ключевых слов WB из Evirma API
    
    Данные о частоте запросов, трендах, истории по месяцам и неделям.
    Источник: Evirma (расширение для WB)
    """
    __tablename__ = "wb_keyword_seasonality"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        nullable=True,
        index=True
    )
    
    keyword = Column(String(500), nullable=False, index=True)
    normquery = Column(String(500), nullable=True)
    
    freq365 = Column(Integer, nullable=True)
    freq_monthly = Column(Integer, nullable=True)
    freq_weekly = Column(Integer, nullable=True)
    weekly_trend = Column(Integer, nullable=True)
    growth_rate = Column(Numeric(5, 3), nullable=True)
    product_count = Column(Integer, nullable=True)
    
    wb_subject_id = Column(Integer, nullable=True)
    wb_subject_name = Column(String(200), nullable=True)
    
    freq_history_monthly = Column(JSONB, nullable=True)
    freq_history_weekly = Column(JSONB, nullable=True)
    
    seasonality_coefficients = Column(
        JSONB,
        nullable=True,
        comment="Coefficients: month_number -> percentage (1-12)"
    )
    
    source = Column(String(50), default="evirma")
    collected_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        Index(
            "uq_wb_keyword_seasonality_keyword_source_collected",
            "keyword", "source", "collected_at",
            unique=True
        ),
    )
