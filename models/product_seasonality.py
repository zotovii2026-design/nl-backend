"""Модель сезонности товаров WB (усреднённая по ключевым словам)"""

import uuid
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from core.database import Base


class WbProductSeasonality(Base):
    """Сезонность товара (усреднённая по его ключевым словам)
    
    Профиль сезонности товара рассчитывается как среднее арифметическое
    коэффициентов сезонности всех доступных ключевых слов из reference_book
    (top_query_1, top_query_2, top_query_3).
    """
    __tablename__ = "wb_product_seasonality"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        nullable=True,
        index=True
    )
    
    nm_id = Column(Integer, nullable=False, index=True)
    vendor_code = Column(String(100), nullable=True)
    
    seasonality_coefficients = Column(
        JSONB,
        nullable=False,
        comment="Average seasonality coefficients: month_number -> percentage (1-12)"
    )
    
    source_keywords = Column(
        ARRAY(String),
        nullable=True,
        comment="Source keywords used for calculation"
    )
    
    collected_at = Column(DateTime(timezone=True), default=datetime.utcnow, nullable=False)
    
    __table_args__ = (
        Index(
            "uq_wb_product_seasonality_nm_org_collected",
            "nm_id", "organization_id", "collected_at",
            unique=True
        ),
    )
