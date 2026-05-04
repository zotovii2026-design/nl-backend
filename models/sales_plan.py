"""Модель плана продаж — плановые и фактические показатели по товарам"""

import uuid
from datetime import date as date_type
from sqlalchemy import Column, String, DateTime, Integer, Numeric, Date, Enum, func, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from core.database import Base
import enum


class PlanType(str, enum.Enum):
    """Тип плана: штуки или сумма"""
    quantity = "quantity"
    revenue = "revenue"


class Seasonality(str, enum.Enum):
    """Сезонность товара"""
    low = "low"
    medium = "medium"
    high = "high"
    peak = "peak"


class SalesPlan(Base):
    """
    План продаж.
    Одна строка = один товар (entity_id) за один период (месяц).
    """
    __tablename__ = "sales_plans"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    entity_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_entities.id", ondelete="SET NULL"),
        nullable=True, index=True
    )

    # Идентификация товара (денормализовано для быстрого доступа)
    nm_id = Column(Integer, nullable=False, index=True)
    vendor_code = Column(String(100), nullable=True)
    size_name = Column(String(50), nullable=True)

    # Период
    period = Column(Date, nullable=False, index=True)  # Первый день месяца

    # Тип и значения плана
    plan_type = Column(Enum(PlanType), default=PlanType.quantity, nullable=False)
    plan_value = Column(Numeric(12, 2), default=0, nullable=False)  # Плановое значение
    actual_value = Column(Numeric(12, 2), default=0, nullable=False)  # Фактическое значение

    # Темп продаж (шт/день или ₽/день)
    sales_temp = Column(Numeric(10, 2), nullable=True)

    # Сезонность
    seasonality = Column(Enum(Seasonality), default=Seasonality.medium, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Отношения
    organization = relationship("Organization")
    entity = relationship("ProductEntity")

    __table_args__ = (
        UniqueConstraint(
            "organization_id", "entity_id", "period", "plan_type",
            name="sales_plans_org_entity_period_type_key"
        ),
    )
