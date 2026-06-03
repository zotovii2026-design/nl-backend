"""Модель тарифов коробной логистики WB (по складам)"""

import uuid
from sqlalchemy import Column, String, DateTime, Numeric, Date, func, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from core.database import Base


class WbBoxTariff(Base):
    """
    Тарифы коробной логистики WB по складам.
    Источник: GET https://common-api.wildberries.ru/api/v1/tariffs/box
    """
    __tablename__ = "wb_box_tariffs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False, index=True
    )

    # Идентификация склада
    warehouse_name = Column(String(255), nullable=False, index=True)
    geo_name = Column(String(255), nullable=True)

    # ФБО-тарифы (доставка со склада WB до клиента)
    box_delivery_base = Column(Numeric(10, 2), nullable=True, comment="ФБО: логистика первый литр, ₽")
    box_delivery_liter = Column(Numeric(10, 2), nullable=True, comment="ФБО: логистика каждый следующий литр, ₽")
    box_delivery_coef = Column(Numeric(10, 2), nullable=True, comment="ФБО: коэффициент логистики, % (уже включён)")

    # ФБС-тарифы (доставка продавцом)
    box_delivery_marketplace_base = Column(Numeric(10, 2), nullable=True, comment="ФБС: логистика первый литр, ₽")
    box_delivery_marketplace_liter = Column(Numeric(10, 2), nullable=True, comment="ФБС: логистика каждый следующий литр, ₽")
    box_delivery_marketplace_coef = Column(Numeric(10, 2), nullable=True, comment="ФБС: коэффициент логистики, % (уже включён)")

    # Хранение
    box_storage_base = Column(Numeric(10, 2), nullable=True, comment="Хранение первый литр/день, ₽")
    box_storage_liter = Column(Numeric(10, 2), nullable=True, comment="Хранение каждый следующий литр/день, ₽")
    box_storage_coef = Column(Numeric(10, 2), nullable=True, comment="Коэффициент хранения, % (уже включён)")

    # Дата тарифов
    snapshot_date = Column(Date, nullable=False, index=True)

    # Мета
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    organization = relationship("Organization")

    __table_args__ = (
        UniqueConstraint(
            "organization_id", "warehouse_name", "snapshot_date",
            name="wb_box_tariffs_org_wh_date_key"
        ),
    )
