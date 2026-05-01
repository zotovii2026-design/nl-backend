"""Модель автоматических WB-данных (тарифы, цены, комиссия)"""

import uuid
from sqlalchemy import Column, String, DateTime, Integer, Numeric, Date, func, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from core.database import Base


class WbTariffSnapshot(Base):
    """
    Снимок WB-данных на дату — автоматически подтягивается по API.
    Одна строка = один nm_id/entity_id на конкретную дату.
    """
    __tablename__ = "wb_tariff_snapshot"

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
    nm_id = Column(Integer, nullable=False, index=True)

    target_date = Column(Date, nullable=False, index=True)

    # Цены
    price_retail = Column(Numeric(10, 2), nullable=True)        # Розничная цена (до СПП)
    price_with_spp = Column(Numeric(10, 2), nullable=True)      # Цена с СПП (продажная)
    spp_pct = Column(Numeric(5, 2), nullable=True)               # СПП %
    discount_pct = Column(Numeric(5, 2), nullable=True)          # Скидка %

    # Комиссия МП
    commission_pct = Column(Numeric(5, 2), nullable=True)        # % комиссии МП (из тарифов)

    # Логистика
    logistics_tariff = Column(Numeric(10, 2), nullable=True)     # Тариф логистики (среднее 3 склада)
    logistics_base = Column(Numeric(10, 2), nullable=True)       # Базовый тариф логистики

    # Хранение
    storage_tariff = Column(Numeric(10, 2), nullable=True)       # Тариф хранения в день
    storage_base = Column(Numeric(10, 2), nullable=True)         # Базовый тариф хранения

    # Приёмка
    acceptance_avg_90d = Column(Numeric(10, 2), nullable=True)   # Средняя приёмка за 90 дней

    # Реклама
    ad_cost_fact = Column(Numeric(10, 2), nullable=True)         # Рекламный расход факт

    # % выкупа
    buyout_pct_fact = Column(Numeric(5, 2), nullable=True)       # % выкупа факт (за 30 дней)

    # WB Клуб
    wb_club_price = Column(Numeric(10, 2), nullable=True)        # Цена с WB Клуб

    # Мета
    fetched_at = Column(DateTime(timezone=True), server_default=func.now())

    organization = relationship("Organization")

    __table_args__ = (
        UniqueConstraint(
            "organization_id", "nm_id", "target_date",
            name="wb_tariff_snapshot_org_nm_date_key"
        ),
    )
