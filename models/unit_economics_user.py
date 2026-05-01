"""Модель для ручных вводов Юнит Экономики"""
from sqlalchemy import Column, String, DateTime, Integer, Numeric, Date, func, ForeignKey, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from core.database import Base
import uuid


class UnitEconomicsUser(Base):
    """Ручные вводы пользователя для Юнит Экономики по entity (сущности размера)"""
    __tablename__ = "unit_economics_user"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)

    # Идентификация — entity_id приоритет, nm_id для совместимости
    entity_id = Column(UUID(as_uuid=True), ForeignKey("product_entities.id", ondelete="SET NULL"), nullable=True, index=True)
    nm_id = Column(Integer, nullable=False, index=True)  # Арт WB
    barcode = Column(String(50), nullable=True)  # Штрихкод (для размера)
    size_name = Column(String(50), nullable=True)  # Размер

    # Ручные вводы
    mp_correction_pct = Column(Numeric(5, 2))  # Коррекция % МП (+/-)
    buyout_niche_pct = Column(Numeric(5, 2))  # % выкупа ниши
    extra_costs = Column(Numeric(10, 2))  # Доп. затраты
    ad_plan_rub = Column(Numeric(10, 2))  # Реклама план ₽
    price_before_spp_plan = Column(Numeric(10, 2))  # Цена до СПП план
    price_before_spp_change = Column(Numeric(10, 2))  # Цена до СПП к изменению
    change_date = Column(Date)  # Дата правок
    tariff_type = Column(String(20), default="box")  # Тип тарифа: box / pallet
    wb_club_discount_pct = Column(Numeric(5, 2))  # Скидка WB Клуб %

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("organization_id", "entity_id", name="unit_economics_user_org_entity_key"),
    )
