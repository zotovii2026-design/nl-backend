"""Модель справочного листа — данные пользователя"""
from sqlalchemy import Column, String, DateTime, Integer, Numeric, Date, func, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID
from core.database import Base
import uuid


class ReferenceSheet(Base):
    """Справочный лист — себестоимость и прочие данные от пользователя"""
    __tablename__ = "reference_sheet"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    target_date = Column(Date, nullable=False, default=func.current_date())

    nm_id = Column(Integer, nullable=False)  # Арт WB
    vendor_code = Column(String(100))  # Арт поставщика
    product_name = Column(String(500))  # Название

    # Данные пользователя
    cost_price = Column(Numeric(12, 2))  # Себестоимость
    purchase_price = Column(Numeric(12, 2))  # Закупочная цена
    packaging_cost = Column(Numeric(10, 2))  # Упаковка
    logistics_cost = Column(Numeric(10, 2))  # Логистика
    other_costs = Column(Numeric(10, 2))  # Прочие расходы
    notes = Column(Text)  # Заметки

    # Юнит Экономика — новые поля
    product_class = Column(String(100))  # Класс товара
    brand = Column(String(200))  # Бренд
    tax_system = Column(String(20))  # Налоговая система: usn / osn / usn_dr
    tax_rate = Column(Numeric(5, 2))  # Ставка налога %
    vat_rate = Column(Numeric(5, 2))  # Ставка НДС % (для ОСН)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
