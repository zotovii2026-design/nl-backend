"""Модель единого справочника — все ручные вводы селлера"""

import uuid
from datetime import date as date_type
from sqlalchemy import Column, String, DateTime, Integer, Numeric, Date, func, ForeignKey, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from core.database import Base


class ReferenceBook(Base):
    """
    Единый справочник — все данные, которые селлер вводит вручную.
    Одна строка = один размер одного артикула (entity_id).
    
    Ключ: organization_id + entity_id + valid_from
    """
    __tablename__ = "reference_book"

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

    # Идентификация
    nm_id = Column(Integer, nullable=False, index=True)            # Артикул WB
    barcode = Column(String(50), nullable=True)                     # Штрихкод
    vendor_code = Column(String(100), nullable=True)               # Арт поставщика
    size_name = Column(String(50), nullable=True)                   # Размер

    # === СЕБЕСТОИМОСТЬ ===
    cost_price = Column(Numeric(12, 2), default=0)                  # Себестоимость
    purchase_cost = Column(Numeric(12, 2), nullable=True)           # Закупочная цена
    packaging_cost = Column(Numeric(10, 2), nullable=True)          # Упаковка
    logistics_cost = Column(Numeric(10, 2), nullable=True)          # Логистика (до склада)
    other_costs = Column(Numeric(10, 2), nullable=True)             # Прочие расходы
    extra_costs = Column(Numeric(10, 2), nullable=True)             # Доп. затраты
    extra_costs = Column(Numeric(10, 2), nullable=True)             # Доп. затраты
    vat = Column(Numeric(10, 2), default=0)                         # НДС (руб)

    # === МАРКЕТПЛЕЙС ===
    mp_base_pct = Column(Numeric(5, 2), nullable=True)              # Базовый % МП
    mp_correction_pct = Column(Numeric(5, 2), nullable=True)        # Коррекция % МП
    fulfillment_model = Column(String(20), default="fbo")           # ФБО/ФБС
    storage_pct = Column(Numeric(5, 2), nullable=True)              # % хранения (заглушка)

    # === ВЫКУП ===
    buyout_niche_pct = Column(Numeric(5, 2), nullable=True)         # % выкупа ниши

    # === ЦЕНЫ И СКИДКИ (план) ===
    price_before_spp_plan = Column(Numeric(10, 2), nullable=True)   # Цена до СПП план
    price_before_spp_change = Column(Numeric(10, 2), nullable=True) # Цена до СПП к изменению
    change_date = Column(Date, nullable=True)                       # Дата правок
    wb_club_discount_pct = Column(Numeric(5, 2), nullable=True)     # Скидка WB Клуб %

    # === РЕКЛАМА ===
    ad_plan_rub = Column(Numeric(10, 2), nullable=True)             # Реклама план руб

    # === НАЛОГИ ===
    tax_system = Column(String(20), nullable=True)                  # Налоговая: usn / osn / usn_dr
    tax_rate = Column(Numeric(5, 2), nullable=True)                 # Ставка налога %
    vat_rate = Column(Numeric(5, 2), nullable=True)                 # Ставка НДС %

    # === КЛАССИФИКАЦИЯ ===
    product_class = Column(String(100), nullable=True)              # AABBCC класс
    brand = Column(String(200), nullable=True)                      # Бренд
    product_status = Column(String(50), nullable=True)              # Статус товара

    # === ПРОЧЕЕ ===
    valid_from = Column(Date, nullable=False, server_default=func.current_date())  # Действует с
    valid_to = Column(Date, nullable=True)                           # Действует по
    source = Column(String(20), default="manual")                   # Источник: manual / api
    notes = Column(Text, nullable=True)                             # Заметки

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Отношения
    organization = relationship("Organization")
    entity = relationship("ProductEntity")

    __table_args__ = (
        UniqueConstraint(
            "organization_id", "entity_id", "valid_from",
            name="reference_book_org_entity_vf_key"
        ),
    )
