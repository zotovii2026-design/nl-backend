"""Модель единого справочника — все ручные вводы селлера"""

import uuid
from datetime import date as date_type
from sqlalchemy import Boolean, Column, String, DateTime, Integer, Numeric, Date, func, ForeignKey, Text, UniqueConstraint
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

    # === НДС ===
    vat_rate = Column(Numeric(5, 2), nullable=True)              # НДС от дохода (нет/5%/7%)

    # === КАТЕГОРИЯ WB ===
    subject_id = Column(Integer, nullable=True)                      # ID категории WB (subjectID)
    subject_name = Column(String(200), nullable=True)                # Название категории WB (subjectName)

    # === СЕБЕСТОИМОСТЬ ===
    cost_price = Column(Numeric(12, 2), default=0)                  # Себестоимость
    purchase_cost = Column(Numeric(12, 2), nullable=True)           # Закупочная цена
    packaging_cost = Column(Numeric(10, 2), nullable=True)          # Упаковка
    logistics_cost = Column(Numeric(10, 2), nullable=True)          # Логистика (до склада)
    other_costs = Column(Numeric(10, 2), nullable=True)             # Прочие расходы
    extra_costs = Column(Numeric(10, 2), nullable=True)             # Доп. затраты
    vat = Column(Numeric(10, 2), default=0)                         # НДС (руб)
    min_price = Column(Numeric(12, 2), nullable=True)                # Минимальная цена продажи
    rrc_price = Column(Numeric(12, 2), nullable=True)                # РРЦ (рекомендованная розничная цена)

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

    # === ЦЕНЫ WB API (факт) ===
    wb_price_fact = Column(Numeric(12, 2), nullable=True)          # Цена со скидкой (discountedPrice из WB API)
    wb_price_retail = Column(Numeric(12, 2), nullable=True)        # Цена до скидки (price из WB API)
    wb_discount_pct = Column(Integer, nullable=True)                # Скидка WB % из API
    wb_prices_updated_at = Column(DateTime(timezone=True), nullable=True)  # Когда обновлено из API

    # === РЕКЛАМА ===
    ad_plan_rub = Column(Numeric(10, 2), nullable=True)             # Реклама план руб

    # === ПОСТАВКИ FBO ===
    supply_days = Column(Integer, nullable=True)                        # Срок поставки (дни)
    min_batch_fbo = Column(Integer, nullable=True)                      # Мин. партия FBO (шт)
    transport_pack_qty = Column(Integer, nullable=True, default=1)      # Количество в транспортной упаковке

    # === НАЛОГИ ===
    tax_system = Column(String(20), nullable=True)                  # Налоговая: usn / osn / usn_dr
    tax_rate = Column(Numeric(5, 2), nullable=True, default=0)          # Ставка налога % (переопределение строки)

    # === КЛАССИФИКАЦИЯ ===
    product_class = Column(String(100), nullable=True)              # AABBCC класс
    brand = Column(String(200), nullable=True)                      # Бренд
    product_status = Column(String(50), nullable=True)              # Статус товара

    # === СЕЗОННОСТЬ (индекс по месяцам) ===
    season_jan = Column(Numeric(5, 2), nullable=True)
    season_feb = Column(Numeric(5, 2), nullable=True)
    season_mar = Column(Numeric(5, 2), nullable=True)
    season_apr = Column(Numeric(5, 2), nullable=True)
    season_may = Column(Numeric(5, 2), nullable=True)
    season_jun = Column(Numeric(5, 2), nullable=True)
    season_jul = Column(Numeric(5, 2), nullable=True)
    season_aug = Column(Numeric(5, 2), nullable=True)
    season_sep = Column(Numeric(5, 2), nullable=True)
    season_oct = Column(Numeric(5, 2), nullable=True)
    season_nov = Column(Numeric(5, 2), nullable=True)
    season_dec = Column(Numeric(5, 2), nullable=True)

    # === ГАБАРИТЫ ПЛАН (ручной ввод) ===
    plan_length = Column(Numeric(8, 2), nullable=True)              # Длина, см
    plan_width = Column(Numeric(8, 2), nullable=True)               # Ширина, см
    plan_height = Column(Numeric(8, 2), nullable=True)              # Высота, см
    plan_volume = Column(Numeric(8, 2), nullable=True)              # Объём, литр
    plan_weight = Column(Numeric(8, 2), nullable=True)              # Вес, гр

    # === СКОРОСТЬ ДОСТАВАЕМОСТИ ===
    delivery_days_to_seller = Column(Integer, nullable=True)        # От закупа до склада поставщика (дни)
    delivery_days_to_mp = Column(Integer, nullable=True)            # От склада поставщика до МП (дни)

    # === ТОП ЗАПРОСЫ ПЛАНИРУЕМЫЕ ===
    top_query_1 = Column(String(200), nullable=True)
    top_query_2 = Column(String(200), nullable=True)
    top_query_3 = Column(String(200), nullable=True)

    # === ОТГРУЗКА ===
    shipment_method = Column(String(50), nullable=True)             # Приоритетный способ отгрузки
    fbs_warehouse = Column(String(200), nullable=True)              # Склад отгрузки FBS

    # === РНП ===
    in_promo = Column(Boolean, nullable=True, default=False)                     # В акции (да/нет)
    ad_shows_organic = Column(Integer, nullable=True)                            # Показы органика (заглушка)
    ad_shows_paid = Column(Integer, nullable=True)                               # Показы рекламные
    ad_strategy = Column(String(200), nullable=True)                             # Стратегия РК (ручной)
    tags = Column(Text, nullable=True)                                           # Теги/вехи
    rating_reviews = Column(Numeric(3, 2), nullable=True)                        # Рейтинг по отзывам (заглушка)
    localization_pct = Column(String(50), nullable=True)

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
            "organization_id", "nm_id", "entity_id", "valid_from",
            name="reference_book_org_nm_eid_vf_key"
        ),
    )
