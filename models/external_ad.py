"""Модель внешней рекламы и самовыкупов"""

import uuid
from sqlalchemy import Column, String, DateTime, Integer, Numeric, Date, Text, func, ForeignKey, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from core.database import Base


class ExternalAd(Base):
    """
    Внешняя реклама и самовыкупы.
    Одна строка = одно рекламное размещение / самовыкуп.
    """
    __tablename__ = "external_ads"

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

    # === ИДЕНТИФИКАЦИЯ ТОВАРА ===
    nm_id = Column(Integer, nullable=True, index=True)             # Артикул WB (авто из БД)
    vendor_code = Column(String(100), nullable=True)               # Арт поставщика
    article = Column(String(100), nullable=True)                   # Артикул (ввод пользователя)
    photo_url = Column(String(1000), nullable=True)                # Фото товара (авто из БД)

    # === ССЫЛКИ ===
    card_url = Column(String(1000), nullable=True)                 # Ссылка на карточку WB (авто)
    substitution_url = Column(String(1000), nullable=True)         # Подменная ссылка
    utm_url = Column(String(1000), nullable=True)                  # Ссылка с UTM

    # === ПАРАМЕТРЫ РЕКЛАМЫ ===
    source = Column(String(200), nullable=True)                    # Источник (канал, блогер и т.д.)
    query = Column(String(500), nullable=True)                     # Поисковый запрос
    ad_date = Column(Date, nullable=True, index=True)              # Дата размещения

    # === МЕТРИКИ ===
    reach = Column(Integer, nullable=True)                         # Охват
    amount = Column(Numeric(12, 2), nullable=True)                 # Сумма затрат
    orders_count = Column(Integer, nullable=True)                  # Кол-во заказов
    orders_avg_weekly = Column(Numeric(10, 2), nullable=True)      # Заказов в среднем за неделю

    # === ТИП ЗАПИСИ ===
    ad_type = Column(String(20), default="ad")                     # ad / buyout

    # === СЛУЖЕБНЫЕ ===
    notes = Column(Text, nullable=True)                            # Заметки
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Отношения
    organization = relationship("Organization")
    entity = relationship("ProductEntity")
