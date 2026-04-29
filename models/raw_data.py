"""Модели для сырых данных WB и технической таблицы состояния"""
import enum
from sqlalchemy import Column, String, DateTime, Integer, Numeric, func, ForeignKey, Text, Date, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from core.database import Base
import uuid


class RawSyncStatus(str, enum.Enum):
    """Статус синка сырых данных"""
    OK = "ok"
    ERROR = "error"
    PARTIAL = "partial"


# ─── СЫРЫЕ ДАННЫЕ (БД) ────────────────────────────────────

class RawApiData(Base):
    """Сырые данные из API WB — единая таблица для всех endpoints"""
    __tablename__ = "raw_api_data"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)

    # Какой endpoint и за какую дату
    api_method = Column(String(100), nullable=False, index=True)  # products, sales, orders, stocks, tariffs, adverts, products_stats
    target_date = Column(Date, nullable=False, index=True)  # Дата за которую собраны данные

    # Сырой ответ
    raw_response = Column(JSONB, nullable=True)  # Полный JSON ответ от WB
    status = Column(String(20), nullable=False, default=RawSyncStatus.OK)  # ok, error, partial
    error_message = Column(Text, nullable=True)
    records_count = Column(Integer, nullable=True)  # Сколько записей в ответе

    # Мета
    fetched_at = Column(DateTime(timezone=True), server_default=func.now())
    is_final = Column(String(5), default="no")  # yes/no — закрыт ли день (15+ дней)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    organization = relationship("Organization")

    __table_args__ = (
        UniqueConstraint("organization_id", "api_method", "target_date", name="raw_api_data_organization_id_api_method_target_date_key"),
    )


class RawBarcode(Base):
    """Штрихкоды товаров — справочник"""
    __tablename__ = "raw_barcodes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)

    nm_id = Column(Integer, nullable=False, index=True)  # Арт WB
    vendor_code = Column(String(100), nullable=True)  # Арт поставщика
    barcode = Column(String(50), nullable=False, index=True)  # Штрихкод
    size_name = Column(String(50), nullable=True)  # Размер (S, M, L...)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    organization = relationship("Organization")

    __table_args__ = (
        UniqueConstraint("organization_id", "barcode", name="raw_barcodes_organization_id_barcode_key"),
    )


class WarehouseRef(Base):
    """Справочник складов WB"""
    __tablename__ = "warehouse_refs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)

    wb_warehouse_id = Column(Integer, nullable=False, unique=True, index=True)  # ID склада WB
    name = Column(String(200), nullable=False)  # Коледино, Электросталь и т.д.

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    organization = relationship("Organization")


# ─── ТЕХНИЧЕСКАЯ ТАБЛИЦА СОСТОЯНИЯ (ТС) ────────────────────

class TechStatus(Base):
    """Техническая таблица состояния — агрегированные данные по дням"""
    __tablename__ = "tech_status"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)

    # Дата строки
    target_date = Column(Date, nullable=False, index=True)

    # 1. Суммарные метрики (products_stats)
    cards_total = Column(Integer, nullable=True)
    cards_archive = Column(Integer, nullable=True)
    cards_draft = Column(Integer, nullable=True)
    cards_active = Column(Integer, nullable=True)

    # 2. Данные по карточке (products)
    nm_id = Column(Integer, nullable=True, index=True)  # Арт WB
    entity_id = Column(UUID(as_uuid=True), ForeignKey("product_entities.id", ondelete="SET NULL"), nullable=True, index=True)  # Сущность (слот размера)
    vendor_code = Column(String(100), nullable=True)  # Арт поставщика
    barcode = Column(String(50), nullable=True)  # Штрихкод
    product_name = Column(String(500), nullable=True)  # Название
    photo_main = Column(String(500), nullable=True)  # Заглавное фото
    photo_count = Column(Integer, nullable=True)  # Кол-во фото
    has_video = Column(String(5), nullable=True)  # yes/no
    description_chars = Column(Integer, nullable=True)  # Символов в описании
    rating = Column(Numeric(3, 2), nullable=True)  # Рейтинг
    sku = Column(String(50), nullable=True)  # SKU

    # 3. Торговля (sales, orders)
    orders_count = Column(Integer, nullable=True)
    buyouts_count = Column(Integer, nullable=True)
    returns_count = Column(Integer, nullable=True)
    impressions = Column(Integer, nullable=True)  # Показы
    clicks = Column(Integer, nullable=True)  # Клики

    # 4. Логистика и деньги
    warehouse_name = Column(String(200), nullable=True)  # Склад
    stock_qty = Column(Integer, nullable=True)  # Остаток
    tariff = Column(Numeric(10, 2), nullable=True)  # Тариф склада
    price = Column(Numeric(10, 2), nullable=True)  # Цена
    price_discount = Column(Numeric(10, 2), nullable=True)  # Цена после скидки
    price_spp = Column(Numeric(10, 2), nullable=True)  # Цена с СПП
    ad_cost = Column(Numeric(10, 2), nullable=True)  # Рекламный расход

    # Статусы (цветовая индикация)
    row_status = Column(String(20), nullable=False, default="active")  # closed(🟢), active(🟡), error(🔴)

    # Статусы по ячейкам (JSON: {"orders": "green", "sales": "red", ...})
    cell_statuses = Column(JSONB, nullable=True)

    # Мета
    last_sync_at = Column(DateTime(timezone=True), nullable=True)
    is_final = Column(String(5), default="no")  # yes = 15+ дней, данные закрыты
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    organization = relationship("Organization")

    __table_args__ = (
        UniqueConstraint("organization_id", "target_date", "entity_id", name="tech_status_org_date_entity_key"),
        # Старый constraint оставлен для совместимости — будет заменён миграцией,
    )
