"""Модели для хранения данных Wildberries"""
import enum
from sqlalchemy import Column, String, DateTime, Integer, Numeric, func, ForeignKey, JSON, Text, Boolean
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from core.database import Base
import uuid


class OrderStatus(str, enum.Enum):
    """Статус заказа"""
    NEW = "new"
    CONFIRMED = "confirmed"
    IN_PROGRESS = "in_progress"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"
    RETURNED = "returned"


class WbProduct(Base):
    """Товары Wildberries"""
    __tablename__ = "wb_products"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    
    # Поля из WB API
    nm_id = Column(Integer, unique=True, nullable=False, index=True)  # Артикул WB
    vendor_code = Column(String(100), nullable=True, index=True)  # Артикул продавца
    name = Column(String(500), nullable=False)  # Название товара
    description = Column(Text, nullable=True)  # Описание
    brand = Column(String(100), nullable=True)  # Бренд
    subject = Column(String(200), nullable=True)  # Категория
    
    # Цены и остатки
    price = Column(Numeric(10, 2), nullable=True)  # Цена
    discount = Column(Integer, nullable=True)  # Скидка в процентах
    stock = Column(Integer, nullable=True)  # Остаток на складе
    
    # Маркировка Chestnyznak
    need_kiz = Column(Boolean, nullable=True, default=False)  # Требуется ли код маркировки
    kiz_marked = Column(Boolean, nullable=True, default=False)  # Подтверждено ли нанесение кода
    
    # Фото и размеры
    photo_url = Column(String(500), nullable=True)  # URL главного фото
    photos = Column(JSON, nullable=True)  # Список URL фотографий
    sizes = Column(JSON, nullable=True)  # Размеры товара
    characteristics = Column(JSON, nullable=True)  # Характеристики
    
    # Даты
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    synced_at = Column(DateTime(timezone=True), server_default=func.now())  # Дата последней синхронизации

    # Отношения
    organization = relationship("Organization")

    __table_args__ = (
        {"schema": None},
    )


class WbSale(Base):
    """Продажи Wildberries"""
    __tablename__ = "wb_sales"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    
    # Уникальный идентификатор продажи (для deduplication)
    sale_id = Column(String(100), unique=True, nullable=False, index=True)
    
    # Период отчёта
    date_from = Column(DateTime(timezone=True), nullable=False, index=True)
    date_to = Column(DateTime(timezone=True), nullable=False, index=True)
    
    # Финансы
    income = Column(Numeric(12, 2), nullable=True)  # Доход
    penalty = Column(Numeric(12, 2), nullable=True)  # Штрафы
    reward = Column(Numeric(12, 2), nullable=True)  # Вознаграждение
    
    # Детали продажи
    g_number = Column(String(50), nullable=True)  # Номер заказа
    subject = Column(String(200), nullable=True)  # Категория товара
    brand = Column(String(100), nullable=True)  # Бренд
    tech_size = Column(String(50), nullable=True)  # Размер
    
    # Количество и цена
    quantity = Column(Integer, nullable=True)  # Количество проданных штук
    total_price = Column(Numeric(12, 2), nullable=True)  # Общая цена продажи
    price_with_disc = Column(Numeric(10, 2), nullable=True)  # Цена с учётом скидки
    
    # Склад и регион
    region_name = Column(String(200), nullable=True)  # Регион доставки
    warehouse_name = Column(String(200), nullable=True)  # Склад
    
    # Метаданные
    nm_id = Column(Integer, nullable=True, index=True)  # Артикул товара (ссылка на WbProduct)
    supplier_oper_name = Column(String(200), nullable=True)  # Название операции
    
    # Даты
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    synced_at = Column(DateTime(timezone=True), server_default=func.now())

    # Отношения
    organization = relationship("Organization")

    __table_args__ = (
        {"schema": None},
    )


class WbOrder(Base):
    """Заказы Wildberries"""
    __tablename__ = "wb_orders"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    
    # Уникальный идентификатор заказа (для deduplication)
    order_id = Column(String(100), unique=True, nullable=False, index=True)
    
    # Информация о заказе
    g_number = Column(String(50), nullable=True, index=True)  # Номер заказа
    order_type = Column(Integer, nullable=True)  # Тип заказа
    date = Column(DateTime(timezone=True), nullable=False, index=True)  # Дата заказа
    last_change_date = Column(DateTime(timezone=True), nullable=True)  # Дата последнего изменения
    
    # Статус
    status = Column(String(50), nullable=False, index=True)  # Статус заказа
    
    # Товар
    nm_id = Column(Integer, nullable=True, index=True)  # Артикул товара
    subject = Column(String(200), nullable=True)  # Категория
    brand = Column(String(100), nullable=True)  # Бренд
    
    # Размер и цена
    tech_size = Column(String(50), nullable=True)  # Размер
    price = Column(Numeric(10, 2), nullable=True)  # Цена товара
    total_price = Column(Numeric(12, 2), nullable=True)  # Общая стоимость
    
    # Количество
    quantity = Column(Integer, nullable=True)  # Количество
    barcode = Column(String(50), nullable=True)  # Штрих-код
    
    # Склад и доставка
    warehouse_name = Column(String(200), nullable=True)  # Склад
    region_name = Column(String(200), nullable=True)  # Регион доставки
    delivery_type = Column(String(50), nullable=True)  # Тип доставки
    
    # Станция метро
    is_supply = Column(String(10), nullable=True)  # Принадлежность к поставке
    is_storno = Column(String(10), nullable=True)  # Признак отмены
    
    # Даты
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    synced_at = Column(DateTime(timezone=True), server_default=func.now())

    # Отношения
    organization = relationship("Organization")

    __table_args__ = (
        {"schema": None},
    )


class SyncLog(Base):
    """Логи синхронизации"""
    __tablename__ = "sync_logs"
    
    # Используем extend_existing=True для избежания конфликта с существующей таблицей
    __table_args__ = ({"extend_existing": True},)

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    
    # Тип синхронизации (соответствует task_name в миграции)
    task_name = Column(String(100), nullable=False, index=True)
    
    # Статус
    status = Column(String(50), nullable=False, index=True)  # running, success, error
    
    # Детали
    synced_count = Column(Integer, nullable=True)  # Количество обработанных записей (соответствует synced_count в миграции)
    error_message = Column(Text, nullable=True)  # Сообщение об ошибке
    
    # Даты
    started_at = Column(DateTime(timezone=True), nullable=True)
    finished_at = Column(DateTime(timezone=True), nullable=True)
    duration_seconds = Column(Integer, nullable=True)  # Длительность в секундах
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Отношения
    organization = relationship("Organization")
