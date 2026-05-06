"""Модели сущностей товаров — стабильные слоты размеров с историей ШК"""
import uuid
from datetime import date as date_type
from sqlalchemy import Column, String, DateTime, Integer, Boolean, Date, func, ForeignKey, UniqueConstraint, Text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from core.database import Base


class ProductEntity(Base):
    """
    Сущность — стабильный слот размера товара.
    Один nm_id + один размер = одна сущность.
    ШК могут меняться, сущность — нет.
    """
    __tablename__ = "product_entities"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    nm_id = Column(Integer, nullable=False, index=True)          # Артикул WB
    vendor_code = Column(String(100), nullable=True)             # Арт поставщика
    size_name = Column(String(50), nullable=False)               # Размер (S, M, L, 42, ONE SIZE...)
    product_name = Column(String(500), nullable=True)            # Название товара
    photo_main = Column(String(500), nullable=True)              # Главное фото

    # Данные из WB Content API
    brand = Column(String(200), nullable=True)                   # Бренд
    subject_name = Column(String(300), nullable=True)            # Категория (предмет)
    tnved = Column(String(50), nullable=True)                    # Код ТНВЭД
    color = Column(String(200), nullable=True)                   # Цвет
    weight = Column(Integer, nullable=True)                      # Вес брутто (г)
    width = Column(Integer, nullable=True)                       # Ширина (см)
    height = Column(Integer, nullable=True)                      # Высота (см)
    length = Column(Integer, nullable=True)                      # Длина (см)
    chrt_id = Column(Integer, nullable=True)                     # ID размера (chart)
    need_kiz = Column(Boolean, nullable=True)                    # Требуется маркировка
    kiz_marked = Column(Boolean, nullable=True)                  # Промаркирован

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Отношения
    barcodes = relationship("EntityBarcode", back_populates="entity", cascade="all, delete-orphan")
    organization = relationship("Organization")

    __table_args__ = (
        UniqueConstraint(
            "organization_id", "nm_id", "size_name",
            name="product_entities_org_nm_size_key"
        ),
    )


class EntityBarcode(Base):
    """
    История ШК по сущности.
    Один ШК привязан к одной сущности.
    ШК не удаляется — только помечается is_active=false.
    """
    __tablename__ = "entity_barcodes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_entities.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    barcode = Column(String(50), nullable=False, index=True)
    size_name = Column(String(50), nullable=True)               # Дубль для удобства поиска

    first_seen = Column(Date, nullable=False)                    # Когда впервые увидели
    last_seen = Column(Date, nullable=False)                     # Когда последний раз был в карточке
    is_active = Column(Boolean, default=True)                    # Видели в последнем синке?

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Отношения
    entity = relationship("ProductEntity", back_populates="barcodes")
    organization = relationship("Organization")

    __table_args__ = (
        UniqueConstraint(
            "entity_id", "barcode",
            name="entity_barcodes_entity_barcode_key"
        ),
    )


class UnmatchedBarcode(Base):
    """
    Буфер для ШК из продаж/заказов, которые ещё не привязаны к сущности.
    Разрешается при следующем синке карточек.
    """
    __tablename__ = "unmatched_barcodes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    barcode = Column(String(50), nullable=False, index=True)
    nm_id = Column(Integer, nullable=True)                       # Если известен
    size_name = Column(String(50), nullable=True)                # Если известен
    source = Column(String(20), nullable=False)                  # sale, order, stock
    raw_data = Column(JSONB, nullable=True)                      # Сырая запись
    target_date = Column(Date, nullable=True)                    # Дата записи
    resolved = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    organization = relationship("Organization")

    __table_args__ = (
        UniqueConstraint(
            "organization_id", "barcode", "source", "target_date",
            name="unmatched_barcodes_org_barcode_source_date_key"
        ),
    )
