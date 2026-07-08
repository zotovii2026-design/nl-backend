"""Модели для акций WB — promotions и товары в акциях"""
import uuid
from sqlalchemy import Boolean, Column, String, DateTime, Integer, Numeric, func, ForeignKey, UniqueConstraint, Date
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from core.database import Base


class WbPromotion(Base):
    """Акция Wildberries"""
    __tablename__ = "wb_promotions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    promotion_id = Column(Integer, nullable=False, index=True)          # ID акции из WB API
    title = Column(String(500), nullable=True)                          # Название акции
    promo_type = Column(String(50), nullable=True)                      # Тип: auto / manual / beta
    start_date = Column(DateTime(timezone=True), nullable=True)         # Начало акции
    end_date = Column(DateTime(timezone=True), nullable=True)           # Конец акции
    max_price = Column(Numeric(12, 2), nullable=True)                   # Макс цена для попадания
    min_discount = Column(Integer, nullable=True)                       # Минимальная скидка %
    has_boost = Column(Boolean, default=False)                          # Есть буст
    boost_value = Column(Numeric(5, 2), nullable=True)                  # Размер буста
    is_active = Column(Boolean, default=True)                           # Сейчас активна
    importance = Column(String(50), nullable=True)                      # Важность: high/medium/low
    raw_data = Column(JSONB, nullable=True)                             # Полный ответ WB
    source = Column(String(20), default="excel")                        # api / excel
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    organization = relationship("Organization")
    products = relationship("WbPromotionProduct", back_populates="promotion", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("organization_id", "promotion_id", name="wb_promotions_org_promo_id_key"),
    )


class WbPromotionProduct(Base):
    """Товары в акциях WB"""
    __tablename__ = "wb_promotion_products"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    promotion_id_col = Column(
        UUID(as_uuid=True),
        ForeignKey("wb_promotions.id", ondelete="CASCADE"),
        nullable=True, index=True
    )
    wb_promotion_ext_id = Column(Integer, nullable=False, index=True)   # promotion_id из WB API
    nm_id = Column(Integer, nullable=False, index=True)                 # Артикул WB
    entity_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_entities.id", ondelete="SET NULL"),
        nullable=True, index=True
    )
    in_action = Column(Boolean, default=False)                          # Уже в акции?
    auto_matched = Column(Boolean, default=False)                       # Автопопадание по цене
    current_price = Column(Numeric(10, 2), nullable=True)               # Текущая цена
    required_price = Column(Numeric(10, 2), nullable=True)              # Какая цена нужна
    price_in_promo = Column(Numeric(10, 2), nullable=True)              # Цена в акции
    profit_in_promo = Column(Numeric(10, 2), nullable=True)             # Прибыль в акции
    margin_delta = Column(Numeric(10, 2), nullable=True)                # Разница маржи
    plan = Column(Boolean, default=False)                                # ЛПР отметил для участия
    decision = Column(String(10), nullable=True)                        # enter / exit — решение по шаблону
    status_text = Column(String(200), nullable=True)                    # Статус из шаблона WB
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    synced_at = Column(DateTime(timezone=True), nullable=True)          # Время последней проверки

    organization = relationship("Organization")
    promotion = relationship("WbPromotion", back_populates="products")
    entity = relationship("ProductEntity")

    __table_args__ = (
        UniqueConstraint("organization_id", "wb_promotion_ext_id", "nm_id", name="wb_promo_products_org_ext_nm_key"),
    )


class WbPromotionSnapshot(Base):
    """Снимок промо из публичного API card.wb.ru для auto-акций"""
    __tablename__ = "wb_promotion_snapshots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False, index=True
    )
    nm_id = Column(Integer, nullable=False, index=True)
    entity_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_entities.id", ondelete="SET NULL"),
        nullable=True, index=True
    )
    snapshot_date = Column(Date, nullable=False, index=True)
    promotions = Column(JSONB, nullable=True)          # promotions[] из card.wb.ru
    sale_conditions = Column(JSONB, nullable=True)     # saleConditions из card.wb.ru
    available_qty = Column(Integer, nullable=True)      # totalQuantity / сумма stocks.qty из card.wb.ru
    available_to_buy = Column(Boolean, default=False)   # Доступен к покупке на витрине WB
    regular_in_promo = Column(Boolean, default=False)   # В regular-акции по WB Calendar nomenclatures
    auto_in_promo = Column(Boolean, default=False)      # В auto-акции по откалиброванному public marker
    in_any_promo = Column(Boolean, default=False)       # regular_in_promo OR auto_in_promo
    regular_promotion_ids = Column(JSONB, nullable=True)
    auto_promotion_ids = Column(JSONB, nullable=True)
    price_basic = Column(Numeric(12, 2), nullable=True)    # price.basic (цена до скидки)
    price_product = Column(Numeric(12, 2), nullable=True)  # price.product (цена покупателя)
    fetched_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    organization = relationship("Organization")
    entity = relationship("ProductEntity")

    __table_args__ = (
        UniqueConstraint("organization_id", "nm_id", "snapshot_date", name="wb_promo_snapshots_org_nm_date_key"),
    )
