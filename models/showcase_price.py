"""Модель снапшотов витринных цен (3 обхода в сутки).

СПП% товара = (1 − витринная цена с СПП / цена продавца до СПП) × 100.
- price_seller  — discountedPrice из кабинета (цена продавца до СПП)
- price_showcase — salePriceU из карточки card.wb.ru v4 (цена на витрине со всеми скидками)
"""
import uuid

from sqlalchemy import (
    Column, Date, DateTime, ForeignKey, Integer, Numeric, String,
    UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import UUID

from core.database import Base


class WbShowcasePriceSnapshot(Base):
    """Снимок витринной цены товара — 3 раза в сутки (03:00/11:00/19:00 UTC+5)"""
    __tablename__ = "wb_showcase_price_snapshots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    nm_id = Column(Integer, nullable=False, index=True)
    entity_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_entities.id", ondelete="SET NULL"),
        nullable=True, index=True,
    )
    snapshot_date = Column(Date, nullable=False, index=True)
    pass_time = Column(DateTime(timezone=True), nullable=False)  # время обхода (UTC)
    price_seller = Column(Numeric(12, 2), nullable=True)    # цена продавца до СПП (кабинет, rub)
    price_showcase = Column(Numeric(12, 2), nullable=True)  # витринная цена с СПП (card v4, rub)
    spp_pct = Column(Numeric(6, 2), nullable=True)          # (1 − showcase/seller) × 100
    source = Column(String(32), nullable=True)              # 'showcase' / 'orders' (заполняется при расчёте РНП)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        UniqueConstraint(
            "organization_id", "nm_id", "snapshot_date", "pass_time",
            name="wb_showcase_price_org_nm_date_pass_key",
        ),
    )
