"""Стратегии и вехи по артикулам."""

import uuid

from sqlalchemy import Column, Date, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from core.database import Base


class StrategyDefinition(Base):
    """Пользовательский справочник стратегий внутри кабинета."""

    __tablename__ = "strategy_definitions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    category = Column(String(50), nullable=False, index=True)
    code = Column(String(30), nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    default_executor = Column(String(255), nullable=True)
    role = Column(String(100), nullable=True)
    status = Column(String(30), nullable=False, default="active")
    sort_order = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    organization = relationship("Organization")
    milestones = relationship("StrategyMilestone", back_populates="strategy")

    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "category",
            "code",
            name="strategy_definitions_org_category_code_key",
        ),
    )


class StrategyMilestone(Base):
    """История применения стратегий к артикулам."""

    __tablename__ = "strategy_milestones"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entity_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_entities.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    strategy_id = Column(
        UUID(as_uuid=True),
        ForeignKey("strategy_definitions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    nm_id = Column(Integer, nullable=False, index=True)
    event_date = Column(Date, nullable=False, index=True)
    date_to = Column(Date, nullable=True)
    category = Column(String(50), nullable=False, index=True)
    strategy_code = Column(String(30), nullable=True)
    executor = Column(String(255), nullable=True)
    role = Column(String(100), nullable=True)
    source_links = Column(JSONB, nullable=True)
    comment = Column(Text, nullable=True)
    result_note = Column(Text, nullable=True)
    meta = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    organization = relationship("Organization")
    entity = relationship("ProductEntity")
    strategy = relationship("StrategyDefinition", back_populates="milestones")
