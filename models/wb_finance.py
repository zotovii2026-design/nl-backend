"""Normalized WB finance report rows used by the OPIU report."""

import uuid

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from core.database import Base


class WbFinanceRow(Base):
    __tablename__ = "wb_finance_rows"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    entity_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_entities.id", ondelete="SET NULL"),
        nullable=True,
    )
    rrd_id = Column(BigInteger, nullable=False)
    report_id = Column(BigInteger, nullable=True)
    report_date_from = Column(Date, nullable=True)
    report_date_to = Column(Date, nullable=True)
    operation_date = Column(DateTime(timezone=True), nullable=True)

    nm_id = Column(BigInteger, nullable=True)
    vendor_code = Column(String(200), nullable=True)
    barcode = Column(String(100), nullable=True)
    size_name = Column(String(100), nullable=True)
    doc_type_name = Column(String(100), nullable=True)
    seller_oper_name = Column(String(300), nullable=True)

    quantity = Column(Numeric(14, 3), nullable=False, default=0)
    return_amount = Column(Numeric(14, 3), nullable=False, default=0)
    retail_price = Column(Numeric(16, 2), nullable=False, default=0)
    retail_amount = Column(Numeric(16, 2), nullable=False, default=0)
    for_pay = Column(Numeric(16, 2), nullable=False, default=0)
    acquiring_fee = Column(Numeric(16, 2), nullable=False, default=0)
    delivery_service = Column(Numeric(16, 2), nullable=False, default=0)
    penalty = Column(Numeric(16, 2), nullable=False, default=0)
    paid_storage = Column(Numeric(16, 2), nullable=False, default=0)
    deduction = Column(Numeric(16, 2), nullable=False, default=0)
    paid_acceptance = Column(Numeric(16, 2), nullable=False, default=0)
    cashback_amount = Column(Numeric(16, 2), nullable=False, default=0)
    cashback_discount = Column(Numeric(16, 2), nullable=False, default=0)
    cashback_commission_change = Column(
        Numeric(16, 2), nullable=False, default=0
    )

    raw_data = Column(JSONB, nullable=False)
    fetched_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "rrd_id",
            name="wb_finance_rows_org_rrd_key",
        ),
        Index(
            "ix_wb_finance_rows_org_operation_date",
            "organization_id",
            "operation_date",
        ),
        Index(
            "ix_wb_finance_rows_org_report",
            "organization_id",
            "report_id",
        ),
        Index(
            "ix_wb_finance_rows_org_entity",
            "organization_id",
            "entity_id",
        ),
    )


class WbFinanceSync(Base):
    __tablename__ = "wb_finance_syncs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    date_from = Column(Date, nullable=False)
    date_to = Column(Date, nullable=False)
    status = Column(String(20), nullable=False, default="running")
    rows_count = Column(Integer, nullable=False, default=0)
    bank_payment_sum = Column(Numeric(16, 2), nullable=True)
    calculated_payment_sum = Column(Numeric(16, 2), nullable=True)
    difference = Column(Numeric(16, 2), nullable=True)
    error_message = Column(Text, nullable=True)
    started_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index(
            "ix_wb_finance_syncs_org_period",
            "organization_id",
            "date_from",
            "date_to",
        ),
    )


class WbOpiuSnapshot(Base):
    __tablename__ = "wb_opiu_snapshots"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
    )
    entity_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_entities.id", ondelete="SET NULL"),
        nullable=True,
    )
    period_from = Column(Date, nullable=False)
    period_to = Column(Date, nullable=False)
    group_key = Column(String(500), nullable=False)
    is_total = Column(Integer, nullable=False, default=0)
    payload = Column(JSONB, nullable=False)
    calculated_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "period_from",
            "period_to",
            "group_key",
            name="wb_opiu_snapshots_org_period_group_key",
        ),
        Index(
            "ix_wb_opiu_snapshots_org_period",
            "organization_id",
            "period_from",
            "period_to",
        ),
    )
