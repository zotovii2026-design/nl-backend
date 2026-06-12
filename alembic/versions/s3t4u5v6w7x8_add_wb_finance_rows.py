"""add WB finance rows for OPIU

Revision ID: s3t4u5v6w7x8
Revises: r2s3t4u5v6w7
Create Date: 2026-06-12
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "s3t4u5v6w7x8"
down_revision: Union[str, None] = "r2s3t4u5v6w7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "wb_finance_rows",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "organization_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("rrd_id", sa.BigInteger(), nullable=False),
        sa.Column("report_id", sa.BigInteger(), nullable=True),
        sa.Column("report_date_from", sa.Date(), nullable=True),
        sa.Column("report_date_to", sa.Date(), nullable=True),
        sa.Column(
            "operation_date", sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column("nm_id", sa.BigInteger(), nullable=True),
        sa.Column("vendor_code", sa.String(length=200), nullable=True),
        sa.Column("barcode", sa.String(length=100), nullable=True),
        sa.Column("size_name", sa.String(length=100), nullable=True),
        sa.Column("doc_type_name", sa.String(length=100), nullable=True),
        sa.Column("seller_oper_name", sa.String(length=300), nullable=True),
        sa.Column(
            "quantity",
            sa.Numeric(precision=14, scale=3),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "return_amount",
            sa.Numeric(precision=14, scale=3),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "retail_price",
            sa.Numeric(precision=16, scale=2),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "retail_amount",
            sa.Numeric(precision=16, scale=2),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "for_pay",
            sa.Numeric(precision=16, scale=2),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "acquiring_fee",
            sa.Numeric(precision=16, scale=2),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "delivery_service",
            sa.Numeric(precision=16, scale=2),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "penalty",
            sa.Numeric(precision=16, scale=2),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "paid_storage",
            sa.Numeric(precision=16, scale=2),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "deduction",
            sa.Numeric(precision=16, scale=2),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "paid_acceptance",
            sa.Numeric(precision=16, scale=2),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "cashback_amount",
            sa.Numeric(precision=16, scale=2),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "cashback_discount",
            sa.Numeric(precision=16, scale=2),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "cashback_commission_change",
            sa.Numeric(precision=16, scale=2),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "raw_data", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["entity_id"], ["product_entities.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "organization_id",
            "rrd_id",
            name="wb_finance_rows_org_rrd_key",
        ),
    )
    op.create_index(
        "ix_wb_finance_rows_org_operation_date",
        "wb_finance_rows",
        ["organization_id", "operation_date"],
    )
    op.create_index(
        "ix_wb_finance_rows_org_report",
        "wb_finance_rows",
        ["organization_id", "report_id"],
    )
    op.create_index(
        "ix_wb_finance_rows_org_entity",
        "wb_finance_rows",
        ["organization_id", "entity_id"],
    )

    op.create_table(
        "wb_finance_syncs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "organization_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("date_from", sa.Date(), nullable=False),
        sa.Column("date_to", sa.Date(), nullable=False),
        sa.Column(
            "status",
            sa.String(length=20),
            server_default="running",
            nullable=False,
        ),
        sa.Column(
            "rows_count",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
        sa.Column(
            "bank_payment_sum", sa.Numeric(precision=16, scale=2), nullable=True
        ),
        sa.Column(
            "calculated_payment_sum",
            sa.Numeric(precision=16, scale=2),
            nullable=True,
        ),
        sa.Column(
            "difference", sa.Numeric(precision=16, scale=2), nullable=True
        ),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_wb_finance_syncs_org_period",
        "wb_finance_syncs",
        ["organization_id", "date_from", "date_to"],
    )

    op.create_table(
        "wb_opiu_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "organization_id", postgresql.UUID(as_uuid=True), nullable=False
        ),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("period_from", sa.Date(), nullable=False),
        sa.Column("period_to", sa.Date(), nullable=False),
        sa.Column("group_key", sa.String(length=500), nullable=False),
        sa.Column(
            "is_total", sa.Integer(), server_default="0", nullable=False
        ),
        sa.Column(
            "payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False
        ),
        sa.Column(
            "calculated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["entity_id"], ["product_entities.id"], ondelete="SET NULL"
        ),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organizations.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "organization_id",
            "period_from",
            "period_to",
            "group_key",
            name="wb_opiu_snapshots_org_period_group_key",
        ),
    )
    op.create_index(
        "ix_wb_opiu_snapshots_org_period",
        "wb_opiu_snapshots",
        ["organization_id", "period_from", "period_to"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_wb_opiu_snapshots_org_period", table_name="wb_opiu_snapshots"
    )
    op.drop_table("wb_opiu_snapshots")
    op.drop_index(
        "ix_wb_finance_syncs_org_period", table_name="wb_finance_syncs"
    )
    op.drop_table("wb_finance_syncs")
    op.drop_index(
        "ix_wb_finance_rows_org_entity", table_name="wb_finance_rows"
    )
    op.drop_index(
        "ix_wb_finance_rows_org_report", table_name="wb_finance_rows"
    )
    op.drop_index(
        "ix_wb_finance_rows_org_operation_date",
        table_name="wb_finance_rows",
    )
    op.drop_table("wb_finance_rows")
