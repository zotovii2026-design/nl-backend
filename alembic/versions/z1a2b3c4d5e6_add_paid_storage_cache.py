"""add_paid_storage_cache

Revision ID: z1a2b3c4d5e6
Revises: y1z2a3b4c5d6
Create Date: 2026-07-20 17:05:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "z1a2b3c4d5e6"
down_revision: Union[str, None] = "y1z2a3b4c5d6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "wb_paid_storage_rows",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("storage_date", sa.Date(), nullable=False),
        sa.Column("nm_id", sa.BigInteger(), nullable=False),
        sa.Column("vendor_code", sa.String(length=200), nullable=True),
        sa.Column("subject_name", sa.String(length=200), nullable=True),
        sa.Column("brand", sa.String(length=200), nullable=True),
        sa.Column(
            "storage_amount",
            sa.Numeric(16, 2),
            nullable=False,
            server_default="0",
        ),
        sa.Column("raw_data", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["entity_id"], ["product_entities.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "organization_id",
            "storage_date",
            "nm_id",
            name="wb_paid_storage_rows_org_date_nm_key",
        ),
    )
    op.create_index(
        "ix_wb_paid_storage_rows_org_date",
        "wb_paid_storage_rows",
        ["organization_id", "storage_date"],
    )
    op.create_index(
        "ix_wb_paid_storage_rows_org_nm",
        "wb_paid_storage_rows",
        ["organization_id", "nm_id"],
    )

    op.create_table(
        "wb_paid_storage_syncs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("date_from", sa.Date(), nullable=False),
        sa.Column("date_to", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="running"),
        sa.Column("rows_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_storage", sa.Numeric(16, 2), nullable=False, server_default="0"),
        sa.Column("task_id", sa.String(length=100), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_wb_paid_storage_syncs_org_period",
        "wb_paid_storage_syncs",
        ["organization_id", "date_from", "date_to"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_wb_paid_storage_syncs_org_period",
        table_name="wb_paid_storage_syncs",
    )
    op.drop_table("wb_paid_storage_syncs")
    op.drop_index(
        "ix_wb_paid_storage_rows_org_nm",
        table_name="wb_paid_storage_rows",
    )
    op.drop_index(
        "ix_wb_paid_storage_rows_org_date",
        table_name="wb_paid_storage_rows",
    )
    op.drop_table("wb_paid_storage_rows")
