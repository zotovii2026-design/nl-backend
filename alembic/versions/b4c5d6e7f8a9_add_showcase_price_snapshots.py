"""add showcase price snapshots (3 passes/day)

Revision ID: b4c5d6e7f8a9
Revises: a1b2c3d4e5f7
Create Date: 2026-09-18

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "b4c5d6e7f8a9"
down_revision: Union[str, None] = "a1b2c3d4e5f7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "wb_showcase_price_snapshots",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("nm_id", sa.Integer(), nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("snapshot_date", sa.Date(), nullable=False),
        sa.Column("pass_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("price_seller", sa.Numeric(12, 2), nullable=True),
        sa.Column("price_showcase", sa.Numeric(12, 2), nullable=True),
        sa.Column("spp_pct", sa.Numeric(6, 2), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["entity_id"], ["product_entities.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "organization_id", "nm_id", "snapshot_date", "pass_time",
            name="wb_showcase_price_org_nm_date_pass_key",
        ),
    )
    op.create_index(
        op.f("ix_wb_showcase_price_snapshots_organization_id"),
        "wb_showcase_price_snapshots", ["organization_id"], unique=False,
    )
    op.create_index(
        op.f("ix_wb_showcase_price_snapshots_nm_id"),
        "wb_showcase_price_snapshots", ["nm_id"], unique=False,
    )
    op.create_index(
        op.f("ix_wb_showcase_price_snapshots_entity_id"),
        "wb_showcase_price_snapshots", ["entity_id"], unique=False,
    )
    op.create_index(
        op.f("ix_wb_showcase_price_snapshots_snapshot_date"),
        "wb_showcase_price_snapshots", ["snapshot_date"], unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_wb_showcase_price_snapshots_snapshot_date"), table_name="wb_showcase_price_snapshots")
    op.drop_index(op.f("ix_wb_showcase_price_snapshots_entity_id"), table_name="wb_showcase_price_snapshots")
    op.drop_index(op.f("ix_wb_showcase_price_snapshots_nm_id"), table_name="wb_showcase_price_snapshots")
    op.drop_index(op.f("ix_wb_showcase_price_snapshots_organization_id"), table_name="wb_showcase_price_snapshots")
    op.drop_table("wb_showcase_price_snapshots")
