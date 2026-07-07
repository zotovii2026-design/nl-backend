"""add_promo_snapshot_flags

Revision ID: u7v8w9x0y1z2
Revises: b5c6d7e8f9a0
Create Date: 2026-07-07 16:20:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "u7v8w9x0y1z2"
down_revision: Union[str, None] = "b5c6d7e8f9a0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("wb_promotion_snapshots", sa.Column("available_qty", sa.Integer(), nullable=True))
    op.add_column(
        "wb_promotion_snapshots",
        sa.Column("available_to_buy", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "wb_promotion_snapshots",
        sa.Column("regular_in_promo", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "wb_promotion_snapshots",
        sa.Column("auto_in_promo", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "wb_promotion_snapshots",
        sa.Column("in_any_promo", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column(
        "wb_promotion_snapshots",
        sa.Column("regular_promotion_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "wb_promotion_snapshots",
        sa.Column("auto_promotion_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_index(
        "idx_wb_promo_snapshots_org_date_available",
        "wb_promotion_snapshots",
        ["organization_id", "snapshot_date", "available_to_buy"],
        unique=False,
    )
    op.create_index(
        "idx_wb_promo_snapshots_org_date_any_promo",
        "wb_promotion_snapshots",
        ["organization_id", "snapshot_date", "in_any_promo"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("idx_wb_promo_snapshots_org_date_any_promo", table_name="wb_promotion_snapshots")
    op.drop_index("idx_wb_promo_snapshots_org_date_available", table_name="wb_promotion_snapshots")
    op.drop_column("wb_promotion_snapshots", "auto_promotion_ids")
    op.drop_column("wb_promotion_snapshots", "regular_promotion_ids")
    op.drop_column("wb_promotion_snapshots", "in_any_promo")
    op.drop_column("wb_promotion_snapshots", "auto_in_promo")
    op.drop_column("wb_promotion_snapshots", "regular_in_promo")
    op.drop_column("wb_promotion_snapshots", "available_to_buy")
    op.drop_column("wb_promotion_snapshots", "available_qty")
