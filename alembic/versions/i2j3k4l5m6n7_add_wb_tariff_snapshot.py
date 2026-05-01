"""add_wb_tariff_snapshot

Создаём таблицу wb_tariff_snapshot для автоматических WB-данных.

Revision ID: i2j3k4l5m6n7
Revises: h1i2j3k4l5m6
Create Date: 2026-05-01

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "i2j3k4l5m6n7"
down_revision = "h1i2j3k4l5m6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "wb_tariff_snapshot",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("organization_id", UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("entity_id", UUID(as_uuid=True), sa.ForeignKey("product_entities.id", ondelete="SET NULL"), nullable=True, index=True),
        sa.Column("nm_id", sa.Integer, nullable=False, index=True),
        sa.Column("target_date", sa.Date, nullable=False, index=True),
        sa.Column("price_retail", sa.Numeric(10, 2)),
        sa.Column("price_with_spp", sa.Numeric(10, 2)),
        sa.Column("spp_pct", sa.Numeric(5, 2)),
        sa.Column("discount_pct", sa.Numeric(5, 2)),
        sa.Column("commission_pct", sa.Numeric(5, 2)),
        sa.Column("logistics_tariff", sa.Numeric(10, 2)),
        sa.Column("logistics_base", sa.Numeric(10, 2)),
        sa.Column("storage_tariff", sa.Numeric(10, 2)),
        sa.Column("storage_base", sa.Numeric(10, 2)),
        sa.Column("acceptance_avg_90d", sa.Numeric(10, 2)),
        sa.Column("ad_cost_fact", sa.Numeric(10, 2)),
        sa.Column("buyout_pct_fact", sa.Numeric(5, 2)),
        sa.Column("wb_club_price", sa.Numeric(10, 2)),
        sa.Column("fetched_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("organization_id", "nm_id", "target_date", name="wb_tariff_snapshot_org_nm_date_key"),
    )


def downgrade() -> None:
    op.drop_table("wb_tariff_snapshot")
