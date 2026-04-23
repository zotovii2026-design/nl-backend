"""add wb_stocks and personal_token

Revision ID: a1b2c3d4e5f6
Revises: 9a7b6c1d2e3f
Create Date: 2026-04-23 11:30:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "a1b2c3d4e5f6"
down_revision = "9a7b6c1d2e3f"
branch_labels = None
depends_on = None


def upgrade():
    # Таблица wb_stocks
    op.create_table(
        "wb_stocks",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("organization_id", UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("nm_id", sa.Integer, nullable=False, index=True),
        sa.Column("vendor_code", sa.String(100), nullable=True),
        sa.Column("warehouse_name", sa.String(200), nullable=False),
        sa.Column("warehouse_id", sa.Integer, nullable=True),
        sa.Column("quantity", sa.Integer, nullable=True, default=0),
        sa.Column("quantity_full", sa.Integer, nullable=True, default=0),
        sa.Column("in_way_to_client", sa.Integer, nullable=True, default=0),
        sa.Column("in_way_from_client", sa.Integer, nullable=True, default=0),
        sa.Column("category", sa.String(200), nullable=True),
        sa.Column("subject", sa.String(200), nullable=True),
        sa.Column("brand", sa.String(100), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), onupdate=sa.func.now()),
        sa.Column("synced_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Personal token в wb_api_keys
    op.add_column("wb_api_keys", sa.Column("personal_token", sa.Text, nullable=True))


def downgrade():
    op.drop_table("wb_stocks")
    op.drop_column("wb_api_keys", "personal_token")
