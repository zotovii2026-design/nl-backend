"""add append-only marketplace matching events

Revision ID: f6g7h8i9j0k1
Revises: e5f6g7h8i9j0
Create Date: 2026-11-26
"""

from typing import Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "f6g7h8i9j0k1"
down_revision: Union[str, None] = "e5f6g7h8i9j0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "marketplace_matching_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("marketplace_listing_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("marketplace_listings.id", ondelete="CASCADE"), nullable=False),
        sa.Column("previous_product_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("products.id", ondelete="SET NULL"), nullable=True),
        sa.Column("product_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("products.id", ondelete="SET NULL"), nullable=True),
        sa.Column("action", sa.String(30), nullable=False),
        sa.Column("reason", sa.String(500), nullable=True),
        sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_marketplace_matching_events_organization_id", "marketplace_matching_events", ["organization_id"])
    op.create_index("ix_marketplace_matching_events_marketplace_listing_id", "marketplace_matching_events", ["marketplace_listing_id"])


def downgrade() -> None:
    op.drop_index("ix_marketplace_matching_events_marketplace_listing_id", table_name="marketplace_matching_events")
    op.drop_index("ix_marketplace_matching_events_organization_id", table_name="marketplace_matching_events")
    op.drop_table("marketplace_matching_events")
