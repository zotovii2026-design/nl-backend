"""add_external_ads

Revision ID: 99db454e2311
Revises: c6b1c95c951a
Create Date: 2026-05-04 18:43:07.947060

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "99db454e2311"
down_revision: Union[str, None] = "c6b1c95c951a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table("external_ads",
    sa.Column("id", sa.UUID(), nullable=False),
    sa.Column("organization_id", sa.UUID(), nullable=False),
    sa.Column("entity_id", sa.UUID(), nullable=True),
    sa.Column("nm_id", sa.Integer(), nullable=True),
    sa.Column("vendor_code", sa.String(length=100), nullable=True),
    sa.Column("article", sa.String(length=100), nullable=True),
    sa.Column("photo_url", sa.String(length=1000), nullable=True),
    sa.Column("card_url", sa.String(length=1000), nullable=True),
    sa.Column("substitution_url", sa.String(length=1000), nullable=True),
    sa.Column("utm_url", sa.String(length=1000), nullable=True),
    sa.Column("source", sa.String(length=200), nullable=True),
    sa.Column("query", sa.String(length=500), nullable=True),
    sa.Column("ad_date", sa.Date(), nullable=True),
    sa.Column("reach", sa.Integer(), nullable=True),
    sa.Column("amount", sa.Numeric(precision=12, scale=2), nullable=True),
    sa.Column("orders_count", sa.Integer(), nullable=True),
    sa.Column("orders_avg_weekly", sa.Numeric(precision=10, scale=2), nullable=True),
    sa.Column("ad_type", sa.String(length=20), nullable=True),
    sa.Column("notes", sa.Text(), nullable=True),
    sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
    sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    sa.ForeignKeyConstraint(["entity_id"], ["product_entities.id"], ondelete="SET NULL"),
    sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], ondelete="CASCADE"),
    sa.PrimaryKeyConstraint("id")
    )
    op.create_index(op.f("ix_external_ads_ad_date"), "external_ads", ["ad_date"], unique=False)
    op.create_index(op.f("ix_external_ads_entity_id"), "external_ads", ["entity_id"], unique=False)
    op.create_index(op.f("ix_external_ads_nm_id"), "external_ads", ["nm_id"], unique=False)
    op.create_index(op.f("ix_external_ads_organization_id"), "external_ads", ["organization_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_external_ads_organization_id"), table_name="external_ads")
    op.drop_index(op.f("ix_external_ads_nm_id"), table_name="external_ads")
    op.drop_index(op.f("ix_external_ads_entity_id"), table_name="external_ads")
    op.drop_index(op.f("ix_external_ads_ad_date"), table_name="external_ads")
    op.drop_table("external_ads")
