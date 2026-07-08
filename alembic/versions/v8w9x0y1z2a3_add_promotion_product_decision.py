"""add_promotion_product_decision

Revision ID: v8w9x0y1z2a3
Revises: u7v8w9x0y1z2
Create Date: 2026-07-08 06:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "v8w9x0y1z2a3"
down_revision: Union[str, None] = "u7v8w9x0y1z2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "wb_promotion_products",
        sa.Column("decision", sa.String(length=10), nullable=True),
    )
    op.create_index(
        "idx_wb_promo_products_org_decision",
        "wb_promotion_products",
        ["organization_id", "decision"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_wb_promo_products_org_decision",
        table_name="wb_promotion_products",
    )
    op.drop_column("wb_promotion_products", "decision")
