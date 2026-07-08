"""add_promotion_normalized_title

Revision ID: w9x0y1z2a3b4
Revises: v8w9x0y1z2a3
Create Date: 2026-07-08 12:35:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "w9x0y1z2a3b4"
down_revision: Union[str, None] = "v8w9x0y1z2a3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "wb_promotions",
        sa.Column("normalized_title", sa.String(length=500), nullable=True),
    )
    op.create_unique_constraint(
        "wb_promotions_org_source_norm_title_key",
        "wb_promotions",
        ["organization_id", "source", "normalized_title"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "wb_promotions_org_source_norm_title_key",
        "wb_promotions",
        type_="unique",
    )
    op.drop_column("wb_promotions", "normalized_title")
