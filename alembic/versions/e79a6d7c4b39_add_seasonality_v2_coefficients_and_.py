"""add_seasonality_v2_coefficients_and_product_table

Revision ID: e79a6d7c4b39
Revises: f1e2d3c4b5a6
Create Date: 2026-07-08 18:51:12.712185

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "e79a6d7c4b39"
down_revision: Union[str, None] = "f1e2d3c4b5a6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add seasonality_coefficients to wb_keyword_seasonality
    op.add_column("wb_keyword_seasonality", 
        sa.Column("seasonality_coefficients", postgresql.JSONB(), nullable=True)
    )
    
    # Create wb_product_seasonality table
    op.create_table("wb_product_seasonality",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("nm_id", sa.Integer(), nullable=False),
        sa.Column("vendor_code", sa.String(length=100), nullable=True),
        sa.Column("seasonality_coefficients", postgresql.JSONB(), nullable=False),
        sa.Column("source_keywords", postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column("collected_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("id")
    )
    
    op.create_index("ix_wb_product_seasonality_nm_id", "wb_product_seasonality", ["nm_id"], unique=False)
    op.create_index("ix_wb_product_seasonality_organization_id", "wb_product_seasonality", ["organization_id"], unique=False)
    op.create_index("uq_wb_product_seasonality_nm_org_collected", "wb_product_seasonality", ["nm_id", "organization_id", "collected_at"], unique=True)


def downgrade() -> None:
    op.drop_index("uq_wb_product_seasonality_nm_org_collected", table_name="wb_product_seasonality")
    op.drop_index("ix_wb_product_seasonality_organization_id", table_name="wb_product_seasonality")
    op.drop_index("ix_wb_product_seasonality_nm_id", table_name="wb_product_seasonality")
    op.drop_table("wb_product_seasonality")
    op.drop_column("wb_keyword_seasonality", "seasonality_coefficients")
