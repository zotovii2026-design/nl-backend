"""add loyalty_cashback_pct to reference_book

Revision ID: a1b2c3d4e5f7
Revises: c1e2a3b4c5d6
Create Date: 2026-08-31 07:45:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = 'a1b2c3d4e5f7'
down_revision = 'c1e2a3b4c5d6'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('reference_book', sa.Column('loyalty_cashback_pct', sa.Numeric(5, 2), nullable=True))

def downgrade():
    op.drop_column('reference_book', 'loyalty_cashback_pct')
