"""add acquiring_pct and other_deductions_pct to reference_book

Revision ID: a8b9c0d1e2f3
Revises: w9x0y1z2a3b4
Create Date: 2026-08-20
"""
from alembic import op
import sqlalchemy as sa

revision = 'a8b9c0d1e2f3'
down_revision = 'd4e5f6g7h8i9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('reference_book', sa.Column('acquiring_pct', sa.Numeric(5, 2), nullable=True))
    op.add_column('reference_book', sa.Column('other_deductions_pct', sa.Numeric(5, 2), nullable=True))
    # П.7: дефолт 2.5% эквайринга для существующих строк
    op.execute("UPDATE reference_book SET acquiring_pct = 2.5 WHERE acquiring_pct IS NULL")


def downgrade() -> None:
    op.drop_column('reference_book', 'other_deductions_pct')
    op.drop_column('reference_book', 'acquiring_pct')
