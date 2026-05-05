"""add_rnp_fields_to_reference_book

Revision ID: 74ad14ab90f5
Revises: 99db454e2311
Create Date: 2026-05-05 10:49:25.327057
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '74ad14ab90f5'
down_revision: Union[str, None] = '99db454e2311'
branch_labels: Union[str, Sequence, Union[str], None] = None
depends_on: Union[str, Sequence, Union[str], None] = None


def upgrade() -> None:
    op.add_column('reference_book', sa.Column('in_promo', sa.Boolean(), nullable=True, server_default=sa.text('false')))
    op.add_column('reference_book', sa.Column('ad_shows_organic', sa.Integer(), nullable=True))
    op.add_column('reference_book', sa.Column('ad_shows_paid', sa.Integer(), nullable=True))
    op.add_column('reference_book', sa.Column('ad_strategy', sa.String(200), nullable=True))
    op.add_column('reference_book', sa.Column('tags', sa.Text(), nullable=True))
    op.add_column('reference_book', sa.Column('rating_reviews', sa.Numeric(3, 2), nullable=True))
    op.add_column('reference_book', sa.Column('localization_pct', sa.String(50), nullable=True))


def downgrade() -> None:
    op.drop_column('reference_book', 'localization_pct')
    op.drop_column('reference_book', 'rating_reviews')
    op.drop_column('reference_book', 'tags')
    op.drop_column('reference_book', 'ad_strategy')
    op.drop_column('reference_book', 'ad_shows_paid')
    op.drop_column('reference_book', 'ad_shows_organic')
    op.drop_column('reference_book', 'in_promo')
