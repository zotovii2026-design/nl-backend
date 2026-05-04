"""add_supply_days_min_batch_fbo

Revision ID: b9ce5d30d478
Revises: i2j3k4l5m6n7
Create Date: 2026-05-04 16:57:48.373876

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'b9ce5d30d478'
down_revision: Union[str, None] = 'i2j3k4l5m6n7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('reference_book', sa.Column('supply_days', sa.Integer(), nullable=True))
    op.add_column('reference_book', sa.Column('min_batch_fbo', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('reference_book', 'min_batch_fbo')
    op.drop_column('reference_book', 'supply_days')
