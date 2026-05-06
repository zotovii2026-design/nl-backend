"""add_wb_card_fields_to_product_entities

Revision ID: b4a32c41d930
Revises: 74ad14ab90f5
Create Date: 2026-05-06 06:34:38.129155

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = 'b4a32c41d930'
down_revision: Union[str, None] = '74ad14ab90f5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('product_entities', sa.Column('brand', sa.String(length=200), nullable=True))
    op.add_column('product_entities', sa.Column('subject_name', sa.String(length=300), nullable=True))
    op.add_column('product_entities', sa.Column('tnved', sa.String(length=50), nullable=True))
    op.add_column('product_entities', sa.Column('color', sa.String(length=200), nullable=True))
    op.add_column('product_entities', sa.Column('weight', sa.Integer(), nullable=True))
    op.add_column('product_entities', sa.Column('width', sa.Integer(), nullable=True))
    op.add_column('product_entities', sa.Column('height', sa.Integer(), nullable=True))
    op.add_column('product_entities', sa.Column('length', sa.Integer(), nullable=True))
    op.add_column('product_entities', sa.Column('chrt_id', sa.Integer(), nullable=True))
    op.add_column('product_entities', sa.Column('need_kiz', sa.Boolean(), nullable=True))
    op.add_column('product_entities', sa.Column('kiz_marked', sa.Boolean(), nullable=True))


def downgrade() -> None:
    op.drop_column('product_entities', 'kiz_marked')
    op.drop_column('product_entities', 'need_kiz')
    op.drop_column('product_entities', 'chrt_id')
    op.drop_column('product_entities', 'length')
    op.drop_column('product_entities', 'height')
    op.drop_column('product_entities', 'width')
    op.drop_column('product_entities', 'weight')
    op.drop_column('product_entities', 'color')
    op.drop_column('product_entities', 'tnved')
    op.drop_column('product_entities', 'subject_name')
    op.drop_column('product_entities', 'brand')
