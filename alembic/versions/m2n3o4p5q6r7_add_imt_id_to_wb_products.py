"""add imt_id to wb_products

Revision ID: m2n3o4p5q6r7
Revises: 323b0e18fa1c
Create Date: 2026-05-28
"""
from alembic import op
import sqlalchemy as sa

revision = 'm2n3o4p5q6r7'
down_revision = 'l1m2n3o4p5q6'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('wb_products', sa.Column('imt_id', sa.Integer, nullable=True))
    op.create_index('ix_wb_products_imt_id', 'wb_products', ['imt_id'])

def downgrade():
    op.drop_index('ix_wb_products_imt_id')
    op.drop_column('wb_products', 'imt_id')
