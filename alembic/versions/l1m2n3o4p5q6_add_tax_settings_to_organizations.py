"""add tax_settings to organizations

Revision ID: l1m2n3o4p5q6
Revises: k4l5m6n7o8p9
Create Date: 2026-05-13
"""
from alembic import op
import sqlalchemy as sa

revision = 'l1m2n3o4p5q6'
down_revision = 'k4l5m6n7o8p9'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('organizations', sa.Column('tax_system', sa.String(50), nullable=True))
    op.add_column('organizations', sa.Column('tax_rate', sa.Numeric(5, 2), nullable=True))
    op.add_column('organizations', sa.Column('vat_type', sa.String(10), nullable=True))

def downgrade():
    op.drop_column('organizations', 'vat_type')
    op.drop_column('organizations', 'tax_rate')
    op.drop_column('organizations', 'tax_system')
