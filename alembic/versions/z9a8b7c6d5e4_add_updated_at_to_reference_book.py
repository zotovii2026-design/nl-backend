"""add updated_at to reference_book

Revision ID: z9a8b7c6d5e4
Revises: t4u5v6w7x8y9
Create Date: 2026-07-01 19:37:00.000000
"""
from alembic import op
import sqlalchemy as sa

revision = 'z9a8b7c6d5e4'
down_revision = 't4u5v6w7x8y9'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('reference_book', sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True))

def downgrade():
    op.drop_column('reference_book', 'updated_at')
