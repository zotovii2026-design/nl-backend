"""add token_status to wb_api_keys

Revision ID: b2c3d4e5f6g7
Revises: z9a8b7c6d5e4
Create Date: 2026-07-13 16:00:00.000000

Добавляет token_status и validated_at в wb_api_keys.
token_status: 'unknown' (по умолчанию) / 'valid' / 'invalid' / 'limited'
"""
from alembic import op
import sqlalchemy as sa

revision = 'b2c3d4e5f6g7'
down_revision = 'e79a6d7c4b39'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('wb_api_keys', sa.Column('token_status', sa.String(20), nullable=False, server_default='unknown'))
    op.add_column('wb_api_keys', sa.Column('validated_at', sa.DateTime(timezone=True), nullable=True))

def downgrade():
    op.drop_column('wb_api_keys', 'token_status')
    op.drop_column('wb_api_keys', 'validated_at')
