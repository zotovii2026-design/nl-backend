"""add sellers and seo_keywords tables

Revision ID: c7d8e9f0a1b2
Revises: z9a8b7c6d5e4
Create Date: 2026-07-01 20:20:00.000000
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = 'c7d8e9f0a1b2'
down_revision = 'z9a8b7c6d5e4'
branch_labels = None
depends_on = None

def upgrade():
    op.create_table(
        'sellers',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column('organization_id', UUID(as_uuid=True), sa.ForeignKey('organizations.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('seller_id', sa.String(100)),
        sa.Column('seller_name', sa.String(200)),
        sa.Column('inn', sa.String(20)),
        sa.Column('seller_type', sa.String(20), server_default='fbo'),
        sa.Column('contact_name', sa.String(200)),
        sa.Column('contact_email', sa.String(200)),
        sa.Column('contact_phone', sa.String(50)),
        sa.Column('role', sa.String(50), server_default='seller'),
        sa.Column('is_active', sa.Boolean, server_default=sa.text('true')),
        sa.Column('notes', sa.Text),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        'seo_keywords',
        sa.Column('id', UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column('organization_id', UUID(as_uuid=True), sa.ForeignKey('organizations.id', ondelete='CASCADE'), nullable=False, index=True),
        sa.Column('nm_id', sa.Integer, nullable=True, index=True),
        sa.Column('vendor_code', sa.String(100)),
        sa.Column('keyword', sa.String(500), nullable=False),
        sa.Column('position', sa.Integer),
        sa.Column('frequency_monthly', sa.Integer),
        sa.Column('frequency_weekly', sa.Integer),
        sa.Column('season_start', sa.String(10)),
        sa.Column('season_end', sa.String(10)),
        sa.Column('season_multiplier', sa.Numeric(5, 2), server_default='1.0'),
        sa.Column('trend', sa.String(20)),
        sa.Column('trend_value', sa.Numeric(10, 2)),
        sa.Column('competition', sa.String(50)),
        sa.Column('target_date', sa.Date),
        sa.Column('source', sa.String(20), server_default='manual'),
        sa.Column('notes', sa.Text),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

def downgrade():
    op.drop_table('seo_keywords')
    op.drop_table('sellers')
