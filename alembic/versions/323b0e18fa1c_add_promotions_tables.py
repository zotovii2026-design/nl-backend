"""add_promotions_tables

Revision ID: 323b0e18fa1c
Revises: l1m2n3o4p5q6
Create Date: 2026-05-25 18:23:25.594506

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = '323b0e18fa1c'
down_revision: Union[str, None] = 'm2n3o4p5q6r7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('wb_promotions',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('organization_id', sa.UUID(), nullable=False),
    sa.Column('promotion_id', sa.Integer(), nullable=False),
    sa.Column('title', sa.String(length=500), nullable=True),
    sa.Column('promo_type', sa.String(length=50), nullable=True),
    sa.Column('start_date', sa.DateTime(timezone=True), nullable=True),
    sa.Column('end_date', sa.DateTime(timezone=True), nullable=True),
    sa.Column('max_price', sa.Numeric(precision=12, scale=2), nullable=True),
    sa.Column('min_discount', sa.Integer(), nullable=True),
    sa.Column('has_boost', sa.Boolean(), nullable=True),
    sa.Column('boost_value', sa.Numeric(precision=5, scale=2), nullable=True),
    sa.Column('is_active', sa.Boolean(), nullable=True),
    sa.Column('importance', sa.String(length=50), nullable=True),
    sa.Column('raw_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('source', sa.String(length=20), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('organization_id', 'promotion_id', name='wb_promotions_org_promo_id_key')
    )
    op.create_index(op.f('ix_wb_promotions_organization_id'), 'wb_promotions', ['organization_id'], unique=False)
    op.create_index(op.f('ix_wb_promotions_promotion_id'), 'wb_promotions', ['promotion_id'], unique=False)
    op.create_table('wb_promotion_products',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('organization_id', sa.UUID(), nullable=False),
    sa.Column('promotion_id_col', sa.UUID(), nullable=True),
    sa.Column('wb_promotion_ext_id', sa.Integer(), nullable=False),
    sa.Column('nm_id', sa.Integer(), nullable=False),
    sa.Column('entity_id', sa.UUID(), nullable=True),
    sa.Column('in_action', sa.Boolean(), nullable=True),
    sa.Column('auto_matched', sa.Boolean(), nullable=True),
    sa.Column('current_price', sa.Numeric(precision=10, scale=2), nullable=True),
    sa.Column('required_price', sa.Numeric(precision=10, scale=2), nullable=True),
    sa.Column('price_in_promo', sa.Numeric(precision=10, scale=2), nullable=True),
    sa.Column('profit_in_promo', sa.Numeric(precision=10, scale=2), nullable=True),
    sa.Column('margin_delta', sa.Numeric(precision=10, scale=2), nullable=True),
    sa.Column('plan', sa.Boolean(), nullable=True),
    sa.Column('status_text', sa.String(length=200), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('synced_at', sa.DateTime(timezone=True), nullable=True),
    sa.ForeignKeyConstraint(['entity_id'], ['product_entities.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['promotion_id_col'], ['wb_promotions.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('organization_id', 'wb_promotion_ext_id', 'nm_id', name='wb_promo_products_org_ext_nm_key')
    )
    op.create_index(op.f('ix_wb_promotion_products_entity_id'), 'wb_promotion_products', ['entity_id'], unique=False)
    op.create_index(op.f('ix_wb_promotion_products_nm_id'), 'wb_promotion_products', ['nm_id'], unique=False)
    op.create_index(op.f('ix_wb_promotion_products_organization_id'), 'wb_promotion_products', ['organization_id'], unique=False)
    op.create_index(op.f('ix_wb_promotion_products_promotion_id_col'), 'wb_promotion_products', ['promotion_id_col'], unique=False)
    op.create_index(op.f('ix_wb_promotion_products_wb_promotion_ext_id'), 'wb_promotion_products', ['wb_promotion_ext_id'], unique=False)


def downgrade() -> None:
    op.drop_table('wb_promotion_products')
    op.drop_table('wb_promotions')
