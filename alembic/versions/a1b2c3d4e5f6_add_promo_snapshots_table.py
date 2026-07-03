"""add_promo_snapshots_table

Revision ID: a1b2c3d4e5f6
Revises: t4u5v6w7x8y9
Create Date: 2026-07-03 15:45:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = 't4u5v6w7x8y9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('wb_promotion_snapshots',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('organization_id', sa.UUID(), nullable=False),
    sa.Column('nm_id', sa.Integer(), nullable=False),
    sa.Column('entity_id', sa.UUID(), nullable=True),
    sa.Column('snapshot_date', sa.Date(), nullable=False),
    sa.Column('promotions', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('sale_conditions', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.Column('price_basic', sa.Numeric(precision=12, scale=2), nullable=True),
    sa.Column('price_product', sa.Numeric(precision=12, scale=2), nullable=True),
    sa.Column('fetched_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    sa.ForeignKeyConstraint(['entity_id'], ['product_entities.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('organization_id', 'nm_id', 'snapshot_date', name='wb_promo_snapshots_org_nm_date_key')
    )
    op.create_index(op.f('ix_wb_promotion_snapshots_entity_id'), 'wb_promotion_snapshots', ['entity_id'], unique=False)
    op.create_index(op.f('ix_wb_promotion_snapshots_nm_id'), 'wb_promotion_snapshots', ['nm_id'], unique=False)
    op.create_index(op.f('ix_wb_promotion_snapshots_organization_id'), 'wb_promotion_snapshots', ['organization_id'], unique=False)
    op.create_index(op.f('ix_wb_promotion_snapshots_snapshot_date'), 'wb_promotion_snapshots', ['snapshot_date'], unique=False)


def downgrade() -> None:
    op.drop_table('wb_promotion_snapshots')