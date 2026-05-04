"""add_sales_plan

Revision ID: c6b1c95c951a
Revises: b9ce5d30d478
Create Date: 2026-05-04 18:10:19.665070

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'c6b1c95c951a'
down_revision: Union[str, None] = 'b9ce5d30d478'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('sales_plans',
    sa.Column('id', sa.UUID(), nullable=False),
    sa.Column('organization_id', sa.UUID(), nullable=False),
    sa.Column('entity_id', sa.UUID(), nullable=True),
    sa.Column('nm_id', sa.Integer(), nullable=False),
    sa.Column('vendor_code', sa.String(length=100), nullable=True),
    sa.Column('size_name', sa.String(length=50), nullable=True),
    sa.Column('period', sa.Date(), nullable=False),
    sa.Column('plan_type', sa.Enum('quantity', 'revenue', name='plantype'), nullable=False),
    sa.Column('plan_value', sa.Numeric(precision=12, scale=2), nullable=False),
    sa.Column('actual_value', sa.Numeric(precision=12, scale=2), nullable=False),
    sa.Column('sales_temp', sa.Numeric(precision=10, scale=2), nullable=True),
    sa.Column('seasonality', sa.Enum('low', 'medium', 'high', 'peak', name='seasonality'), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
    sa.ForeignKeyConstraint(['entity_id'], ['product_entities.id'], ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('organization_id', 'entity_id', 'period', 'plan_type', name='sales_plans_org_entity_period_type_key')
    )
    op.create_index(op.f('ix_sales_plans_entity_id'), 'sales_plans', ['entity_id'], unique=False)
    op.create_index(op.f('ix_sales_plans_nm_id'), 'sales_plans', ['nm_id'], unique=False)
    op.create_index(op.f('ix_sales_plans_organization_id'), 'sales_plans', ['organization_id'], unique=False)
    op.create_index(op.f('ix_sales_plans_period'), 'sales_plans', ['period'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_sales_plans_period'), table_name='sales_plans')
    op.drop_index(op.f('ix_sales_plans_organization_id'), table_name='sales_plans')
    op.drop_index(op.f('ix_sales_plans_nm_id'), table_name='sales_plans')
    op.drop_index(op.f('ix_sales_plans_entity_id'), table_name='sales_plans')
    op.drop_table('sales_plans')
