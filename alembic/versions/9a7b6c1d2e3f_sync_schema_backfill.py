"""sync schema backfill for tech_status and raw_barcodes

Revision ID: 9a7b6c1d2e3f
Revises: 878bbd306692
Create Date: 2026-04-23 08:56:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '9a7b6c1d2e3f'
down_revision: Union[str, None] = '878bbd306692'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Создаём raw_barcodes, если не существует
    op.execute("CREATE TABLE IF NOT EXISTS raw_barcodes (id UUID PRIMARY KEY, organization_id UUID NOT NULL, barcode VARCHAR(100), nm_id INTEGER, source VARCHAR(50), raw_data JSONB, created_at TIMESTAMP WITH TIME ZONE DEFAULT now(), synced_at TIMESTAMP WITH TIME ZONE DEFAULT now())")
    op.execute("CREATE INDEX IF NOT EXISTS ix_raw_barcodes_organization_id ON raw_barcodes(organization_id)")
    op.add_column('raw_barcodes', sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True))

    # Создаём tech_status, если не существует
    op.execute("CREATE TABLE IF NOT EXISTS tech_status (id UUID PRIMARY KEY, organization_id UUID NOT NULL, nm_id INTEGER, status VARCHAR(50), created_at TIMESTAMP WITH TIME ZONE DEFAULT now())")
    op.execute("CREATE INDEX IF NOT EXISTS ix_tech_status_organization_id ON tech_status(organization_id)")
    op.add_column('tech_status', sa.Column('cards_total', sa.Integer(), nullable=True))
    op.add_column('tech_status', sa.Column('cards_archive', sa.Integer(), nullable=True))
    op.add_column('tech_status', sa.Column('cards_draft', sa.Integer(), nullable=True))
    op.add_column('tech_status', sa.Column('cards_active', sa.Integer(), nullable=True))
    op.add_column('tech_status', sa.Column('photo_count', sa.Integer(), nullable=True))
    op.add_column('tech_status', sa.Column('has_video', sa.String(length=5), nullable=True))
    op.add_column('tech_status', sa.Column('description_chars', sa.Integer(), nullable=True))
    op.add_column('tech_status', sa.Column('cell_statuses', postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column('tech_status', sa.Column('last_sync_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('tech_status', sa.Column('sku', sa.String(length=50), nullable=True))
    op.add_column('tech_status', sa.Column('impressions', sa.Integer(), nullable=True))
    op.add_column('tech_status', sa.Column('clicks', sa.Integer(), nullable=True))
    # These columns existed in production before the schema was fully captured
    # by Alembic. Keep the historical migration self-contained so a clean
    # database can reach the current schema.
    op.add_column('tech_status', sa.Column('target_date', sa.Date(), nullable=True))
    op.add_column('tech_status', sa.Column('vendor_code', sa.String(length=100), nullable=True))
    op.add_column('tech_status', sa.Column('barcode', sa.String(length=50), nullable=True))
    op.add_column('tech_status', sa.Column('product_name', sa.String(length=500), nullable=True))
    op.add_column('tech_status', sa.Column('photo_main', sa.String(length=500), nullable=True))
    op.add_column('tech_status', sa.Column('rating', sa.Numeric(3, 2), nullable=True))
    op.add_column('tech_status', sa.Column('orders_count', sa.Integer(), nullable=True))
    op.add_column('tech_status', sa.Column('buyouts_count', sa.Integer(), nullable=True))
    op.add_column('tech_status', sa.Column('returns_count', sa.Integer(), nullable=True))
    op.add_column('tech_status', sa.Column('warehouse_name', sa.String(length=200), nullable=True))
    op.add_column('tech_status', sa.Column('stock_qty', sa.Integer(), nullable=True))
    op.add_column('tech_status', sa.Column('stock_fbo_qty', sa.Integer(), server_default='0', nullable=True))
    op.add_column('tech_status', sa.Column('tariff', sa.Numeric(10, 2), nullable=True))
    op.add_column('tech_status', sa.Column('price', sa.Numeric(10, 2), nullable=True))
    op.add_column('tech_status', sa.Column('price_discount', sa.Numeric(10, 2), nullable=True))
    op.add_column('tech_status', sa.Column('price_spp', sa.Numeric(10, 2), nullable=True))
    op.add_column('tech_status', sa.Column('ad_cost', sa.Numeric(10, 2), nullable=True))
    op.add_column('tech_status', sa.Column('row_status', sa.String(length=20), server_default='active', nullable=False))
    op.add_column('tech_status', sa.Column('is_final', sa.String(length=5), server_default='no', nullable=True))
    op.add_column('tech_status', sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True))
    op.create_index('idx_tech_status_target_date', 'tech_status', ['target_date'], unique=False)
    op.create_index('idx_tech_status_nm_id', 'tech_status', ['nm_id'], unique=False)


def downgrade() -> None:
    op.drop_index('idx_tech_status_nm_id', table_name='tech_status')
    op.drop_index('idx_tech_status_target_date', table_name='tech_status')
    op.drop_column('tech_status', 'updated_at')
    op.drop_column('tech_status', 'is_final')
    op.drop_column('tech_status', 'row_status')
    op.drop_column('tech_status', 'ad_cost')
    op.drop_column('tech_status', 'price_spp')
    op.drop_column('tech_status', 'price_discount')
    op.drop_column('tech_status', 'price')
    op.drop_column('tech_status', 'tariff')
    op.drop_column('tech_status', 'stock_fbo_qty')
    op.drop_column('tech_status', 'stock_qty')
    op.drop_column('tech_status', 'warehouse_name')
    op.drop_column('tech_status', 'returns_count')
    op.drop_column('tech_status', 'buyouts_count')
    op.drop_column('tech_status', 'orders_count')
    op.drop_column('tech_status', 'rating')
    op.drop_column('tech_status', 'photo_main')
    op.drop_column('tech_status', 'product_name')
    op.drop_column('tech_status', 'barcode')
    op.drop_column('tech_status', 'vendor_code')
    op.drop_column('tech_status', 'target_date')
    op.drop_column('tech_status', 'clicks')
    op.drop_column('tech_status', 'impressions')
    op.drop_column('tech_status', 'sku')
    op.drop_column('tech_status', 'last_sync_at')
    op.drop_column('tech_status', 'cell_statuses')
    op.drop_column('tech_status', 'description_chars')
    op.drop_column('tech_status', 'has_video')
    op.drop_column('tech_status', 'photo_count')
    op.drop_column('tech_status', 'cards_active')
    op.drop_column('tech_status', 'cards_draft')
    op.drop_column('tech_status', 'cards_archive')
    op.drop_column('tech_status', 'cards_total')

    op.drop_column('raw_barcodes', 'updated_at')
