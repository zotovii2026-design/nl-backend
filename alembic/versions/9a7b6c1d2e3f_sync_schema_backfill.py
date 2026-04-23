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


def downgrade() -> None:
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
