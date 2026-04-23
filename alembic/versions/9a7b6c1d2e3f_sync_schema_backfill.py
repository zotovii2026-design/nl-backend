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
    op.add_column('raw_barcodes', sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True))

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
