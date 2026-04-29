"""add_product_entities_and_entity_barcodes

Revision ID: d82393c17c2d
Revises: a1b2c3d4e5f6
Create Date: 2026-04-29 09:40:20.923502

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'd82393c17c2d'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ─── 1. Создаём таблицу product_entities ───────────────────
    op.create_table('product_entities',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('organization_id', sa.UUID(), nullable=False),
        sa.Column('nm_id', sa.Integer(), nullable=False),
        sa.Column('vendor_code', sa.String(length=100), nullable=True),
        sa.Column('size_name', sa.String(length=50), nullable=False),
        sa.Column('product_name', sa.String(length=500), nullable=True),
        sa.Column('photo_main', sa.String(length=500), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('organization_id', 'nm_id', 'size_name', name='product_entities_org_nm_size_key')
    )
    op.create_index('ix_product_entities_nm_id', 'product_entities', ['nm_id'], unique=False)
    op.create_index('ix_product_entities_organization_id', 'product_entities', ['organization_id'], unique=False)

    # ─── 2. Создаём таблицу entity_barcodes ────────────────────
    op.create_table('entity_barcodes',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('entity_id', sa.UUID(), nullable=False),
        sa.Column('organization_id', sa.UUID(), nullable=False),
        sa.Column('barcode', sa.String(length=50), nullable=False),
        sa.Column('size_name', sa.String(length=50), nullable=True),
        sa.Column('first_seen', sa.Date(), nullable=False),
        sa.Column('last_seen', sa.Date(), nullable=False),
        sa.Column('is_active', sa.Boolean(), default=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.ForeignKeyConstraint(['entity_id'], ['product_entities.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('entity_id', 'barcode', name='entity_barcodes_entity_barcode_key')
    )
    op.create_index('ix_entity_barcodes_barcode', 'entity_barcodes', ['barcode'], unique=False)
    op.create_index('ix_entity_barcodes_entity_id', 'entity_barcodes', ['entity_id'], unique=False)
    op.create_index('ix_entity_barcodes_organization_id', 'entity_barcodes', ['organization_id'], unique=False)

    # ─── 3. Создаём таблицу unmatched_barcodes ─────────────────
    op.create_table('unmatched_barcodes',
        sa.Column('id', sa.UUID(), nullable=False),
        sa.Column('organization_id', sa.UUID(), nullable=False),
        sa.Column('barcode', sa.String(length=50), nullable=False),
        sa.Column('nm_id', sa.Integer(), nullable=True),
        sa.Column('size_name', sa.String(length=50), nullable=True),
        sa.Column('source', sa.String(length=20), nullable=False),
        sa.Column('raw_data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('target_date', sa.Date(), nullable=True),
        sa.Column('resolved', sa.Boolean(), default=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.ForeignKeyConstraint(['organization_id'], ['organizations.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('organization_id', 'barcode', 'source', 'target_date',
                            name='unmatched_barcodes_org_barcode_source_date_key')
    )
    op.create_index('ix_unmatched_barcodes_barcode', 'unmatched_barcodes', ['barcode'], unique=False)
    op.create_index('ix_unmatched_barcodes_organization_id', 'unmatched_barcodes', ['organization_id'], unique=False)

    # ─── 4. Добавляем entity_id в tech_status ──────────────────
    op.add_column('tech_status', sa.Column('entity_id', sa.UUID(), nullable=True))

    # Новый unique constraint по entity_id
    op.create_unique_constraint(
        'tech_status_org_date_entity_key', 'tech_status',
        ['organization_id', 'target_date', 'entity_id']
    )

    # Создаём FK на product_entities
    op.create_foreign_key(
        'fk_tech_status_entity_id', 'tech_status', 'product_entities',
        ['entity_id'], ['id'], ondelete='SET NULL'
    )

    # Индекс на entity_id
    op.create_index('ix_tech_status_entity_id', 'tech_status', ['entity_id'], unique=False)

    # ─── 5. Обратная совместимость: старый constraint оставляем ─
    # (он будет использоваться пока entity_id не заполнен для всех строк)


def downgrade() -> None:
    # Удаляем FK и constraint
    op.drop_constraint('fk_tech_status_entity_id', 'tech_status', type_='foreignkey')
    op.drop_constraint('tech_status_org_date_entity_key', 'tech_status', type_='unique')
    op.drop_index('ix_tech_status_entity_id', table_name='tech_status')
    op.drop_column('tech_status', 'entity_id')

    # Удаляем новые таблицы
    op.drop_table('unmatched_barcodes')
    op.drop_table('entity_barcodes')
    op.drop_table('product_entities')
