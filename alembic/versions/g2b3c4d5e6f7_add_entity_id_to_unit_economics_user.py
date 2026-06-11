"""add entity_id to unit_economics_user

Revision ID: g2b3c4d5e6f7
Revises: f1a2b3c4d5e6
Create Date: 2026-05-01 08:29:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision: str = 'g2b3c4d5e6f7'
down_revision: Union[str, None] = 'f1a2b3c4d5e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # This table also predated its first Alembic migration in production.
    op.execute("""
        CREATE TABLE IF NOT EXISTS unit_economics_user (
            id UUID PRIMARY KEY,
            organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
            nm_id INTEGER NOT NULL,
            barcode VARCHAR(50),
            size_name VARCHAR(50),
            mp_correction_pct NUMERIC(5, 2),
            buyout_niche_pct NUMERIC(5, 2),
            extra_costs NUMERIC(10, 2),
            ad_plan_rub NUMERIC(10, 2),
            price_before_spp_plan NUMERIC(10, 2),
            price_before_spp_change NUMERIC(10, 2),
            change_date DATE,
            tariff_type VARCHAR(20) DEFAULT 'box',
            wb_club_discount_pct NUMERIC(5, 2),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
            updated_at TIMESTAMP WITH TIME ZONE,
            CONSTRAINT unit_economics_user_organization_id_nm_id_barcode_key
                UNIQUE (organization_id, nm_id, barcode)
        )
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_unit_economics_user_organization_id
        ON unit_economics_user(organization_id)
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS ix_unit_economics_user_nm_id
        ON unit_economics_user(nm_id)
    """)

    # 1. Добавляем entity_id (nullable)
    op.add_column('unit_economics_user', sa.Column('entity_id', UUID(as_uuid=True), nullable=True))

    # 2. Backfill: определяем entity_id по nm_id (+ barcode если есть)
    op.execute("""
        UPDATE unit_economics_user ue
        SET entity_id = eb.entity_id
        FROM entity_barcodes eb
        WHERE ue.organization_id = eb.organization_id
          AND ue.barcode IS NOT NULL
          AND ue.barcode != ''
          AND eb.barcode = ue.barcode
          AND eb.is_active = true
          AND ue.entity_id IS NULL
    """)

    # 3. Fallback: первая сущность для nm_id
    op.execute("""
        UPDATE unit_economics_user ue
        SET entity_id = sub.entity_id
        FROM (
            SELECT DISTINCT ON (ue2.id) ue2.id as ue_id, pe.id as entity_id
            FROM unit_economics_user ue2
            JOIN product_entities pe ON pe.organization_id = ue2.organization_id AND pe.nm_id = ue2.nm_id
            WHERE ue2.entity_id IS NULL
            ORDER BY ue2.id, pe.size_name
        ) sub
        WHERE ue.id = sub.ue_id
    """)

    # 4. Новый unique constraint: (organization_id, entity_id)
    op.drop_constraint('unit_economics_user_organization_id_nm_id_barcode_key', 'unit_economics_user', type_='unique')
    op.create_unique_constraint(
        'unit_economics_user_org_entity_key',
        'unit_economics_user',
        ['organization_id', 'entity_id']
    )

    # 5. FK
    op.create_foreign_key(
        'unit_economics_user_entity_id_fkey',
        'unit_economics_user', 'product_entities',
        ['entity_id'], ['id'],
        ondelete='SET NULL'
    )

    # 6. Индекс
    op.create_index('ix_ue_user_entity_id', 'unit_economics_user', ['entity_id'])


def downgrade() -> None:
    op.drop_index('ix_ue_user_entity_id', table_name='unit_economics_user')
    op.drop_constraint('unit_economics_user_org_entity_key', 'unit_economics_user', type_='unique')
    op.drop_constraint('unit_economics_user_entity_id_fkey', 'unit_economics_user', type_='foreignkey')
    op.create_unique_constraint(
        'unit_economics_user_organization_id_nm_id_barcode_key',
        'unit_economics_user',
        ['organization_id', 'nm_id', 'barcode']
    )
    op.drop_column('unit_economics_user', 'entity_id')
