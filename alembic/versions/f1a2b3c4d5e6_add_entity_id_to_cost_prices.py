"""add entity_id to cost_prices

Revision ID: f1a2b3c4d5e6
Revises: d82393c17c2d
Create Date: 2026-05-01 08:19:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision: str = 'f1a2b3c4d5e6'
down_revision: Union[str, None] = 'd82393c17c2d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Добавляем колонку entity_id (пока nullable)
    op.add_column('cost_prices', sa.Column('entity_id', UUID(as_uuid=True), nullable=True))

    # 2. Backfill: маппим nm_id -> entity_id
    # Сначала пробуем точное совпадение по size_name
    # Потом fallback: первая сущность для nm_id (если size_name пустой)
    op.execute("""
        UPDATE cost_prices cp
        SET entity_id = pe.id
        FROM product_entities pe
        WHERE cp.organization_id = pe.organization_id
          AND cp.nm_id = pe.nm_id
          AND cp.entity_id IS NULL
          AND (
              (cp.size_name IS NOT NULL AND cp.size_name != '' AND pe.size_name = cp.size_name)
              OR
              (cp.size_name IS NULL OR cp.size_name = '')
          )
    """)

    # 3. Fallback для записей где не нашлось по size_name — берём первую сущность
    op.execute("""
        UPDATE cost_prices cp
        SET entity_id = sub.entity_id
        FROM (
            SELECT DISTINCT ON (cp2.id) cp2.id as cp_id, pe.id as entity_id
            FROM cost_prices cp2
            JOIN product_entities pe ON pe.organization_id = cp2.organization_id AND pe.nm_id = cp2.nm_id
            WHERE cp2.entity_id IS NULL
            ORDER BY cp2.id, pe.size_name
        ) sub
        WHERE cp.id = sub.cp_id
    """)

    # 4. Добавляем FK
    op.create_foreign_key(
        'cost_prices_entity_id_fkey',
        'cost_prices', 'product_entities',
        ['entity_id'], ['id'],
        ondelete='SET NULL'
    )

    # 5. Новый unique constraint (organization_id, entity_id, valid_from)
    # Сначала старый dropping
    op.drop_constraint('cost_prices_org_nm_vf_key', 'cost_prices', type_='unique')
    # Новый — только для записей с entity_id
    op.create_unique_constraint(
        'cost_prices_org_entity_vf_key',
        'cost_prices',
        ['organization_id', 'entity_id', 'valid_from']
    )

    # 6. Индекс по entity_id
    op.create_index('ix_cost_prices_entity_id', 'cost_prices', ['entity_id'])


def downgrade() -> None:
    op.drop_index('ix_cost_prices_entity_id', table_name='cost_prices')
    op.drop_constraint('cost_prices_org_entity_vf_key', 'cost_prices', type_='unique')
    op.create_unique_constraint('cost_prices_org_nm_vf_key', 'cost_prices', ['organization_id', 'nm_id', 'valid_from'])
    op.drop_constraint('cost_prices_entity_id_fkey', 'cost_prices', type_='foreignkey')
    op.drop_column('cost_prices', 'entity_id')
