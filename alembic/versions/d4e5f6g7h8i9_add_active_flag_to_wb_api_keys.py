"""add active flag to wb_api_keys

Revision ID: d4e5f6g7h8i9
Revises: b3c4d5e6f7g8
Create Date: 2026-08-11
"""

from alembic import op


revision = "d4e5f6g7h8i9"
down_revision = "b3c4d5e6f7g8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE wb_api_keys "
        "ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT false"
    )
    op.execute(
        """
        WITH chosen AS (
            SELECT DISTINCT ON (organization_id) id
            FROM wb_api_keys
            WHERE token_status = 'valid'
            ORDER BY organization_id, validated_at DESC NULLS LAST, created_at DESC NULLS LAST
        )
        UPDATE wb_api_keys k
        SET is_active = true
        FROM chosen
        WHERE k.id = chosen.id AND k.is_active = false
        """
    )
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_wb_api_keys_one_active_per_org
        ON wb_api_keys (organization_id)
        WHERE is_active
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_wb_api_keys_one_active_per_org")
    op.execute("ALTER TABLE wb_api_keys DROP COLUMN IF EXISTS is_active")
