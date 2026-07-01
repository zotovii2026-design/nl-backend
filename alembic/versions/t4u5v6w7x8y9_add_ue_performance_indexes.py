"""add UE performance indexes

Revision ID: t4u5v6w7x8y9
Revises: s3t4u5v6w7x8
Create Date: 2026-07-01
"""
from alembic import op

revision = 't4u5v6w7x8y9'
down_revision = 's3t4u5v6w7x8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS '
        'idx_wb_tariff_snap_org_nm_date_desc '
        'ON wb_tariff_snapshot (organization_id, nm_id, target_date DESC)'
    )


def downgrade() -> None:
    op.execute('DROP INDEX CONCURRENTLY IF EXISTS idx_wb_tariff_snap_org_nm_date_desc')
