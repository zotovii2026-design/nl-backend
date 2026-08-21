"""drop legacy sync tables (wb_products, wb_sales, wb_orders, wb_stocks, sync_logs)

Revision ID: c1e2a3b4c5d6
Revises: a8b9c0d1e2f3
Create Date: 2026-08-21

Все таблицы пустые, данные собираются пайплайном v2 (raw_api_data -> tech_status).
Legacy /sync/* endpoints удалены вместе с моделями.
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = 'c1e2a3b4c5d6'
down_revision = 'a8b9c0d1e2f3'
branch_labels = None
depends_on = None

LEGACY_TABLES = ['wb_stocks', 'wb_orders', 'wb_sales', 'wb_products', 'sync_logs']


def upgrade() -> None:
    for t in LEGACY_TABLES:
        op.drop_table(t)


def downgrade() -> None:
    # Восстановление структуры (таблицы были пустые — данных нет)
    op.create_table(
        'sync_logs',
        sa.Column('id', UUID(as_uuid=True), primary_key=True),
        sa.Column('organization_id', UUID(as_uuid=True), nullable=False),
        sa.Column('task_name', sa.String(100), nullable=False),
        sa.Column('status', sa.String(20), nullable=False),
        sa.Column('started_at', sa.DateTime(), nullable=False),
        sa.Column('finished_at', sa.DateTime(), nullable=True),
        sa.Column('error_message', sa.Text(), nullable=True),
    )
    # wb_products/wb_sales/wb_orders/wb_stocks при откате восстанавливаются
    # из schema_snapshot_20260821.sql при необходимости (таблицы пустые).
