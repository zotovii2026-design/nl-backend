"""add sync.py model

Revision ID: 004_add_sync_model
Revises: 003_add_sync_logs_table
Create Date: 2026-04-03

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '004_add_sync_model'
down_revision: Union[str, None] = '003_add_sync_logs_table'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Добавляем модель SyncLog в метаданные Alembic
    pass  # Модель уже создана через миграцию 003


def downgrade() -> None:
    pass
