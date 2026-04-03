"""add is_superuser to users

Revision ID: 001_add_is_superuser_to_users
Revises:
Create Date: 2026-04-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '001_add_is_superuser_to_users'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Добавление колонки is_superuser с значением по умолчанию False
    op.add_column('users',
        sa.Column('is_superuser', sa.Boolean(), nullable=False, server_default='false')
    )
    op.add_column('users',
        sa.Column('last_login', sa.DateTime(timezone=True), nullable=True)
    )


def downgrade() -> None:
    # Удаление колонок при откатае
    op.drop_column('users', 'last_login')
    op.drop_column('users', 'is_superuser')
