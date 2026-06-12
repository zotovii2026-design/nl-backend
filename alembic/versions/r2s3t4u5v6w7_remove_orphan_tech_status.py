"""Remove legacy tech status rows without a product entity.

Revision ID: r2s3t4u5v6w7
Revises: q1r2s3t4u5v6
"""

from typing import Sequence, Union

from alembic import op


revision: str = "r2s3t4u5v6w7"
down_revision: Union[str, None] = "q1r2s3t4u5v6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DELETE FROM tech_status WHERE entity_id IS NULL")


def downgrade() -> None:
    # Deleted legacy rows cannot be reconstructed reliably.
    pass
