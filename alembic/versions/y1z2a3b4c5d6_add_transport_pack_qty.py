"""add_transport_pack_qty

Revision ID: y1z2a3b4c5d6
Revises: b2c3d4e5f6g7
Create Date: 2026-07-14 15:25:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "y1z2a3b4c5d6"
down_revision: Union[str, None] = "b2c3d4e5f6g7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "reference_book",
        sa.Column(
            "transport_pack_qty",
            sa.Integer(),
            nullable=True,
            server_default="1",
            comment="Количество в транспортной упаковке",
        ),
    )
    op.execute("UPDATE reference_book SET transport_pack_qty = 1 WHERE transport_pack_qty IS NULL")


def downgrade() -> None:
    op.drop_column("reference_book", "transport_pack_qty")
