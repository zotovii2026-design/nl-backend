"""add subject_id and subject_name to reference_book

Revision ID: j3k4l5m6n7o8
Revises: i2j3k4l5m6n7
Create Date: 2026-05-08

"""
from alembic import op
import sqlalchemy as sa

revision = "j3k4l5m6n7o8"
down_revision = '5edfd0adf3c9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("reference_book", sa.Column("subject_id", sa.Integer(), nullable=True))
    op.add_column("reference_book", sa.Column("subject_name", sa.String(200), nullable=True))


def downgrade() -> None:
    op.drop_column("reference_book", "subject_name")
    op.drop_column("reference_book", "subject_id")
