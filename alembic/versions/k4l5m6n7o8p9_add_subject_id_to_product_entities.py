"""add subject_id to product_entities

Revision ID: k4l5m6n7o8p9
Revises: j3k4l5m6n7o8
Create Date: 2026-05-08

"""
from alembic import op
import sqlalchemy as sa

revision = "k4l5m6n7o8p9"
down_revision = "j3k4l5m6n7o8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("product_entities", sa.Column("subject_id", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("product_entities", "subject_id")
