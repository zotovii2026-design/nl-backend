"""add celery task run journal

Revision ID: q1r2s3t4u5v6
Revises: p9q0r1s2t3u4
Create Date: 2026-06-11
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "q1r2s3t4u5v6"
down_revision: Union[str, None] = "p9q0r1s2t3u4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "celery_task_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("task_id", sa.String(length=64), nullable=False),
        sa.Column("task_name", sa.String(length=150), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Integer(), nullable=True),
        sa.Column("result_summary", sa.Text(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_id"),
    )
    op.create_index(
        "ix_celery_task_runs_task_name",
        "celery_task_runs",
        ["task_name"],
        unique=False,
    )
    op.create_index(
        "ix_celery_task_runs_status",
        "celery_task_runs",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_celery_task_runs_status", table_name="celery_task_runs")
    op.drop_index("ix_celery_task_runs_task_name", table_name="celery_task_runs")
    op.drop_table("celery_task_runs")
