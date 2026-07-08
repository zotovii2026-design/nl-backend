"""add_strategy_milestones

Revision ID: x0y1z2a3b4c5
Revises: w9x0y1z2a3b4
Create Date: 2026-07-08 15:35:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "x0y1z2a3b4c5"
down_revision: Union[str, None] = "w9x0y1z2a3b4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "strategy_definitions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("category", sa.String(length=50), nullable=False),
        sa.Column("code", sa.String(length=30), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("default_executor", sa.String(length=255), nullable=True),
        sa.Column("role", sa.String(length=100), nullable=True),
        sa.Column("status", sa.String(length=30), nullable=False, server_default="active"),
        sa.Column("sort_order", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("organization_id", "category", "code", name="strategy_definitions_org_category_code_key"),
    )
    op.create_index("ix_strategy_definitions_category", "strategy_definitions", ["category"])
    op.create_index("ix_strategy_definitions_organization_id", "strategy_definitions", ["organization_id"])

    op.create_table(
        "strategy_milestones",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("strategy_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("nm_id", sa.Integer(), nullable=False),
        sa.Column("event_date", sa.Date(), nullable=False),
        sa.Column("date_to", sa.Date(), nullable=True),
        sa.Column("category", sa.String(length=50), nullable=False),
        sa.Column("strategy_code", sa.String(length=30), nullable=True),
        sa.Column("executor", sa.String(length=255), nullable=True),
        sa.Column("role", sa.String(length=100), nullable=True),
        sa.Column("source_links", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("result_note", sa.Text(), nullable=True),
        sa.Column("meta", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["entity_id"], ["product_entities.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["organization_id"], ["organizations.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategy_definitions.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_strategy_milestones_category", "strategy_milestones", ["category"])
    op.create_index("ix_strategy_milestones_entity_id", "strategy_milestones", ["entity_id"])
    op.create_index("ix_strategy_milestones_event_date", "strategy_milestones", ["event_date"])
    op.create_index("ix_strategy_milestones_nm_id", "strategy_milestones", ["nm_id"])
    op.create_index("ix_strategy_milestones_organization_id", "strategy_milestones", ["organization_id"])
    op.create_index("ix_strategy_milestones_strategy_id", "strategy_milestones", ["strategy_id"])


def downgrade() -> None:
    op.drop_index("ix_strategy_milestones_strategy_id", table_name="strategy_milestones")
    op.drop_index("ix_strategy_milestones_organization_id", table_name="strategy_milestones")
    op.drop_index("ix_strategy_milestones_nm_id", table_name="strategy_milestones")
    op.drop_index("ix_strategy_milestones_event_date", table_name="strategy_milestones")
    op.drop_index("ix_strategy_milestones_entity_id", table_name="strategy_milestones")
    op.drop_index("ix_strategy_milestones_category", table_name="strategy_milestones")
    op.drop_table("strategy_milestones")
    op.drop_index("ix_strategy_definitions_organization_id", table_name="strategy_definitions")
    op.drop_index("ix_strategy_definitions_category", table_name="strategy_definitions")
    op.drop_table("strategy_definitions")
