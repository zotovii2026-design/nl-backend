"""add scoped marketplace credentials and append-only use audit

Revision ID: g7h8i9j0k1l2
Revises: f6g7h8i9j0k1
Create Date: 2026-11-27
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "g7h8i9j0k1l2"
down_revision = "f6g7h8i9j0k1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "marketplace_credentials",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("marketplace", sa.String(20), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("purpose", sa.String(50), nullable=False),
        sa.Column("capabilities", postgresql.JSONB(), nullable=False),
        sa.Column("encrypted_secret", sa.Text(), nullable=False),
        sa.Column("encrypted_client_id", sa.Text(), nullable=True),
        sa.Column("status", sa.String(30), nullable=False, server_default="unverified"),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("last_validated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by_user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("marketplace IN ('wb', 'ozon')", name="ck_marketplace_credentials_marketplace"),
    )
    op.create_index("ix_marketplace_credentials_organization_id", "marketplace_credentials", ["organization_id"])
    op.create_index("ix_marketplace_credentials_lookup", "marketplace_credentials", ["organization_id", "marketplace", "is_enabled"])
    op.create_table(
        "marketplace_credential_use_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("organization_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False),
        sa.Column("credential_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("marketplace_credentials.id", ondelete="SET NULL"), nullable=True),
        sa.Column("marketplace", sa.String(20), nullable=False),
        sa.Column("action", sa.String(80), nullable=False),
        sa.Column("object_type", sa.String(80), nullable=True),
        sa.Column("object_id", sa.String(255), nullable=True),
        sa.Column("outcome", sa.String(30), nullable=False),
        sa.Column("reason", sa.String(500), nullable=True),
        sa.Column("requested_by_user_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index("ix_marketplace_credential_use_events_organization_id", "marketplace_credential_use_events", ["organization_id"])
    op.create_index("ix_marketplace_credential_use_events_credential_id", "marketplace_credential_use_events", ["credential_id"])


def downgrade() -> None:
    op.drop_index("ix_marketplace_credential_use_events_credential_id", table_name="marketplace_credential_use_events")
    op.drop_index("ix_marketplace_credential_use_events_organization_id", table_name="marketplace_credential_use_events")
    op.drop_table("marketplace_credential_use_events")
    op.drop_index("ix_marketplace_credentials_lookup", table_name="marketplace_credentials")
    op.drop_index("ix_marketplace_credentials_organization_id", table_name="marketplace_credentials")
    op.drop_table("marketplace_credentials")

