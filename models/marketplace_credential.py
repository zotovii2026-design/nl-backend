"""Scoped marketplace credentials for the parallel WB/Ozon connections UI."""

import uuid

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from core.database import Base


class MarketplaceCredential(Base):
    __tablename__ = "marketplace_credentials"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    marketplace = Column(String(20), nullable=False)
    name = Column(String(255), nullable=False)
    purpose = Column(String(50), nullable=False)
    capabilities = Column(JSONB, nullable=False, default=list)
    encrypted_secret = Column(Text, nullable=False)
    encrypted_client_id = Column(Text, nullable=True)
    status = Column(String(30), nullable=False, default="unverified", server_default="unverified")
    is_enabled = Column(Boolean, nullable=False, default=False, server_default="false")
    last_validated_at = Column(DateTime(timezone=True), nullable=True)
    created_by_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=True, onupdate=func.now())

    organization = relationship("Organization")


class MarketplaceCredentialUseEvent(Base):
    __tablename__ = "marketplace_credential_use_events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    credential_id = Column(
        UUID(as_uuid=True),
        ForeignKey("marketplace_credentials.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    marketplace = Column(String(20), nullable=False)
    action = Column(String(80), nullable=False)
    object_type = Column(String(80), nullable=True)
    object_id = Column(String(255), nullable=True)
    outcome = Column(String(30), nullable=False)
    reason = Column(String(500), nullable=True)
    requested_by_user_id = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

