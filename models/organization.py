import enum
from sqlalchemy import Column, String, DateTime, func, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID, ENUM
from sqlalchemy.orm import relationship
from core.database import Base
import uuid


class SubscriptionTier(str, enum.Enum):
    """Тарифный план"""
    TRIAL = "trial"
    BASIC = "basic"
    PRO = "pro"
    ENTERPRISE = "enterprise"


class SubscriptionStatus(str, enum.Enum):
    """Статус подписки"""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    CANCELLED = "cancelled"


class Role(str, enum.Enum):
    """Роль в организации"""
    OWNER = "owner"
    ADMIN = "admin"
    VIEWER = "viewer"


class InvitationStatus(str, enum.Enum):
    """Статус приглашения"""
    PENDING = "pending"
    ACCEPTED = "accepted"
    DECLINED = "declined"
    EXPIRED = "expired"


class Organization(Base):
    __tablename__ = "organizations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    subscription_tier = Column(ENUM(SubscriptionTier), default=SubscriptionTier.TRIAL)
    subscription_status = Column(ENUM(SubscriptionStatus), default=SubscriptionStatus.ACTIVE)
    wb_seller_id = Column(String(20), nullable=True)  # ID магазина WB (oid из JWT)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Отношения
    memberships = relationship("Membership", back_populates="organization", cascade="all, delete-orphan")
    invitations = relationship("Invitation", back_populates="organization", cascade="all, delete-orphan")
    wb_api_keys = relationship("WbApiKey", back_populates="organization", cascade="all, delete-orphan")


class Membership(Base):
    __tablename__ = "memberships"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    role = Column(ENUM(Role), default=Role.VIEWER)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Отношения
    user = relationship("User")
    organization = relationship("Organization", back_populates="memberships")

    # Уникальный индекс на user + organization
    __table_args__ = (
        {"schema": None},
    )


class Invitation(Base):
    __tablename__ = "invitations"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), nullable=False)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    role = Column(ENUM(Role), default=Role.VIEWER)
    token = Column(String(255), unique=True, nullable=False, index=True)
    status = Column(ENUM(InvitationStatus), default=InvitationStatus.PENDING)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Отношения
    organization = relationship("Organization", back_populates="invitations")


class WbApiKey(Base):
    __tablename__ = "wb_api_keys"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    name = Column(String(255), nullable=False)
    api_key = Column(Text, nullable=False)  # Зашифрованный ключ
    personal_token = Column(Text, nullable=True)  # Personal token для analytics API (зашифрованный)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Отношения
    organization = relationship("Organization", back_populates="wb_api_keys")
