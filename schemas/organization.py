from pydantic import BaseModel
from datetime import datetime
from uuid import UUID
from typing import Optional
from models.organization import Role, SubscriptionTier


class OrganizationCreate(BaseModel):
    name: str
    description: Optional[str] = None


class OrganizationUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


class OrganizationResponse(BaseModel):
    id: UUID
    name: str
    description: Optional[str]
    subscription_tier: SubscriptionTier
    subscription_status: str
    created_at: datetime
    updated_at: datetime | None

    class Config:
        from_attributes = True


class MembershipResponse(BaseModel):
    id: UUID
    user_id: UUID
    organization_id: UUID
    role: Role
    created_at: datetime

    class Config:
        from_attributes = True


class InvitationCreate(BaseModel):
    email: str
    role: Role


class InvitationResponse(BaseModel):
    id: UUID
    email: str
    organization_id: UUID
    role: Role
    status: str
    expires_at: datetime
    created_at: datetime

    class Config:
        from_attributes = True


class WbApiKeyCreate(BaseModel):
    name: str
    api_key: str


class WbApiKeyResponse(BaseModel):
    id: UUID
    organization_id: UUID
    name: str
    created_at: datetime
    updated_at: datetime | None

    class Config:
        from_attributes = True
