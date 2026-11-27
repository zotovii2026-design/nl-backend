from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, model_validator


MarketplaceName = Literal["wb", "ozon"]


class MarketplaceCredentialCreate(BaseModel):
    marketplace: MarketplaceName
    name: str = Field(min_length=1, max_length=255)
    purpose: str = Field(min_length=1, max_length=50)
    capabilities: list[str] = Field(min_length=1)
    secret: str = Field(min_length=10)
    client_id: str | None = Field(default=None, max_length=255)

    @model_validator(mode="after")
    def require_ozon_client_id(self):
        if self.marketplace == "ozon" and not (self.client_id or "").strip():
            raise ValueError("Для Ozon обязателен Client-Id")
        return self


class MarketplaceCredentialUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=255)
    purpose: str | None = Field(default=None, min_length=1, max_length=50)
    capabilities: list[str] | None = Field(default=None, min_length=1)
    secret: str | None = Field(default=None, min_length=10)
    client_id: str | None = Field(default=None, max_length=255)
    is_enabled: bool | None = None


class MarketplaceCredentialRead(BaseModel):
    id: UUID
    marketplace: MarketplaceName
    name: str
    purpose: str
    capabilities: list[str]
    status: str
    is_enabled: bool
    has_client_id: bool
    last_validated_at: datetime | None
    created_at: datetime

