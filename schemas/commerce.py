"""Response schemas for the read-only neutral commerce API."""

from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class MarketplaceAccountRead(BaseModel):
    id: UUID
    marketplace: str
    external_account_id: str
    name: str
    status: str


class ListingVariantRead(BaseModel):
    id: UUID
    product_variant_id: Optional[UUID] = None
    external_variant_id: Optional[str] = None
    variant_name: Optional[str] = None
    status: str


class MarketplaceListingRead(BaseModel):
    id: UUID
    marketplace_account_id: UUID
    marketplace: str
    external_listing_id: str
    seller_sku: Optional[str] = None
    title: Optional[str] = None
    status: str
    variants: list[ListingVariantRead] = Field(default_factory=list)


class ProductVariantRead(BaseModel):
    id: UUID
    internal_sku: Optional[str] = None
    variant_name: str
    barcode: Optional[str] = None
    status: str


class ProductRead(BaseModel):
    id: UUID
    internal_sku: Optional[str] = None
    name: str
    brand: Optional[str] = None
    status: str
    variants: list[ProductVariantRead] = Field(default_factory=list)
    listings: list[MarketplaceListingRead] = Field(default_factory=list)


class ProductPageRead(BaseModel):
    items: list[ProductRead]
    limit: int
    offset: int
    returned: int


class LegacyWbResolutionRead(BaseModel):
    organization_id: UUID
    marketplace_account_id: UUID
    product_id: UUID
    marketplace_listing_id: UUID
    external_listing_id: str


class LegacyEntityResolutionRead(LegacyWbResolutionRead):
    product_variant_id: UUID
    listing_variant_id: UUID
    product_entity_id: UUID
