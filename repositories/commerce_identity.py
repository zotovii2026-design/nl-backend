"""Read-only resolution between legacy WB keys and neutral commerce IDs."""

from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.commerce import (
    LegacyProductEntityLink,
    ListingVariant,
    MarketplaceAccount,
    MarketplaceListing,
)


@dataclass(frozen=True)
class NeutralListingIdentity:
    organization_id: UUID
    marketplace_account_id: UUID
    product_id: UUID
    marketplace_listing_id: UUID
    external_listing_id: str


@dataclass(frozen=True)
class NeutralVariantIdentity(NeutralListingIdentity):
    product_variant_id: UUID
    listing_variant_id: UUID
    product_entity_id: UUID


async def resolve_wb_listing(
    db: AsyncSession, organization_id: UUID, nm_id: int
) -> Optional[NeutralListingIdentity]:
    """Resolve an organization-scoped WB card without exposing other tenants."""
    result = await db.execute(
        select(MarketplaceListing, MarketplaceAccount)
        .join(
            MarketplaceAccount,
            MarketplaceAccount.id == MarketplaceListing.marketplace_account_id,
        )
        .where(
            MarketplaceListing.organization_id == organization_id,
            MarketplaceAccount.organization_id == organization_id,
            MarketplaceAccount.marketplace == "wildberries",
            MarketplaceListing.external_listing_id == str(nm_id),
        )
    )
    row = result.first()
    if row is None:
        return None
    listing, account = row
    return NeutralListingIdentity(
        organization_id=organization_id,
        marketplace_account_id=account.id,
        product_id=listing.product_id,
        marketplace_listing_id=listing.id,
        external_listing_id=listing.external_listing_id,
    )


async def resolve_product_entity(
    db: AsyncSession, organization_id: UUID, product_entity_id: UUID
) -> Optional[NeutralVariantIdentity]:
    """Resolve a legacy entity through the explicit bridge and tenant checks."""
    result = await db.execute(
        select(
            LegacyProductEntityLink,
            MarketplaceListing,
            MarketplaceAccount,
            ListingVariant,
        )
        .join(
            MarketplaceListing,
            MarketplaceListing.id == LegacyProductEntityLink.marketplace_listing_id,
        )
        .join(
            MarketplaceAccount,
            MarketplaceAccount.id == MarketplaceListing.marketplace_account_id,
        )
        .join(
            ListingVariant,
            ListingVariant.id == LegacyProductEntityLink.listing_variant_id,
        )
        .where(
            LegacyProductEntityLink.product_entity_id == product_entity_id,
            MarketplaceListing.organization_id == organization_id,
            MarketplaceAccount.organization_id == organization_id,
            ListingVariant.organization_id == organization_id,
            MarketplaceAccount.marketplace == "wildberries",
        )
    )
    row = result.first()
    if row is None:
        return None
    link, listing, account, listing_variant = row
    return NeutralVariantIdentity(
        organization_id=organization_id,
        marketplace_account_id=account.id,
        product_id=link.product_id,
        marketplace_listing_id=listing.id,
        external_listing_id=listing.external_listing_id,
        product_variant_id=link.product_variant_id,
        listing_variant_id=listing_variant.id,
        product_entity_id=link.product_entity_id,
    )
