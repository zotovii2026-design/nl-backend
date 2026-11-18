"""Read-only marketplace-neutral identity API."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.tenant_auth import require_query_organization_access
from models.commerce import (
    ListingVariant,
    MarketplaceAccount,
    MarketplaceListing,
    Product,
    ProductVariant,
)
from repositories.commerce_identity import resolve_product_entity, resolve_wb_listing
from schemas.commerce import (
    LegacyEntityResolutionRead,
    LegacyWbResolutionRead,
    MarketplaceAccountRead,
    ProductPageRead,
)
from services.reference import resolve_org_id


router = APIRouter(
    prefix="/api/v1/commerce",
    tags=["commerce"],
    dependencies=[Depends(require_query_organization_access)],
)


async def _organization_uuid(org_id: str, db: AsyncSession) -> UUID:
    return UUID(await resolve_org_id(org_id, db))


@router.get("/accounts", response_model=list[MarketplaceAccountRead])
async def get_marketplace_accounts(
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    organization_id = await _organization_uuid(org_id, db)
    result = await db.execute(
        select(MarketplaceAccount)
        .where(MarketplaceAccount.organization_id == organization_id)
        .order_by(MarketplaceAccount.marketplace, MarketplaceAccount.name)
    )
    return [
        MarketplaceAccountRead(
            id=account.id,
            marketplace=account.marketplace,
            external_account_id=account.external_account_id,
            name=account.name,
            status=account.status,
        )
        for account in result.scalars().all()
    ]


@router.get("/products", response_model=ProductPageRead)
async def get_products(
    org_id: str,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    organization_id = await _organization_uuid(org_id, db)
    products_result = await db.execute(
        select(Product)
        .where(Product.organization_id == organization_id)
        .order_by(Product.name, Product.id)
        .limit(limit)
        .offset(offset)
    )
    products = products_result.scalars().all()
    product_ids = [product.id for product in products]
    if not product_ids:
        return {"items": [], "limit": limit, "offset": offset, "returned": 0}

    variants_result = await db.execute(
        select(ProductVariant)
        .where(
            ProductVariant.organization_id == organization_id,
            ProductVariant.product_id.in_(product_ids),
        )
        .order_by(ProductVariant.product_id, ProductVariant.variant_name)
    )
    listings_result = await db.execute(
        select(MarketplaceListing, MarketplaceAccount.marketplace)
        .join(
            MarketplaceAccount,
            MarketplaceAccount.id == MarketplaceListing.marketplace_account_id,
        )
        .where(
            MarketplaceListing.organization_id == organization_id,
            MarketplaceAccount.organization_id == organization_id,
            MarketplaceListing.product_id.in_(product_ids),
        )
        .order_by(MarketplaceListing.product_id, MarketplaceAccount.marketplace)
    )
    listing_rows = listings_result.all()
    listing_ids = [listing.id for listing, _ in listing_rows]
    listing_variants = []
    if listing_ids:
        listing_variants_result = await db.execute(
            select(ListingVariant)
            .where(
                ListingVariant.organization_id == organization_id,
                ListingVariant.marketplace_listing_id.in_(listing_ids),
            )
            .order_by(ListingVariant.marketplace_listing_id, ListingVariant.variant_name)
        )
        listing_variants = listing_variants_result.scalars().all()

    variants_by_product: dict[UUID, list[dict]] = {}
    for variant in variants_result.scalars().all():
        variants_by_product.setdefault(variant.product_id, []).append(
            {
                "id": variant.id,
                "internal_sku": variant.internal_sku,
                "variant_name": variant.variant_name,
                "barcode": variant.barcode,
                "status": variant.status,
            }
        )

    listing_variants_by_listing: dict[UUID, list[dict]] = {}
    for variant in listing_variants:
        listing_variants_by_listing.setdefault(variant.marketplace_listing_id, []).append(
            {
                "id": variant.id,
                "product_variant_id": variant.product_variant_id,
                "external_variant_id": variant.external_variant_id,
                "variant_name": variant.variant_name,
                "status": variant.status,
            }
        )

    listings_by_product: dict[UUID, list[dict]] = {}
    for listing, marketplace in listing_rows:
        listings_by_product.setdefault(listing.product_id, []).append(
            {
                "id": listing.id,
                "marketplace_account_id": listing.marketplace_account_id,
                "marketplace": marketplace,
                "external_listing_id": listing.external_listing_id,
                "seller_sku": listing.seller_sku,
                "title": listing.title,
                "status": listing.status,
                "variants": listing_variants_by_listing.get(listing.id, []),
            }
        )

    items = [
        {
            "id": product.id,
            "internal_sku": product.internal_sku,
            "name": product.name,
            "brand": product.brand,
            "status": product.status,
            "variants": variants_by_product.get(product.id, []),
            "listings": listings_by_product.get(product.id, []),
        }
        for product in products
    ]
    return {"items": items, "limit": limit, "offset": offset, "returned": len(items)}


@router.get("/resolve/wb/{nm_id}", response_model=LegacyWbResolutionRead)
async def resolve_legacy_wb_listing(
    nm_id: int,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    organization_id = await _organization_uuid(org_id, db)
    identity = await resolve_wb_listing(db, organization_id, nm_id)
    if identity is None:
        raise HTTPException(status_code=404, detail="WB listing is not mapped")
    return identity


@router.get(
    "/resolve/entity/{product_entity_id}", response_model=LegacyEntityResolutionRead
)
async def resolve_legacy_entity(
    product_entity_id: UUID,
    org_id: str,
    db: AsyncSession = Depends(get_db),
):
    organization_id = await _organization_uuid(org_id, db)
    identity = await resolve_product_entity(db, organization_id, product_entity_id)
    if identity is None:
        raise HTTPException(status_code=404, detail="Product entity is not mapped")
    return identity
