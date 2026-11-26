"""Read-only marketplace-neutral identity API."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import get_db
from core.dependencies import get_current_user
from core.tenant_auth import require_query_organization_access
from core.role_deps import require_organization_role
from models.organization import Role
from models.user import User
from models.commerce import (
    ListingVariant,
    MarketplaceAccount,
    MarketplaceListing,
    MarketplaceMatchingEvent,
    Product,
    ProductVariant,
)
from repositories.commerce_identity import resolve_product_entity, resolve_wb_listing
from schemas.commerce import (
    LegacyEntityResolutionRead,
    LegacyWbResolutionRead,
    MarketplaceAccountRead,
    ListingAttachRequest,
    ListingAttachResult,
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


def _variant_key(value: str | None) -> str:
    return " ".join(str(value or "").strip().casefold().split())


@router.post("/products/{product_id}/attach-listing", response_model=ListingAttachResult)
async def attach_listing_to_product(
    product_id: UUID,
    payload: ListingAttachRequest,
    org_id: str,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    organization_id = await _organization_uuid(org_id, db)
    await require_organization_role(organization_id, Role.ADMIN, current_user, db)
    product = await db.scalar(select(Product).where(Product.id == product_id, Product.organization_id == organization_id))
    listing = await db.scalar(select(MarketplaceListing).where(MarketplaceListing.id == payload.listing_id, MarketplaceListing.organization_id == organization_id))
    if product is None or listing is None:
        raise HTTPException(status_code=404, detail="Товар или артикул не найден")
    if listing.product_id == product.id:
        raise HTTPException(status_code=409, detail="Артикул уже входит в это сопоставление")

    target_variants = (await db.execute(select(ProductVariant).where(ProductVariant.organization_id == organization_id, ProductVariant.product_id == product.id))).scalars().all()
    listing_variants = (await db.execute(select(ListingVariant).where(ListingVariant.organization_id == organization_id, ListingVariant.marketplace_listing_id == listing.id))).scalars().all()
    variants_by_name = {}
    for variant in target_variants:
        variants_by_name.setdefault(_variant_key(variant.variant_name), []).append(variant)

    matched = 0
    for listing_variant in listing_variants:
        candidates = variants_by_name.get(_variant_key(listing_variant.variant_name), [])
        if len(candidates) == 1:
            listing_variant.product_variant_id = candidates[0].id
            matched += 1
        elif len(target_variants) == 1 and len(listing_variants) == 1:
            listing_variant.product_variant_id = target_variants[0].id
            matched += 1
        else:
            listing_variant.product_variant_id = None

    previous_product_id = listing.product_id
    listing.product_id = product.id
    db.add(MarketplaceMatchingEvent(
        organization_id=organization_id,
        marketplace_listing_id=listing.id,
        previous_product_id=previous_product_id,
        product_id=product.id,
        action="attach",
        reason=(payload.reason or "Ручное сопоставление в Справочнике общем").strip()[:500],
        created_by_user_id=current_user.id,
    ))
    await db.commit()
    return ListingAttachResult(
        listing_id=listing.id,
        previous_product_id=previous_product_id,
        product_id=product.id,
        matched_variants=matched,
        unmatched_variants=len(listing_variants) - matched,
    )


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
