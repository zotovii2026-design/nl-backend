"""Marketplace-neutral identity spine for NL Commerce OS."""

import uuid

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship

from core.database import Base


class MarketplaceAccount(Base):
    __tablename__ = "marketplace_accounts"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    marketplace = Column(String(20), nullable=False)
    external_account_id = Column(String(100), nullable=False)
    name = Column(String(255), nullable=False)
    status = Column(String(30), nullable=False, default="active", server_default="active")
    settings = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    organization = relationship("Organization")
    listings = relationship("MarketplaceListing", back_populates="marketplace_account")

    __table_args__ = (
        UniqueConstraint(
            "organization_id",
            "marketplace",
            "external_account_id",
            name="marketplace_accounts_org_marketplace_external_key",
        ),
    )


class Product(Base):
    __tablename__ = "products"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    internal_sku = Column(String(100), nullable=True)
    name = Column(String(500), nullable=False)
    brand = Column(String(200), nullable=True)
    status = Column(String(30), nullable=False, default="active", server_default="active")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    organization = relationship("Organization")
    variants = relationship("ProductVariant", back_populates="product")
    listings = relationship("MarketplaceListing", back_populates="product")


class ProductVariant(Base):
    __tablename__ = "product_variants"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    product_id = Column(
        UUID(as_uuid=True),
        ForeignKey("products.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    internal_sku = Column(String(100), nullable=True)
    variant_name = Column(String(100), nullable=False)
    barcode = Column(String(100), nullable=True)
    status = Column(String(30), nullable=False, default="active", server_default="active")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    organization = relationship("Organization")
    product = relationship("Product", back_populates="variants")
    listing_variants = relationship("ListingVariant", back_populates="product_variant")
    barcodes = relationship("VariantBarcode", back_populates="product_variant")

    __table_args__ = (
        UniqueConstraint(
            "product_id", "variant_name", name="product_variants_product_name_key"
        ),
    )


class MarketplaceListing(Base):
    __tablename__ = "marketplace_listings"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    marketplace_account_id = Column(
        UUID(as_uuid=True),
        ForeignKey("marketplace_accounts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    product_id = Column(
        UUID(as_uuid=True),
        ForeignKey("products.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    external_listing_id = Column(String(100), nullable=False)
    seller_sku = Column(String(100), nullable=True)
    title = Column(String(500), nullable=True)
    status = Column(String(30), nullable=False, default="active", server_default="active")
    raw_identity = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    organization = relationship("Organization")
    marketplace_account = relationship("MarketplaceAccount", back_populates="listings")
    product = relationship("Product", back_populates="listings")
    variants = relationship("ListingVariant", back_populates="marketplace_listing")

    __table_args__ = (
        UniqueConstraint(
            "marketplace_account_id",
            "external_listing_id",
            name="marketplace_listings_account_external_key",
        ),
    )


class ListingVariant(Base):
    __tablename__ = "listing_variants"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    marketplace_listing_id = Column(
        UUID(as_uuid=True),
        ForeignKey("marketplace_listings.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    product_variant_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_variants.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    external_variant_id = Column(String(100), nullable=True)
    variant_name = Column(String(100), nullable=True)
    status = Column(String(30), nullable=False, default="active", server_default="active")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    organization = relationship("Organization")
    marketplace_listing = relationship("MarketplaceListing", back_populates="variants")
    product_variant = relationship("ProductVariant", back_populates="listing_variants")

    __table_args__ = (
        UniqueConstraint(
            "marketplace_listing_id",
            "external_variant_id",
            name="listing_variants_listing_external_key",
        ),
    )


class VariantBarcode(Base):
    __tablename__ = "variant_barcodes"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    product_variant_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_variants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    barcode = Column(String(100), nullable=False, index=True)
    first_seen = Column(Date, nullable=False)
    last_seen = Column(Date, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True, server_default="true")
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    organization = relationship("Organization")
    product_variant = relationship("ProductVariant", back_populates="barcodes")

    __table_args__ = (
        UniqueConstraint(
            "product_variant_id", "barcode", name="variant_barcodes_variant_barcode_key"
        ),
        Index(
            "uq_variant_barcodes_active_org_barcode",
            "organization_id",
            "barcode",
            unique=True,
            postgresql_where=text("is_active"),
        ),
    )


class LegacyProductEntityLink(Base):
    __tablename__ = "legacy_product_entity_links"

    product_entity_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_entities.id", ondelete="CASCADE"),
        primary_key=True,
    )
    product_id = Column(
        UUID(as_uuid=True), ForeignKey("products.id", ondelete="CASCADE"), nullable=False
    )
    product_variant_id = Column(
        UUID(as_uuid=True),
        ForeignKey("product_variants.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    marketplace_listing_id = Column(
        UUID(as_uuid=True),
        ForeignKey("marketplace_listings.id", ondelete="CASCADE"),
        nullable=False,
    )
    listing_variant_id = Column(
        UUID(as_uuid=True),
        ForeignKey("listing_variants.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    created_at = Column(DateTime(timezone=True), server_default=func.now())
