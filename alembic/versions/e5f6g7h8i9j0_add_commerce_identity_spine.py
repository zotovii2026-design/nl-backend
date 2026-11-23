"""add marketplace-neutral commerce identity spine

Revision ID: e5f6g7h8i9j0
Revises: b4c5d6e7f8a9
Create Date: 2026-11-18
"""

from typing import Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "e5f6g7h8i9j0"
down_revision: Union[str, None] = "b4c5d6e7f8a9"
branch_labels = None
depends_on = None


def _uuid_from(value_sql: str) -> str:
    """Return deterministic UUID SQL without requiring a PostgreSQL extension."""
    digest = f"md5({value_sql})"
    return (
        f"(substr({digest},1,8)||'-'||substr({digest},9,4)||'-'||"
        f"substr({digest},13,4)||'-'||substr({digest},17,4)||'-'||"
        f"substr({digest},21,12))::uuid"
    )


def upgrade() -> None:
    op.create_table(
        "marketplace_accounts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "organization_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("marketplace", sa.String(20), nullable=False),
        sa.Column("external_account_id", sa.String(100), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("status", sa.String(30), nullable=False, server_default="active"),
        sa.Column("settings", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "organization_id",
            "marketplace",
            "external_account_id",
            name="marketplace_accounts_org_marketplace_external_key",
        ),
    )
    op.create_index(
        "ix_marketplace_accounts_organization_id",
        "marketplace_accounts",
        ["organization_id"],
    )

    op.create_table(
        "products",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "organization_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("internal_sku", sa.String(100), nullable=True),
        sa.Column("name", sa.String(500), nullable=False),
        sa.Column("brand", sa.String(200), nullable=True),
        sa.Column("status", sa.String(30), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_products_organization_id", "products", ["organization_id"])

    op.create_table(
        "product_variants",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "organization_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "product_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("products.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("internal_sku", sa.String(100), nullable=True),
        sa.Column("variant_name", sa.String(100), nullable=False),
        sa.Column("barcode", sa.String(100), nullable=True),
        sa.Column("status", sa.String(30), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "product_id", "variant_name", name="product_variants_product_name_key"
        ),
    )
    op.create_index(
        "ix_product_variants_organization_id", "product_variants", ["organization_id"]
    )
    op.create_index("ix_product_variants_product_id", "product_variants", ["product_id"])

    op.create_table(
        "marketplace_listings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "organization_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "marketplace_account_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("marketplace_accounts.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "product_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("products.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("external_listing_id", sa.String(100), nullable=False),
        sa.Column("seller_sku", sa.String(100), nullable=True),
        sa.Column("title", sa.String(500), nullable=True),
        sa.Column("status", sa.String(30), nullable=False, server_default="active"),
        sa.Column("raw_identity", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "marketplace_account_id",
            "external_listing_id",
            name="marketplace_listings_account_external_key",
        ),
    )
    op.create_index(
        "ix_marketplace_listings_organization_id",
        "marketplace_listings",
        ["organization_id"],
    )
    op.create_index(
        "ix_marketplace_listings_marketplace_account_id",
        "marketplace_listings",
        ["marketplace_account_id"],
    )
    op.create_index(
        "ix_marketplace_listings_product_id", "marketplace_listings", ["product_id"]
    )

    op.create_table(
        "listing_variants",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "organization_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "marketplace_listing_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("marketplace_listings.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "product_variant_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("product_variants.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("external_variant_id", sa.String(100), nullable=True),
        sa.Column("variant_name", sa.String(100), nullable=True),
        sa.Column("status", sa.String(30), nullable=False, server_default="active"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint(
            "marketplace_listing_id",
            "external_variant_id",
            name="listing_variants_listing_external_key",
        ),
    )
    op.create_index(
        "ix_listing_variants_organization_id", "listing_variants", ["organization_id"]
    )
    op.create_index(
        "ix_listing_variants_marketplace_listing_id",
        "listing_variants",
        ["marketplace_listing_id"],
    )
    op.create_index(
        "ix_listing_variants_product_variant_id",
        "listing_variants",
        ["product_variant_id"],
    )

    op.create_table(
        "variant_barcodes",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "organization_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("organizations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "product_variant_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("product_variants.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("barcode", sa.String(100), nullable=False),
        sa.Column("first_seen", sa.Date(), nullable=False),
        sa.Column("last_seen", sa.Date(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint(
            "product_variant_id", "barcode", name="variant_barcodes_variant_barcode_key"
        ),
    )
    op.create_index(
        "ix_variant_barcodes_organization_id", "variant_barcodes", ["organization_id"]
    )
    op.create_index(
        "ix_variant_barcodes_product_variant_id",
        "variant_barcodes",
        ["product_variant_id"],
    )
    op.create_index("ix_variant_barcodes_barcode", "variant_barcodes", ["barcode"])
    op.create_index(
        "uq_variant_barcodes_active_org_barcode",
        "variant_barcodes",
        ["organization_id", "barcode"],
        unique=True,
        postgresql_where=sa.text("is_active"),
    )

    op.create_table(
        "legacy_product_entity_links",
        sa.Column(
            "product_entity_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("product_entities.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "product_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("products.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "product_variant_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("product_variants.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column(
            "marketplace_listing_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("marketplace_listings.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "listing_variant_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("listing_variants.id", ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    account_id = _uuid_from("'nl-commerce:wb-account:' || o.id::text")
    product_id = _uuid_from(
        "'nl-commerce:product:' || pe.organization_id::text || ':' || pe.nm_id::text"
    )
    listing_id = _uuid_from(
        "'nl-commerce:wb-listing:' || pe.organization_id::text || ':' || pe.nm_id::text"
    )
    listing_variant_id = _uuid_from("'nl-commerce:wb-listing-variant:' || pe.id::text")

    op.execute(
        f"""
        INSERT INTO marketplace_accounts
            (id, organization_id, marketplace, external_account_id, name, status)
        SELECT
            {account_id}, o.id, 'wildberries',
            COALESCE(o.wb_seller_id::text, 'legacy-org:' || o.id::text),
            o.name || ' / Wildberries', 'active'
        FROM organizations o
        WHERE EXISTS (
            SELECT 1 FROM product_entities pe WHERE pe.organization_id = o.id
        )
        ON CONFLICT (organization_id, marketplace, external_account_id) DO NOTHING
        """
    )
    op.execute(
        f"""
        INSERT INTO products
            (id, organization_id, internal_sku, name, brand, status)
        SELECT
            {product_id}, pe.organization_id,
            NULLIF(MAX(pe.vendor_code), ''),
            COALESCE(NULLIF(MAX(pe.product_name), ''), 'WB ' || pe.nm_id::text),
            NULLIF(MAX(pe.brand), ''), 'active'
        FROM product_entities pe
        GROUP BY pe.organization_id, pe.nm_id
        ON CONFLICT (id) DO NOTHING
        """
    )
    op.execute(
        f"""
        INSERT INTO marketplace_listings
            (id, organization_id, marketplace_account_id, product_id,
             external_listing_id, seller_sku, title, status, raw_identity)
        SELECT
            {listing_id}, pe.organization_id,
            {_uuid_from("'nl-commerce:wb-account:' || pe.organization_id::text")},
            {product_id}, pe.nm_id::text,
            NULLIF(MAX(pe.vendor_code), ''), NULLIF(MAX(pe.product_name), ''),
            'active', jsonb_build_object('legacy_nm_id', pe.nm_id)
        FROM product_entities pe
        GROUP BY pe.organization_id, pe.nm_id
        ON CONFLICT (id) DO NOTHING
        """
    )
    op.execute(
        f"""
        INSERT INTO product_variants
            (id, organization_id, product_id, internal_sku, variant_name, barcode, status)
        SELECT
            pe.id, pe.organization_id, {product_id},
            NULLIF(pe.vendor_code, ''), pe.size_name,
            (SELECT eb.barcode FROM entity_barcodes eb
             WHERE eb.entity_id = pe.id AND eb.is_active
             ORDER BY eb.last_seen DESC, eb.created_at DESC LIMIT 1),
            'active'
        FROM product_entities pe
        ON CONFLICT (id) DO NOTHING
        """
    )
    op.execute(
        f"""
        INSERT INTO listing_variants
            (id, organization_id, marketplace_listing_id, product_variant_id,
             external_variant_id, variant_name, status)
        SELECT
            {listing_variant_id}, pe.organization_id, {listing_id}, pe.id,
            pe.chrt_id::text, pe.size_name, 'active'
        FROM product_entities pe
        ON CONFLICT (id) DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO variant_barcodes
            (id, organization_id, product_variant_id, barcode,
             first_seen, last_seen, is_active, created_at)
        SELECT
            eb.id, eb.organization_id, eb.entity_id, eb.barcode,
            eb.first_seen, eb.last_seen, eb.is_active, eb.created_at
        FROM entity_barcodes eb
        ON CONFLICT (id) DO NOTHING
        """
    )
    op.execute(
        f"""
        INSERT INTO legacy_product_entity_links
            (product_entity_id, product_id, product_variant_id,
             marketplace_listing_id, listing_variant_id)
        SELECT pe.id, {product_id}, pe.id, {listing_id}, {listing_variant_id}
        FROM product_entities pe
        ON CONFLICT (product_entity_id) DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_table("legacy_product_entity_links")
    op.drop_index("uq_variant_barcodes_active_org_barcode", table_name="variant_barcodes")
    op.drop_index("ix_variant_barcodes_barcode", table_name="variant_barcodes")
    op.drop_index("ix_variant_barcodes_product_variant_id", table_name="variant_barcodes")
    op.drop_index("ix_variant_barcodes_organization_id", table_name="variant_barcodes")
    op.drop_table("variant_barcodes")
    op.drop_index("ix_listing_variants_product_variant_id", table_name="listing_variants")
    op.drop_index("ix_listing_variants_marketplace_listing_id", table_name="listing_variants")
    op.drop_index("ix_listing_variants_organization_id", table_name="listing_variants")
    op.drop_table("listing_variants")
    op.drop_index("ix_marketplace_listings_product_id", table_name="marketplace_listings")
    op.drop_index(
        "ix_marketplace_listings_marketplace_account_id", table_name="marketplace_listings"
    )
    op.drop_index("ix_marketplace_listings_organization_id", table_name="marketplace_listings")
    op.drop_table("marketplace_listings")
    op.drop_index("ix_product_variants_product_id", table_name="product_variants")
    op.drop_index("ix_product_variants_organization_id", table_name="product_variants")
    op.drop_table("product_variants")
    op.drop_index("ix_products_organization_id", table_name="products")
    op.drop_table("products")
    op.drop_index("ix_marketplace_accounts_organization_id", table_name="marketplace_accounts")
    op.drop_table("marketplace_accounts")
