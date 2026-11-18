# NL Commerce OS: marketplace-neutral data model

Status: proposed for implementation

Date: 2026-11-18

Scope: application schema and migration bridge only. No production changes.

## Decision

Keep the existing `organizations` table as the tenant/client boundary and add a
marketplace-neutral product core beneath it. A client may own multiple WB and
Ozon accounts. A product belongs to the client, while a listing belongs to one
marketplace account.

Target hierarchy:

```text
organization (client / legal and financial boundary)
  -> marketplace_account (one WB or Ozon seller account)
    -> marketplace_listing (one card/offer on that account)

organization
  -> product (client-level product identity)
    -> product_variant (size/color/managed SKU)
      -> listing_variant (variant on a WB/Ozon listing)
```

This preserves tenant authorization through `organization_id`, prevents WB
identifiers from becoming global product identifiers, and allows the same
client product to be linked to WB and Ozon independently.

## Why the current model cannot simply be extended

Current production-oriented code has three coupled meanings:

- `organizations` is both a tenant and a WB seller cabinet;
- `product_entities` is a managed size/SKU but is keyed by WB `nm_id`;
- many facts, plans and strategies point directly to `nm_id` and sometimes to
  `product_entities.id`.

Adding `ozon_product_id` columns to these tables would mix two marketplace
namespaces, make cross-marketplace products ambiguous, and encourage accidental
duplication of card-level facts across variants.

## New tables

### `marketplace_accounts`

One connected seller account on one marketplace.

Required columns:

- `id UUID PK`
- `organization_id UUID NOT NULL FK organizations(id)`
- `marketplace VARCHAR(20) NOT NULL` — initially `wildberries` or `ozon`
- `external_account_id VARCHAR(100) NOT NULL`
- `name VARCHAR(255) NOT NULL`
- `status VARCHAR(30) NOT NULL DEFAULT 'active'`
- `settings JSONB NULL` — non-secret account settings only
- `created_at`, `updated_at`

Constraints:

- unique `(organization_id, marketplace, external_account_id)`
- all account queries remain scoped by `organization_id`
- credentials do not live in `settings`; they use a separate protected
  credential record/Secret Store reference

### `products`

The client-level commercial product. It is not a WB card and not an Ozon offer.

Required columns:

- `id UUID PK`
- `organization_id UUID NOT NULL FK organizations(id)`
- `internal_sku VARCHAR(100) NULL`
- `name VARCHAR(500) NOT NULL`
- `brand VARCHAR(200) NULL`
- `status VARCHAR(30) NOT NULL DEFAULT 'active'`
- `created_at`, `updated_at`

Constraints:

- optional unique partial key `(organization_id, internal_sku)` when
  `internal_sku IS NOT NULL`
- no marketplace identifiers on this table

### `product_variants`

The client-managed SKU/size. Purchase cost and safety limits ultimately belong
here, because they can differ by size.

Required columns:

- `id UUID PK`
- `organization_id UUID NOT NULL FK organizations(id)`
- `product_id UUID NOT NULL FK products(id)`
- `internal_sku VARCHAR(100) NULL`
- `variant_name VARCHAR(100) NOT NULL`
- `barcode VARCHAR(100) NULL` only as a convenient current value; barcode
  history remains separate
- `status VARCHAR(30) NOT NULL DEFAULT 'active'`
- `created_at`, `updated_at`

Constraints:

- unique `(product_id, variant_name)` for the first migration stage
- every referenced `product_id` must belong to the same `organization_id`

### `marketplace_listings`

One card/offer on one marketplace account.

Required columns:

- `id UUID PK`
- `organization_id UUID NOT NULL FK organizations(id)`
- `marketplace_account_id UUID NOT NULL FK marketplace_accounts(id)`
- `product_id UUID NULL FK products(id)` — nullable until matching is confirmed
- `external_listing_id VARCHAR(100) NOT NULL` — WB `nm_id`, Ozon product/offer id
- `seller_sku VARCHAR(100) NULL`
- `title VARCHAR(500) NULL`
- `status VARCHAR(30) NOT NULL DEFAULT 'active'`
- `raw_identity JSONB NULL`
- `created_at`, `updated_at`

Constraints:

- unique `(marketplace_account_id, external_listing_id)`
- the account, product and listing must share one `organization_id`

### `listing_variants`

The marketplace representation of a managed product variant.

Required columns:

- `id UUID PK`
- `organization_id UUID NOT NULL FK organizations(id)`
- `marketplace_listing_id UUID NOT NULL FK marketplace_listings(id)`
- `product_variant_id UUID NULL FK product_variants(id)`
- `external_variant_id VARCHAR(100) NULL` — WB `chrt_id` or Ozon equivalent
- `variant_name VARCHAR(100) NULL`
- `status VARCHAR(30) NOT NULL DEFAULT 'active'`
- `created_at`, `updated_at`

Constraints:

- unique `(marketplace_listing_id, external_variant_id)` when external id exists
- a listing variant may stay unmatched until identification is reliable

### `variant_barcodes`

Marketplace-neutral barcode history.

Required columns:

- `id UUID PK`
- `organization_id UUID NOT NULL FK organizations(id)`
- `product_variant_id UUID NOT NULL FK product_variants(id)`
- `barcode VARCHAR(100) NOT NULL`
- `first_seen`, `last_seen`, `is_active`

Constraints:

- an active barcode cannot point to two variants within one organization

## Existing-to-target mapping

| Current object | Target object | Migration rule |
| --- | --- | --- |
| `organizations` | `organizations` | Keep IDs; reinterpret as client/tenant boundary. |
| `organizations.wb_seller_id` | `marketplace_accounts.external_account_id` | Create one WB account for each organization with a seller id. Keep old column during compatibility period. |
| `wb_api_keys` | account credential binding | Add `marketplace_account_id`; do not move plaintext secrets through migrations/logs. |
| WB `nm_id` | `marketplace_listings.external_listing_id` | Store as text under the WB account. |
| `product_entities` grouped by `(organization_id, nm_id)` | `products` + WB `marketplace_listings` | Initially create one product per existing WB card; later allow confirmed WB/Ozon matching. |
| `product_entities` row | `product_variants` + `listing_variants` | Preserve current UUID through a bridge table; map `chrt_id` to `external_variant_id`. |
| `entity_barcodes` | `variant_barcodes` | Copy history without deleting the source rows. |
| `reference_book` | variant commercial settings | During transition keep current table and add `product_variant_id`; split price/account-specific fields later. |
| `strategy_definitions` | client strategy catalog | Keep under `organization_id`. |
| `strategy_milestones` | product/variant/listing strategy assignments | Add neutral foreign keys; retain `nm_id` as a legacy lookup until backfill is verified. |
| WB facts (`tech_status`, ads, finance, promotions) | WB fact tables linked to account/listing/variant | Add neutral foreign keys gradually; do not rewrite historical facts in the first migration. |

## Compatibility bridge

The first implementation must be additive. Existing endpoints continue to use
`organization_id`, `nm_id` and `entity_id` while new code resolves them through
an explicit bridge:

```text
(organization_id, nm_id)
  -> WB marketplace_account
  -> marketplace_listing

product_entities.id
  -> product_variant
  -> listing_variant
```

Recommended bridge table for a reversible rollout:

### `legacy_product_entity_links`

- `product_entity_id UUID PK FK product_entities(id)`
- `product_id UUID NOT NULL FK products(id)`
- `product_variant_id UUID NOT NULL FK product_variants(id)`
- `marketplace_listing_id UUID NOT NULL FK marketplace_listings(id)`
- `listing_variant_id UUID NOT NULL FK listing_variants(id)`
- `created_at`

It makes old/new parity measurable and avoids silently changing the meaning of
the existing `product_entities.id` key.

## Migration sequence

1. Add the six neutral core tables and the compatibility bridge. No old table
   or column is removed.
2. Backfill one WB marketplace account per current organization.
3. Group current entities by `(organization_id, nm_id)` and create one initial
   `product` plus one WB `marketplace_listing` for every group.
4. Create `product_variants` and `listing_variants` from `product_entities`,
   then copy barcode history.
5. Verify parity: counts, duplicate identifiers, tenant ownership and complete
   bridge coverage.
6. Add a read-only repository/API that returns the neutral hierarchy. Existing
   UI and sync jobs remain unchanged.
7. Connect Ozon read-only into the neutral tables. Matching an Ozon listing to
   an existing product requires an explicit rule or human confirmation.
8. Move business modules one at a time: reference data, plans/strategies, then
   derived facts and the Marketer Desk.
9. Remove legacy keys only after all consumers have migrated and a separate
   production approval is given.

## Required invariants and tests

- Every row is tenant-scoped and cannot reference an object from another
  organization.
- Marketplace external IDs are unique only inside their account namespace.
- Card-level facts are never multiplied by the number of variants.
- Variant-level facts require a reliable variant/barcode match; ambiguous rows
  remain unmatched.
- Existing WB endpoints return unchanged results during the additive phase.
- Backfill is idempotent and rollback removes only newly created neutral rows.
- No migration reads, prints or rewrites marketplace credentials.

## First implementation slice

Implement only the neutral identity spine:

1. SQLAlchemy models and Alembic migration for `marketplace_accounts`,
   `products`, `product_variants`, `marketplace_listings`, `listing_variants`,
   `variant_barcodes`, and `legacy_product_entity_links`.
2. Idempotent backfill using existing WB organizations/entities.
3. Characterization tests proving old WB behavior is unchanged.
4. A read-only internal repository for resolving legacy WB keys into neutral
   IDs.

Do not implement Ozon ingestion, price operations, UI changes or production
deployment in this slice.

## Implemented read-only API

The first neutral API is registered under `/api/v1/commerce` and requires the
existing query-tenant authorization through `org_id`:

- `GET /api/v1/commerce/accounts?org_id=...`
- `GET /api/v1/commerce/products?org_id=...&limit=50&offset=0`
- `GET /api/v1/commerce/resolve/wb/{nm_id}?org_id=...`
- `GET /api/v1/commerce/resolve/entity/{product_entity_id}?org_id=...`

It exposes no credentials, account settings or raw marketplace identity
payloads. There are no write endpoints in this router.
