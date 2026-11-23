import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODEL_PATH = ROOT / "models" / "commerce.py"
MIGRATION_PATH = (
    ROOT / "alembic" / "versions" / "e5f6g7h8i9j0_add_commerce_identity_spine.py"
)
REPOSITORY_PATH = ROOT / "repositories" / "commerce_identity.py"
ROUTER_PATH = ROOT / "api" / "v1" / "routers" / "commerce.py"
SCHEMA_PATH = ROOT / "schemas" / "commerce.py"
MAIN_PATH = ROOT / "main.py"


def _class_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {node.name for node in tree.body if isinstance(node, ast.ClassDef)}


def test_neutral_identity_models_exist():
    assert {
        "MarketplaceAccount",
        "Product",
        "ProductVariant",
        "MarketplaceListing",
        "ListingVariant",
        "VariantBarcode",
        "LegacyProductEntityLink",
    }.issubset(_class_names(MODEL_PATH))


def test_migration_is_additive_and_backfills_all_bridge_layers():
    source = MIGRATION_PATH.read_text(encoding="utf-8")
    assert 'down_revision: Union[str, None] = "b4c5d6e7f8a9"' in source
    for table in (
        "marketplace_accounts",
        "products",
        "product_variants",
        "marketplace_listings",
        "listing_variants",
        "variant_barcodes",
        "legacy_product_entity_links",
    ):
        assert f'INSERT INTO {table}' in source
        assert f'op.create_table(\n        "{table}"' in source
    assert "ON CONFLICT" in source
    assert "DROP TABLE product_entities" not in source.upper()
    assert "DROP TABLE organizations" not in source.upper()


def test_backfill_uses_scoped_wb_identifiers_and_no_credentials():
    source = MIGRATION_PATH.read_text(encoding="utf-8")
    assert "pe.organization_id::text || ':' || pe.nm_id::text" in source
    assert "'wildberries'" in source
    assert "legacy_nm_id" in source
    assert "api_key" not in source
    assert "personal_token" not in source


def test_resolver_requires_tenant_scope_for_listing_and_variant():
    source = REPOSITORY_PATH.read_text(encoding="utf-8")
    assert "MarketplaceListing.organization_id == organization_id" in source
    assert source.count("MarketplaceAccount.organization_id == organization_id") == 2
    assert "ListingVariant.organization_id == organization_id" in source
    assert 'MarketplaceAccount.marketplace == "wildberries"' in source


def test_existing_wb_models_remain_unchanged_by_new_model_module():
    source = MODEL_PATH.read_text(encoding="utf-8")
    assert "class WbProduct" not in source
    assert "class ProductEntity" not in source
    assert "nm_id = Column" not in source


def test_neutral_api_is_read_only_and_tenant_protected():
    source = ROUTER_PATH.read_text(encoding="utf-8")
    assert "dependencies=[Depends(require_query_organization_access)]" in source
    assert source.count("@router.get(") == 4
    assert "@router.post(" not in source
    assert "@router.put(" not in source
    assert "@router.patch(" not in source
    assert "@router.delete(" not in source
    assert source.count("organization_id == organization_id") >= 6


def test_neutral_api_does_not_expose_secrets_or_raw_identity():
    router_source = ROUTER_PATH.read_text(encoding="utf-8")
    schema_source = SCHEMA_PATH.read_text(encoding="utf-8")
    for forbidden in ("api_key", "personal_token", "settings", "raw_identity"):
        assert forbidden not in router_source
        assert forbidden not in schema_source


def test_neutral_router_is_registered():
    source = MAIN_PATH.read_text(encoding="utf-8")
    assert "seasonality, commerce" in source
    assert "app.include_router(commerce.router)" in source
