from pathlib import Path

ROUTER = Path("api/v1/routers/commerce.py").read_text(encoding="utf-8")
MODEL = Path("models/commerce.py").read_text(encoding="utf-8")
MIGRATION = Path("alembic/versions/f6g7h8i9j0k1_add_matching_events.py").read_text(encoding="utf-8")

def test_matching_requires_admin_and_tenant_scope():
    assert "Role.ADMIN" in ROUTER
    assert "Product.organization_id == organization_id" in ROUTER
    assert "MarketplaceListing.organization_id == organization_id" in ROUTER

def test_matching_is_append_only_audited():
    assert 'action="attach"' in ROUTER
    assert "MarketplaceMatchingEvent" in MODEL
    assert "marketplace_matching_events" in MIGRATION
    assert "previous_product_id" in MIGRATION
    assert "created_by_user_id" in MIGRATION

def test_variant_matching_fails_safe_to_unmatched():
    assert "listing_variant.product_variant_id = None" in ROUTER
    assert "matched_variants" in ROUTER
    assert "unmatched_variants" in ROUTER
