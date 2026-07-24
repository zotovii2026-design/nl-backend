from pathlib import Path


REFERENCE_SERVICE = Path("services/reference.py").read_text(encoding="utf-8")
REFERENCE_REPOSITORY = Path("repositories/reference.py").read_text(encoding="utf-8")
WB_FETCH = Path("tasks/sync/wb_fetch.py").read_text(encoding="utf-8")


def test_reference_helper_creates_missing_rows_from_product_entities():
    assert "async def ensure_reference_book_for_entities" in REFERENCE_SERVICE
    assert "FROM product_entities pe" in REFERENCE_SERVICE
    assert "NOT EXISTS" in REFERENCE_SERVICE
    assert "rb.entity_id = pe.id" in REFERENCE_SERVICE
    assert "ON CONFLICT ON CONSTRAINT reference_book_org_nm_eid_vf_key DO NOTHING" in REFERENCE_SERVICE
    assert "'auto-created from product_entities'" in REFERENCE_SERVICE


def test_products_sync_ensures_reference_rows_after_entity_sync():
    assert "from services.reference import ensure_reference_book_for_entities" in WB_FETCH
    assert "entity_result = await sync_entities_from_raw(db, org_id, today)" in WB_FETCH
    assert "reference_created = await ensure_reference_book_for_entities(db, org_id, today)" in WB_FETCH
    assert '"reference_created": reference_created' in WB_FETCH


def test_cost_prices_joins_one_product_name_per_nm():
    assert "SELECT DISTINCT ON (nm_id) nm_id, product_name" in REFERENCE_REPOSITORY
    assert "ORDER BY nm_id, target_date DESC" in REFERENCE_REPOSITORY
    assert "SELECT DISTINCT nm_id, product_name FROM tech_status" not in REFERENCE_REPOSITORY
