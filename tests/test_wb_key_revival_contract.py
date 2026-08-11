from pathlib import Path


IDENTITY_SOURCE = Path("api/v1/routers/identity.py").read_text(encoding="utf-8")
DASHBOARD_SOURCE = Path("templates/nl_v2.html").read_text(encoding="utf-8")


def test_wb_key_add_blocks_duplicate_shop_rebinding():
    assert "def _duplicate_shop_detail" in IDENTITY_SOURCE
    assert '"code": "shop_already_connected"' in IDENTITY_SOURCE
    assert "existing = await _find_user_org_by_seller_id(db, current_user.id, oid)" in IDENTITY_SOURCE
    assert "str(existing[0].id) != str(org_id)" in IDENTITY_SOURCE
    assert "Этот ключ относится к WB ID" in IDENTITY_SOURCE
    assert "if not org.wb_seller_id:" in IDENTITY_SOURCE
    assert "org.wb_seller_id = oid" in IDENTITY_SOURCE


def test_existing_shop_cards_can_revive_wb_key():
    assert "function addWbKeyToShop(btn)" in DASHBOARD_SOURCE
    assert "Ключ не добавлен" in DASHBOARD_SOURCE
    assert "data-orgid=\"' + s.id + '\"" in DASHBOARD_SOURCE
    assert "POST" in DASHBOARD_SOURCE
    assert "/api/v1/nl/wb-keys?org_id=" in DASHBOARD_SOURCE
    assert "function apiErrorMessage(err, fallback)" in DASHBOARD_SOURCE
    assert "shop_already_connected" in DASHBOARD_SOURCE
