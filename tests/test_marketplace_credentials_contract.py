from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_capabilities_are_fail_closed_and_deduplicated():
    service = (ROOT / "services" / "marketplace_credentials.py").read_text()
    assert "invalid = set(normalized) - CAPABILITIES.get(marketplace, set())" in service
    assert 'raise ValueError("Выберите хотя бы одно право")' in service
    assert "sorted({" in service


def test_every_write_action_requires_a_specific_capability():
    service = (ROOT / "services" / "marketplace_credentials.py").read_text()
    assert '("wb", "prices.update"): "prices_write"' in service
    assert '("ozon", "stocks.update"): "stocks_write"' in service
    assert 'raise PermissionError("Операция не включена во внутренний allowlist NL")' in service


def test_parallel_ui_does_not_replace_legacy_connectors():
    template = (ROOT / "templates" / "nl_v2.html").read_text()
    assert "navTo('connectors',this)" in template
    assert "navTo('marketconnections',this)" in template
    assert 'id="page-connectors"' in template
    assert 'id="page-marketconnections"' in template


def test_api_never_returns_secret_fields():
    router = (ROOT / "api" / "v1" / "routers" / "marketplace_credentials.py").read_text()
    schema = (ROOT / "schemas" / "marketplace_credentials.py").read_text()
    read_model = schema.split("class MarketplaceCredentialRead", 1)[1]
    assert "encrypted_secret" not in read_model
    assert "secret:" not in read_model
    assert "decrypt_data" not in router


def test_migration_keeps_legacy_wb_key_table_untouched():
    migration = (ROOT / "alembic" / "versions" / "g7h8i9j0k1l2_add_marketplace_credentials.py").read_text()
    assert '"marketplace_credentials"' in migration
    assert '"marketplace_credential_use_events"' in migration
    assert "wb_api_keys" not in migration
