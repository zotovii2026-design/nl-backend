import pytest

from tasks.sync import utils


@pytest.mark.asyncio
async def test_wb_key_org_filter_limits_existing_sync_helpers(monkeypatch):
    async def fake_get_all_keys(_sf):
        return [
            ("org-a", "token-a"),
            ("org-b", "token-b"),
        ]

    monkeypatch.setattr(utils, "_get_all_keys_imported", fake_get_all_keys)

    token = utils.set_wb_key_org_filter("org-b")
    try:
        assert await utils._get_all_keys(object()) == [("org-b", "token-b")]
    finally:
        utils.reset_wb_key_org_filter(token)

    assert await utils._get_all_keys(object()) == [
        ("org-a", "token-a"),
        ("org-b", "token-b"),
    ]
