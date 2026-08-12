from types import SimpleNamespace

import pytest

from tasks import initial_sync, seasonality_sync


@pytest.mark.asyncio
async def test_specific_seasonality_collect_raises_on_org_error(monkeypatch):
    async def broken_collect_for_org(_sf, org_id, nm_ids=None):
        raise RuntimeError(f"boom {org_id}")

    monkeypatch.setattr(seasonality_sync, "_collect_for_org", broken_collect_for_org)

    with pytest.raises(RuntimeError, match="boom org-1"):
        await seasonality_sync._collect(object(), org_id="org-1")


@pytest.mark.asyncio
async def test_seasonality_collect_reports_partial_weekly_errors(monkeypatch):
    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def execute(self, _query):
            return SimpleNamespace(all=lambda: [("org-ok",), ("org-bad",)])

    def session_factory():
        return FakeSession()

    async def fake_collect_for_org(_sf, org_id, nm_ids=None):
        if org_id == "org-bad":
            raise RuntimeError("evirma failed")
        return {"org_id": org_id, "status": "ok"}

    monkeypatch.setattr(seasonality_sync, "_collect_for_org", fake_collect_for_org)

    result = await seasonality_sync._collect(session_factory)

    assert result["status"] == "partial"
    assert result["organizations"] == 2
    assert result["errors"] == [
        {"org_id": "org-bad", "status": "error", "error": "evirma failed"}
    ]


@pytest.mark.asyncio
async def test_initial_sync_repairs_existing_data_without_top_queries(monkeypatch):
    states = [
        {
            "has_products": True,
            "has_tech_status": True,
            "reference_products": 257,
            "products_with_top_query": 0,
            "keyword_seasonality": 0,
            "product_seasonality": 0,
            "reference_with_seasonality": 0,
        },
        {
            "has_products": True,
            "has_tech_status": True,
            "reference_products": 257,
            "products_with_top_query": 257,
            "keyword_seasonality": 0,
            "product_seasonality": 0,
            "reference_with_seasonality": 0,
        },
    ]

    async def fake_state(_sf, _org_id):
        return states.pop(0)

    async def fake_auto_extract(_sf, _org_id):
        return {"status": "ok", "extracted": 257}

    class FakeRedis:
        @classmethod
        def from_url(cls, *_args, **_kwargs):
            return cls()

        async def set(self, *_args, **_kwargs):
            return True

        async def get(self, *_args):
            return "task-1"

        async def delete(self, *_args):
            return 1

        async def aclose(self):
            return None

    monkeypatch.setattr(initial_sync, "_get_initial_data_state", fake_state)
    monkeypatch.setattr(initial_sync, "_auto_extract_top_queries", fake_auto_extract)
    monkeypatch.setattr(initial_sync, "Redis", FakeRedis)

    result = await initial_sync._do_initial_sync(object(), "org-1", task_id="task-1")

    assert result["status"] == "repair"
    assert result["mode"] == "repair"
    assert result["needs_seasonality"] is True
    assert result["steps"]["auto_extract_queries"]["result"]["extracted"] == 257
