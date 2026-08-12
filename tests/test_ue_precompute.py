import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from api.v1.routers import unit_economics as ue_router
from repositories.unit_economics import count_products, get_products, get_supporting_rows
from tasks.ue_precompute import _get_org_ids, precompute_ue_cache


class _SessionContext:
    async def __aenter__(self):
        return AsyncMock()

    async def __aexit__(self, exc_type, exc, traceback):
        return False


@pytest.mark.asyncio
async def test_precompute_uses_injected_service_and_session():
    build = AsyncMock(return_value={"total": 3})
    session_factory = lambda: _SessionContext()

    await precompute_ue_cache(["org-1"], session_factory, build)

    build.assert_awaited_once()
    assert build.await_args.args[0] == "org-1"


@pytest.mark.asyncio
async def test_get_org_ids_uses_supplied_session_factory():
    db = AsyncMock()
    result = MagicMock()
    result.all.return_value = [("org-1",), ("org-2",)]
    db.execute.return_value = result

    class _DatabaseContext:
        async def __aenter__(self):
            return db

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    result = await _get_org_ids(lambda: _DatabaseContext())

    assert result == ["org-1", "org-2"]
    db.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_supporting_queries_reuse_injected_session():
    db = AsyncMock()
    result = MagicMock()
    result.all.side_effect = [["reference"], ["snapshot"], ["box"]]
    db.execute.return_value = result

    rows = await get_supporting_rows("org-1", db=db)

    assert rows == (["reference"], ["snapshot"], ["box"])
    assert db.execute.await_count == 3


@pytest.mark.asyncio
async def test_supporting_queries_can_scope_to_visible_products():
    db = AsyncMock()
    result = MagicMock()
    result.all.side_effect = [["reference"], ["snapshot"], ["box"]]
    db.execute.return_value = result
    products = [
        ("entity-2", 1002, "vendor-2"),
        ("entity-1", 1001, "vendor-1"),
        ("entity-1", 1001, "vendor-1"),
    ]

    rows = await get_supporting_rows("org-1", products=products, db=db)

    assert rows == (["reference"], ["snapshot"], ["box"])
    first_query = str(db.execute.await_args_list[0].args[0])
    second_query = str(db.execute.await_args_list[1].args[0])
    params = db.execute.await_args_list[0].args[1]
    assert "entity_id::text = ANY" in first_query
    assert "nm_id = ANY" in first_query
    assert "nm_id = ANY" in second_query
    assert params["entity_ids"] == ["entity-1", "entity-2"]
    assert params["nm_ids"] == [1001, 1002]


@pytest.mark.asyncio
async def test_product_query_accepts_search_and_limit_params():
    db = AsyncMock()
    result = MagicMock()
    result.all.return_value = [("product",)]
    db.execute.return_value = result

    rows = await get_products(db, "org-1", "2026-08-12", search="Boot", limit=50)

    assert rows == [("product",)]
    params = db.execute.await_args.args[1]
    assert params["search"] == "boot"
    assert params["search_like"] == "%boot%"
    assert params["limit"] == 50


@pytest.mark.asyncio
async def test_product_count_uses_same_search_params():
    db = AsyncMock()
    result = MagicMock()
    result.first.return_value = (7,)
    db.execute.return_value = result

    total = await count_products(db, "org-1", "2026-08-12", search="123")

    assert total == 7
    params = db.execute.await_args.args[1]
    assert params["search"] == "123"
    assert params["search_like"] == "%123%"


@pytest.mark.asyncio
async def test_limited_unit_economics_does_not_store_partial_cache(monkeypatch):
    class FakeRedis:
        def __init__(self):
            self.setex = MagicMock()

        def delete(self, key):
            return None

        def get(self, key):
            return None

    fake_redis = FakeRedis()
    monkeypatch.setitem(
        sys.modules,
        "redis",
        SimpleNamespace(from_url=lambda url: fake_redis),
    )
    monkeypatch.setattr(ue_router, "get_unit_economics_latest_date", AsyncMock(return_value="2026-08-12"))
    monkeypatch.setattr(ue_router, "count_unit_economics_products", AsyncMock(return_value=123))
    monkeypatch.setattr(
        ue_router,
        "get_unit_economics_products",
        AsyncMock(return_value=[
            (
                "entity-1", 1001, "vendor", "name", "", "barcode", 1000, 900,
                12, 0, "42", "subject", 10, 20, 30,
            )
        ]),
    )
    monkeypatch.setattr(
        ue_router,
        "get_unit_economics_supporting_rows",
        AsyncMock(return_value=([], [], [])),
    )

    org_result = MagicMock()
    org_result.fetchone.return_value = (None, 0, None, None)
    db = AsyncMock()
    db.execute.return_value = org_result

    result = await ue_router.build_unit_economics("org-1", db, limit=50)

    assert result["total"] == 123
    assert len(result["items"]) == 1
    ue_router.get_unit_economics_products.assert_awaited_once()
    assert ue_router.get_unit_economics_products.await_args.kwargs["limit"] == 50
    assert ue_router.get_unit_economics_supporting_rows.await_args.kwargs["products"]
    fake_redis.setex.assert_not_called()


@pytest.mark.asyncio
async def test_unit_economics_keeps_product_entities_without_tech_status(monkeypatch):
    class FakeRedis:
        def delete(self, key):
            return None

        def get(self, key):
            return None

        def setex(self, *args, **kwargs):
            return None

    monkeypatch.setitem(
        sys.modules,
        "redis",
        SimpleNamespace(from_url=lambda url: FakeRedis()),
    )
    monkeypatch.setattr(ue_router, "get_unit_economics_latest_date", AsyncMock(return_value=None))
    monkeypatch.setattr(ue_router, "count_unit_economics_products", AsyncMock(return_value=1))
    monkeypatch.setattr(
        ue_router,
        "get_unit_economics_products",
        AsyncMock(return_value=[
            (
                "entity-new", 2001, "new-vendor", "New product", "", "", None,
                None, None, None, "ONE SIZE", "subject", None, None, None,
            )
        ]),
    )
    monkeypatch.setattr(
        ue_router,
        "get_unit_economics_supporting_rows",
        AsyncMock(return_value=([], [], [])),
    )

    org_result = MagicMock()
    org_result.fetchone.return_value = (None, 0, None, None)
    db = AsyncMock()
    db.execute.return_value = org_result

    result = await ue_router.build_unit_economics("org-1", db, limit=50)

    assert result["total"] == 1
    assert result["items"][0]["entity_id"] == "entity-new"
    assert result["items"][0]["nm_id"] == 2001
    assert result["items"][0]["price_before_spp"] == 0
    ue_router.get_unit_economics_products.assert_awaited_once()
    assert ue_router.get_unit_economics_products.await_args.args[2] is None
    ue_router.get_unit_economics_supporting_rows.assert_awaited_once()
