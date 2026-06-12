from unittest.mock import AsyncMock, MagicMock

import pytest

from repositories.unit_economics import get_supporting_rows
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
