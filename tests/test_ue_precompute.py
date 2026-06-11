from unittest.mock import AsyncMock

import pytest

from tasks.ue_precompute import precompute_ue_cache


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
