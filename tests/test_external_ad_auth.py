from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from api.v1.external_ad import get_external_ads


@pytest.mark.asyncio
async def test_external_ads_reject_malformed_organization_id():
    db = AsyncMock()

    with pytest.raises(HTTPException) as exc:
        await get_external_ads("not-a-uuid", db=db)

    assert exc.value.status_code == 400
    db.execute.assert_not_awaited()
