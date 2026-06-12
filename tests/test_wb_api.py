import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from core.security import decrypt_data
from services.wb_api.client import WBApiClient
from services.wb_api.keys import (
    add_wb_api_key,
    delete_wb_api_key,
    get_wb_api_keys,
)


@pytest.mark.asyncio
async def test_add_wb_api_key_encrypts_token():
    db = AsyncMock()
    db.add = MagicMock()
    organization_id = uuid.uuid4()

    wb_key = await add_wb_api_key(
        db=db,
        organization_id=str(organization_id),
        name="Test key",
        api_key="test-wb-key",
    )

    assert wb_key.organization_id == str(organization_id)
    assert wb_key.api_key != "test-wb-key"
    assert decrypt_data(wb_key.api_key) == "test-wb-key"
    db.add.assert_called_once_with(wb_key)
    db.commit.assert_awaited_once()
    db.refresh.assert_awaited_once_with(wb_key)


@pytest.mark.asyncio
async def test_get_wb_api_keys_keeps_organization_scope():
    expected = [SimpleNamespace(id=uuid.uuid4())]
    result = SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: expected))
    db = AsyncMock()
    db.execute.return_value = result

    keys = await get_wb_api_keys(db, str(uuid.uuid4()))

    assert keys == expected
    db.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_wb_api_key_returns_false_when_missing():
    result = SimpleNamespace(scalar_one_or_none=lambda: None)
    db = AsyncMock()
    db.execute.return_value = result

    deleted = await delete_wb_api_key(
        db,
        key_id=str(uuid.uuid4()),
        organization_id=str(uuid.uuid4()),
    )

    assert deleted is False
    db.delete.assert_not_awaited()
    db.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_wb_connection_check_handles_http_status_without_network():
    async def handler(request):
        assert request.headers["Authorization"] == "Bearer fake-key"
        return httpx.Response(401, request=request)

    client = WBApiClient("fake-key")
    await client.client.aclose()
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        assert await client.test_connection() is False
    finally:
        await client.client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code, body", [(204, b""), (200, b"")])
async def test_fbo_stocks_treats_empty_success_response_as_no_stocks(
    status_code, body
):
    async def handler(request):
        return httpx.Response(status_code, content=body, request=request)

    client = WBApiClient("fake-key")
    await client.client.aclose()
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        assert await client.get_stocks_warehouses() == []
    finally:
        await client.client.aclose()


@pytest.mark.asyncio
async def test_fbo_stocks_reports_invalid_json_response_details():
    async def handler(request):
        return httpx.Response(
            200,
            text="<html>temporary upstream error</html>",
            headers={"content-type": "text/html"},
            request=request,
        )

    client = WBApiClient("fake-key")
    await client.client.aclose()
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(ValueError, match=r"status=200.*content-type=text/html"):
            await client.get_stocks_warehouses()
    finally:
        await client.client.aclose()


@pytest.mark.asyncio
async def test_fbo_stocks_unwraps_nested_items():
    async def handler(request):
        return httpx.Response(
            200,
            json={"data": {"items": [{"nmID": 123, "quantity": 4}]}},
            request=request,
        )

    client = WBApiClient("fake-key")
    await client.client.aclose()
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        assert await client.get_stocks_warehouses() == [
            {"nmID": 123, "quantity": 4}
        ]
    finally:
        await client.client.aclose()
