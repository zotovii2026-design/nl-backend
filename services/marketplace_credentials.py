"""Fail-closed selection of marketplace keys for explicitly allowed actions."""

from dataclasses import dataclass
from uuid import UUID

import httpx
from sqlalchemy import select

from core.security import decrypt_data
from models.marketplace_credential import MarketplaceCredential


CAPABILITIES = {
    "wb": {"read", "prices_write", "stocks_write", "content_write", "ads_write"},
    "ozon": {"read", "prices_write", "stocks_write", "content_write", "ads_write"},
}

# An action must be added here before any NL code can obtain a write credential.
ACTION_POLICY = {
    ("wb", "catalog.read"): "read",
    ("wb", "prices.update"): "prices_write",
    ("wb", "stocks.update"): "stocks_write",
    ("wb", "content.update"): "content_write",
    ("wb", "ads.update"): "ads_write",
    ("ozon", "catalog.read"): "read",
    ("ozon", "prices.update"): "prices_write",
    ("ozon", "stocks.update"): "stocks_write",
    ("ozon", "content.update"): "content_write",
    ("ozon", "ads.update"): "ads_write",
}


@dataclass(frozen=True)
class ResolvedMarketplaceCredential:
    id: UUID
    marketplace: str
    secret: str
    client_id: str | None


WB_PROBE_BY_CAPABILITY = {
    "read": ("POST", "https://content-api.wildberries.ru/content/v2/get/cards/list", {"settings": {"cursor": {"limit": 1}}, "filter": {}}),
    "prices_write": ("GET", "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter?limit=1&offset=0", None),
    "stocks_write": ("GET", "https://marketplace-api.wildberries.ru/api/v3/warehouses", None),
    "content_write": ("POST", "https://content-api.wildberries.ru/content/v2/get/cards/list", {"settings": {"cursor": {"limit": 1}}, "filter": {}}),
    "ads_write": ("GET", "https://advert-api.wildberries.ru/adv/v1/promotion/count", None),
}


def validate_capabilities(marketplace: str, capabilities: list[str]) -> list[str]:
    normalized = sorted({str(item).strip() for item in capabilities if str(item).strip()})
    invalid = set(normalized) - CAPABILITIES.get(marketplace, set())
    if invalid:
        raise ValueError(f"Недопустимые права: {', '.join(sorted(invalid))}")
    if not normalized:
        raise ValueError("Выберите хотя бы одно право")
    return normalized


async def verify_credential_auth(marketplace: str, secret: str, client_id: str | None, capabilities: list[str]) -> dict:
    """Perform one read-only probe. It proves authentication, never write permission."""
    async with httpx.AsyncClient(timeout=15) as client:
        if marketplace == "wb":
            capability = next((item for item in capabilities if item in WB_PROBE_BY_CAPABILITY), "read")
            method, url, body = WB_PROBE_BY_CAPABILITY[capability]
            response = await client.request(method, url, headers={"Authorization": secret}, json=body)
        elif marketplace == "ozon":
            if not client_id:
                raise ValueError("Для Ozon обязателен Client-Id")
            response = await client.post(
                "https://api-seller.ozon.ru/v3/product/list",
                headers={"Client-Id": client_id, "Api-Key": secret},
                json={"filter": {"visibility": "ALL"}, "last_id": "", "limit": 1},
            )
        else:
            raise ValueError("Неизвестный маркетплейс")
    if response.status_code in (200, 204, 429):
        return {"valid": True, "status_code": response.status_code}
    if response.status_code in (401, 403):
        return {"valid": False, "status_code": response.status_code, "message": "Ключ отклонён API или не имеет доступа к безопасному проверочному методу"}
    return {"valid": False, "status_code": response.status_code, "message": f"API вернул HTTP {response.status_code}"}


async def resolve_credential_for_action(db, organization_id: UUID, marketplace: str, action: str):
    required = ACTION_POLICY.get((marketplace, action))
    if not required:
        raise PermissionError("Операция не включена во внутренний allowlist NL")
    result = await db.execute(
        select(MarketplaceCredential)
        .where(
            MarketplaceCredential.organization_id == organization_id,
            MarketplaceCredential.marketplace == marketplace,
            MarketplaceCredential.status == "valid",
            MarketplaceCredential.is_enabled.is_(True),
            MarketplaceCredential.capabilities.contains([required]),
        )
        .order_by(MarketplaceCredential.created_at)
    )
    credential = result.scalars().first()
    if not credential:
        raise PermissionError(f"Нет активного проверенного ключа с правом {required}")
    return ResolvedMarketplaceCredential(
        id=credential.id,
        marketplace=credential.marketplace,
        secret=decrypt_data(credential.encrypted_secret),
        client_id=decrypt_data(credential.encrypted_client_id) if credential.encrypted_client_id else None,
    )
