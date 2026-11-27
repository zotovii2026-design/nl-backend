"""Fail-closed selection of marketplace keys for explicitly allowed actions."""

from dataclasses import dataclass
from uuid import UUID

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


def validate_capabilities(marketplace: str, capabilities: list[str]) -> list[str]:
    normalized = sorted({str(item).strip() for item in capabilities if str(item).strip()})
    invalid = set(normalized) - CAPABILITIES.get(marketplace, set())
    if invalid:
        raise ValueError(f"Недопустимые права: {', '.join(sorted(invalid))}")
    if not normalized:
        raise ValueError("Выберите хотя бы одно право")
    return normalized


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

