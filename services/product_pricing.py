"""Shared product price resolution helpers."""


def price_before_spp_sql(
    *,
    snapshot_alias: str = "snp",
    tech_alias: str = "ts",
    fallback_sql: str = "NULL",
) -> str:
    """Return SQL expression for the seller price before SPP.

    WB promo snapshots expose both base price and buyer/product price. For NL UI
    "Цена до СПП" must use the effective seller price after seller discount,
    before WB SPP. The base price is only a fallback.
    """
    return (
        "COALESCE("
        f"NULLIF({snapshot_alias}.price_product, 0), "
        f"NULLIF({tech_alias}.price_discount, 0), "
        f"NULLIF({snapshot_alias}.price_basic, 0), "
        f"NULLIF({tech_alias}.price, 0), "
        f"NULLIF({fallback_sql}, 0)"
        ")"
    )
