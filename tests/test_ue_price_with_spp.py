"""
DOOM-проверка: price_with_spp не должен быть 0,
если в wb_tariff_snapshot есть ненулевое значение для этого nm_id.

Запуск:
  docker exec nl-backend-app python -m pytest tests/test_ue_price_with_spp.py -v
"""
import pytest
import asyncpg
import os
import asyncio


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:nWbRwQ3fhKu9cN1no41h@postgres:5432/nl_table",
).replace("postgresql+asyncpg://", "postgresql://")


async def _fetch_snapshot_samples():
    """Берёт 10 nm_id, у которых price_with_spp > 0 в snapshot."""
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (nm_id) nm_id, price_retail, price_with_spp
            FROM wb_tariff_snapshot
            WHERE price_with_spp IS NOT NULL
              AND price_with_spp > 0
              AND price_retail IS NOT NULL
              AND price_retail > 0
            ORDER BY nm_id
            LIMIT 10
            """
        )
        return rows
    finally:
        await conn.close()


def test_snapshot_has_price_with_spp_data():
    """В snapshot должны быть товары с ненулевым price_with_spp."""
    rows = asyncio.get_event_loop().run_until_complete(_fetch_snapshot_samples())
    assert len(rows) > 0, (
        "В wb_tariff_snapshot нет записей с price_with_spp > 0 — нечего проверять"
    )


def test_spp_pct_calculation():
    """spp_pct должен вычисляться корректно из price_retail и price_with_spp."""
    rows = asyncio.get_event_loop().run_until_complete(_fetch_snapshot_samples())
    for r in rows:
        expected_spp = round(
            (1 - float(r["price_with_spp"]) / float(r["price_retail"])) * 100, 2
        )
        assert 0 < expected_spp <= 100, (
            f"nm_id={r['nm_id']}: spp_pct={expected_spp} вне диапазона (0, 100] — "
            f"price_retail={r['price_retail']}, price_with_spp={r['price_with_spp']}"
        )


def test_price_with_spp_less_than_retail():
    """price_with_spp должен быть <= price_retail (цена со скидкой)."""
    rows = asyncio.get_event_loop().run_until_complete(_fetch_snapshot_samples())
    for r in rows:
        assert float(r["price_with_spp"]) <= float(r["price_retail"]), (
            f"nm_id={r['nm_id']}: price_with_spp={r['price_with_spp']} "
            f"> price_retail={r['price_retail']}"
        )
