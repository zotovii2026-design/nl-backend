#!/usr/bin/env python3
"""Скрипт сбора сезонности из Evirma API

v2: с расчётом коэффициентов долей и антибаном
"""
import asyncio
import logging
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Set, Optional, Tuple
import json

sys.path.insert(0, "/app")

import httpx
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.database import async_session
from models.keyword_seasonality import WbKeywordSeasonality

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
_log = logging.getLogger(__name__)

EVIRMA_API_URL = "https://evirma.ru/api/v1/keyword/list"
EVIRMA_HEADERS = {
    "Content-Type": "application/json",
    "evirma-wb-deviceid": "479b3b53-ac65-4f80-84f5-05049f778962",
    "evirma-wb-sellerid": "66311627-1775-4841-8c7d-b303edd9753e",
    "evirma-wb-userid": "27346217",
    "Origin": "https://www.wildberries.ru",
    "Referer": "https://www.wildberries.ru/",
}
BATCH_SIZE = 5  # Антибан: батчи по 5 ключей
SLEEP_SECONDS = 5  # Антибан: 5 секунд между батчами

EXCLUDED_KEYWORDS = {"тест", "test", "0", "-", "", " "}

def is_valid_keyword(keyword: str) -> bool:
    if not keyword:
        return False
    k = keyword.lower().strip()
    if k in EXCLUDED_KEYWORDS:
        return False
    if k.isdigit():
        return False
    return len(k) > 1


def calculate_seasonality_coefficients(freq_history_monthly: Optional[List[List[int]]]) -> Optional[Dict[str, float]]:
    """Рассчитать коэффициенты сезонности как доли от годовой суммы
    
    Логика:
    1. Берём freq_history_monthly из ответа Evirma
    2. Формат: [[month_num, "YYYY-MM-DD", freq], ...]
    3. Отбрасываем последний неполный месяц (текущий месяц - июль 2026)
    4. Берём 12 фактических месяцев
    5. Суммируем freq по 12 месяцам = годовой объём
    6. Доля месяца = month_freq / sum_12 × 100 (2 знака)
    7. Сумма всех 12 долей = ~100%
    """
    if not freq_history_monthly or len(freq_history_monthly) < 12:
        return None
    
    # freq_history_monthly: [[month_num, YYYY-MM-DD, freq], ...]
    # Берём последние 13 точек и отбрасываем последнюю (текущий месяц)
    recent = freq_history_monthly[-13:] if len(freq_history_monthly) >= 13 else freq_history_monthly[-12:]
    
    # Отбрасываем последнюю точку (неполный месяц)
    last_12 = recent[:-1]
    
    if len(last_12) < 12:
        return None
    
    # Суммируем частоты
    total_freq = sum(point[2] for point in last_12 if len(point) >= 3)
    if total_freq == 0:
        return None
    
    # Рассчитываем доли в процентах
    coefficients = {}
    for point in last_12:
        if len(point) >= 3:
            month_num = point[0]
            freq = point[2]
            percentage = (freq / total_freq) * 100
            coefficients[str(month_num)] = round(percentage, 2)
    
    # Проверяем, что сумма примерно 100%
    total_pct = sum(coefficients.values())
    if abs(total_pct - 100.0) > 1.0:
        _log.warning(f"Seasonality coefficients sum to {total_pct:.2f}%, expected ~100%")
    
    return coefficients


async def get_unique_keywords(org_id: str, nm_id: Optional[int] = None) -> Set[str]:
    async with async_session() as db:
        query = """
            SELECT DISTINCT top_query_1, top_query_2, top_query_3
            FROM reference_book
            WHERE organization_id = :org_id
              AND (top_query_1 IS NOT NULL OR top_query_2 IS NOT NULL OR top_query_3 IS NOT NULL)
        """
        params = {"org_id": org_id}
        
        if nm_id is not None:
            query += " AND nm_id = :nm_id"
            params["nm_id"] = nm_id
        
        result = await db.execute(text(query), params)
        keywords = set()
        for row in result.all():
            keywords.update([k for k in [row[0], row[1], row[2]] if is_valid_keyword(k)])
        _log.info(f"Found {len(keywords)} keywords for org={org_id} nm_id={nm_id}")
        return sorted(keywords)


async def fetch_evirma(keywords: List[str]) -> Tuple[Optional[Dict], Optional[int]]:
    """Запрос к Evirma API
    
    Возвращает:
    - данные (или None при ошибке)
    - remaining лимит (или None)
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.post(EVIRMA_API_URL, headers=EVIRMA_HEADERS, json={"keywords": keywords, "an": False})
            
            # Логируем оставшийся лимит
            remaining = int(resp.headers.get("x-ratelimit-remaining", 0)) if "x-ratelimit-remaining" in resp.headers else None
            if remaining is not None:
                _log.info(f"Evirma rate limit remaining: {remaining}")
            
            if resp.status_code in (403, 429):
                _log.error(f"Evirma error {resp.status_code}: {resp.text[:200]}")
                return None, remaining
            
            resp.raise_for_status()
            return resp.json(), remaining
        except Exception as e:
            _log.error(f"Evirma fetch error: {e}")
            return None, None


def parse_evirma(data: Dict, org_id: str) -> List[Dict]:
    records = []
    if "data" not in data or "keywords" not in data["data"]:
        return records
    
    for kw, kw_data in data["data"]["keywords"].items():
        cluster = kw_data.get("cluster", {})
        subjects = kw_data.get("subjects", [])
        wb_subject_id = subjects[0] if subjects else None
        wb_subject_name = None
        weekly_trend = cluster.get("freq_common", {}).get("weekly_trend")
        
        # Рассчитываем коэффициенты сезонности
        freq_history_monthly = kw_data.get("freq_history_monthly")
        seasonality_coefficients = calculate_seasonality_coefficients(freq_history_monthly)
        
        records.append({
            "organization_id": org_id,
            "keyword": kw,
            "normquery": kw_data.get("normquery"),
            "freq365": kw_data.get("freq365"),
            "freq_monthly": kw_data.get("freq", {}).get("monthly"),
            "freq_weekly": kw_data.get("freq", {}).get("weekly"),
            "weekly_trend": weekly_trend,
            "growth_rate": kw_data.get("growth_rate"),
            "product_count": kw_data.get("product_count"),
            "wb_subject_id": wb_subject_id,
            "wb_subject_name": wb_subject_name,
            "freq_history_monthly": freq_history_monthly,
            "freq_history_weekly": kw_data.get("freq_history_weekly"),
            "seasonality_coefficients": seasonality_coefficients,
            "source": "evirma",
            "collected_at": datetime.now(timezone.utc),
        })
    return records


async def save_seasonality(records: List[Dict], dry_run: bool = False) -> int:
    if not records:
        return 0
    
    if dry_run:
        _log.info(f"[DRY RUN] Would save {len(records)} records")
        for r in records[:3]:  # Показать первые 3
            _log.info(f"  - {r['keyword']}: {r.get('seasonality_coefficients')}")
        return len(records)
    
    async with async_session() as db:
        saved = 0
        for r in records:
            try:
                # Upsert: вставить или обновить
                stmt = pg_insert(WbKeywordSeasonality).values(
                    organization_id=r["organization_id"],
                    keyword=r["keyword"],
                    source=r["source"],
                    collected_at=r["collected_at"],
                    normquery=r.get("normquery"),
                    freq365=r.get("freq365"),
                    freq_monthly=r.get("freq_monthly"),
                    freq_weekly=r.get("freq_weekly"),
                    weekly_trend=r.get("weekly_trend"),
                    growth_rate=r.get("growth_rate"),
                    product_count=r.get("product_count"),
                    wb_subject_id=r.get("wb_subject_id"),
                    wb_subject_name=r.get("wb_subject_name"),
                    freq_history_monthly=r.get("freq_history_monthly"),
                    freq_history_weekly=r.get("freq_history_weekly"),
                    seasonality_coefficients=r.get("seasonality_coefficients"),
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["keyword", "source", "collected_at"],
                    set_={
                        "normquery": stmt.excluded.normquery,
                        "freq365": stmt.excluded.freq365,
                        "freq_monthly": stmt.excluded.freq_monthly,
                        "freq_weekly": stmt.excluded.freq_weekly,
                        "weekly_trend": stmt.excluded.weekly_trend,
                        "growth_rate": stmt.excluded.growth_rate,
                        "product_count": stmt.excluded.product_count,
                        "wb_subject_id": stmt.excluded.wb_subject_id,
                        "wb_subject_name": stmt.excluded.wb_subject_name,
                        "freq_history_monthly": stmt.excluded.freq_history_monthly,
                        "freq_history_weekly": stmt.excluded.freq_history_weekly,
                        "seasonality_coefficients": stmt.excluded.seasonality_coefficients,
                    }
                )
                await db.execute(stmt)
                saved += 1
            except Exception as e:
                _log.error(f"Save error for keyword {r.get('keyword')}: {e}")
        
        await db.commit()
        return saved


async def collect(org_id: str, test_mode: bool = False, nm_id: Optional[int] = None, dry_run: bool = False):
    start = datetime.now()
    _log.info(f"Starting collection for org={org_id} test={test_mode} nm_id={nm_id} dry_run={dry_run}")
    
    if test_mode:
        keywords = ["шахматы", "шахматы деревянные", "шахматы magnetic"]
        keywords = [k for k in keywords if is_valid_keyword(k)]
    else:
        keywords = await get_unique_keywords(org_id, nm_id)
    
    if not keywords:
        _log.warning("No keywords to process")
        return
    
    total = len(keywords)
    processed = 0
    success = 0
    
    for i in range(0, total, BATCH_SIZE):
        batch = list(keywords[i:i+BATCH_SIZE])
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        _log.info(f"Batch {batch_num}/{total_batches}: {len(batch)} keywords: {batch}")
        
        data, remaining = await fetch_evirma(batch)
        if not data:
            _log.error(f"Batch {batch_num} failed, stopping collection (anti-ban: 403/429)")
            break
        
        records = parse_evirma(data, org_id)
        saved = await save_seasonality(records, dry_run=dry_run)
        processed += len(batch)
        success += saved
        _log.info(f"Batch {batch_num} done: {saved}/{len(batch)} saved")
        
        # Антибан: пауза между батчами
        if i + BATCH_SIZE < total:
            _log.info(f"Sleeping {SLEEP_SECONDS}s before next batch...")
            await asyncio.sleep(SLEEP_SECONDS)
    
    duration = (datetime.now() - start).total_seconds()
    _log.info(f"Done: {processed}/{total} processed, {success} saved, {duration:.1f}s")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Сбор сезонности из Evirma API (v2)")
    parser.add_argument("--org-id", required=True, help="Organization ID")
    parser.add_argument("--nm-id", type=int, help="Collect for single product (nm_id)")
    parser.add_argument("--test", action="store_true", help="Test mode (3 keywords)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be collected without saving")
    args = parser.parse_args()
    
    asyncio.run(collect(args.org_id, args.test, args.nm_id, args.dry_run))
