#!/usr/bin/env python3
import asyncio
import logging
import sys
from datetime import datetime, timezone
from typing import List, Dict, Set, Optional

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
BATCH_SIZE = 10
SLEEP_SECONDS = 1

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

async def get_unique_keywords(org_id: str) -> Set[str]:
    async with async_session() as db:
        result = await db.execute(text("""
            SELECT DISTINCT top_query_1, top_query_2, top_query_3
            FROM reference_book
            WHERE organization_id = :org_id
              AND (top_query_1 IS NOT NULL OR top_query_2 IS NOT NULL OR top_query_3 IS NOT NULL)
        """), {"org_id": org_id})
        keywords = set()
        for row in result.all():
            keywords.update([k for k in [row[0], row[1], row[2]] if is_valid_keyword(k)])
        _log.info(f"Found {len(keywords)} keywords")
        return keywords

async def fetch_evirma(keywords: List[str]) -> Optional[Dict]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.post(EVIRMA_API_URL, headers=EVIRMA_HEADERS, json={"keywords": keywords, "an": False})
            if resp.status_code in (403, 429):
                _log.error(f"Evirma error {resp.status_code}: {resp.text[:200]}")
                return None
            resp.raise_for_status()
            if "x-ratelimit-remaining" in resp.headers:
                _log.info(f"Evirma usage remaining: {resp.headers['x-ratelimit-remaining']}")
            return resp.json()
        except Exception as e:
            _log.error(f"Evirma fetch error: {e}")
            return None

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
            "freq_history_monthly": kw_data.get("freq_history_monthly"),
            "freq_history_weekly": kw_data.get("freq_history_weekly"),
            "source": "evirma",
            "collected_at": datetime.now(timezone.utc),
        })
    return records

async def save_seasonality(records: List[Dict]) -> int:
    if not records:
        return 0
    async with async_session() as db:
        saved = 0
        for r in records:
            try:
                # Simple insert without upsert for testing
                db.add(WbKeywordSeasonality(**r))
                saved += 1
            except Exception as e:
                _log.error(f"Save error: {e}")
        await db.commit()
        return saved

async def collect(org_id: str, test_mode: bool = False, test_keywords: Optional[List[str]] = None):
    start = datetime.now()
    _log.info(f"Starting collection for org={org_id} test={test_mode}")
    
    if test_mode:
        keywords = [k for k in (test_keywords or ["куртка зимняя", "юбка женская", "платье"]) if is_valid_keyword(k)]
    else:
        keywords = await get_unique_keywords(org_id)
    
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
        _log.info(f"Batch {batch_num}/{total_batches}: {len(batch)} keywords")
        
        data = await fetch_evirma(batch)
        if not data:
            _log.error(f"Batch {batch_num} failed")
            continue
        
        records = parse_evirma(data, org_id)
        saved = await save_seasonality(records)
        processed += len(batch)
        success += saved
        _log.info(f"Batch {batch_num} done: {saved}/{len(batch)} saved")
        
        if i + BATCH_SIZE < total:
            await asyncio.sleep(SLEEP_SECONDS)
    
    duration = (datetime.now() - start).total_seconds()
    _log.info(f"Done: {processed}/{total} processed, {success} saved, {duration:.1f}s")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--org-id", required=True)
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--test-keywords", nargs="+")
    args = parser.parse_args()
    
    asyncio.run(collect(args.org_id, args.test, args.test_keywords))