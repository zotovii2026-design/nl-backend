#!/usr/bin/env python3
"""Скрипт расчёта взвешенной сезонности товаров

Читает сезонность ключевых слов и считает профиль по top_query_1..3.
"""
import asyncio
import logging
import sys
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set

sys.path.insert(0, "/app")

from sqlalchemy import text, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from core.database import async_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
_log = logging.getLogger(__name__)


async def get_products_with_keywords(org_id: str, nm_ids: Optional[List[int]] = None) -> List[Dict]:
    """Получить товары с их ключевыми словами из reference_book"""
    async with async_session() as db:
        sql = """
            SELECT DISTINCT
                nm_id,
                vendor_code,
                organization_id,
                top_query_1,
                top_query_2,
                top_query_3
            FROM reference_book
            WHERE organization_id = :org_id
              AND (top_query_1 IS NOT NULL OR top_query_2 IS NOT NULL OR top_query_3 IS NOT NULL)
        """
        params = {"org_id": org_id}
        if nm_ids:
            sql += " AND nm_id = ANY(:nm_ids)"
            params["nm_ids"] = nm_ids

        result = await db.execute(text(sql), params)
        
        products = []
        for row in result.all():
            keywords = [k for k in [row.top_query_1, row.top_query_2, row.top_query_3] if k and k.strip()]
            if keywords:
                products.append({
                    "nm_id": row.nm_id,
                    "vendor_code": row.vendor_code,
                    "organization_id": str(row.organization_id),
                    "keywords": keywords
                })
        
        _log.info(f"Found {len(products)} products with keywords for nm_ids={nm_ids}")
        return products


async def get_keyword_coefficients(org_id: str, keywords: List[str], days_back: int = 7) -> Dict[str, Dict[str, float]]:
    """Получить коэффициенты сезонности (case-insensitive matching)"""
    """Получить коэффициенты сезонности для ключевых слов
    
    Берём последние данные за последние days_back дней.
    """
    if not keywords:
        return {}
    
    async with async_session() as db:
        keywords = [k.lower() for k in keywords]
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
        
        result = await db.execute(text("""
            SELECT keyword, seasonality_coefficients
            FROM wb_keyword_seasonality
            WHERE organization_id = :org_id
              AND LOWER(keyword) = ANY(:keywords)
              AND jsonb_typeof(seasonality_coefficients) = 'object'
              AND seasonality_coefficients <> '{}'::jsonb
              AND collected_at >= :cutoff
            ORDER BY keyword, collected_at DESC
        """), {
            "org_id": org_id,
            "keywords": keywords,
            "cutoff": cutoff
        })
        
        # Берём самые последние коэффициенты для каждого ключевого слова
        coeffs = {}
        seen_keywords = set()
        for row in result.all():
            kw = row.keyword
            if kw not in seen_keywords:
                coeffs[kw.lower()] = row.seasonality_coefficients
                seen_keywords.add(kw)
        
        return coeffs


def normalize_coefficients(coeffs: Dict[str, float]) -> Dict[str, float]:
    """Привести коэффициенты к шкале: средний месяц = 1.0.

    Старые записи хранили долю года в процентах и суммировались примерно в 100.
    Новые записи суммируются примерно в 12. Поддерживаем оба формата, чтобы
    пересчёт товаров не зависел от полного пересбора ключей.
    """
    normalized = {}
    for month, value in coeffs.items():
        try:
            normalized[str(month)] = float(value)
        except (TypeError, ValueError):
            continue

    total = sum(normalized.values())
    if 90 <= total <= 110:
        return {month: round(value / (100 / 12), 2) for month, value in normalized.items()}
    return normalized


def weights_for_keyword_count(count: int) -> List[float]:
    if count <= 1:
        return [1.0]
    if count == 2:
        return [0.7, 0.3]
    return [0.5, 0.3, 0.2]


def weighted_coefficients(coefficients_list: List[Dict[str, float]]) -> Dict[str, float]:
    """Взвесить коэффициенты по порядку запросов в Справочнике
    
    Пример:
    [{"1": 0.1, "12": 6.0}, {"1": 0.2, "12": 5.0}]
    -> при весах 70/30 {"1": 0.13, "12": 5.7}
    """
    if not coefficients_list:
        return {}

    normalized_list = [normalize_coefficients(coeffs) for coeffs in coefficients_list]
    weights = weights_for_keyword_count(len(normalized_list))
    sums = {}
    weight_sums = {}

    for idx, coeffs in enumerate(normalized_list):
        weight = weights[idx] if idx < len(weights) else 0
        for month, value in coeffs.items():
            if month not in sums:
                sums[month] = 0.0
                weight_sums[month] = 0.0
            sums[month] += value * weight
            weight_sums[month] += weight

    result = {}
    for month in sums:
        result[month] = round(sums[month] / weight_sums[month], 2)

    total = sum(result.values())
    if abs(total - 12.0) > 2.0:
        _log.warning(f"Weighted coefficients sum to {total:.2f}, expected ~12")

    return result


async def save_product_seasonality(product: Dict, coefficients: Dict[str, float], source_keywords: List[str], dry_run: bool = False) -> bool:
    if not coefficients:
        _log.debug(f"Skip product {product['nm_id']}: no seasonality coefficients")
        return False

    if dry_run:
        _log.info(f"[DRY RUN] Would save product {product['nm_id']} ({product.get('vendor_code')}): {len(coefficients)} months from {len(source_keywords)} keywords")
        return True
    
    async with async_session() as db:
        try:
            # Сначала удаляем старые записи для этого товара
            await db.execute(text("""
                DELETE FROM wb_product_seasonality
                WHERE nm_id = :nm_id AND organization_id = :org_id
            """), {"nm_id": product["nm_id"], "org_id": product["organization_id"]})
            
            # Вставляем новую запись
            await db.execute(text("""
                INSERT INTO wb_product_seasonality (id, organization_id, nm_id, vendor_code, seasonality_coefficients, source_keywords, collected_at)
                VALUES (gen_random_uuid(), :org_id, :nm_id, :vendor_code, :coefficients, :source_keywords, NOW())
            """), {
                "org_id": product["organization_id"],
                "nm_id": product["nm_id"],
                "vendor_code": product.get("vendor_code"),
                "coefficients": json.dumps(coefficients),
                "source_keywords": source_keywords,
            })
            
            await db.commit()
            return True
        except Exception as e:
            _log.error(f"Save error for product {product['nm_id']}: {e}")
            await db.rollback()
            return False


async def calculate_product_seasonality(
    org_id: str,
    dry_run: bool = False,
    nm_ids: Optional[List[int]] = None,
):
    """Основная функция расчёта товарной сезонности"""
    start = datetime.now()
    _log.info(f"Starting product seasonality calculation for org={org_id} nm_ids={nm_ids} dry_run={dry_run}")
    
    products = await get_products_with_keywords(org_id, nm_ids=nm_ids)
    if not products:
        _log.warning("No products to process")
        return
    
    processed = 0
    success = 0
    
    for product in products:
        keywords = product["keywords"]
        coeffs_dict = await get_keyword_coefficients(org_id, keywords)
        
        # Фильтруем ключевые слова, для которых есть данные
        valid_keywords_lower = [k.lower() for k in keywords if k.lower() in coeffs_dict and coeffs_dict[k.lower()]]
        coeffs_list = [coeffs_dict[kl] for kl in valid_keywords_lower]
        valid_keywords = [k for k in keywords if k.lower() in valid_keywords_lower]
        
        if not valid_keywords:
            _log.debug(f"No seasonality data for product {product['nm_id']} ({product.get('vendor_code')}) keywords: {keywords}")
            continue
        
        # Берём коэффициенты для каждого ключевого слова
        
        # Взвешиваем: первый запрос самый важный, второй/третий уточняют профиль.
        avg_coeffs = weighted_coefficients(coeffs_list)
        
        # Сохраняем
        if await save_product_seasonality(product, avg_coeffs, valid_keywords, dry_run):
            success += 1
        
        processed += 1
        
        if processed % 10 == 0:
            _log.info(f"Progress: {processed}/{len(products)} products, {success} saved")
    
    duration = (datetime.now() - start).total_seconds()
    _log.info(f"Done: {processed}/{len(products)} processed, {success} saved, {duration:.1f}s")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Расчёт усреднённой сезонности товаров")
    parser.add_argument("--org-id", required=True, help="Organization ID")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be calculated without saving")
    args = parser.parse_args()
    
    asyncio.run(calculate_product_seasonality(args.org_id, args.dry_run))
