"""Подтяжка фото товаров через перебор корзин WB CDN"""
import logging
import httpx
from typing import Optional
import asyncio

logger = logging.getLogger(__name__)

# WB CDN корзины — параллельная проверка
MAX_BASKET = 32  # корзины basket-01 .. basket-32
CDN_TEMPLATE = "https://basket-{basket}.wbbasket.ru/vol{vol}/part{part}/{nm_id}/images/hq/1.webp"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
}


def _build_url(nm_id: int, basket_num: int) -> str:
    vol = nm_id // 100000
    part = nm_id // 1000
    b = str(basket_num).zfill(2)
    return CDN_TEMPLATE.format(basket=b, vol=vol, part=part, nm_id=nm_id)


async def _check_url(client: httpx.AsyncClient, url: str) -> bool:
    """HEAD запрос — вернёт True если 200"""
    try:
        resp = await client.head(url, follow_redirects=True)
        return resp.status_code == 200
    except:
        return False


async def fetch_photo_for_nm(nm_id: int) -> Optional[str]:
    """
    Найти фото товара перебором корзин WB CDN.
    Параллельно проверяем все корзины (~32 HEAD запроса).
    """
    urls = [(i, _build_url(nm_id, i)) for i in range(1, MAX_BASKET + 1)]
    
    async with httpx.AsyncClient(timeout=5.0, headers=HEADERS) as client:
        tasks = [_check_url(client, url) for _, url in urls]
        results = await asyncio.gather(*tasks)
        
        for (basket_num, url), found in zip(urls, results):
            if found:
                return url
    
    return None


async def fetch_photos_batch(nm_ids: list[int]) -> dict[int, str]:
    """
    Получить фото для списка nm_id.
    Каждый nm_id — перебор корзин параллельно.
    Группируем по 5 nm_id одновременно (чтобы не DDOSить CDN).
    """
    result = {}
    semaphore = asyncio.Semaphore(5)
    
    async def _fetch_one(nm_id: int):
        async with semaphore:
            url = await fetch_photo_for_nm(nm_id)
            if url:
                result[nm_id] = url
    
    tasks = [_fetch_one(nm) for nm in nm_ids]
    await asyncio.gather(*tasks)
    
    logger.info(f"photo_fetch: {len(result)}/{len(nm_ids)} photos found")
    return result
