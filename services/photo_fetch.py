"""Подтяжка фото товаров через публичный API WB"""
import logging
import httpx
from typing import Optional

logger = logging.getLogger(__name__)

# Публичный API WB для деталей товара
WB_CARD_URL = "https://card.wb.ru/cards/v1/detail"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


async def fetch_photo_for_nm(nm_id: int) -> Optional[str]:
    """
    Получить URL главного фото товара через публичный API WB.
    Возвращает URL фото или None если не найдено.
    """
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                WB_CARD_URL,
                params={
                    "appType": 1,
                    "curr": "rub",
                    "dest": -1257786,
                    "nm": nm_id,
                },
                headers=HEADERS,
            )
            if resp.status_code != 200:
                logger.debug(f"photo_fetch: nm={nm_id} status={resp.status_code}")
                return None

            data = resp.json()
            products = data.get("data", {}).get("products", [])
            if not products:
                return None

            product = products[0]
            photos = product.get("photos", [])
            if not photos:
                return None

            # Берём maxRes (самое большое), fallback на big
            photo = photos[0]
            url = photo.get("maxRes", "") or photo.get("big", "") or photo.get("small", "")
            return url if url else None

    except Exception as e:
        logger.debug(f"photo_fetch error nm={nm_id}: {e}")
        return None


async def fetch_photos_batch(nm_ids: list[int]) -> dict[int, str]:
    """
    Получить фото для списка nm_id.
    WB отдаёт до ~100 товаров за запрос.
    Возвращает {nm_id: photo_url}.
    """
    result = {}
    if not nm_ids:
        return result

    # WB принимает nm как строку с запятыми
    nm_str = ",".join(str(n) for n in nm_ids)

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(
                WB_CARD_URL,
                params={
                    "appType": 1,
                    "curr": "rub",
                    "dest": -1257786,
                    "nm": nm_str,
                },
                headers=HEADERS,
            )
            if resp.status_code != 200:
                logger.warning(f"photo_fetch batch: status={resp.status_code}")
                return result

            data = resp.json()
            products = data.get("data", {}).get("products", [])

            for product in products:
                nm = product.get("id")
                if not nm:
                    continue
                photos = product.get("photos", [])
                if photos:
                    url = photos[0].get("maxRes", "") or photos[0].get("big", "")
                    if url:
                        result[nm] = url

    except Exception as e:
        logger.error(f"photo_fetch batch error: {e}")

    return result
