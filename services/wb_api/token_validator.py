"""
Валидация WB API токена.

Проверяет токен против всех API endpoints, используемых системой.
Возвращает детальный отчёт: какие endpoints работают, какие нет.

Результат:
  - token_status: 'valid' (всё ок), 'invalid' (протух/невалиден), 'limited' (базовый токен, не все endpoints)
  - details: по каждому endpoint статус
  - message: человекочитаемое описание
"""
import asyncio
import base64
import json
import logging
import httpx
from datetime import datetime, timezone
from typing import Dict, Tuple, List

logger = logging.getLogger(__name__)

# Все API endpoints, которые проверяем.
# (название, метод, URL, body или None, ожидаемые статус-коды)
VALIDATION_ENDPOINTS: List[Tuple[str, str, str, dict | None]] = [
    (
        "cards",
        "POST",
        "https://content-api.wildberries.ru/content/v2/get/cards/list",
        {"settings": {"cursor": {"limit": 1}}, "filter": {}},
    ),
    (
        "prices",
        "GET",
        "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter?limit=1&offset=0",
        None,
    ),
    (
        "sales",
        "GET",
        "https://statistics-api.wildberries.ru/api/v1/supplier/sales?dateFrom=2026-07-01&limit=1",
        None,
    ),
    (
        "orders",
        "GET",
        "https://statistics-api.wildberries.ru/api/v1/supplier/orders?dateFrom=2026-07-01&limit=1",
        None,
    ),
    (
        "stocks_fbo",
        "POST",
        "https://seller-analytics-api.wildberries.ru/api/analytics/v1/stocks-report/wb-warehouses",
        {"limit": 1},
    ),
    (
        "promo",
        "GET",
        "https://dp-calendar-api.wildberries.ru/api/v1/calendar/promotions?limit=1",
        None,
    ),
    (
        "adverts",
        "GET",
        "https://advert-api.wildberries.ru/adv/v1/promotion/count",
        None,
    ),
]


def decode_jwt_payload(token: str) -> dict:
    """Декодирует payload JWT без проверки подписи."""
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1]
    payload += "=" * (4 - len(payload) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(payload))
    except Exception:
        return {}


async def validate_wb_token(token: str) -> dict:
    """
    Полная валидация токена WB.

    Возвращает:
      {
        "token_status": "valid" | "invalid" | "limited",
        "acc_level": int,  # 1=base, 3=full
        "details": {endpoint: {"status": int, "ok": bool}},
        "failed_endpoints": [str, ...],
        "message": "человекочитаемое описание",
        "validated_at": "ISO timestamp",
      }
    """
    headers = {"Authorization": token}
    details = {}
    failed_403 = []  # endpoints с 403 (base token)
    failed_401 = []  # endpoints с 401 (протух)
    failed_other = []
    ok_count = 0
    total = len(VALIDATION_ENDPOINTS)

    # Декодируем JWT для информации
    jwt_data = decode_jwt_payload(token)
    acc_level = jwt_data.get("acc", 0)

    async with httpx.AsyncClient(timeout=15) as client:
        for name, method, url, body in VALIDATION_ENDPOINTS:
            try:
                if method == "GET":
                    r = await client.get(url, headers=headers)
                else:
                    r = await client.post(url, json=body or {}, headers=headers)

                status = r.status_code
                # 200 и 204 — OK. 429 — временный лимит, токен валиден.
                is_ok = status in (200, 204, 429)
                details[name] = {"status": status, "ok": is_ok}

                if is_ok:
                    ok_count += 1
                elif status == 401:
                    failed_401.append(name)
                elif status == 403:
                    failed_403.append(name)
                else:
                    failed_other.append((name, status))

            except Exception as e:
                details[name] = {"status": 0, "ok": False, "error": str(e)[:100]}
                failed_other.append((name, 0))

    # Определяем итоговый статус
    if failed_401:
        # 401 на любом endpoint — токен протух/невалиден
        token_status = "invalid"
        message = f"Токен невалиден (401 на: {', '.join(failed_401)}). Пересоздайте ключ в кабинете WB."
    elif failed_403:
        # 403 — базовый токен, нет доступа к части API
        token_status = "limited"
        message = (
            f"Базовый токен (acc={acc_level}). Недоступно: {', '.join(failed_403)}. "
            f"Пересоздайте ключ в кабинете WB с полным доступом (полный токен)."
        )
    elif failed_other:
        # Другие ошибки — возможно временные. Считаем unknown, но не блокируем если большинство OK.
        if ok_count >= total - 1:
            token_status = "valid"
            message = "Токен валиден. Некоторые endpoints временно недоступны."
        else:
            token_status = "invalid"
            failed_names = ", ".join(f"{n}({s})" for n, s in failed_other)
            message = f"Ошибки валидации: {failed_names}. Проверьте ключ."
    else:
        token_status = "valid"
        message = "Токен валиден. Все API endpoints доступны."

    return {
        "token_status": token_status,
        "acc_level": acc_level,
        "details": details,
        "failed_endpoints": failed_401 + failed_403 + [n for n, _ in failed_other],
        "message": message,
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "ok_count": ok_count,
        "total": total,
    }
