"""Legacy sync API (manual WB sync endpoints removed 2026-08-21).

Ручные sync-эндпоинты /products,/sales,/orders,/stocks и /logs удалены:
данные собираются Celery-пайплайном v2 (raw_api_data -> parse_raw -> tech_status).
Если понадобится ручной sync — использовать tasks/sync/initial_sync.py.
"""
from fastapi import APIRouter

router = APIRouter(tags=["sync"])
