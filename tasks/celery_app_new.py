from celery import Celery
from core.config import settings
import logging

logger = logging.getLogger(__name__)

celery_app_new = Celery(
    "nl_backend_sync",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=["tasks.sync_tasks", "tasks.wb_sync", "tasks.daily_sync"]
)

celery_app_new.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    beat_schedule={
        # Ежедневная полная синхронизация (raw data + ТС) — раз в сутки
        "daily-wb-sync": {
            "task": "wb.daily_sync",
            "schedule": 60 * 60 * 24,
        },
    },
)
