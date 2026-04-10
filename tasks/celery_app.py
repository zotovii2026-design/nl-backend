from celery import Celery
from core.config import settings

# Создание Celery приложения
celery_app = Celery(
    "nl_backend",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=["tasks.sync_tasks", "tasks.wb_sync"]
)

# Настройки Celery
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    beat_schedule={
        # Пример: синхронизация каждые 30 минут
        # "sync-wb-data": {
        #     "task": "tasks.sync_tasks.sync_wb_data",
        #     "schedule": 30 * 60,
        # },
    },
)
