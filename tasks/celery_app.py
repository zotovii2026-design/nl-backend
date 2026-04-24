from celery import Celery
from core.config import settings

celery_app = Celery(
    "nl_backend",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=["tasks.sync_tasks", "tasks.wb_sync", "tasks.ad_sync"]
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    beat_schedule={
        "sync-ad-stats-daily": {
            "task": "wb.sched.ad_stats",
            "schedule": 86400,  # раз в сутки
            "kwargs": {"days_back": 1},
        },
    },
)
