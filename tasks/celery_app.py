from celery import Celery
from core.config import settings

celery_app = Celery(
    "nl_backend",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=["tasks.sync_tasks", "tasks.wb_sync", "tasks.ad_sync", "tasks.scheduled_sync"]
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    beat_schedule={
        # --- Основной цикл: синк → parse_raw ---
        # Parse raw 2 раза в час: после синков данные сразу попадают в tech_status
        "parse-raw-frequent": {
            "task": "wb.sched.parse_raw",
            "schedule": 1800,  # каждые 30 мин
        },
        "sync-ad-stats-daily": {
            "task": "wb.sched.ad_stats",
            "schedule": 86400,  # раз в сутки
            "kwargs": {"days_back": 1},
        },
        "commission-daily": {
            "task": "wb.sched.commission",
            "schedule": 86400,  # раз в сутки
        },
        "tariff-snapshot-daily": {
            "task": "wb.sched.tariff_snapshot",
            "schedule": 86400,  # раз в сутки
        },
    },
)
