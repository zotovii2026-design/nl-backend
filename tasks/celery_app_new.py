from celery import Celery
from celery.schedules import crontab
from core.config import settings
import logging

logger = logging.getLogger(__name__)

celery_app_new = Celery(
    "nl_backend_sync",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=[
        "tasks.sync_tasks",
        "tasks.wb_sync",
        "tasks.daily_sync",
        "tasks.scheduled_sync",
    ]
)

celery_app_new.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Europe/Moscow",
    enable_utc=True,
    beat_schedule={
        # ─── 03:00 MSK — Ночной сбор ─────────────────────
        "night-products": {
            "task": "wb.sched.products",
            "schedule": crontab(hour=0, minute=5),   # UTC 00:05 = MSK 03:05
        },
        "night-warehouses": {
            "task": "wb.sched.warehouses",
            "schedule": crontab(hour=0, minute=15),   # UTC 00:15 = MSK 03:15
        },

        # ─── 08:00 MSK — Утренний сбор ───────────────────
        "morning-sales": {
            "task": "wb.sched.sales",
            "schedule": crontab(hour=5, minute=0),    # UTC 05:00 = MSK 08:00
        },
        "morning-orders": {
            "task": "wb.sched.orders",
            "schedule": crontab(hour=5, minute=5),    # UTC 05:05
        },

        # ─── 14:00 MSK — Дневной сбор ────────────────────
        "day-stocks": {
            "task": "wb.sched.stocks",
            "schedule": crontab(hour=11, minute=0),   # UTC 11:00 = MSK 14:00
        },
        "day-sales": {
            "task": "wb.sched.sales",
            "schedule": crontab(hour=11, minute=5),   # UTC 11:05
        },

        # ─── 20:00 MSK — Вечерний сбор ───────────────────
        "evening-tariffs": {
            "task": "wb.sched.tariffs",
            "schedule": crontab(hour=17, minute=0),   # UTC 17:00 = MSK 20:00
        },
        "evening-adverts": {
            "task": "wb.sched.adverts",
            "schedule": crontab(hour=17, minute=5),   # UTC 17:05
        },
        "evening-orders": {
            "task": "wb.sched.orders",
            "schedule": crontab(hour=17, minute=10),  # UTC 17:10
        },

        # ─── 22:00 MSK — Парсинг raw → tech_status ───────
        "night-parse": {
            "task": "wb.sched.parse_raw",
            "schedule": crontab(hour=19, minute=0),   # UTC 19:00 = MSK 22:00
        },

        # --- 23:00 MSK --- Snapshot tariffov dlya unitki ---
        "tariff-snapshot": {
            "task": "wb.sched.tariff_snapshot",
            "schedule": crontab(hour=20, minute=0),   # UTC 20:00 = MSK 23:00
        },
    },
)
