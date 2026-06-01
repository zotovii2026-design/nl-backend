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
        "tasks.ad_sync",
        "tasks.promo_sync",
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
        "night-stocks": {
            "task": "wb.sched.stocks",
            "schedule": crontab(hour=0, minute=20),   # UTC 00:20 = MSK 03:20
        },
        "night-stocks-fbo": {
            "task": "wb.sched.stocks_fbo",
            "schedule": crontab(hour=0, minute=23),   # UTC 00:23 = MSK 03:23
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
        "day-stocks-fbo": {
            "task": "wb.sched.stocks_fbo",
            "schedule": crontab(hour=11, minute=3),   # UTC 11:03 = MSK 14:03
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


        # ─── Sales Funnel: показы/клики по товарам (1 раз в день) ──
        "sales-funnel-daily": {
            "task": "wb.sched.sales_funnel",
            "schedule": crontab(hour=18, minute=30),   # UTC 18:30 = MSK 21:30 (до вечернего parse)
        },

        # ─── Парсинг raw → tech_status (3 раза в день) ──
        "parse-morning": {
            "task": "wb.sched.parse_raw",
            "schedule": crontab(hour=6, minute=30),   # UTC 06:30 = MSK 09:30 (после утреннего сбора)
        },
        "parse-afternoon": {
            "task": "wb.sched.parse_raw",
            "schedule": crontab(hour=12, minute=30),   # UTC 12:30 = MSK 15:30 (после дневного сбора)
        },
        "parse-evening": {
            "task": "wb.sched.parse_raw",
            "schedule": crontab(hour=19, minute=0),   # UTC 19:00 = MSK 22:00 (после вечернего сбора)
        },

        # --- 23:00 MSK --- Snapshot tariffov dlya unitki ---
        "tariff-snapshot": {
            "task": "wb.sched.tariff_snapshot",
            "schedule": crontab(hour=20, minute=0),   # UTC 20:00 = MSK 23:00
        },

        # --- Каждый час --- Цены из WB discounts-prices-api v2 ---
        # --- 21:00 MSK --- Рекламная статистика ---
        "ad-stats-daily": {
            "task": "wb.sched.ad_stats",
            "schedule": crontab(hour=18, minute=0),   # UTC 18:00 = MSK 21:00
            "kwargs": {"days_back": 1},
        },

        "hourly-prices": {
            "task": "wb.sched.prices",
            "schedule": crontab(minute=30),   # каждый час в :30
        },

        # --- Каждые 2 часа --- Синхронизация акций WB ---
        "promo-sync-every-2h": {
            "task": "wb.sched.promo_sync",
            "schedule": crontab(minute=0, hour="*/2"),
        },
    },
)
