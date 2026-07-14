from celery.schedules import crontab


TASK_MODULES = (
    "tasks.sync.parse_raw",
    "tasks.sync.wb_fetch",
    "tasks.ad_sync",
    "tasks.promo_sync",
    "tasks.box_tariffs_sync",
    "tasks.monitoring",
    "tasks.opiu_sync",
    "tasks.seasonality_sync",
)


# Celery interprets these crontabs in Europe/Moscow, configured on the app.
BEAT_SCHEDULE = {
    "night-products": {
        "task": "wb.sched.products",
        "schedule": crontab(hour=3, minute=5),
    },
    "night-warehouses": {
        "task": "wb.sched.warehouses",
        "schedule": crontab(hour=3, minute=15),
    },
    "night-stocks-fbo": {
        "task": "wb.sched.stocks_fbo",
        "schedule": crontab(hour=3, minute=23),
    },
    "opiu-finance-daily": {
        "task": "wb.sched.opiu_finance",
        "schedule": crontab(hour=4, minute=0),
    },
    "morning-sales": {
        "task": "wb.sched.sales",
        "schedule": crontab(hour=8, minute=0),
    },
    "morning-orders": {
        "task": "wb.sched.orders",
        "schedule": crontab(hour=8, minute=5),
    },
    "box-tariffs-daily": {
        "task": "wb.sched.box_tariffs",
        "schedule": crontab(hour=8, minute=30),
    },
    "morning-ad-stats-yesterday": {
        "task": "wb.sched.ad_stats",
        "schedule": crontab(hour=10, minute=0),
        "kwargs": {"days_back": 2},
    },
    "parse-morning": {
        "task": "wb.sched.parse_raw",
        "schedule": crontab(hour=9, minute=30),
    },
    "day-stocks-fbo": {
        "task": "wb.sched.stocks_fbo",
        "schedule": crontab(hour=14, minute=3),
    },
    "day-sales": {
        "task": "wb.sched.sales",
        "schedule": crontab(hour=14, minute=5),
    },
    "parse-afternoon": {
        "task": "wb.sched.parse_raw",
        "schedule": crontab(hour=15, minute=30),
    },
    "evening-tariffs": {
        "task": "wb.sched.tariffs",
        "schedule": crontab(hour=20, minute=0),
    },
    "evening-adverts": {
        "task": "wb.sched.adverts",
        "schedule": crontab(hour=20, minute=5),
    },
    "ad-stats-daily": {
        "task": "wb.sched.ad_stats",
        "schedule": crontab(hour=21, minute=0),
        "kwargs": {"days_back": 9},
    },
    "ad-stats-weekly-backfill": {
        "task": "wb.sched.ad_stats",
        "schedule": crontab(hour=4, minute=30, day_of_week="sun"),
        "kwargs": {"days_back": 7},
    },
    "sales-funnel-daily": {
        "task": "wb.sched.sales_funnel",
        "schedule": crontab(hour=21, minute=30),
    },
    "parse-evening": {
        "task": "wb.sched.parse_raw",
        "schedule": crontab(hour=22, minute=0),
    },
    "tariffs-commission-daily": {
        "task": "wb.sched.commission",
        "schedule": crontab(hour=8, minute=15),
    },
    "tariff-snapshot": {
        "task": "wb.sched.tariff_snapshot",
        "schedule": crontab(hour=23, minute=0),
    },
    "hourly-prices": {
        "task": "wb.sched.prices",
        "schedule": crontab(minute=30),
    },
    "promo-sync-every-2h": {
        "task": "wb.sched.promo_sync",
        "schedule": crontab(minute=0, hour="*/2"),
    },
    "nightly-promo-snapshot": {
        "task": "wb.sched.promo_snapshot",
        "schedule": crontab(hour=2, minute=0),
    },
    "freshness-check-hourly": {
        "task": "wb.sched.freshness",
        "schedule": crontab(minute=45),
    },
    "seasonality-weekly": {
        "task": "seasonality.collect",
        "schedule": crontab(hour=2, minute=0, day_of_week="mon"),
    },
}


__all__ = ["BEAT_SCHEDULE", "TASK_MODULES"]
