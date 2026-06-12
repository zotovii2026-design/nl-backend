from datetime import timedelta

from tasks.monitoring import FRESHNESS_LIMITS


def test_orders_freshness_covers_daily_schedule():
    assert FRESHNESS_LIMITS["orders"] > timedelta(hours=24)


def test_sales_freshness_covers_longest_schedule_gap():
    assert FRESHNESS_LIMITS["sales"] >= timedelta(hours=18)
