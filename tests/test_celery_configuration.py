import importlib

import httpx
import pytest

from tasks.celery_observability import find_result_errors
from tasks.celery_schedule import BEAT_SCHEDULE, TASK_MODULES
from tasks.scheduled_sync import _fetch_with_retry


EXPECTED_LOCAL_TIMES = {
    "night-products": (3, 5),
    "night-warehouses": (3, 15),
    "night-stocks-fbo": (3, 23),
    "opiu-finance-daily": (4, 0),
    "morning-sales": (8, 0),
    "morning-orders": (8, 5),
    "box-tariffs-daily": (8, 30),
    "parse-morning": (9, 30),
    "day-stocks-fbo": (14, 3),
    "day-sales": (14, 5),
    "parse-afternoon": (15, 30),
    "evening-tariffs": (20, 0),
    "evening-adverts": (20, 5),
    "ad-stats-daily": (21, 0),
    "sales-funnel-daily": (21, 30),
    "parse-evening": (22, 0),
    "tariff-snapshot": (23, 0),
}


def _single_int(value):
    values = set(value)
    assert len(values) == 1
    return next(iter(values))


def test_schedule_uses_documented_moscow_times():
    for entry_name, (hour, minute) in EXPECTED_LOCAL_TIMES.items():
        schedule = BEAT_SCHEDULE[entry_name]["schedule"]
        assert _single_int(schedule.hour) == hour
        assert _single_int(schedule.minute) == minute


def test_every_scheduled_task_is_registered():
    from tasks.celery_app import celery_app

    for module_name in TASK_MODULES:
        importlib.import_module(module_name)
    celery_app.loader.import_default_modules()
    celery_app.finalize()

    scheduled_tasks = {entry["task"] for entry in BEAT_SCHEDULE.values()}
    assert scheduled_tasks <= set(celery_app.tasks)


def test_deprecated_supplier_stocks_tasks_are_not_scheduled():
    scheduled_tasks = {entry["task"] for entry in BEAT_SCHEDULE.values()}

    assert "wb.sched.stocks" not in scheduled_tasks
    assert "wb.sched.stocks_fbs" not in scheduled_tasks


def test_nested_wb_errors_are_not_reported_as_success():
    result = {
        "organization": {
            "sales": {"status": "ok"},
            "orders": {"status": "error", "error": "WB returned 429"},
        }
    }

    assert find_result_errors(result) == [
        "result.organization.orders: WB returned 429",
        "result.organization.orders.error: WB returned 429",
    ]
    assert find_result_errors({"2026-06-11": "error: timeout"}) == [
        "result.2026-06-11: error: timeout"
    ]


def test_successful_or_skipped_results_do_not_trigger_alerts():
    assert find_result_errors({"status": "ok", "count": 3}) == []
    assert find_result_errors({"status": "skipped", "reason": "no_keys"}) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [429, 502])
async def test_wb_retry_handles_rate_limit_and_server_errors(
    monkeypatch, status_code
):
    attempts = 0

    async def request():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            response = httpx.Response(
                status_code,
                request=httpx.Request("POST", "https://wb.test/fbo"),
            )
            raise httpx.HTTPStatusError(
                "temporary WB error",
                request=response.request,
                response=response,
            )
        return ["ok"]

    async def no_sleep(_delay):
        return None

    monkeypatch.setattr("tasks.scheduled_sync.asyncio.sleep", no_sleep)

    assert await _fetch_with_retry(request, label="test", max_retries=1) == ["ok"]
    assert attempts == 2
