import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from urllib import request

from celery.signals import task_failure, task_postrun, task_prerun
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from core.config import settings


logger = logging.getLogger(__name__)
MAX_SUMMARY_LENGTH = 4000


def find_result_errors(value, path="result"):
    """Return error-like values from nested task results."""
    errors = []
    if isinstance(value, dict):
        status = str(value.get("status", "")).lower()
        if status in {"error", "failed", "failure", "stale"}:
            detail = value.get("error") or value.get("reason") or status
            errors.append(f"{path}: {detail}")
        for key, nested in value.items():
            if key == "status":
                continue
            if key in {"error", "errors"} and nested:
                errors.append(f"{path}.{key}: {nested}")
            else:
                errors.extend(find_result_errors(nested, f"{path}.{key}"))
    elif isinstance(value, (list, tuple)):
        for index, nested in enumerate(value):
            errors.extend(find_result_errors(nested, f"{path}[{index}]"))
    elif isinstance(value, str) and value.lower().startswith("error"):
        errors.append(f"{path}: {value}")
    return list(dict.fromkeys(errors))


def _summary(value):
    try:
        serialized = json.dumps(value, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        serialized = repr(value)
    return serialized[:MAX_SUMMARY_LENGTH]


async def _execute(statement, params):
    engine = create_async_engine(settings.DATABASE_URL, pool_pre_ping=True)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(statement), params)
    finally:
        await engine.dispose()


def _run_db(statement, params):
    try:
        asyncio.run(_execute(statement, params))
    except Exception:
        logger.exception("Unable to update celery_task_runs")


def _send_alert(task_name, task_id, errors):
    payload = {
        "source": "nl-table-celery",
        "task_name": task_name,
        "task_id": task_id,
        "errors": errors[:20],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.error("Celery task reported errors: %s", _summary(payload))
    if not settings.CELERY_ALERT_WEBHOOK_URL:
        return

    try:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        webhook_request = request.Request(
            settings.CELERY_ALERT_WEBHOOK_URL,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(webhook_request, timeout=5):
            pass
    except Exception:
        logger.exception("Unable to send Celery alert webhook")


@task_prerun.connect
def record_task_start(task_id=None, task=None, **kwargs):
    if not task_id or task is None:
        return
    _run_db(
        """
        INSERT INTO celery_task_runs (id, task_id, task_name, status, started_at)
        VALUES (:run_id, :task_id, :task_name, 'running', now())
        ON CONFLICT (task_id) DO UPDATE
        SET task_name = EXCLUDED.task_name,
            status = 'running',
            started_at = now(),
            finished_at = NULL,
            duration_seconds = NULL,
            result_summary = NULL,
            error_message = NULL
        """,
        {
            "run_id": uuid.uuid4(),
            "task_id": task_id,
            "task_name": task.name,
        },
    )


@task_postrun.connect
def record_task_finish(
    task_id=None,
    task=None,
    retval=None,
    state=None,
    **kwargs,
):
    if not task_id or task is None:
        return
    errors = find_result_errors(retval)
    status = "warning" if errors else str(state or "success").lower()
    _run_db(
        """
        UPDATE celery_task_runs
        SET status = :status,
            finished_at = now(),
            duration_seconds = GREATEST(
                0,
                EXTRACT(EPOCH FROM (now() - started_at))::integer
            ),
            result_summary = :result_summary,
            error_message = COALESCE(:error_message, error_message)
        WHERE task_id = :task_id
        """,
        {
            "task_id": task_id,
            "status": status,
            "result_summary": _summary(retval),
            "error_message": "\n".join(errors)[:MAX_SUMMARY_LENGTH] or None,
        },
    )
    if errors:
        _send_alert(task.name, task_id, errors)


@task_failure.connect
def record_task_failure(
    task_id=None,
    sender=None,
    exception=None,
    traceback=None,
    **kwargs,
):
    if not task_id:
        return
    task_name = getattr(sender, "name", "unknown")
    error_message = str(exception)[:MAX_SUMMARY_LENGTH]
    _run_db(
        """
        UPDATE celery_task_runs
        SET status = 'failure',
            finished_at = now(),
            duration_seconds = GREATEST(
                0,
                EXTRACT(EPOCH FROM (now() - started_at))::integer
            ),
            error_message = :error_message
        WHERE task_id = :task_id
        """,
        {"task_id": task_id, "error_message": error_message},
    )
    _send_alert(task_name, task_id, [error_message])


__all__ = ["find_result_errors"]
