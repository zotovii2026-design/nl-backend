from celery import Celery

from core.config import settings
from tasks.celery_schedule import BEAT_SCHEDULE, TASK_MODULES


celery_app = Celery(
    "nl_backend",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=TASK_MODULES,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="Europe/Moscow",
    enable_utc=True,
    beat_schedule=BEAT_SCHEDULE,
    task_track_started=True,
    worker_send_task_events=True,
    task_send_sent_event=True,
    broker_connection_retry_on_startup=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_soft_time_limit=3000,
    task_time_limit=3600,
    result_expires=86400,
)

# Registers lifecycle signals after the canonical app has been created.
import tasks.celery_observability  # noqa: E402,F401


__all__ = ["celery_app"]
