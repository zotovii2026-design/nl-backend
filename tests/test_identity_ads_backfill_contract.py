from pathlib import Path


def test_connect_wb_enqueues_ads_backfill_after_token_save():
    source = Path("api/v1/routers/identity.py").read_text(encoding="utf-8")

    assert "ADS_INITIAL_BACKFILL_DAYS = 9" in source
    assert 'celery_app.send_task("wb.initial_sync", kwargs={"org_id": org_id})' in source
    assert 'celery_app.send_task(\n            "wb.sched.ad_stats"' in source
    assert '"days_back": ADS_INITIAL_BACKFILL_DAYS' in source
    assert '"org_id": org_id' in source
    assert '"include_current_day": True' in source
    assert "task_ids, task_warnings = _enqueue_wb_initial_tasks(str(org.id))" in source


def test_wb_key_add_enqueues_ads_backfill_for_existing_org():
    source = Path("api/v1/routers/identity.py").read_text(encoding="utf-8")

    assert "task_ids, task_warnings = _enqueue_wb_initial_tasks(str(org_id))" in source
    assert '"ads_backfill_task_id":' not in source
    assert "**task_ids" in source
