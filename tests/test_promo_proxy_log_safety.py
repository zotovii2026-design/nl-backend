from pathlib import Path


PROMO_SYNC_SOURCE = Path("tasks/promo_sync.py").read_text(encoding="utf-8")


def test_proxy_exception_log_does_not_render_request_url():
    assert 'proxy batch error: {exc}' not in PROMO_SYNC_SOURCE
    assert '"[promo_snapshot] proxy batch error: type=%s status=%s"' in PROMO_SYNC_SOURCE
    assert "type(exc).__name__" in PROMO_SYNC_SOURCE
