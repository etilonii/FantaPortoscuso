import time

from apps.api.app.routes import data as d


def test_enqueue_sync_complete_background_prevents_duplicate_runs(monkeypatch):
    def _slow_worker(**_kwargs):
        try:
            time.sleep(0.25)
        finally:
            with d._SYNC_COMPLETE_BACKGROUND_LOCK:
                d._SYNC_COMPLETE_BACKGROUND_RUNNING = False

    monkeypatch.setattr(d, "_sync_complete_background_worker", _slow_worker)

    with d._SYNC_COMPLETE_BACKGROUND_LOCK:
        d._SYNC_COMPLETE_BACKGROUND_RUNNING = False

    first = d._enqueue_sync_complete_background(
        run_pipeline=True,
        fetch_quotazioni=True,
        fetch_global_stats=True,
        formations_matchday=26,
    )
    second = d._enqueue_sync_complete_background(
        run_pipeline=True,
        fetch_quotazioni=True,
        fetch_global_stats=True,
        formations_matchday=26,
    )

    assert first.get("ok") is True
    assert first.get("queued") is True
    assert second.get("ok") is True
    assert second.get("queued") is False
    assert second.get("running") is True

    time.sleep(0.35)
    third = d._enqueue_sync_complete_background(
        run_pipeline=True,
        fetch_quotazioni=True,
        fetch_global_stats=True,
        formations_matchday=26,
    )
    assert third.get("ok") is True
    assert third.get("queued") is True
