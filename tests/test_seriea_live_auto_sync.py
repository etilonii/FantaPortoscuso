from types import SimpleNamespace

from apps.api.app.routes import data as d


def test_run_auto_seriea_live_context_sync_skips_when_not_due(monkeypatch):
    monkeypatch.setattr(d, "_claim_scheduled_job_run", lambda *_args, **_kwargs: False)

    payload = d.run_auto_seriea_live_context_sync(
        db=object(),
        min_interval_seconds=300,
    )

    assert payload["ok"] is True
    assert payload["skipped"] is True
    assert payload["reason"] == "not_due_or_claimed_by_other_instance"


def test_run_auto_seriea_live_context_sync_runs_script_with_inferred_round(monkeypatch):
    monkeypatch.setattr(d, "_leghe_sync_reference_round_now", lambda: None)
    monkeypatch.setattr(d, "_load_status_matchday", lambda: 26)
    monkeypatch.setattr(d, "_infer_matchday_from_fixtures", lambda: 25)
    monkeypatch.setattr(d, "_infer_matchday_from_stats", lambda: None)
    monkeypatch.setattr(d, "_normalize_season_slug", lambda _value: "2025-26")

    captured = {}

    def _fake_run(argv, cwd, capture_output, text, check, env):
        captured["argv"] = list(argv)
        captured["cwd"] = str(cwd)
        return SimpleNamespace(returncode=0, stdout="[ok] seriea sync", stderr="")

    monkeypatch.setattr(d.subprocess, "run", _fake_run)

    payload = d.run_auto_seriea_live_context_sync(
        db=object(),
        min_interval_seconds=None,
    )

    assert payload["ok"] is True
    assert payload["round"] == 26
    assert payload["season"] == "2025-26"
    assert "--season" in captured["argv"]
    assert "2025-26" in captured["argv"]
    assert "--round" in captured["argv"]
    assert "26" in captured["argv"]
