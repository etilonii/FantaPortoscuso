import json
from pathlib import Path

from apps.api.app import leghe_sync as ls


def test_status_matchday_prefers_latest_context_over_selected_xlsx(monkeypatch, tmp_path: Path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(ls, "_data_dir", lambda: data_dir)
    monkeypatch.setattr(ls, "_build_leghe_opener", lambda: (object(), []))
    monkeypatch.setattr(
        ls,
        "fetch_leghe_context",
        lambda opener, alias: ls.LegheContext(
            alias=alias,
            app_key="appkey",
            competition_id=53267,
            competition_name="FantaPortoscuso",
            current_turn=26,
            last_calculated_matchday=25,
            suggested_formations_matchday=26,
        ),
    )
    monkeypatch.setattr(ls, "leghe_login", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(
        ls,
        "download_formazioni_context_html",
        lambda *args, **kwargs: {"ok": True, "path": str(kwargs.get("out_path", ""))},
    )
    monkeypatch.setattr(
        ls,
        "download_leghe_formazioni_xlsx_with_fallback",
        lambda *args, **kwargs: {
            "ok": True,
            "selected_matchday": 25,
            "rows": 10,
        },
    )

    result = ls.run_leghe_sync_and_pipeline(
        alias="fantaportoscuso",
        username="user",
        password="pass",
        formations_matchday=26,
        download_rose=False,
        download_classifica=False,
        download_formazioni=True,
        download_formazioni_xlsx=True,
        fetch_quotazioni=False,
        fetch_global_stats=False,
        run_pipeline=False,
    )

    status_path = data_dir / "status.json"
    payload = json.loads(status_path.read_text(encoding="utf-8"))

    assert result["ok"] is True
    assert result["context"]["effective_formations_matchday"] == 25
    assert payload["result"] == "ok"
    assert payload["matchday"] == 26


def test_sync_seriea_uses_status_matchday_instead_of_selected_xlsx(monkeypatch, tmp_path: Path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(ls, "_data_dir", lambda: data_dir)
    monkeypatch.setattr(ls, "_repo_root", lambda: tmp_path)
    monkeypatch.setattr(ls, "_build_leghe_opener", lambda: (object(), []))
    monkeypatch.setattr(
        ls,
        "fetch_leghe_context",
        lambda opener, alias: ls.LegheContext(
            alias=alias,
            app_key="appkey",
            competition_id=53267,
            competition_name="FantaPortoscuso",
            current_turn=26,
            last_calculated_matchday=25,
            suggested_formations_matchday=26,
        ),
    )
    monkeypatch.setattr(ls, "leghe_login", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(
        ls,
        "download_formazioni_context_html",
        lambda *args, **kwargs: {"ok": True, "path": str(kwargs.get("out_path", ""))},
    )
    monkeypatch.setattr(
        ls,
        "download_leghe_formazioni_xlsx_with_fallback",
        lambda *args, **kwargs: {
            "ok": True,
            "selected_matchday": 25,
            "rows": 10,
        },
    )

    captured_argv = []

    def _fake_run_subprocess(argv, **kwargs):
        captured_argv.append(list(argv))
        return {
            "argv": list(argv),
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "started_at": "",
            "ended_at": "",
            "duration_seconds": 0.0,
        }

    monkeypatch.setattr(ls, "_run_subprocess", _fake_run_subprocess)

    result = ls.run_leghe_sync_and_pipeline(
        alias="fantaportoscuso",
        username="user",
        password="pass",
        formations_matchday=26,
        download_rose=False,
        download_classifica=False,
        download_formazioni=True,
        download_formazioni_xlsx=True,
        fetch_quotazioni=False,
        fetch_global_stats=False,
        run_pipeline=True,
    )

    sync_cmds = [
        argv
        for argv in captured_argv
        if any("sync_seriea_live_context.py" in str(item) for item in argv)
    ]
    assert result["ok"] is True
    assert len(sync_cmds) == 1
    assert "--round" in sync_cmds[0]
    assert sync_cmds[0][sync_cmds[0].index("--round") + 1] == "26"
