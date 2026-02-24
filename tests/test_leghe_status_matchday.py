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
