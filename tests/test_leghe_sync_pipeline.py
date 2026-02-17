from pathlib import Path

from apps.api.app import leghe_sync as ls


def _fake_context() -> ls.LegheContext:
    return ls.LegheContext(
        alias="fantaportoscuso",
        app_key="APPKEY",
        competition_id=53267,
        competition_name="Fanta",
        current_turn=26,
        last_calculated_matchday=25,
        suggested_formations_matchday=26,
    )


def test_run_leghe_sync_and_pipeline_runs_extended_steps(monkeypatch):
    status_writes = []

    monkeypatch.setattr(ls, "_build_leghe_opener", lambda: (object(), []))
    monkeypatch.setattr(ls, "fetch_leghe_context", lambda opener, *, alias: _fake_context())
    monkeypatch.setattr(ls, "leghe_login", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(ls, "_write_status", lambda payload: status_writes.append(payload))

    def _ok_run(argv, *, cwd):
        return {
            "argv": list(argv),
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "started_at": "",
            "ended_at": "",
            "duration_seconds": 0.0,
        }

    monkeypatch.setattr(ls, "_run_subprocess", _ok_run)

    result = ls.run_leghe_sync_and_pipeline(
        alias="fantaportoscuso",
        username="user",
        password="pass",
        date_stamp="2026-02-20",
        formations_matchday=26,
        download_rose=False,
        download_classifica=False,
        download_formazioni=False,
        download_formazioni_xlsx=False,
        run_pipeline=True,
    )

    assert result["ok"] is True
    assert result.get("warnings") == []

    scripts = [
        Path(str(run.get("argv", ["", ""])[1])).name
        for run in result.get("pipeline", [])
        if isinstance(run, dict)
    ]
    assert scripts == [
        "pipeline_v2.py",
        "update_data.py",
        "clean_stats_batch.py",
        "update_fixtures.py",
        "build_player_tiers.py",
        "build_team_strength_ranking.py",
        "build_season_predictions.py",
    ]

    assert status_writes
    final_status = status_writes[-1]
    steps = dict(final_status.get("steps") or {})
    assert final_status.get("result") == "ok"
    assert steps.get("rose") == "ok"
    assert steps.get("stats") == "ok"
    assert steps.get("strength") == "ok"


def test_run_leghe_sync_and_pipeline_keeps_running_on_optional_step_failures(monkeypatch):
    monkeypatch.setattr(ls, "_build_leghe_opener", lambda: (object(), []))
    monkeypatch.setattr(ls, "fetch_leghe_context", lambda opener, *, alias: _fake_context())
    monkeypatch.setattr(ls, "leghe_login", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(ls, "_write_status", lambda payload: None)

    def _run_with_optional_fail(argv, *, cwd):
        script_name = Path(str(argv[1])).name if len(argv) > 1 else ""
        rc = 1 if script_name in {"update_fixtures.py", "build_season_predictions.py"} else 0
        return {
            "argv": list(argv),
            "returncode": rc,
            "stdout": "",
            "stderr": "boom" if rc else "",
            "started_at": "",
            "ended_at": "",
            "duration_seconds": 0.0,
        }

    monkeypatch.setattr(ls, "_run_subprocess", _run_with_optional_fail)

    result = ls.run_leghe_sync_and_pipeline(
        alias="fantaportoscuso",
        username="user",
        password="pass",
        date_stamp="2026-02-20",
        formations_matchday=26,
        download_rose=False,
        download_classifica=False,
        download_formazioni=False,
        download_formazioni_xlsx=False,
        run_pipeline=True,
    )

    assert result["ok"] is True
    warnings = result.get("warnings") or []
    assert any("update_fixtures failed" in item for item in warnings)
    assert any("build_season_predictions failed" in item for item in warnings)


def test_extract_fantacalcio_quotazioni_rows_from_html_parses_rows():
    html = """
    <table>
      <tr class="player-row"
          data-filter-role-classic="a">
        <th class="player-name"><a><span>Martinez L.</span></a></th>
        <td data-col-key="sq">INT</td>
        <td data-col-key="c_qi">34</td>
        <td data-col-key="c_qa">37</td>
      </tr>
      <tr class="player-row"
          data-filter-role-classic="p">
        <th class="player-name"><a><span>Sommer</span></a></th>
        <td data-col-key="sq">INT</td>
        <td data-col-key="c_qi">16</td>
        <td data-col-key="c_qa">17</td>
      </tr>
    </table>
    """

    rows = ls._extract_fantacalcio_quotazioni_rows_from_html(html)

    assert len(rows) == 2
    assert rows[0]["Giocatore"] == "Martinez L."
    assert rows[0]["Ruolo"] == "A"
    assert rows[0]["Squadra"] == "Inter"
    assert rows[0]["PrezzoIniziale"] == 34
    assert rows[0]["PrezzoAttuale"] == 37


def test_extract_fantacalcio_stats_rows_from_html_parses_rows():
    html = """
    <table>
      <tr class="player-row"
          data-filter-role-classic="a">
        <th class="player-name"><a><span>Martinez L.</span></a></th>
        <td data-col-key="sq">INT</td>
        <td data-col-key="pg">24</td>
        <td data-col-key="mv">6,82</td>
        <td data-col-key="mfv">8,15</td>
        <td data-col-key="gol">12</td>
        <td data-col-key="gs">0</td>
        <td data-col-key="rig">3 / 4</td>
        <td data-col-key="rp">0</td>
        <td data-col-key="ass">4</td>
        <td data-col-key="amm">1</td>
        <td data-col-key="esp">0</td>
      </tr>
    </table>
    """

    rows = ls._extract_fantacalcio_stats_rows_from_html(html)
    assert len(rows) == 1
    row = rows[0]
    assert row["Giocatore"] == "Martinez L."
    assert row["Posizione"] == "A"
    assert row["Squadra"] == "INT"
    assert row["pg"] == 24
    assert row["mv"] == 6.82
    assert row["mfv"] == 8.15
    assert row["gol"] == 12
    assert row["rigori_segnati"] == 3
    assert row["rigori_sbagliati"] == 1
    assert row["ass"] == 4


def test_run_leghe_sync_fetches_global_stats_when_enabled(monkeypatch):
    monkeypatch.setattr(ls, "_build_leghe_opener", lambda: (object(), []))
    monkeypatch.setattr(ls, "fetch_leghe_context", lambda opener, *, alias: _fake_context())
    monkeypatch.setattr(ls, "leghe_login", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(ls, "_write_status", lambda payload: None)
    monkeypatch.setattr(
        ls,
        "_run_subprocess",
        lambda argv, *, cwd: {
            "argv": list(argv),
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "started_at": "",
            "ended_at": "",
            "duration_seconds": 0.0,
        },
    )
    captured_stats_kwargs = {}

    def _fake_download_stats(**kwargs):
        captured_stats_kwargs.update(kwargs)
        return {"ok": True, "rows": 123, "files": []}

    monkeypatch.setattr(ls, "download_fantacalcio_stats_csv_bundle", _fake_download_stats)

    result = ls.run_leghe_sync_and_pipeline(
        alias="fantaportoscuso",
        username="user",
        password="pass",
        date_stamp="2026-02-20",
        formations_matchday=26,
        download_rose=False,
        download_classifica=False,
        download_formazioni=False,
        download_formazioni_xlsx=False,
        fetch_global_stats=True,
        run_pipeline=True,
    )

    assert result["ok"] is True
    assert "global_stats" in (result.get("downloaded") or {})
    assert captured_stats_kwargs.get("username") == "user"
    assert captured_stats_kwargs.get("password") == "pass"


def test_run_leghe_sync_fetches_quotazioni_with_auth_credentials(monkeypatch):
    monkeypatch.setattr(ls, "_build_leghe_opener", lambda: (object(), []))
    monkeypatch.setattr(ls, "fetch_leghe_context", lambda opener, *, alias: _fake_context())
    monkeypatch.setattr(ls, "leghe_login", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(ls, "_write_status", lambda payload: None)
    monkeypatch.setattr(
        ls,
        "_run_subprocess",
        lambda argv, *, cwd: {
            "argv": list(argv),
            "returncode": 0,
            "stdout": "",
            "stderr": "",
            "started_at": "",
            "ended_at": "",
            "duration_seconds": 0.0,
        },
    )

    captured_quot_kwargs = {}

    def _fake_download_quot(**kwargs):
        captured_quot_kwargs.update(kwargs)
        return {"ok": True, "rows": 321}

    monkeypatch.setattr(ls, "download_fantacalcio_quotazioni_csv", _fake_download_quot)

    result = ls.run_leghe_sync_and_pipeline(
        alias="fantaportoscuso",
        username="user",
        password="pass",
        date_stamp="2026-02-20",
        formations_matchday=26,
        download_rose=False,
        download_classifica=False,
        download_formazioni=False,
        download_formazioni_xlsx=False,
        fetch_quotazioni=True,
        run_pipeline=True,
    )

    assert result["ok"] is True
    assert "quotazioni" in (result.get("downloaded") or {})
    assert captured_quot_kwargs.get("username") == "user"
    assert captured_quot_kwargs.get("password") == "pass"


def test_download_fantacalcio_stats_prefers_authenticated_xlsx(monkeypatch, tmp_path):
    monkeypatch.setattr(ls, "_build_cookie_opener", lambda: (object(), []))
    monkeypatch.setattr(ls, "_fantacalcio_login", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(
        ls,
        "_download_fantacalcio_excel_authenticated",
        lambda *args, **kwargs: {"ok": True, "path": str(kwargs.get("out_path"))},
    )
    monkeypatch.setattr(
        ls,
        "_extract_fantacalcio_stats_rows_from_xlsx",
        lambda path: [
            {
                "Giocatore": "Martinez L.",
                "Posizione": "A",
                "Squadra": "Inter",
                "gol": 12,
                "ass": 4,
                "amm": 1,
                "esp": 0,
                "autogol": 0,
                "rp": 0,
                "gs": 0,
                "rigori_segnati": 3,
                "rigori_sbagliati": 1,
                "pg": 24,
                "mv": 6.82,
                "mfv": 8.15,
            }
        ],
    )
    monkeypatch.setattr(
        ls,
        "_http_read_bytes",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("HTML fallback should not be used")),
    )

    result = ls.download_fantacalcio_stats_csv_bundle(
        season_slug="2025-26",
        date_stamp="2026-02-20",
        out_dir=tmp_path,
        username="user",
        password="pass",
    )

    assert result["ok"] is True
    assert result["source"] == "xlsx_authenticated"
    assert result["rows"] == 1
    assert (tmp_path / "gol_2026-02-20.csv").exists()


def test_download_fantacalcio_quotazioni_prefers_authenticated_xlsx(monkeypatch, tmp_path):
    monkeypatch.setattr(ls, "_build_cookie_opener", lambda: (object(), []))
    monkeypatch.setattr(ls, "_fantacalcio_login", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(
        ls,
        "_download_fantacalcio_excel_authenticated",
        lambda *args, **kwargs: {"ok": True, "path": str(kwargs.get("out_path"))},
    )
    monkeypatch.setattr(
        ls,
        "_extract_fantacalcio_quotazioni_rows_from_xlsx",
        lambda path: [
            {
                "Giocatore": "Martinez L.",
                "Squadra": "Inter",
                "Ruolo": "A",
                "PrezzoIniziale": 34,
                "PrezzoAttuale": 37,
            }
        ],
    )
    monkeypatch.setattr(
        ls,
        "_http_read_bytes",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("HTML fallback should not be used")),
    )

    out_path = tmp_path / "quotazioni_2026-02-20.csv"
    result = ls.download_fantacalcio_quotazioni_csv(
        season_slug="2025-26",
        date_stamp="2026-02-20",
        out_path=out_path,
        username="user",
        password="pass",
    )

    assert result["ok"] is True
    assert result["source"] == "xlsx_authenticated"
    assert result["rows"] == 1
    assert out_path.exists()
