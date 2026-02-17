from apps.api.app.routes import data as d


def test_build_live_standings_rows_adds_live_totals(monkeypatch):
    monkeypatch.setattr(
        d,
        "_load_standings_rows",
        lambda: [
            {"pos": 1, "team": "Alpha", "played": 24, "points": 50.0},
            {"pos": 2, "team": "Beta", "played": 24, "points": 48.0},
        ],
    )
    monkeypatch.setattr(
        d,
        "_build_standings_index",
        lambda: {
            "alpha": {"team": "Alpha", "pos": 1},
            "beta": {"team": "Beta", "pos": 2},
        },
    )
    monkeypatch.setattr(d, "_load_status_matchday", lambda: 25)
    monkeypatch.setattr(d, "_infer_matchday_from_fixtures", lambda: None)
    monkeypatch.setattr(d, "_infer_matchday_from_stats", lambda: None)
    monkeypatch.setattr(
        d,
        "_load_real_formazioni_rows",
        lambda standings_index: (
            [{"team": "Alpha", "round": 25}, {"team": "Beta", "round": 25}],
            [25],
            None,
        ),
    )
    monkeypatch.setattr(d, "_load_live_round_context", lambda db, round_value: {})

    def _fake_attach(items, live_context):
        for item in items:
            if item.get("team") == "Alpha":
                item["totale_live"] = 70.0
            elif item.get("team") == "Beta":
                item["totale_live"] = 66.0

    monkeypatch.setattr(d, "_attach_live_scores_to_formations", _fake_attach)

    payload = d._build_live_standings_rows(db=None, requested_round=None)
    items = payload["items"]

    assert payload["round"] == 25
    assert payload["source"] == "real"
    assert len(items) == 2

    assert items[0]["team"] == "Alpha"
    assert items[0]["points_base"] == 50.0
    assert items[0]["live_total"] == 70.0
    assert items[0]["points_live"] == 120.0
    assert items[0]["played"] == 25
    assert items[0]["pos"] == 1

    assert items[1]["team"] == "Beta"
    assert items[1]["points_live"] == 114.0
    assert items[1]["played"] == 25
    assert items[1]["pos"] == 2


def test_backfill_standings_played_if_missing_prefers_status_matchday(monkeypatch):
    rows = [
        {"pos": 1, "team": "Alpha", "played": 0, "points": 50.0},
        {"pos": 2, "team": "Beta", "played": 0, "points": 48.0},
    ]
    monkeypatch.setattr(d, "_load_status_matchday", lambda: 24)
    monkeypatch.setattr(d, "_max_completed_round_from_fixtures", lambda: 25)
    monkeypatch.setattr(d, "_infer_matchday_from_stats", lambda: 26)

    out = d._backfill_standings_played_if_missing(rows)

    assert all(int(item.get("played") or 0) == 24 for item in out)


def test_build_live_standings_rows_promotes_completed_live_votes_round(monkeypatch):
    monkeypatch.setattr(
        d,
        "_load_standings_rows",
        lambda: [
            {"pos": 1, "team": "Alpha", "played": 24, "points": 50.0},
            {"pos": 2, "team": "Beta", "played": 24, "points": 48.0},
        ],
    )
    monkeypatch.setattr(
        d,
        "_build_standings_index",
        lambda: {
            "alpha": {"team": "Alpha", "pos": 1},
            "beta": {"team": "Beta", "pos": 2},
        },
    )
    monkeypatch.setattr(d, "_load_status_matchday", lambda: 24)
    monkeypatch.setattr(d, "_infer_matchday_from_fixtures", lambda: None)
    monkeypatch.setattr(d, "_infer_matchday_from_stats", lambda: None)
    monkeypatch.setattr(d, "_latest_round_with_live_votes", lambda db: 25)
    monkeypatch.setattr(d, "_is_round_completed_from_fixtures", lambda round_value: int(round_value or 0) == 25)
    monkeypatch.setattr(
        d,
        "_load_real_formazioni_rows",
        lambda standings_index: (
            [{"team": "Alpha", "round": 25}, {"team": "Beta", "round": 25}],
            [25],
            None,
        ),
    )
    monkeypatch.setattr(d, "_load_live_round_context", lambda db, round_value: {})

    def _fake_attach(items, live_context):
        for item in items:
            if item.get("team") == "Alpha":
                item["totale_live"] = 70.0
            elif item.get("team") == "Beta":
                item["totale_live"] = 66.0

    monkeypatch.setattr(d, "_attach_live_scores_to_formations", _fake_attach)

    payload = d._build_live_standings_rows(db=object(), requested_round=None)
    items = payload["items"]

    assert payload["round"] == 25
    assert payload["latest_live_votes_round"] == 25
    assert items[0]["played"] == 25
    assert items[1]["played"] == 25
