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
        lambda standings_index, preferred_round=None: (
            [{"team": "Alpha", "round": 25}, {"team": "Beta", "round": 25}],
            [25],
            None,
        ),
    )
    monkeypatch.setattr(d, "_load_live_round_context", lambda db, round_value: {"votes_by_team_player": {"x": {}}})

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
        lambda standings_index, preferred_round=None: (
            [{"team": "Alpha", "round": 25}, {"team": "Beta", "round": 25}],
            [25],
            None,
        ),
    )
    monkeypatch.setattr(d, "_load_live_round_context", lambda db, round_value: {"votes_by_team_player": {"x": {}}})

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


def test_build_live_standings_rows_skips_live_increment_without_votes(monkeypatch):
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
    monkeypatch.setattr(
        d,
        "_load_real_formazioni_rows",
        lambda standings_index, preferred_round=None: (
            [{"team": "Alpha", "round": 24}, {"team": "Beta", "round": 24}],
            [24],
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

    assert items[0]["played"] == 24
    assert items[1]["played"] == 24
    assert items[0]["points_live"] == 50.0
    assert items[1]["points_live"] == 48.0


def test_build_live_standings_rows_applies_precalc_when_played_is_backfilled(monkeypatch):
    monkeypatch.setattr(
        d,
        "_load_standings_rows",
        lambda: [
            {"pos": 1, "team": "Alpha", "played": 25, "points": 1852.5, "played_backfilled": True},
            {"pos": 2, "team": "Beta", "played": 25, "points": 1846.0, "played_backfilled": True},
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
        lambda standings_index, preferred_round=None: (
            [{"team": "Alpha", "round": 25}, {"team": "Beta", "round": 25}],
            [25],
            None,
        ),
    )
    monkeypatch.setattr(d, "_load_live_round_context", lambda db, round_value: {})

    def _fake_attach(items, live_context):
        for item in items:
            if item.get("team") == "Alpha":
                item["totale_live"] = 59.5
                item["totale_source"] = "precalc"
            elif item.get("team") == "Beta":
                item["totale_live"] = 78.5
                item["totale_source"] = "precalc"

    monkeypatch.setattr(d, "_attach_live_scores_to_formations", _fake_attach)

    payload = d._build_live_standings_rows(db=object(), requested_round=None)
    items = payload["items"]

    assert items[0]["team"] == "Beta"
    assert items[0]["played_base"] == 24
    assert items[0]["played"] == 25
    assert items[0]["points_live"] == 1924.5

    assert items[1]["team"] == "Alpha"
    assert items[1]["played_base"] == 24
    assert items[1]["played"] == 25
    assert items[1]["points_live"] == 1912.0


def test_build_live_standings_rows_auto_imports_votes_when_missing(monkeypatch):
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
    monkeypatch.setattr(d, "_is_round_completed_from_fixtures", lambda round_value: True)
    monkeypatch.setattr(d, "_latest_round_with_live_votes", lambda db: None)
    monkeypatch.setattr(
        d,
        "_load_real_formazioni_rows",
        lambda standings_index, preferred_round=None: (
            [{"team": "Alpha", "round": 25}, {"team": "Beta", "round": 25}],
            [25],
            None,
        ),
    )

    state = {"calls": 0}

    def _fake_context(db, round_value):
        state["calls"] += 1
        if state["calls"] == 1:
            return {}
        return {"votes_by_team_player": {"alpha": {"x": 1}}}

    monkeypatch.setattr(d, "_load_live_round_context", _fake_context)

    imported = {"count": 0}

    def _fake_import(db, round_value=None, season=None, source_url=None, source_html=None):
        imported["count"] += 1
        return {"ok": True, "round": round_value}

    monkeypatch.setattr(d, "_import_live_votes_internal", _fake_import)

    def _fake_attach(items, live_context):
        has_votes = isinstance(live_context.get("votes_by_team_player"), dict) and bool(
            live_context.get("votes_by_team_player")
        )
        for item in items:
            if has_votes:
                item["totale_live"] = 70.0 if item.get("team") == "Alpha" else 66.0
            else:
                item["totale_live"] = None

    monkeypatch.setattr(d, "_attach_live_scores_to_formations", _fake_attach)

    class DummyDb:
        def query(self, *_args, **_kwargs):
            return None

    d._AUTO_VOTI_IMPORT_ATTEMPTED_ROUNDS.clear()
    payload = d._build_live_standings_rows(db=DummyDb(), requested_round=None)

    assert imported["count"] == 1
    assert payload["items"][0]["points_live"] == 120.0
    assert payload["items"][1]["points_live"] == 114.0
