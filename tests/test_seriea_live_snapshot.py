from apps.api.app.routes import data as d


def test_build_seriea_live_snapshot_applies_live_fixture_score():
    table_rows = [
        {"Pos": 1, "Squad": "Alpha", "Pts": 40, "MP": 20, "GF": 30, "GA": 18, "GD": 12},
        {"Pos": 2, "Squad": "Beta", "Pts": 39, "MP": 20, "GF": 28, "GA": 17, "GD": 11},
    ]
    fixture_rows = [
        {
            "round": 26,
            "home_team": "Beta",
            "away_team": "Alpha",
            "home_score": 2,
            "away_score": 1,
            "match_status": 1,
            "kickoff_iso": "2026-02-20T20:45",
        }
    ]

    payload = d._build_seriea_live_snapshot(table_rows, fixture_rows, preferred_round=26)
    items = payload["table"]

    assert payload["round"] == 26
    assert len(items) == 2
    assert items[0]["team"] == "Beta"
    assert items[0]["points_base"] == 39
    assert items[0]["points_live"] == 42
    assert items[0]["live_delta"] == 3
    assert items[0]["position_delta"] == 1

    assert items[1]["team"] == "Alpha"
    assert items[1]["points_base"] == 40
    assert items[1]["points_live"] == 40
    assert items[1]["live_delta"] == 0
    assert items[1]["position_delta"] == -1


def test_build_seriea_live_snapshot_ignores_scheduled_fixture():
    table_rows = [
        {"Pos": 1, "Squad": "Alpha", "Pts": 40, "MP": 20, "GF": 30, "GA": 18, "GD": 12},
        {"Pos": 2, "Squad": "Beta", "Pts": 39, "MP": 20, "GF": 28, "GA": 17, "GD": 11},
    ]
    fixture_rows = [
        {
            "round": 26,
            "home_team": "Beta",
            "away_team": "Alpha",
            "home_score": 0,
            "away_score": 0,
            "match_status": 0,
            "kickoff_iso": "2026-02-20T20:45",
        }
    ]

    payload = d._build_seriea_live_snapshot(table_rows, fixture_rows, preferred_round=26)
    items = payload["table"]

    assert items[0]["team"] == "Alpha"
    assert items[0]["points_live"] == 40
    assert items[0]["live_delta"] == 0
    assert items[1]["team"] == "Beta"
    assert items[1]["points_live"] == 39
    assert items[1]["live_delta"] == 0


def test_build_seriea_live_snapshot_backfills_missing_rounds_before_target():
    table_rows = [
        {"Pos": 1, "Squad": "Inter", "Pts": 58, "MP": 24, "GF": 57, "GA": 19, "GD": 38},
        {"Pos": 2, "Squad": "Milan", "Pts": 50, "MP": 23, "GF": 38, "GA": 17, "GD": 21},
        {"Pos": 6, "Squad": "Como", "Pts": 41, "MP": 23, "GF": 37, "GA": 16, "GD": 21},
        {"Pos": 19, "Squad": "Pisa", "Pts": 15, "MP": 24, "GF": 19, "GA": 40, "GD": -21},
    ]
    fixture_rows = [
        {
            "round": 24,
            "home_team": "Milan",
            "away_team": "Como",
            "home_score": 1,
            "away_score": 1,
            "match_status": 2,
            "kickoff_iso": "2026-02-09T20:45",
        },
        {
            "round": 25,
            "home_team": "Pisa",
            "away_team": "Milan",
            "home_score": 1,
            "away_score": 2,
            "match_status": 2,
            "kickoff_iso": "2026-02-16T20:45",
        },
    ]

    payload = d._build_seriea_live_snapshot(table_rows, fixture_rows, preferred_round=25)
    items = {row["team"]: row for row in payload["table"]}
    milan = items["Milan"]

    assert milan["played_live"] == 25
    assert milan["points_live"] == 54
    assert milan["gf_live"] == 41
    assert milan["ga_live"] == 19
