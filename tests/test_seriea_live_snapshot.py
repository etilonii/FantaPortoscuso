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
