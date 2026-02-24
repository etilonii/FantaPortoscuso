from apps.api.app.routes import data as d


def test_seriea_live_snapshot_returns_all_round_fixtures():
    table_rows = [
        {"Pos": 1, "Squad": "Inter", "MP": 25, "GF": 50, "GA": 20, "Pts": 61, "Last5": "W W D W W"},
        {"Pos": 2, "Squad": "Milan", "MP": 25, "GF": 41, "GA": 19, "Pts": 54, "Last5": "W D W W L"},
    ]
    fixture_rows = [
        {
            "round": 26,
            "home_team": "Inter",
            "away_team": "Milan",
            "home_score": 2,
            "away_score": 1,
            "match_status": 4,
            "kickoff_iso": "2026-02-22T20:45",
            "match_url": "https://example.test/26/inter-milan",
            "match_id": 1001,
        },
        {
            "round": 27,
            "home_team": "Milan",
            "away_team": "Inter",
            "home_score": None,
            "away_score": None,
            "match_status": 0,
            "kickoff_iso": "2026-02-28T20:45",
            "match_url": "https://example.test/27/milan-inter",
            "match_id": 1002,
        },
    ]

    snapshot = d._build_seriea_live_snapshot(
        table_rows,
        fixture_rows,
        preferred_round=26,
    )

    fixture_rounds = sorted({int(row.get("round") or 0) for row in snapshot.get("fixtures", [])})
    assert snapshot.get("round") == 26
    assert fixture_rounds == [26, 27]
