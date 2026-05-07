from apps.api.app.services.team_trend import build_team_trend_payload


def test_team_trend_available_when_history_exists():
    payload = build_team_trend_payload(
        "Pi-Ciaccio",
        [
            {"round": 30, "score": 68.5, "position": None, "live_status": "final"},
            {"round": 31, "score": 74.0, "position": None, "live_status": "final"},
        ],
    )

    assert payload["available"] is True
    assert payload["team"] == "Pi-Ciaccio"
    assert [item["round"] for item in payload["rounds"]] == [30, 31]


def test_team_trend_unavailable_without_history():
    payload = build_team_trend_payload("Pi-Ciaccio", [])

    assert payload["available"] is False
    assert payload["rounds"] == []
    assert payload["last_5"] == []
    assert payload["message"] == "Andamento non ancora disponibile."


def test_team_trend_calculates_average_last_5():
    payload = build_team_trend_payload(
        "Pi-Ciaccio",
        [
            {"round": 30, "score": 68.5},
            {"round": 31, "score": 74.0},
            {"round": 32, "score": 71.5},
            {"round": 33, "score": 77.0},
            {"round": 34, "score": 70.0},
            {"round": 35, "score": 69.5},
        ],
    )

    assert [item["round"] for item in payload["last_5"]] == [31, 32, 33, 34, 35]
    assert payload["average_last_5"] == 72.4


def test_team_trend_exposes_best_and_worst_round():
    payload = build_team_trend_payload(
        "Pi-Ciaccio",
        [
            {"round": 30, "score": 68.5},
            {"round": 31, "score": 74.0},
            {"round": 32, "score": 63.5},
            {"round": 33, "score": 77.0},
        ],
    )

    assert payload["best_round"] == {"round": 33, "score": 77.0}
    assert payload["worst_round"] == {"round": 32, "score": 63.5}


def test_team_trend_position_trend_unknown_without_positions():
    payload = build_team_trend_payload(
        "Pi-Ciaccio",
        [
            {"round": 30, "score": 68.5, "position": None},
            {"round": 31, "score": 74.0, "position": None},
            {"round": 32, "score": 71.5, "position": None},
        ],
    )

    assert payload["position_trend"] == "unknown"
