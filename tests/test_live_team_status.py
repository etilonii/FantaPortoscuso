from apps.api.app.services.live_team_status import build_live_team_status


def _score(source="manual", *, vote=6.5, fantavote=6.5, is_sv=False, is_absent=False):
    return {
        "source": source,
        "vote": vote,
        "fantavote": fantavote,
        "is_sv": is_sv,
        "is_absent": is_absent,
    }


def _base_item():
    starters = {
        "portiere": "Portiere Uno",
        "difensori": ["Difensore A", "Difensore B", "Difensore C"],
        "centrocampisti": ["Centro A", "Centro B", "Centro C", "Centro D"],
        "attaccanti": ["Attaccante A", "Attaccante B", "Attaccante C"],
    }
    original_lineup = {"modulo": "343", **starters}
    effective = {**starters}
    item = {
        **effective,
        "original_lineup": original_lineup,
        "substitutions": [],
        "missing_players": [],
        "panchina": ["Riserva P", "Riserva D", "Riserva C", "Riserva A"],
        "panchina_details": [
            {"id": "r1", "name": "Riserva P", "role": "P"},
            {"id": "r2", "name": "Riserva D", "role": "D"},
            {"id": "r3", "name": "Riserva C", "role": "C"},
            {"id": "r4", "name": "Riserva A", "role": "A"},
        ],
        "capitano": "Centro A",
        "vice_capitano": "Attaccante A",
        "mod_capitano": 0.5,
    }
    player_scores = {}
    for name in [
        starters["portiere"],
        *starters["difensori"],
        *starters["centrocampisti"],
        *starters["attaccanti"],
        "Riserva P",
        "Riserva D",
        "Riserva C",
        "Riserva A",
    ]:
        player_scores[name] = _score()
    item["player_scores"] = player_scores
    return item


def _regulation():
    return {
        "modifiers": {
            "difesa": {"enabled": True, "requires_defenders_min": 3, "include_goalkeeper_vote": True},
            "capitano": {"enabled": True, "vice_captain_enabled": True},
        }
    }


def test_live_team_status_final_when_only_irrelevant_bench_is_pending():
    item = _base_item()
    item["player_scores"]["Riserva D"] = _score(source="default")

    result = build_live_team_status(item, regulation=_regulation())

    assert result["live_status"] == "final"
    assert result["pending_players"] == []


def test_live_team_status_in_progress_when_starter_is_still_pending():
    item = _base_item()
    item["player_scores"]["Attaccante C"] = _score(source="default")

    result = build_live_team_status(item, regulation=_regulation())

    assert result["live_status"] == "in_progress"
    assert "Attaccante C" in result["pending_players"]


def test_live_team_status_final_when_sv_starter_has_voted_substitute():
    item = _base_item()
    item["original_lineup"]["attaccanti"] = ["Attaccante A", "Attaccante B", "Titolare SV"]
    item["attaccanti"] = ["Attaccante A", "Attaccante B", "Riserva A"]
    item["substitutions"] = [
        {"out": "Titolare SV", "out_role": "A", "in": "Riserva A", "in_role": "A", "source": "same_role"}
    ]
    item["player_scores"]["Titolare SV"] = _score(vote=None, fantavote=None, is_sv=True)

    result = build_live_team_status(item, regulation=_regulation())

    assert result["live_status"] == "final"
    assert result["resolved_substitutions"] == 1


def test_live_team_status_in_progress_when_sv_starter_still_needs_substitute():
    item = _base_item()
    item["original_lineup"]["attaccanti"] = ["Attaccante A", "Attaccante B", "Titolare SV"]
    item["missing_players"] = ["Titolare SV"]
    item["player_scores"]["Titolare SV"] = _score(vote=None, fantavote=None, is_sv=True)

    result = build_live_team_status(item, regulation=_regulation())

    assert result["live_status"] == "in_progress"
    assert result["pending_substitutions"] == 1


def test_live_team_status_in_progress_when_defense_modifier_is_not_determinable():
    item = _base_item()
    item["player_scores"]["Difensore C"] = _score(source="default")

    result = build_live_team_status(item, regulation=_regulation())

    assert result["live_status"] == "in_progress"
    assert result["modifiers_status"]["difesa"] == "in_progress"


def test_live_team_status_needs_review_when_lineup_is_incomplete():
    item = _base_item()
    item["attaccanti"] = ["Attaccante A", "Attaccante B"]
    item["original_lineup"]["attaccanti"] = ["Attaccante A", "Attaccante B"]

    result = build_live_team_status(item, regulation=_regulation())

    assert result["live_status"] == "needs_review"
    assert result["live_status_label"] == "Da verificare"
