from apps.api.app.routes.data import (
    _appkey_bonus_event_counts,
    _default_regulation,
    _overlay_decisive_badges_from_appkey,
    _reg_appkey_bonus_indexes,
)


def test_appkey_bonus_event_counts_uses_default_indexes():
    bonus_indexes = _reg_appkey_bonus_indexes(_default_regulation())
    bonus_array = [0] * 16
    bonus_array[8] = 1
    bonus_array[9] = 2

    counts = _appkey_bonus_event_counts(bonus_array, bonus_indexes)

    assert counts["gol_vittoria"] == 1
    assert counts["gol_pareggio"] == 2


def test_overlay_decisive_badges_updates_matching_rows():
    rows = [
        {
            "team": "CLAN",
            "player": "Hojlund",
            "gol_vittoria": 0,
            "gol_pareggio": 0,
        },
        {
            "team": "CLAN",
            "player": "Kalulu",
            "gol_vittoria": 0,
            "gol_pareggio": 0,
        },
    ]
    decisive_map = {
        "hojlund": {"gol_vittoria": 1, "gol_pareggio": 0},
        "kalulu": {"gol_vittoria": 0, "gol_pareggio": 1},
    }

    applied = _overlay_decisive_badges_from_appkey(rows, decisive_map)

    assert applied == 2
    assert rows[0]["gol_vittoria"] == 1
    assert rows[0]["gol_pareggio"] == 0
    assert rows[1]["gol_vittoria"] == 0
    assert rows[1]["gol_pareggio"] == 1


def test_overlay_decisive_badges_keeps_existing_higher_values():
    rows = [
        {
            "team": "CLAN",
            "player": "Hojlund",
            "gol_vittoria": 2,
            "gol_pareggio": 0,
        }
    ]
    decisive_map = {
        "hojlund": {"gol_vittoria": 1, "gol_pareggio": 0},
    }

    applied = _overlay_decisive_badges_from_appkey(rows, decisive_map)

    assert applied == 0
    assert rows[0]["gol_vittoria"] == 2
    assert rows[0]["gol_pareggio"] == 0


def test_overlay_decisive_badges_skips_ambiguous_homonyms():
    rows = [
        {"team": "Inter", "player": "Rossi", "gol_vittoria": 0, "gol_pareggio": 0},
        {"team": "Milan", "player": "Rossi", "gol_vittoria": 0, "gol_pareggio": 0},
    ]
    decisive_map = {"rossi": {"gol_vittoria": 1, "gol_pareggio": 0}}

    applied = _overlay_decisive_badges_from_appkey(rows, decisive_map)

    assert applied == 0
    assert rows[0]["gol_vittoria"] == 0
    assert rows[1]["gol_vittoria"] == 0
