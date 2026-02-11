from apps.api.app.routes.data import (
    _default_regulation,
    _infer_decisive_events_from_fantavote,
    _reg_bonus_map,
)


def _base_events(**overrides):
    events = {
        "goal": 0,
        "assist": 0,
        "assist_da_fermo": 0,
        "rigore_segnato": 0,
        "rigore_parato": 0,
        "rigore_sbagliato": 0,
        "autogol": 0,
        "gol_subito_portiere": 0,
        "ammonizione": 0,
        "espulsione": 0,
        "gol_vittoria": 0,
        "gol_pareggio": 0,
    }
    events.update(overrides)
    return events


def test_infers_gol_vittoria_from_fantavote_delta():
    bonus_map = _reg_bonus_map(_default_regulation())
    events = _base_events(goal=2)

    inferred = _infer_decisive_events_from_fantavote(
        vote_value=7.5,
        fantavote_value=14.5,
        event_counts=events,
        bonus_map=bonus_map,
    )

    assert inferred["gol_vittoria"] == 1
    assert inferred["gol_pareggio"] == 0


def test_infers_gol_pareggio_when_delta_is_half_point():
    bonus_map = _reg_bonus_map(_default_regulation())
    events = _base_events(goal=1)

    inferred = _infer_decisive_events_from_fantavote(
        vote_value=6.0,
        fantavote_value=9.5,
        event_counts=events,
        bonus_map=bonus_map,
    )

    assert inferred["gol_vittoria"] == 0
    assert inferred["gol_pareggio"] == 1


def test_keeps_decisive_zero_when_fantavote_already_matches_other_events():
    bonus_map = _reg_bonus_map(_default_regulation())
    events = _base_events(assist=3, ammonizione=1)

    inferred = _infer_decisive_events_from_fantavote(
        vote_value=8.5,
        fantavote_value=11.0,
        event_counts=events,
        bonus_map=bonus_map,
    )

    assert inferred["gol_vittoria"] == 0
    assert inferred["gol_pareggio"] == 0


def test_does_not_override_explicit_decisive_input():
    bonus_map = _reg_bonus_map(_default_regulation())
    events = _base_events(goal=1, gol_vittoria=1)

    inferred = _infer_decisive_events_from_fantavote(
        vote_value=7.0,
        fantavote_value=11.0,
        event_counts=events,
        bonus_map=bonus_map,
    )

    assert inferred["gol_vittoria"] == 1
    assert inferred["gol_pareggio"] == 0
