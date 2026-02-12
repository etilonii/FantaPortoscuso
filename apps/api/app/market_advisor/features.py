from __future__ import annotations

from typing import Dict, List

from .io import safe_float


def _sv(stats: Dict[str, object], key: str) -> float:
    return safe_float(stats.get(key), 0.0)


def extract_features(player: Dict[str, object], games_min: int = 10) -> Dict[str, float]:
    stats = dict(player.get("stats") or {})
    team_context = safe_float(player.get("team_context"), 50.0)
    games = max(0.0, _sv(stats, "Partite"))
    games_den = max(games, 1.0)

    gol = _sv(stats, "Gol")
    autogol = _sv(stats, "Autogol")
    rigori_parati = _sv(stats, "RigoriParati")
    rigori_segnati = _sv(stats, "RigoriSegnati")
    rigori_sbagliati = _sv(stats, "RigoriSbagliati")
    assist = _sv(stats, "Assist")
    amm = _sv(stats, "Ammonizioni")
    esp = _sv(stats, "Espulsioni")
    clean = _sv(stats, "Cleansheet")
    gol_v = _sv(stats, "GolVittoria")
    gol_p = _sv(stats, "GolPareggio")
    gol_subiti = _sv(stats, "GolSubiti")
    mv = _sv(stats, "Mediavoto")
    fm = _sv(stats, "Fantamedia")

    discipline_index = (amm * 0.5) + (esp * 1.0) + (autogol * 1.5) + (rigori_sbagliati * 1.0)
    decisive_index = (gol_v * 1.0) + (gol_p * 0.5)
    clean_pg = clean / games_den
    concede_pg = gol_subiti / games_den
    rigori_bonus_pg = rigori_segnati / games_den
    rigori_parati_pg = rigori_parati / games_den

    gol_pg = gol / games_den
    assist_pg = assist / games_den
    decisive_pg = decisive_index / games_den
    discipline_pg = discipline_index / games_den

    # Upside proxy: above-average FM over MV + FVM support.
    fvm = safe_float(player.get("fvm"), 0.0)
    prezzo_attuale = max(1.0, safe_float(player.get("prezzo_attuale"), 0.0))
    upside_signal = max(0.0, fm - mv) + max(0.0, (fvm - prezzo_attuale) / prezzo_attuale)

    # Stability proxy rewards volume.
    availability = min(1.0, games / max(1.0, float(games_min)))

    return {
        "games": games,
        "mv": mv,
        "fm": fm,
        "gol_pg": gol_pg,
        "assist_pg": assist_pg,
        "discipline_index": discipline_index,
        "discipline_pg": discipline_pg,
        "decisive_index": decisive_index,
        "decisive_pg": decisive_pg,
        "clean_pg": clean_pg,
        "concede_pg": concede_pg,
        "rigori_bonus_pg": rigori_bonus_pg,
        "rigori_parati_pg": rigori_parati_pg,
        "upside_signal": upside_signal,
        "availability": availability,
        "team_context": max(0.0, min(100.0, team_context)),
    }


def attach_features(players: List[Dict[str, object]], games_min: int = 10) -> None:
    for player in players:
        player["features"] = extract_features(player, games_min=games_min)
