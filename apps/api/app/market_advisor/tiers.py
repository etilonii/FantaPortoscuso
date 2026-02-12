from __future__ import annotations

from typing import Dict, List, Mapping


TIER_ORDER = ["Top", "SemiTop", "Titolare", "Scommessa", "1 credito", "Scarti"]
TIER_VALUE = {
    "Top": 6.0,
    "SemiTop": 4.0,
    "Titolare": 2.0,
    "Scommessa": 1.0,
    "1 credito": 0.5,
    "Scarti": 0.0,
}

# Thresholds tuned by macro-role to keep tiers readable across role distributions.
ROLE_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "Por": {"top": 82.0, "semi": 70.0, "tit": 56.0, "sco": 44.0},
    "Dif": {"top": 84.0, "semi": 72.0, "tit": 58.0, "sco": 45.0},
    "Cen": {"top": 85.0, "semi": 73.0, "tit": 59.0, "sco": 46.0},
    "Att": {"top": 87.0, "semi": 75.0, "tit": 61.0, "sco": 48.0},
}


def _role_bucket(player: Mapping[str, object]) -> str:
    role = str(player.get("mantra_role_best") or "")
    if role == "Por":
        return "Por"
    if role in {"Dc", "Dd", "Ds", "B"}:
        return "Dif"
    if role in {"E", "M", "C", "T", "W"}:
        return "Cen"
    if role in {"A", "Pc"}:
        return "Att"
    return "Cen"


def assign_tier(
    player: Mapping[str, object],
    *,
    games_min: int = 10,
    score_key: str = "MarketScoreFinal",
) -> str:
    score = float(player.get(score_key) or 0.0)
    role_bucket = _role_bucket(player)
    th = ROLE_THRESHOLDS.get(role_bucket, ROLE_THRESHOLDS["Cen"])

    cost = float(player.get("prezzo_attuale") or 0.0)
    if cost <= 1.0:
        # Keep strict 1-credit bucket independent from score.
        return "1 credito"

    if score >= th["top"]:
        return "Top"
    if score >= th["semi"]:
        return "SemiTop"
    if score >= th["tit"]:
        return "Titolare"

    games = float((player.get("features") or {}).get("games") or 0.0)
    potential = float(player.get("PotentialScore") or 0.0)
    if score >= th["sco"] or (games < float(games_min) and potential >= 68.0):
        return "Scommessa"
    return "Scarti"


def assign_tiers(players: List[Dict[str, object]], games_min: int = 10) -> None:
    for player in players:
        player["Tier"] = assign_tier(player, games_min=games_min)


def tier_value(tier: str) -> float:
    return float(TIER_VALUE.get(str(tier or ""), 0.0))
