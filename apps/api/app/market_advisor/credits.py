from __future__ import annotations

from typing import Dict, List, Tuple

from apps.api.app.utils.names import normalize_name

from .io import credits_path, read_csv_rows, safe_float


def load_residual_credits_map() -> Dict[str, float]:
    rows = read_csv_rows(credits_path())
    out: Dict[str, float] = {}
    for row in rows:
        team = str(row.get("Team") or "").strip()
        if not team:
            continue
        out[normalize_name(team)] = safe_float(row.get("CreditiResidui"), 0.0)
    return out


def resolve_team_credits(
    team_name: str,
    roster_rows: List[Dict[str, object]],
    initial_budget: float = 250.0,
) -> Tuple[float, str]:
    team_key = normalize_name(team_name)
    credits_map = load_residual_credits_map()
    if team_key in credits_map:
        return float(credits_map[team_key]), "credits_file"
    paid_sum = sum(safe_float(row.get("PrezzoAcquisto"), 0.0) for row in roster_rows)
    return max(0.0, float(initial_budget) - paid_sum), "fallback_initial_budget"


def in_cost(player: Dict[str, object], source: str = "current") -> float:
    if str(source).lower() == "initial":
        return safe_float(player.get("prezzo_iniziale"), 0.0)
    return safe_float(player.get("prezzo_attuale"), 0.0)


def out_value(roster_player: Dict[str, object], mode: str = "current") -> float:
    if str(mode).lower() == "paid":
        return safe_float(roster_player.get("prezzo_acquisto"), 0.0)
    # Prefer rose PrezzoAttuale, fallback to quotazioni prezzo_attuale if missing.
    current = safe_float(roster_player.get("prezzo_attuale_rosa"), -1.0)
    if current >= 0:
        return current
    return safe_float(roster_player.get("prezzo_attuale"), 0.0)

