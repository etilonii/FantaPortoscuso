from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

from .credits import in_cost, out_value, resolve_team_credits
from .features import attach_features
from .io import (
    build_player_universe,
    merge_roster_with_universe,
    repo_root,
    roster_for_team,
)
from .report import write_market_advisor_reports
from .roles import ROLE_REPARTO
from .rules import list_market_windows, resolve_market_window, validate_roster
from .scoring import compute_scores
from .tiers import assign_tiers
from .transfers import plan_market_campaign


def _role_rankings(players: List[Dict[str, object]], top_n: int) -> List[Dict[str, object]]:
    grouped: Dict[str, List[Dict[str, object]]] = {}
    for player in players:
        role = str(player.get("mantra_role_best") or "")
        if not role:
            continue
        grouped.setdefault(role, []).append(player)

    out: List[Dict[str, object]] = []
    for role in sorted(grouped.keys()):
        ranked = sorted(
            grouped[role],
            key=lambda p: (
                float(p.get("MarketScoreFinal") or 0.0),
                float(p.get("CreditEfficiencyScore") or 0.0),
            ),
            reverse=True,
        )
        out.extend(ranked[: max(1, int(top_n))])
    return out


def _attach_reparto(rows: List[Dict[str, object]]) -> None:
    for row in rows:
        role = str(row.get("mantra_role_best") or "")
        row["reparto"] = ROLE_REPARTO.get(role, "")


def _public_player_view(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for row in rows:
        out.append(
            {
                "name": row.get("name", ""),
                "club": row.get("club", ""),
                "mantra_role_best": row.get("mantra_role_best", ""),
                "reparto": row.get("reparto", ROLE_REPARTO.get(str(row.get("mantra_role_best") or ""), "")),
                "prezzo_attuale": row.get("prezzo_attuale", row.get("prezzo_attuale_rosa", 0)),
                "prezzo_acquisto": row.get("prezzo_acquisto", 0),
                "RealScore": row.get("RealScore", 0),
                "PotentialScore": row.get("PotentialScore", 0),
                "CreditEfficiencyScore": row.get("CreditEfficiencyScore", 0),
                "MarketScoreFinal": row.get("MarketScoreFinal", 0),
                "Tier": row.get("Tier", ""),
            }
        )
    return out


def _prepare_in_candidates(
    *,
    all_players: List[Dict[str, object]],
    team_roster: List[Dict[str, object]],
    games_min: int,
    cost_source: str,
) -> List[Dict[str, object]]:
    roster_keys = {str(player.get("name_key") or "") for player in team_roster}
    candidates: List[Dict[str, object]] = []

    for player in all_players:
        if str(player.get("name_key") or "") in roster_keys:
            continue
        if not str(player.get("mantra_role_best") or ""):
            continue

        games = float((player.get("features") or {}).get("games") or 0.0)
        potential = float(player.get("PotentialScore") or 0.0)
        if games < float(games_min) and potential < 68.0:
            continue

        candidate = dict(player)
        candidate["in_cost"] = round(in_cost(candidate, source=cost_source), 2)
        candidates.append(candidate)

    return candidates


def _prepare_out_players(team_roster: List[Dict[str, object]], sell_valuation: str) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for player in team_roster:
        row = dict(player)
        row["out_value"] = round(out_value(row, mode=sell_valuation), 2)
        rows.append(row)
    return rows


def run_market_advisor(
    *,
    team_name: str,
    market_window: str,
    initial_budget: float = 250.0,
    games_min: int = 10,
    top_n: int = 25,
    outdir: str = "data/reports",
    snapshot: bool = False,
    credits_cost_source: str = "current",
    sell_valuation: str = "current",
    min_delta: float = 4.0,
    beam_width: int = 30,
    max_k: Optional[int] = None,
    out_candidate_limit: int = 20,
    in_candidate_limit: int = 40,
    top_plans: int = 3,
    enforce_initial_bands: bool = False,
) -> Dict[str, object]:
    team = str(team_name or "").strip()
    if not team:
        raise ValueError("--team-name obbligatorio")

    window = resolve_market_window(market_window)
    max_changes = int(window.max_changes)
    max_k_effective = max_changes if max_k is None else max(1, min(int(max_k), max_changes))

    all_players = build_player_universe()
    if not all_players:
        raise ValueError("Impossibile costruire universo giocatori (controlla data/quotazioni.csv)")

    attach_features(all_players, games_min=games_min)
    compute_scores(all_players, in_cost_source=credits_cost_source)
    assign_tiers(all_players, games_min=games_min)

    team_rows = roster_for_team(team)
    if not team_rows:
        raise ValueError(f"Team non trovato in data/rose_fantaportoscuso.csv: {team}")

    team_roster = merge_roster_with_universe(team_rows, all_players)
    attach_features(team_roster, games_min=games_min)
    compute_scores(team_roster, in_cost_source=credits_cost_source)
    assign_tiers(team_roster, games_min=games_min)
    _attach_reparto(team_roster)

    credits_residual, credits_source = resolve_team_credits(team, team_rows, initial_budget=initial_budget)

    roster_ok, roster_reasons, roster_details = validate_roster(
        team_roster,
        enforce_initial_bands=enforce_initial_bands,
        team_cap=3,
    )

    in_candidates = _prepare_in_candidates(
        all_players=all_players,
        team_roster=team_roster,
        games_min=games_min,
        cost_source=credits_cost_source,
    )
    out_players = _prepare_out_players(team_roster, sell_valuation=sell_valuation)

    plans_payload = plan_market_campaign(
        roster_players=out_players,
        in_candidates=in_candidates,
        credits_residual=float(credits_residual),
        max_changes=max_changes,
        max_k=max_k_effective,
        min_delta=float(min_delta),
        min_delta_multi=10.0,
        beam_width=int(beam_width),
        out_candidate_limit=int(out_candidate_limit),
        in_candidate_limit=int(in_candidate_limit),
        top_plans=int(top_plans),
        team_cap=3,
        enforce_initial_bands=enforce_initial_bands,
    )

    ranking_rows = _role_rankings(all_players, top_n=top_n)

    outdir_path = Path(outdir)
    if not outdir_path.is_absolute():
        outdir_path = repo_root() / outdir_path
    outdir_path.mkdir(parents=True, exist_ok=True)

    report_paths = write_market_advisor_reports(
        outdir=outdir_path,
        team_name=team,
        role_rankings=ranking_rows,
        squad_audit_rows=team_roster,
        plans=plans_payload.get("plans", []),
        snapshot=bool(snapshot),
    )

    best_plans = plans_payload.get("plans", [])

    return {
        "team": team,
        "window": {
            "key": window.key,
            "start": window.start.isoformat(),
            "end": window.end.isoformat(),
            "max_changes": max_changes,
            "available": list_market_windows(),
        },
        "credits": {
            "residual": round(float(credits_residual), 2),
            "source": credits_source,
            "credits_cost_source": credits_cost_source,
            "sell_valuation": sell_valuation,
        },
        "roster_audit": {
            "ok": roster_ok,
            "reasons": roster_reasons,
            "details": roster_details,
        },
        "squad_audit": _public_player_view(team_roster),
        "candidate_pool": {
            "universe_players": len(all_players),
            "in_candidates": len(in_candidates),
            "out_candidates": len(out_players),
        },
        "search": {
            "beam_width": int(beam_width),
            "max_k": max_k_effective,
            "max_changes": max_changes,
            "evaluated": int(plans_payload.get("evaluated", 0)),
            "notes": plans_payload.get("notes", []),
        },
        "best_plans": best_plans,
        "role_rankings": _public_player_view(ranking_rows),
        "role_rankings_count": len(ranking_rows),
        "reports": report_paths,
    }
