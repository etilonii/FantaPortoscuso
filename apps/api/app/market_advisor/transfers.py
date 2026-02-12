from __future__ import annotations

from dataclasses import dataclass
from itertools import islice
from typing import Dict, Iterable, List, Mapping, Optional, Sequence

from apps.api.app.utils.names import normalize_name

from .roles import ROLE_REPARTO
from .rules import deficits_after_out, roster_after_swap, validate_roster
from .tiers import tier_value


@dataclass
class _BeamState:
    start_index: int
    selected: List[Dict[str, object]]
    score: float


@dataclass
class _InBeamState:
    start_index: int
    selected: List[Dict[str, object]]
    score: float
    spent: float
    remaining_deficits: Dict[str, int]
    club_counts: Dict[str, int]


def _player_uid(player: Mapping[str, object]) -> str:
    key = str(player.get("name_key") or "").strip()
    club = normalize_name(str(player.get("club") or "").strip())
    return f"{key}|{club}"


def _macro_role(player: Mapping[str, object]) -> str:
    role = str(player.get("mantra_role_best") or "").strip()
    return str(ROLE_REPARTO.get(role) or "")


def _sell_score(player: Mapping[str, object]) -> float:
    market = float(player.get("MarketScoreFinal") or 0.0)
    discipline = float((player.get("features") or {}).get("discipline_pg") or 0.0)
    availability = float((player.get("features") or {}).get("availability") or 0.0)
    return (100.0 - market) + (discipline * 15.0) + max(0.0, (0.6 - availability) * 20.0)


def _out_value(player: Mapping[str, object]) -> float:
    return float(player.get("out_value") or 0.0)


def _in_cost(player: Mapping[str, object]) -> float:
    return float(player.get("in_cost") or 0.0)


def _tier_gain(in_players: Sequence[Mapping[str, object]], out_players: Sequence[Mapping[str, object]]) -> float:
    return sum(tier_value(str(p.get("Tier") or "")) for p in in_players) - sum(
        tier_value(str(p.get("Tier") or "")) for p in out_players
    )


def _discipline_penalty(in_players: Sequence[Mapping[str, object]], out_players: Sequence[Mapping[str, object]]) -> float:
    in_disc = sum(float((p.get("features") or {}).get("discipline_pg") or 0.0) for p in in_players)
    out_disc = sum(float((p.get("features") or {}).get("discipline_pg") or 0.0) for p in out_players)
    return max(0.0, in_disc - out_disc) * 8.0


def _package_gain(in_players: Sequence[Mapping[str, object]], out_players: Sequence[Mapping[str, object]]) -> float:
    base = sum(float(p.get("MarketScoreFinal") or 0.0) for p in in_players) - sum(
        float(p.get("MarketScoreFinal") or 0.0) for p in out_players
    )
    return base + (2.0 * _tier_gain(in_players, out_players)) - _discipline_penalty(in_players, out_players)


def _top_unique_by_uid(players: Iterable[Mapping[str, object]], limit: int) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    seen: set[str] = set()
    for raw in players:
        player = dict(raw)
        uid = _player_uid(player)
        if not uid or uid in seen:
            continue
        seen.add(uid)
        out.append(player)
        if len(out) >= max(1, int(limit)):
            break
    return out


def _beam_out_packages(out_candidates: List[Dict[str, object]], k: int, beam_width: int) -> List[List[Dict[str, object]]]:
    states: List[_BeamState] = [_BeamState(start_index=0, selected=[], score=0.0)]
    for _ in range(k):
        expanded: List[_BeamState] = []
        for state in states:
            for idx in range(state.start_index, len(out_candidates)):
                candidate = out_candidates[idx]
                expanded.append(
                    _BeamState(
                        start_index=idx + 1,
                        selected=[*state.selected, candidate],
                        score=state.score + _sell_score(candidate),
                    )
                )
        expanded.sort(key=lambda s: s.score, reverse=True)
        states = list(islice(expanded, max(1, beam_width)))
        if not states:
            break
    return [s.selected for s in states if len(s.selected) == k]


def _beam_in_packages(
    in_candidates: List[Dict[str, object]],
    *,
    k: int,
    deficits: Dict[str, int],
    budget: float,
    club_counts_seed: Dict[str, int],
    team_cap: int,
    beam_width: int,
) -> List[List[Dict[str, object]]]:
    states: List[_InBeamState] = [
        _InBeamState(
            start_index=0,
            selected=[],
            score=0.0,
            spent=0.0,
            remaining_deficits=dict(deficits),
            club_counts=dict(club_counts_seed),
        )
    ]

    target_fill = sum(max(0, int(v)) for v in deficits.values())
    force_exact_deficits = target_fill == k

    for _ in range(k):
        expanded: List[_InBeamState] = []
        for state in states:
            for idx in range(state.start_index, len(in_candidates)):
                candidate = in_candidates[idx]
                reparto = _macro_role(candidate)
                if not reparto:
                    continue

                if force_exact_deficits and int(state.remaining_deficits.get(reparto, 0)) <= 0:
                    continue

                cost = _in_cost(candidate)
                next_spent = state.spent + cost
                if next_spent - budget > 1e-9:
                    continue

                club = str(candidate.get("club") or "").strip()
                next_club_counts = dict(state.club_counts)
                if club:
                    current = int(next_club_counts.get(club, 0))
                    if current + 1 > team_cap:
                        continue
                    next_club_counts[club] = current + 1

                next_deficits = dict(state.remaining_deficits)
                if reparto in next_deficits:
                    next_deficits[reparto] = int(next_deficits.get(reparto, 0)) - 1

                score = state.score + float(candidate.get("MarketScoreFinal") or 0.0)
                # Tiny cost regularizer to prefer efficient packages at equal score.
                score -= cost * 0.02

                expanded.append(
                    _InBeamState(
                        start_index=idx + 1,
                        selected=[*state.selected, candidate],
                        score=score,
                        spent=next_spent,
                        remaining_deficits=next_deficits,
                        club_counts=next_club_counts,
                    )
                )

        expanded.sort(key=lambda s: s.score, reverse=True)
        states = list(islice(expanded, max(1, beam_width)))
        if not states:
            break

    out: List[List[Dict[str, object]]] = []
    for state in states:
        if len(state.selected) != k:
            continue
        if force_exact_deficits and any(int(v) != 0 for v in state.remaining_deficits.values()):
            continue
        out.append(state.selected)
    return out


def _plan_dedup_key(plan: Mapping[str, object]) -> str:
    out_names = sorted(str(x) for x in plan.get("out_players", []))
    in_names = sorted(str(x) for x in plan.get("in_players", []))
    return f"OUT:{'|'.join(out_names)}::IN:{'|'.join(in_names)}"


def plan_market_campaign(
    *,
    roster_players: List[Dict[str, object]],
    in_candidates: List[Dict[str, object]],
    credits_residual: float,
    max_changes: int,
    max_k: Optional[int] = None,
    min_delta: float = 4.0,
    min_delta_multi: float = 10.0,
    beam_width: int = 30,
    out_candidate_limit: int = 20,
    in_candidate_limit: int = 40,
    top_plans: int = 3,
    team_cap: int = 3,
    enforce_initial_bands: bool = False,
) -> Dict[str, object]:
    if not roster_players:
        return {"plans": [], "evaluated": 0, "notes": ["Rosa vuota"]}

    k_limit = int(max_k) if max_k is not None else int(max_changes)
    k_limit = max(1, min(int(max_changes), k_limit))

    out_ranked = sorted(roster_players, key=_sell_score, reverse=True)
    out_ranked = _top_unique_by_uid(out_ranked, out_candidate_limit)

    in_ranked = sorted(in_candidates, key=lambda p: float(p.get("MarketScoreFinal") or 0.0), reverse=True)
    in_ranked = _top_unique_by_uid(in_ranked, in_candidate_limit)

    all_plans: List[Dict[str, object]] = []
    evaluated = 0

    for k in range(1, k_limit + 1):
        out_packages = _beam_out_packages(out_ranked, k=k, beam_width=max(1, beam_width))
        if not out_packages:
            continue

        for out_pkg in out_packages:
            out_uids = {_player_uid(p) for p in out_pkg}
            out_keys = {str(p.get("name_key") or "") for p in out_pkg}
            roster_after_out = [p for p in roster_players if _player_uid(p) not in out_uids]

            deficits = deficits_after_out(roster_after_out)
            credits_out_total = sum(_out_value(p) for p in out_pkg)
            credits_before = float(credits_residual)
            credits_budget = credits_before + credits_out_total

            club_seed: Dict[str, int] = {}
            for p in roster_after_out:
                club = str(p.get("club") or "").strip()
                if not club:
                    continue
                club_seed[club] = club_seed.get(club, 0) + 1

            current_roster_uids = {_player_uid(p) for p in roster_after_out}
            filtered_in = [p for p in in_ranked if _player_uid(p) not in current_roster_uids]
            if not filtered_in:
                continue

            in_packages = _beam_in_packages(
                filtered_in,
                k=k,
                deficits=deficits,
                budget=credits_budget,
                club_counts_seed=club_seed,
                team_cap=team_cap,
                beam_width=max(1, beam_width),
            )

            for in_pkg in in_packages:
                evaluated += 1
                credits_in_total = sum(_in_cost(p) for p in in_pkg)
                credits_after = credits_budget - credits_in_total
                if credits_after < -1e-9:
                    continue

                final_roster = roster_after_swap(roster_players, out_keys=out_keys, in_players=in_pkg)
                valid, reasons, _ = validate_roster(
                    final_roster,
                    enforce_initial_bands=enforce_initial_bands,
                    team_cap=team_cap,
                )
                if not valid:
                    continue

                gain = _package_gain(in_pkg, out_pkg)
                threshold = float(min_delta) if k == 1 else float(min_delta_multi)
                if gain < threshold:
                    continue

                notes = [
                    "vincoli reparto ok",
                    "team-cap ok",
                    "budget ok",
                ]
                if reasons:
                    notes.extend(reasons)

                all_plans.append(
                    {
                        "plan_id": "",
                        "step": 1,
                        "k": k,
                        "out_players": [str(p.get("name") or "") for p in out_pkg],
                        "in_players": [str(p.get("name") or "") for p in in_pkg],
                        "credits_out_total": round(credits_out_total, 2),
                        "credits_in_total": round(credits_in_total, 2),
                        "credits_residual_before": round(credits_before, 2),
                        "credits_residual_after": round(credits_after, 2),
                        "package_gain": round(gain, 2),
                        "notes": "; ".join(notes),
                    }
                )

    unique_plans: List[Dict[str, object]] = []
    seen: set[str] = set()
    for plan in sorted(
        all_plans,
        key=lambda p: (
            float(p.get("package_gain") or 0.0),
            float(p.get("credits_residual_after") or 0.0),
        ),
        reverse=True,
    ):
        key = _plan_dedup_key(plan)
        if key in seen:
            continue
        seen.add(key)
        unique_plans.append(plan)

    selected = unique_plans[: max(1, int(top_plans))]
    for idx, plan in enumerate(selected, start=1):
        plan["plan_id"] = f"P{idx}"

    return {
        "plans": selected,
        "evaluated": evaluated,
        "notes": [] if selected else ["Nessun piano valido trovato con i vincoli correnti"],
        "candidate_out_considered": len(out_ranked),
        "candidate_in_considered": len(in_ranked),
    }
