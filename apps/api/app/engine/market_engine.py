from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Tuple
import re
import math
import random


def clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def num(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def safe_div(num_value: float, den_value: float, default: float = 0.0) -> float:
    if den_value == 0:
        return default
    return num_value / den_value


def normalize_map(values: Dict[str, float]) -> Dict[str, float]:
    if not values:
        return {}
    min_v = min(values.values())
    max_v = max(values.values())
    if max_v == min_v:
        return {k: 0.5 for k in values}
    return {k: (v - min_v) / (max_v - min_v) for k, v in values.items()}


def rate_recent(value_r8: float, pv_r8: float, eps: float) -> float:
    return safe_div(value_r8, pv_r8 + eps, 0.0)


def rate_season(value_s: float, pv_s: float, eps: float) -> float:
    return safe_div(value_s, pv_s + eps, 0.0)


def titolarita(player: dict, team_players: Iterable[dict], eps: float = 0.5, k: float = 8.0, w: float = 0.70) -> float:
    # Recent
    pt_r8 = num(player.get("PT_R8"))
    pv_r8 = num(player.get("PV_R8"))
    min_r8 = num(player.get("MIN_R8"))
    start_rate_r = safe_div(pt_r8, pv_r8 + eps, 0.0)
    mpa_r = safe_div(min_r8, pv_r8 + eps, 0.0)
    min_rate_r = clamp(mpa_r / 90.0, 0.0, 1.0)
    tit_base_r = 0.65 * start_rate_r + 0.35 * min_rate_r
    vote_rate_r = clamp(safe_div(pv_r8, 8.0, 0.0), 0.0, 1.0)
    tit_r = 0.75 * tit_base_r + 0.25 * vote_rate_r

    # Competition recent
    role = str(player.get("ruolo_base", "")).upper()
    club = str(player.get("club", "")).strip().lower()
    conc_min = 0.0
    for other in team_players:
        if str(other.get("club", "")).strip().lower() != club:
            continue
        if str(other.get("ruolo_base", "")).upper() != role:
            continue
        conc_min += num(other.get("MIN_R8"))
    share_r = safe_div(min_r8, conc_min + 1.0, 0.0)
    comp_r = sigmoid(k * (share_r - 0.5))
    tit_final_r = 0.70 * tit_r + 0.30 * comp_r

    # Season
    pt_s = num(player.get("PT_S"))
    pv_s = num(player.get("PV_S"))
    min_s = num(player.get("MIN_S"))
    start_rate_s = safe_div(pt_s, pv_s + eps, 0.0)
    mpa_s = safe_div(min_s, pv_s + eps, 0.0)
    min_rate_s = clamp(mpa_s / 90.0, 0.0, 1.0)
    tit_base_s = 0.65 * start_rate_s + 0.35 * min_rate_s
    vote_rate_s = clamp(safe_div(pv_s, pv_s + 5.0, 0.0), 0.0, 1.0)
    tit_s = 0.75 * tit_base_s + 0.25 * vote_rate_s

    conc_min_s = 0.0
    for other in team_players:
        if str(other.get("club", "")).strip().lower() != club:
            continue
        if str(other.get("ruolo_base", "")).upper() != role:
            continue
        conc_min_s += num(other.get("MIN_S"))
    share_s = safe_div(min_s, conc_min_s + 1.0, 0.0)
    comp_s = sigmoid(k * (share_s - 0.5))
    tit_final_s = 0.70 * tit_s + 0.30 * comp_s

    return w * tit_final_r + (1.0 - w) * tit_final_s


def pen_tit(t: float, t0: float = 0.55, tmin: float = 0.25, gamma: float = 1.5) -> float:
    if t >= t0:
        return 1.0
    if t <= tmin:
        return 0.0
    return ((t - tmin) / (t0 - tmin)) ** gamma


def efp_player(player: dict, eps: float = 0.5, theta: float = 0.35) -> float:
    role = str(player.get("ruolo_base", "")).upper()

    pv_r8 = num(player.get("PV_R8"))
    g_r8 = num(player.get("G_R8"))
    a_r8 = num(player.get("A_R8"))
    xg_r8 = num(player.get("xG_R8"))
    xa_r8 = num(player.get("xA_R8"))
    amm_r8 = num(player.get("AMM_R8"))
    esp_r8 = num(player.get("ESP_R8"))
    aut_r8 = num(player.get("AUTOGOL_R8"))
    rigseg_r8 = num(player.get("RIGSEG_R8"))

    pk_role = num(player.get("PKRole", 0))

    rate_g = rate_recent(g_r8, pv_r8, eps)
    rate_a = rate_recent(a_r8, pv_r8, eps)
    rate_xg = rate_recent(xg_r8, pv_r8, eps)
    rate_xa = rate_recent(xa_r8, pv_r8, eps)
    rate_amm = rate_recent(amm_r8, pv_r8, eps)
    rate_esp = rate_recent(esp_r8, pv_r8, eps)
    rate_aut = rate_recent(aut_r8, pv_r8, eps)
    rate_rigseg = rate_recent(rigseg_r8, pv_r8, eps)

    if role == "P":
        gols_r8 = num(player.get("GOLS_R8"))
        rigpar_r8 = num(player.get("RIGPAR_R8"))
        cs_r8 = num(player.get("CS_R8"))
        rate_gols = rate_recent(gols_r8, pv_r8, eps)
        rate_rigpar = rate_recent(rigpar_r8, pv_r8, eps)
        rate_cs = rate_recent(cs_r8, pv_r8, eps)
        return (-1.0) * rate_gols + 3.0 * rate_rigpar + theta * rate_cs

    if role == "A":
        lambda_role = 0.55
        mu_role = 0.45
        malus_mult = 1.00
    elif role == "C":
        lambda_role = 0.45
        mu_role = 0.55
        malus_mult = 1.05
    else:  # D
        lambda_role = 0.35
        mu_role = 0.40
        malus_mult = 1.10

    g_eff = lambda_role * rate_g + (1.0 - lambda_role) * rate_xg
    a_eff = mu_role * rate_a + (1.0 - mu_role) * rate_xa
    pts_gol = 3.0 * g_eff
    pts_assist = 1.0 * a_eff
    pts_rig = 3.0 * rate_rigseg * (0.6 + 0.4 * pk_role)
    pts_malus = (-0.5) * rate_amm + (-1.0) * rate_esp + (-2.0) * rate_aut
    return pts_gol + pts_assist + pts_rig + malus_mult * pts_malus


def team_context(team: dict, role: str, team_strength: float, team_momentum: float) -> float:
    role = role.upper()
    coach_style = num(team.get(f"CoachStyle_{role}", team.get("CoachStyle", 0.5)))
    coach_stability = num(team.get("CoachStability", 0.5))
    coach_boost = num(team.get("CoachBoost", 0.5))
    coach_factor = 0.55 * coach_style + 0.35 * coach_stability + 0.10 * coach_boost

    if role == "A":
        w1, w2, w3 = 0.22, 0.18, 0.10
    elif role == "C":
        w1, w2, w3 = 0.16, 0.14, 0.12
    elif role == "D":
        w1, w2, w3 = 0.10, 0.12, 0.10
    else:  # P
        w1, w2, w3 = 0.06, 0.10, 0.06

    return 1.0 + w1 * (team_strength - 0.5) + w2 * (team_momentum - 0.5) + w3 * (
        coach_factor - 0.5
    )


def compute_team_strengths(teams: Dict[str, dict]) -> Dict[str, float]:
    attack = {k: num(v.get("GFpg_S")) for k, v in teams.items()}
    defense = {k: -num(v.get("GApg_S")) for k, v in teams.items()}
    table = {k: num(v.get("PPG_S")) for k, v in teams.items()}
    attack_n = normalize_map(attack)
    defense_n = normalize_map(defense)
    table_n = normalize_map(table)
    return {
        k: 0.45 * attack_n.get(k, 0.5) + 0.35 * table_n.get(k, 0.5) + 0.20 * defense_n.get(k, 0.5)
        for k in teams
    }


def compute_team_momentum(teams: Dict[str, dict]) -> Dict[str, float]:
    trend_pts = {}
    trend_gf = {}
    trend_ga = {}
    for k, v in teams.items():
        ppg_s = num(v.get("PPG_S"))
        ppg_r8 = num(v.get("PPG_R8"))
        gf_s = num(v.get("GFpg_S"))
        gf_r8 = num(v.get("GFpg_R8"))
        ga_s = num(v.get("GApg_S"))
        ga_r8 = num(v.get("GApg_R8"))
        trend_pts[k] = clamp(0.5 + ((ppg_r8 - ppg_s) / 0.8) * 0.5, 0.0, 1.0)
        trend_gf[k] = clamp(0.5 + ((gf_r8 - gf_s) / 0.6) * 0.5, 0.0, 1.0)
        trend_ga[k] = clamp(0.5 + ((ga_s - ga_r8) / 0.6) * 0.5, 0.0, 1.0)
    team_form = {
        k: 0.50 * trend_pts[k] + 0.30 * trend_gf[k] + 0.20 * trend_ga[k] for k in teams
    }
    return {
        k: 0.55 * team_form[k] + 0.45 * num(teams[k].get("MoodTeam", 0.5)) for k in teams
    }


def compute_sos(teams: Dict[str, dict], fixtures: List[dict], current_round: int) -> Dict[str, float]:
    strengths = compute_team_strengths(teams)
    opp_means = {}
    for team in teams:
        opps = [
            f.get("opponent")
            for f in fixtures
            if f.get("team") == team and int(f.get("round", 0)) >= current_round
        ]
        if opps:
            opp_means[team] = sum(strengths.get(o, 0.5) for o in opps) / len(opps)
        else:
            opp_means[team] = 0.5
    sos_idx = normalize_map(opp_means)
    return sos_idx


def sos_role_multiplier(role: str, sos_idx: float) -> float:
    role = role.upper()
    if role == "A":
        s_role = 0.22
    elif role == "C":
        s_role = 0.14
    elif role == "D":
        s_role = 0.16
    else:
        s_role = 0.18
    return 1.0 + s_role * (0.5 - sos_idx)


def games_remaining(teams: Dict[str, dict], fixtures: List[dict], current_round: int) -> Dict[str, int]:
    remaining = {k: 0 for k in teams}
    for f in fixtures:
        team = f.get("team")
        if team not in remaining:
            continue
        if int(f.get("round", 0)) >= current_round:
            remaining[team] += 1
    # fallback if no fixtures: use team-provided value
    for k, v in teams.items():
        if remaining[k] == 0:
            remaining[k] = int(v.get("GamesRemaining", 0) or 0)
    return remaining


def value_season(player: dict, players: Iterable[dict], teams: Dict[str, dict], fixtures: List[dict], current_round: int) -> float:
    club = str(player.get("club", "")).strip()
    role = str(player.get("ruolo_base", "")).upper()
    if club not in teams:
        return 0.0
    strengths = compute_team_strengths(teams)
    momentum = compute_team_momentum(teams)
    sos_idx = compute_sos(teams, fixtures, current_round)
    games_left = games_remaining(teams, fixtures, current_round)

    tit = titolarita(player, players)
    pen = pen_tit(tit)
    efp = efp_player(player)
    ctx = team_context(teams[club], role, strengths.get(club, 0.5), momentum.get(club, 0.5))
    sos_mult = sos_role_multiplier(role, sos_idx.get(club, 0.5))
    efp_star = efp * ctx * sos_mult
    return games_left.get(club, 0) * tit * pen * efp_star


@dataclass
class Swap:
    out_player: dict
    in_player: dict
    gain: float
    qa_out: float
    qa_in: float

    @property
    def cost_net(self) -> float:
        return self.qa_in - self.qa_out


@dataclass
class Solution:
    swaps: List[Swap]
    budget_initial: float
    budget_final: float
    total_gain: float
    recommended_outs: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


def suggest_transfers(
    user_squad: List[dict],
    credits_residui: float,
    players_pool: List[dict],
    teams_data: Dict[str, dict],
    fixtures: List[dict],
    current_round: int,
    max_changes: int = 5,
    k_pool: int = 60,
    m_out: int = 8,
    beam_width: int = 200,
    seed: int | None = None,
    allow_overbudget: bool = False,
    max_negative_gain: float = -2.0,
    max_negative_swaps: int = 2,
    max_negative_sum: float = 3.0,
    require_roles: set[str] | None = None,
) -> List[Solution]:
    rng = random.Random(seed) if seed is not None else None
    def is_star(name: str) -> bool:
        return str(name or "").strip().endswith(" *")

    def norm_name(name: str) -> str:
        value = str(name or "").lower()
        value = re.sub(r"[^a-z0-9]+", "", value)
        return value

    top_tokens = {
        "calhanoglu",
        "pulisic",
        "mandragora",
        "dimarco",
        "maignan",
        "svilar",
        "martinezl",
        "nicopaz",
        "pazn",
    }

    def is_top_absolute(name: str) -> bool:
        key = norm_name(name)
        return any(token in key for token in top_tokens)

    squad_names = {p.get("nome") or p.get("Giocatore") for p in user_squad}
    squad_names = {n for n in squad_names if n}

    recommended_outs = [p for p in user_squad if is_star(p.get("nome") or p.get("Giocatore"))]
    recommended_out_names = {p.get("nome") or p.get("Giocatore") for p in recommended_outs}
    recommended_out_names = {n for n in recommended_out_names if n}

    # Precompute ValueSeason and custom logic scores
    value_raw = {}
    bonus_raw = {}
    vote_raw = {}
    cheap_raw = {}
    defmod_raw = {}
    tit_map = {}
    for p in players_pool:
        name = p.get("nome") or p.get("Giocatore")
        if not name:
            continue
        value_raw[name] = value_season(p, players_pool, teams_data, fixtures, current_round)
        bonus_raw[name] = efp_player(p)
        vote_raw[name] = num(p.get("PV_S"))
        qa = num(p.get("QA", p.get("PrezzoAttuale", 0)))
        cheap_raw[name] = safe_div(1.0, qa + 1.0, 0.0)
        if str(p.get("ruolo_base", "")).upper() == "D":
            cs_s = num(p.get("CS_S"))
            pv_s = num(p.get("PV_S"))
            defmod_raw[name] = safe_div(cs_s, pv_s + 0.5, 0.0)
        else:
            defmod_raw[name] = 0.0
        tit_map[name] = titolarita(p, players_pool)

    value_norm = normalize_map(value_raw)
    bonus_norm = normalize_map(bonus_raw)
    vote_norm = normalize_map(vote_raw)
    cheap_norm = normalize_map(cheap_raw)
    defmod_norm = normalize_map(defmod_raw)

    value_map = {}
    for p in players_pool:
        name = p.get("nome") or p.get("Giocatore")
        if not name:
            continue
        role = str(p.get("ruolo_base", "")).upper()
        base_score = (
            0.45 * value_norm.get(name, 0.5)
            + 0.25 * bonus_norm.get(name, 0.5)
            + 0.15 * vote_norm.get(name, 0.5)
            + 0.10 * cheap_norm.get(name, 0.5)
            + 0.05 * defmod_norm.get(name, 0.5)
        )
        if role == "D":
            base_score += 0.03 * defmod_norm.get(name, 0.5)
        if is_top_absolute(name):
            base_score += 0.08
        if cheap_norm.get(name, 0.0) > 0.75 and bonus_norm.get(name, 0.0) > 0.60:
            base_score += 0.06
        if rng:
            base_score += rng.uniform(-0.02, 0.02)
        value_map[name] = base_score

    # Build pools per role
    in_pool = {r: [] for r in ["P", "D", "C", "A"]}
    out_pool = {r: [] for r in ["P", "D", "C", "A"]}

    for p in players_pool:
        name = p.get("nome") or p.get("Giocatore")
        role = str(p.get("ruolo_base", p.get("Ruolo"))).upper()
        if role not in in_pool:
            continue
        if name in squad_names:
            continue
        if is_star(name):
            continue
        in_pool[role].append(p)

    for p in user_squad:
        name = p.get("nome") or p.get("Giocatore")
        role = str(p.get("ruolo_base", p.get("Ruolo"))).upper()
        if role not in out_pool:
            continue
        out_pool[role].append(p)

    def top_k_by_value(players: List[dict]) -> List[dict]:
        return sorted(players, key=lambda x: value_map.get(x.get("nome") or x.get("Giocatore"), 0), reverse=True)[:k_pool]

    def top_k_by_eff(players: List[dict]) -> List[dict]:
        def eff(p):
            name = p.get("nome") or p.get("Giocatore")
            qa = num(p.get("QA", p.get("PrezzoAttuale", 0)))
            return safe_div(value_map.get(name, 0), qa, 0.0)
        return sorted(players, key=eff, reverse=True)[:k_pool]

    for role in in_pool:
        combined = {p.get("id") or (p.get("nome") or p.get("Giocatore")): p for p in top_k_by_value(in_pool[role])}
        for p in top_k_by_eff(in_pool[role]):
            key = p.get("id") or (p.get("nome") or p.get("Giocatore"))
            combined[key] = p
        in_pool[role] = list(combined.values())

    for role in out_pool:
        out_pool[role] = sorted(
            out_pool[role],
            key=lambda x: value_map.get(x.get("nome") or x.get("Giocatore"), 0),
        )[:m_out]

    # Mandatory swaps for players with asterisk
    star_outs = []
    for p in user_squad:
        name = p.get("nome") or p.get("Giocatore")
        if is_star(name):
            star_outs.append(p)

    max_total_changes = 7 if star_outs else 5
    max_changes = min(max_changes, max_total_changes - len(star_outs))
    if max_changes < 0:
        max_changes = 0

    mandatory_swaps: List[Swap] = []
    used_in = set()
    used_out = set()
    for out_p in star_outs:
        name_out = out_p.get("nome") or out_p.get("Giocatore")
        role = str(out_p.get("ruolo_base", out_p.get("Ruolo"))).upper()
        candidates = [
            p for p in in_pool.get(role, [])
            if (p.get("nome") or p.get("Giocatore")) not in squad_names
        ]
        if not candidates:
            continue
        candidates.sort(
            key=lambda x: value_map.get(x.get("nome") or x.get("Giocatore"), 0),
            reverse=True,
        )
        best_in = candidates[0]
        name_in = best_in.get("nome") or best_in.get("Giocatore")
        if not name_in or name_in in used_in or name_out in used_out:
            continue
        qa_out = num(out_p.get("QA", out_p.get("PrezzoAttuale", 0)))
        qa_in = num(best_in.get("QA", best_in.get("PrezzoAttuale", 0)))
        gain = value_map.get(name_in, 0.0) - value_map.get(name_out, 0.0)
        mandatory_swaps.append(Swap(out_p, best_in, gain, qa_out, qa_in))
        used_in.add(name_in)
        used_out.add(name_out)
        in_pool[role] = [p for p in in_pool.get(role, []) if (p.get("nome") or p.get("Giocatore")) != name_in]
        out_pool[role] = [p for p in out_pool.get(role, []) if (p.get("nome") or p.get("Giocatore")) != name_out]

    # Candidate swaps
    GK_MIN_TIT = 0.65
    GK_MIN_GAIN = 4.0
    candidates: List[Swap] = []
    for role in ["P", "D", "C", "A"]:
        for out_p in out_pool[role]:
            name_out = out_p.get("nome") or out_p.get("Giocatore")
            qa_out = num(out_p.get("QA", out_p.get("PrezzoAttuale", 0)))
            val_out = value_map.get(name_out, 0.0)
            for in_p in in_pool[role]:
                name_in = in_p.get("nome") or in_p.get("Giocatore")
                if not name_in or name_in in squad_names:
                    continue
                qa_in = num(in_p.get("QA", in_p.get("PrezzoAttuale", 0)))
                val_in = value_map.get(name_in, 0.0)
                gain = val_in - val_out
                if role == "P":
                    tit_in = tit_map.get(name_in, 0.0)
                    if tit_in < GK_MIN_TIT or gain < GK_MIN_GAIN:
                        continue
                candidates.append(Swap(out_p, in_p, gain, qa_out, qa_in))

    # Beam search builder (supports excluding specific swap pairs)
    @dataclass
    class State:
        swaps: List[Swap]
        out_set: set
        in_set: set
        spent: float
        earned: float
        gain_total: float
        neg_count: int
        neg_sum: float
        best_gain: float

    base_spent = sum(s.qa_in for s in mandatory_swaps)
    base_earned = sum(s.qa_out for s in mandatory_swaps)
    base_gain = sum(s.gain for s in mandatory_swaps)
    base_out = {s.out_player.get("nome") or s.out_player.get("Giocatore") for s in mandatory_swaps}
    base_in = {s.in_player.get("nome") or s.in_player.get("Giocatore") for s in mandatory_swaps}
    base_neg = sum(1 for s in mandatory_swaps if s.gain < 0)
    base_neg_sum = sum(abs(s.gain) for s in mandatory_swaps if s.gain < 0)
    base_best = max([s.gain for s in mandatory_swaps], default=0.0)

    def swap_key(swap: Swap) -> tuple[str, str]:
        return (
            norm_name(swap.out_player.get("nome") or swap.out_player.get("Giocatore") or ""),
            norm_name(swap.in_player.get("nome") or swap.in_player.get("Giocatore") or ""),
        )

    def build_solutions(
        exclude_swaps: set[tuple[str, str]] | None,
        exclude_outs: set[str] | None,
    ) -> List[Solution]:
        exclude_swaps = exclude_swaps or set()
        exclude_outs = exclude_outs or set()
        filtered = []
        for c in candidates:
            out_name = norm_name(c.out_player.get("nome") or c.out_player.get("Giocatore") or "")
            if out_name in exclude_outs:
                continue
            if swap_key(c) in exclude_swaps:
                continue
            filtered.append(c)

        beam = [
            State(
                swaps=list(mandatory_swaps),
                out_set=set(base_out),
                in_set=set(base_in),
                spent=base_spent,
                earned=base_earned,
                gain_total=base_gain,
                neg_count=base_neg,
                neg_sum=base_neg_sum,
                best_gain=base_best,
            ),
        ]

        for _ in range(max_changes):
            next_beam = []
            for state in beam:
                for cand in filtered:
                    name_out = cand.out_player.get("nome") or cand.out_player.get("Giocatore")
                    name_in = cand.in_player.get("nome") or cand.in_player.get("Giocatore")
                    if name_out in state.out_set or name_in in state.in_set:
                        continue
                spent = state.spent + cand.qa_in
                earned = state.earned + cand.qa_out
                if not allow_overbudget and spent > credits_residui + earned:
                    continue
                neg_count = state.neg_count + (1 if cand.gain < 0 else 0)
                if cand.gain < 0:
                    if cand.gain < max_negative_gain:
                        continue
                    if neg_count > max_negative_swaps:
                        continue
                neg_sum = state.neg_sum + (abs(cand.gain) if cand.gain < 0 else 0.0)
                if neg_sum > max_negative_sum:
                    continue
                best_gain = max(state.best_gain, cand.gain)
                next_beam.append(
                    State(
                        swaps=state.swaps + [cand],
                        out_set=state.out_set | {name_out},
                        in_set=state.in_set | {name_in},
                        spent=spent,
                        earned=earned,
                        gain_total=state.gain_total + cand.gain,
                        neg_count=neg_count,
                        neg_sum=neg_sum,
                        best_gain=best_gain,
                    )
                )
            if not next_beam:
                break
            next_beam.sort(key=lambda s: s.gain_total, reverse=True)
            beam = next_beam[:beam_width]

        solutions = []
        for state in beam:
            if not state.swaps:
                continue
            if state.neg_count > 0:
                if state.best_gain < 1.0:
                    continue
            if require_roles:
                roles = {str(s.in_player.get("ruolo_base") or s.in_player.get("Ruolo") or "").upper() for s in state.swaps}
                if not require_roles.issubset(roles):
                    continue
            # Role swap bounds: max 3 per role (including GK), min 1 per role excluding GK
            role_counts = {"P": 0, "D": 0, "C": 0, "A": 0}
            for s in state.swaps:
                role = str(s.in_player.get("ruolo_base") or s.in_player.get("Ruolo") or "").upper()
                if role in role_counts:
                    role_counts[role] += 1
            if any(count > 3 for count in role_counts.values()):
                continue
            if role_counts["D"] < 1 or role_counts["C"] < 1 or role_counts["A"] < 1:
                continue
            final_club_counts = {}
            for p in user_squad:
                club = str(p.get("club") or p.get("Squadra", "")).strip()
                if not club:
                    continue
                final_club_counts[club] = final_club_counts.get(club, 0) + 1
            for s in state.swaps:
                club_out = str(s.out_player.get("club") or s.out_player.get("Squadra", "")).strip()
                club_in = str(s.in_player.get("club") or s.in_player.get("Squadra", "")).strip()
                if club_out:
                    final_club_counts[club_out] = final_club_counts.get(club_out, 0) - 1
                if club_in:
                    final_club_counts[club_in] = final_club_counts.get(club_in, 0) + 1
            if any(v > 3 for v in final_club_counts.values()):
                continue

            warnings = []
            if state.neg_count > 0:
                warnings.append("Presente cambio negativo")
            for s in state.swaps:
                t = titolarita(s.in_player, players_pool)
                if t < 0.55:
                    warnings.append(f"Titolare incerto: {s.in_player.get('nome') or s.in_player.get('Giocatore')}")
            budget_final = credits_residui + state.earned - state.spent
            solutions.append(
                Solution(
                    swaps=state.swaps,
                    budget_initial=credits_residui,
                    budget_final=budget_final,
                    total_gain=state.gain_total,
                    recommended_outs=[p.get("nome") or p.get("Giocatore") for p in recommended_outs],
                    warnings=sorted(set(warnings)),
                )
            )

        solutions.sort(key=lambda s: s.total_gain, reverse=True)
        unique = []
        seen = set()
        for sol in solutions:
            sig = tuple(sorted((s.out_player.get("nome") or s.out_player.get("Giocatore") or "",
                                s.in_player.get("nome") or s.in_player.get("Giocatore") or "") for s in sol.swaps))
            if sig in seen:
                continue
            seen.add(sig)
            unique.append(sol)
        return unique

    def diff_swaps(a: Solution, b: Solution) -> int:
        a_set = set(swap_key(s) for s in a.swaps)
        b_set = set(swap_key(s) for s in b.swaps)
        return len(a_set - b_set)

    selected: List[Solution] = []
    exclude: set[tuple[str, str]] = set()
    exclude_outs: set[str] = set()
    for _ in range(3):
        pool = build_solutions(exclude, exclude_outs)
        if not pool:
            break
        pick = None
        for sol in pool:
            if all(diff_swaps(sol, prev) >= 3 for prev in selected):
                pick = sol
                break
        if not pick:
            break
        selected.append(pick)
        top_swaps = sorted(pick.swaps, key=lambda s: s.gain, reverse=True)
        added = 0
        for s in top_swaps:
            raw_out_name = s.out_player.get("nome") or s.out_player.get("Giocatore") or ""
            if is_star(raw_out_name):
                continue
            exclude.add(swap_key(s))
            exclude_outs.add(norm_name(raw_out_name))
            added += 1
            if added >= 3:
                break

    return selected
