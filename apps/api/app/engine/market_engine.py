from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
import re
import math
import random
import logging

logger = logging.getLogger(__name__)


_NAME_LIST_CACHE: Dict[str, dict] = {}


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

    if pv_r8 == 0 and min_r8 == 0 and pt_r8 == 0:
        tit_final_r = tit_final_s

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
    pv_s = num(player.get("PV_S"))
    use_season = pv_r8 == 0 and pv_s > 0
    g_r8 = num(player.get("G_R8" if not use_season else "G_S"))
    a_r8 = num(player.get("A_R8" if not use_season else "A_S"))
    xg_r8 = num(player.get("xG_R8" if not use_season else "xG_S"))
    xa_r8 = num(player.get("xA_R8" if not use_season else "xA_S"))
    amm_r8 = num(player.get("AMM_R8" if not use_season else "AMM_S"))
    esp_r8 = num(player.get("ESP_R8" if not use_season else "ESP_S"))
    aut_r8 = num(player.get("AUTOGOL_R8" if not use_season else "AUTOGOL_S"))
    rigseg_r8 = num(player.get("RIGSEG_R8" if not use_season else "RIGSEG_S"))
    gdecwin_r8 = num(player.get("GDECWIN_R8" if not use_season else "GDECWIN_S"))
    gdecpar_r8 = num(player.get("GDECPAR_R8" if not use_season else "GDECPAR_S"))

    pk_role = num(player.get("PKRole", 0))

    pv_ref = pv_r8 if not use_season else pv_s
    rate_g = rate_recent(g_r8, pv_ref, eps)
    rate_a = rate_recent(a_r8, pv_ref, eps)
    rate_xg = rate_recent(xg_r8, pv_ref, eps)
    rate_xa = rate_recent(xa_r8, pv_ref, eps)
    rate_amm = rate_recent(amm_r8, pv_ref, eps)
    rate_esp = rate_recent(esp_r8, pv_ref, eps)
    rate_aut = rate_recent(aut_r8, pv_ref, eps)
    rate_rigseg = rate_recent(rigseg_r8, pv_ref, eps)
    rate_gdecwin = rate_recent(gdecwin_r8, pv_ref, eps)
    rate_gdecpar = rate_recent(gdecpar_r8, pv_ref, eps)

    if role == "P":
        gols_r8 = num(player.get("GOLS_R8" if not use_season else "GOLS_S"))
        rigpar_r8 = num(player.get("RIGPAR_R8" if not use_season else "RIGPAR_S"))
        cs_r8 = num(player.get("CS_R8" if not use_season else "CS_S"))
        rate_gols = rate_recent(gols_r8, pv_ref, eps)
        rate_rigpar = rate_recent(rigpar_r8, pv_ref, eps)
        rate_cs = rate_recent(cs_r8, pv_ref, eps)
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
    pts_dec = 1.0 * rate_gdecwin + 0.5 * rate_gdecpar
    pts_malus = (-0.5) * rate_amm + (-1.0) * rate_esp + (-2.0) * rate_aut
    return pts_gol + pts_assist + pts_rig + pts_dec + malus_mult * pts_malus


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
    strengths = compute_team_strengths(teams) if teams else {}
    momentum = compute_team_momentum(teams) if teams else {}
    sos_idx = compute_sos(teams, fixtures, current_round) if teams else {}
    games_left = games_remaining(teams, fixtures, current_round) if teams else {}
    if club not in teams:
        neutral_strength = 0.5
        neutral_momentum = 0.5
        neutral_sos = 0.5
        if games_left:
            avg_games = sum(games_left.values()) / max(len(games_left), 1)
        else:
            avg_games = 10
        tit = titolarita(player, players)
        pen = pen_tit(tit)
        efp = efp_player(player)
        ctx = team_context({}, role, neutral_strength, neutral_momentum)
        sos_mult = sos_role_multiplier(role, neutral_sos)
        efp_star = efp * ctx * sos_mult
        return avg_games * tit * pen * efp_star

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
    required_outs: List[str] | None = None,
    exclude_ins: List[str] | None = None,
    fixed_swaps: List[Tuple[str, str]] | None = None,
    include_outs_any: List[str] | None = None,
    emergency_relax: bool = False,
    debug: bool = False,
) -> List[Solution]:
    def log(msg: str):
        if debug:
            logger.info(msg)
    def _load_name_list(path: Path) -> set[str]:
        if not path.exists():
            return set()
        key = str(path)
        mtime = path.stat().st_mtime
        cached = _NAME_LIST_CACHE.get(key)
        if cached and cached.get("mtime") == mtime:
            return cached.get("data", set())
        data = set()
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            name = line.strip()
            if not name or name.startswith("#"):
                continue
            data.add(norm_name(name))
        _NAME_LIST_CACHE[key] = {"mtime": mtime, "data": data}
        return data

    def _load_injury_weights(path: Path) -> Dict[str, float]:
        if not path.exists():
            return {}
        key = f"{path}::weights"
        mtime = path.stat().st_mtime
        cached = _NAME_LIST_CACHE.get(key)
        if cached and cached.get("mtime") == mtime:
            return cached.get("data", {})
        data: Dict[str, float] = {}
        try:
            with path.open("r", encoding="utf-8") as f:
                header = f.readline()
                if header:
                    pass
                for line in f:
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) < 2:
                        continue
                    name = norm_name(parts[0])
                    if not name:
                        continue
                    try:
                        weight = float(parts[1].replace(",", "."))
                    except ValueError:
                        continue
                    data[name] = weight
        except OSError:
            data = {}
        _NAME_LIST_CACHE[key] = {"mtime": mtime, "data": data}
        return data

    def _load_weight_map(path: Path) -> Dict[str, float]:
        if not path.exists():
            return {}
        key = f"{path}::weights_generic"
        mtime = path.stat().st_mtime
        cached = _NAME_LIST_CACHE.get(key)
        if cached and cached.get("mtime") == mtime:
            return cached.get("data", {})
        data: Dict[str, float] = {}
        try:
            with path.open("r", encoding="utf-8") as f:
                header = f.readline()
                if header:
                    pass
                for line in f:
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) < 2:
                        continue
                    name = norm_name(parts[0])
                    if not name:
                        continue
                    try:
                        weight = float(parts[1].replace(",", "."))
                    except ValueError:
                        continue
                    data[name] = weight
        except OSError:
            data = {}
        _NAME_LIST_CACHE[key] = {"mtime": mtime, "data": data}
        return data

    def norm_name(name: str) -> str:
        value = str(name or "").lower()
        value = re.sub(r"[^a-z0-9]+", "", value)
        return value

    def name_of(player: dict) -> str:
        return str(player.get("nome") or player.get("Giocatore") or "").strip()

    def role_of(player: dict) -> str:
        return str(player.get("ruolo_base") or player.get("Ruolo") or "").upper()

    def club_of(player: dict) -> str:
        return str(player.get("club") or player.get("Squadra") or "").strip()

    def qa_of(player: dict) -> float:
        return num(player.get("QA", player.get("PrezzoAttuale", 0)))

    def is_star(name: str) -> bool:
        return str(name or "").strip().endswith(" *")

    root_dir = Path(__file__).resolve().parents[4]
    data_dir = root_dir / "data"
    injured_list = _load_name_list(data_dir / "infortunati_clean.txt")
    injury_return_allow = _load_name_list(data_dir / "infortunati_whitelist.txt")
    injury_weights = _load_injury_weights(data_dir / "infortunati_weights.csv")
    newcomers_allow = _load_name_list(data_dir / "nuovi_arrivi.txt")
    newcomers_weights = _load_weight_map(data_dir / "nuovi_arrivi_weights.csv")
    if newcomers_weights:
        newcomers_allow = newcomers_allow | set(newcomers_weights.keys())

    def has_recent_minutes(player: dict) -> bool:
        return num(player.get("PV_R8")) > 0 or num(player.get("MIN_R8")) > 0 or num(player.get("PT_R8")) > 0

    def has_season_minutes(player: dict) -> bool:
        return num(player.get("MIN_S")) >= 450 or num(player.get("PV_S")) >= 7

    def has_strong_season(player: dict) -> bool:
        return num(player.get("MIN_S")) >= 900 or num(player.get("PV_S")) >= 12 or num(player.get("PT_S")) >= 10

    def is_new_arrival(player: dict) -> bool:
        name_key = norm_name(name_of(player))
        if name_key in newcomers_allow:
            return True
        pv_s = num(player.get("PV_S"))
        min_s = num(player.get("MIN_S"))
        qa = qa_of(player)
        return pv_s <= 3 and min_s <= 180 and qa >= 12

    def is_bench_profile(player: dict) -> bool:
        pv_s = num(player.get("PV_S"))
        min_s = num(player.get("MIN_S"))
        return pv_s < 6 and min_s < 360

    def injury_factor(name_key: str) -> float:
        if name_key not in injured_list:
            return 1.0
        base = injury_weights.get(name_key, 0.55)
        if name_key in injury_return_allow:
            return max(base, 0.75)
        return base

    def is_long_injury(name_key: str) -> bool:
        if name_key not in injured_list:
            return False
        if name_key in injury_return_allow:
            return False
        return injury_weights.get(name_key, 0.55) <= 0.55

    def is_dead_profile(player: dict) -> bool:
        if is_new_arrival(player):
            return False
        pv_s = num(player.get("PV_S"))
        min_s = num(player.get("MIN_S"))
        g_s = num(player.get("G_S"))
        a_s = num(player.get("A_S"))
        g_r8 = num(player.get("G_R8"))
        a_r8 = num(player.get("A_R8"))
        if pv_s < 3 and min_s < 180 and (g_s + a_s + g_r8 + a_r8) == 0:
            return True
        return False

    def new_arrival_factor(name_key: str) -> float:
        if name_key not in newcomers_weights:
            return 1.0
        weight = max(0.0, min(1.0, newcomers_weights.get(name_key, 0.5)))
        return 0.6 + 0.8 * weight

    def new_arrival_floor(player: dict, games_left: int) -> float:
        name_key = norm_name(name_of(player))
        weight = newcomers_weights.get(name_key, 0.0)
        if weight < 0.85:
            return 0.0
        if has_recent_minutes(player):
            return 0.0
        if num(player.get("MIN_S")) > 0 or num(player.get("PV_S")) > 0 or num(player.get("PT_S")) > 0:
            return 0.0
        role = role_of(player)
        if role == "A":
            per_match = 0.20
        elif role == "C":
            per_match = 0.18
        elif role == "D":
            per_match = 0.15
        else:
            per_match = 0.12
        return games_left * per_match

    def eligible_in_player(player: dict) -> bool:
        name_key = norm_name(name_of(player))
        if not name_key:
            return False
        if is_long_injury(name_key):
            return False
        if name_key in injured_list and name_key not in injury_return_allow:
            if not has_strong_season(player):
                return False
            starter = titolarita(player, players_pool)
            if starter < 0.60:
                return False
        if is_star(name_of(player)):
            return False
        if has_recent_minutes(player):
            return True
        if is_new_arrival(player):
            return True
        if is_dead_profile(player):
            return False
        if is_bench_profile(player) and not has_strong_season(player):
            return True
        if has_strong_season(player):
            starter = titolarita(player, players_pool)
            if starter >= 0.55:
                return True
        return name_key in injury_return_allow and has_season_minutes(player)

    required_outs_set = set()
    if required_outs:
        required_outs_set = {norm_name(n) for n in required_outs if str(n).strip()}

    exclude_ins_set = set()
    if exclude_ins:
        exclude_ins_set = {norm_name(n) for n in exclude_ins if str(n).strip()}

    include_outs_set = set()
    if include_outs_any:
        include_outs_set = {norm_name(n) for n in include_outs_any if str(n).strip()}

    fixed_pairs: Dict[str, str] = {}
    if fixed_swaps:
        for out_name, in_name in fixed_swaps:
            out_key = norm_name(out_name)
            in_key = norm_name(in_name)
            if out_key and in_key:
                fixed_pairs[out_key] = in_key

    if required_outs_set:
        max_changes = len(required_outs_set)
    else:
        star_count = sum(1 for p in user_squad if is_star(name_of(p)))
        max_changes = max(0, min(int(max_changes or 5) + star_count, 5 + star_count))
    if max_changes == 0:
        return []

    log(
        f"suggest_transfers: max_changes={max_changes} k_pool={k_pool} m_out={m_out} "
        f"beam_width={beam_width} required_outs={len(required_outs_set)} "
        f"exclude_ins={len(exclude_ins_set)} include_outs_any={len(include_outs_set)}"
    )

    squad_names = {norm_name(name_of(p)) for p in user_squad if name_of(p)}

    value_map: Dict[str, float] = {}
    games_left_map = games_remaining(teams_data, fixtures, current_round) if teams_data else {}
    all_players = list(players_pool) + list(user_squad)
    for p in all_players:
        name = name_of(p)
        if not name or name in value_map:
            continue
        key = norm_name(name)
        value = value_season(p, players_pool, teams_data, fixtures, current_round)
        club = club_of(p)
        games_left = games_left_map.get(club, 10)
        floor = new_arrival_floor(p, games_left)
        if floor > value:
            value = floor
        value = value * injury_factor(key) * new_arrival_factor(key)
        if is_bench_profile(p) and not has_strong_season(p):
            value *= 0.65
        value_map[name] = value

    in_pool = {r: [] for r in ["P", "D", "C", "A"]}
    out_pool = {r: [] for r in ["P", "D", "C", "A"]}

    for p in players_pool:
        name = name_of(p)
        role = role_of(p)
        if not name or role not in in_pool:
            continue
        if norm_name(name) in squad_names:
            continue
        if norm_name(name) in exclude_ins_set:
            continue
        if not eligible_in_player(p):
            continue
        in_pool[role].append(p)

    for p in user_squad:
        name = name_of(p)
        role = role_of(p)
        if not name or role not in out_pool:
            continue
        out_pool[role].append(p)

    def top_k_by_value(players: List[dict]) -> List[dict]:
        return sorted(players, key=lambda x: value_map.get(name_of(x), 0.0), reverse=True)[:k_pool]

    def top_k_by_eff(players: List[dict]) -> List[dict]:
        def eff(p):
            return safe_div(value_map.get(name_of(p), 0.0), qa_of(p), 0.0)
        return sorted(players, key=eff, reverse=True)[:k_pool]

    for role in in_pool:
        combined = {p.get("id") or name_of(p): p for p in top_k_by_value(in_pool[role])}
        for p in top_k_by_eff(in_pool[role]):
            key = p.get("id") or name_of(p)
            combined[key] = p
        in_pool[role] = list(combined.values())

    log(
        "pool sizes: "
        + ", ".join(f"{r}: in={len(in_pool[r])} out={len(out_pool[r])}" for r in ["P","D","C","A"])
    )

    # protect top performers in squad to avoid suggesting obvious keepers
    keep_top_per_role = 2
    protected_outs: set[str] = set()
    if not emergency_relax:
        for role in out_pool:
            role_players = [p for p in out_pool[role] if name_of(p)]
            role_players.sort(key=lambda x: value_map.get(name_of(x), 0.0), reverse=True)
            for p in role_players[:keep_top_per_role]:
                protected_outs.add(norm_name(name_of(p)))

    for role in out_pool:
        base = sorted(
            out_pool[role],
            key=lambda x: value_map.get(name_of(x), 0.0),
        )[:m_out]
        stars = [p for p in out_pool[role] if is_star(name_of(p))]
        combined = {p.get("id") or name_of(p): p for p in base + stars if name_of(p)}
        filtered = list(combined.values())
        if required_outs_set:
            filtered = [p for p in filtered if norm_name(name_of(p)) in required_outs_set]
        else:
            if include_outs_set:
                filtered = [
                    p
                    for p in filtered
                    if norm_name(name_of(p)) not in protected_outs
                    or norm_name(name_of(p)) in include_outs_set
                ]
            else:
                filtered = [p for p in filtered if norm_name(name_of(p)) not in protected_outs]
        out_pool[role] = filtered

    candidates: List[Swap] = []
    for role in ["P", "D", "C", "A"]:
        for out_p in out_pool[role]:
            name_out = name_of(out_p)
            if not name_out:
                continue
            if required_outs_set and norm_name(name_out) not in required_outs_set:
                continue
            if fixed_pairs and norm_name(name_out) in fixed_pairs:
                continue
            qa_out = qa_of(out_p)
            val_out = value_map.get(name_out, 0.0)
            for in_p in in_pool[role]:
                name_in = name_of(in_p)
                if not name_in:
                    continue
                gain = value_map.get(name_in, 0.0) - val_out
                candidates.append(Swap(out_p, in_p, gain, qa_out, qa_of(in_p)))

    log(f"candidates: {len(candidates)}")

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
        big_gain_count: int

    def build_solutions(
        extra_exclude_ins: set[str],
        extra_exclude_outs: set[str],
        relax_level: int = 0,
    ) -> List[Solution]:
        fixed_swaps_list: List[Swap] = []
        spent_init = 0.0
        earned_init = 0.0
        gain_init = 0.0
        neg_count_init = 0
        neg_sum_init = 0.0
        best_gain_init = 0.0
        big_gain_init = 0
        out_set_init = set()
        in_set_init = set()

        if fixed_pairs:
            name_to_player = {norm_name(name_of(p)): p for p in players_pool if name_of(p)}
            squad_map = {norm_name(name_of(p)): p for p in user_squad if name_of(p)}
            for out_key, in_key in fixed_pairs.items():
                out_p = squad_map.get(out_key)
                in_p = name_to_player.get(in_key)
                if not out_p or not in_p:
                    continue
                qa_out = qa_of(out_p)
                qa_in = qa_of(in_p)
                gain = value_map.get(name_of(in_p), 0.0) - value_map.get(name_of(out_p), 0.0)
                fixed_swaps_list.append(Swap(out_p, in_p, gain, qa_out, qa_in))
                out_set_init.add(out_key)
                in_set_init.add(in_key)
                spent_init += qa_in
                earned_init += qa_out
                gain_init += gain
                if gain < 0:
                    neg_count_init += 1
                    neg_sum_init += abs(gain)
                best_gain_init = max(best_gain_init, gain)
                if gain >= 5.0:
                    big_gain_init += 1

        beam = [
            State(
                swaps=fixed_swaps_list,
                out_set=out_set_init,
                in_set=in_set_init,
                spent=spent_init,
                earned=earned_init,
                gain_total=gain_init,
                neg_count=neg_count_init,
                neg_sum=neg_sum_init,
                best_gain=best_gain_init,
                big_gain_count=big_gain_init,
            )
        ]

        if include_outs_set and not fixed_swaps_list:
            seeded = []
            for cand in candidates:
                name_out = norm_name(name_of(cand.out_player))
                name_in = norm_name(name_of(cand.in_player))
                if not name_out or not name_in:
                    continue
                if name_out not in include_outs_set:
                    continue
                if name_out in extra_exclude_outs:
                    continue
                if name_in in exclude_ins_set or name_in in extra_exclude_ins:
                    continue
                spent = cand.qa_in
                earned = cand.qa_out
                if spent > credits_residui + earned:
                    continue
                neg_count = 0
                neg_sum = 0.0
                best_gain = cand.gain
                if cand.gain < 0:
                    if cand.gain < -1.0:
                        continue
                    neg_count = 1
                    neg_sum = abs(cand.gain)
                seeded.append(
                    State(
                        swaps=[cand],
                        out_set={name_out},
                        in_set={name_in},
                        spent=spent,
                        earned=earned,
                        gain_total=cand.gain,
                        neg_count=neg_count,
                        neg_sum=neg_sum,
                        best_gain=best_gain,
                        big_gain_count=1 if cand.gain >= 5.0 else 0,
                    )
                )
            if seeded:
                beam = seeded

        remaining_changes = max_changes - len(beam[0].swaps)
        for _ in range(remaining_changes):
            next_beam = []
            for state in beam:
                for cand in candidates:
                    name_out = norm_name(name_of(cand.out_player))
                    name_in = norm_name(name_of(cand.in_player))
                    if not name_out or not name_in:
                        continue
                    if name_out in state.out_set or name_in in state.in_set:
                        continue
                    if name_out in extra_exclude_outs:
                        continue
                    if name_in in exclude_ins_set or name_in in extra_exclude_ins:
                        continue
                    spent = state.spent + cand.qa_in
                    earned = state.earned + cand.qa_out
                    if spent > credits_residui + earned:
                        continue
                    neg_count = state.neg_count
                    neg_sum = state.neg_sum
                    if cand.gain < 0:
                        max_neg_gain = (
                            -1.0 if relax_level == 0 else (-2.0 if relax_level == 1 else -3.0)
                        )
                        max_neg_swaps = 1 if relax_level == 0 else (1 if relax_level == 1 else 2)
                        if cand.gain < max_neg_gain:
                            continue
                        if neg_count + 1 > max_neg_swaps:
                            continue
                        neg_count += 1
                        neg_sum += abs(cand.gain)
                    best_gain = max(state.best_gain, cand.gain)
                    big_gain_count = state.big_gain_count + (1 if cand.gain >= 5.0 else 0)
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
                            big_gain_count=big_gain_count,
                        )
                    )
            if not next_beam:
                break
            next_beam.sort(key=lambda s: s.gain_total, reverse=True)
            beam = next_beam[:beam_width]

        solutions: List[Solution] = []
        for state in beam:
            if not state.swaps:
                continue
            if state.neg_count > 0:
                if relax_level == 0:
                    if state.big_gain_count < 2:
                        continue
                    if state.gain_total < 6.0 * state.neg_sum:
                        continue
                elif relax_level == 1:
                    if state.big_gain_count < 1:
                        continue
                    if state.gain_total < 4.0 * state.neg_sum:
                        continue

            final_club_counts: Dict[str, int] = {}
            for p in user_squad:
                club = club_of(p)
                if not club:
                    continue
                final_club_counts[club] = final_club_counts.get(club, 0) + 1
            for s in state.swaps:
                club_out = club_of(s.out_player)
                club_in = club_of(s.in_player)
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
                    pass
            budget_final = credits_residui + state.earned - state.spent
            solutions.append(
                Solution(
                    swaps=state.swaps,
                    budget_initial=credits_residui,
                    budget_final=budget_final,
                    total_gain=state.gain_total,
                    warnings=sorted(set(warnings)),
                )
            )

        if required_outs_set:
            filtered_solutions = []
            for sol in solutions:
                out_set = {norm_name(name_of(s.out_player)) for s in sol.swaps}
                if out_set == required_outs_set:
                    filtered_solutions.append(sol)
            solutions = filtered_solutions
        elif include_outs_set:
            filtered_solutions = []
            for sol in solutions:
                out_set = {norm_name(name_of(s.out_player)) for s in sol.swaps}
                if out_set & include_outs_set:
                    filtered_solutions.append(sol)
            solutions = filtered_solutions

        solutions.sort(key=lambda s: s.total_gain, reverse=True)
        return solutions

    def top_in_names(sol: Solution, count: int = 4) -> List[str]:
        swaps_sorted = sorted(sol.swaps, key=lambda s: s.gain, reverse=True)
        names = []
        for s in swaps_sorted:
            name = norm_name(name_of(s.in_player))
            if name and name not in names:
                names.append(name)
            if len(names) >= count:
                break
        return names

    def top_out_names(sol: Solution, count: int = 3) -> List[str]:
        swaps_sorted = sorted(sol.swaps, key=lambda s: s.gain, reverse=True)
        names = []
        for s in swaps_sorted:
            name = norm_name(name_of(s.out_player))
            if name and name not in names:
                names.append(name)
            if len(names) >= count:
                break
        return names

    selected: List[Solution] = []
    base_exclude = set()
    base_exclude_outs = set()

    pool1 = build_solutions(base_exclude, base_exclude_outs, relax_level=0)
    if pool1:
        selected.append(pool1[0])
    if len(selected) >= 3:
        return selected[:3]
    log(f"pool1={len(pool1)} selected={len(selected)}")

    def min_gain_threshold(best_gain: float, relax: float = 0.0) -> float:
        if best_gain <= 0:
            return 0.0
        base = max(4.0, best_gain * 0.4)
        if relax > 0:
            base = max(2.5, best_gain * 0.25)
        return base

    best_gain = selected[0].total_gain if selected else 0.0
    min_gain = min_gain_threshold(best_gain)

    exclude2 = base_exclude | set(top_in_names(selected[0], 4)) if selected else base_exclude
    exclude2_outs = base_exclude_outs | set(top_out_names(selected[0], 3)) if selected else base_exclude_outs
    pool2 = build_solutions(exclude2, exclude2_outs, relax_level=0)
    def out_overlap(a: Solution, b: Solution) -> int:
        outs_a = {norm_name(name_of(s.out_player)) for s in a.swaps}
        outs_b = {norm_name(name_of(s.out_player)) for s in b.swaps}
        return len(outs_a & outs_b)

    for sol in pool2:
        if sol in selected:
            continue
        if out_overlap(sol, selected[0]) > 3:
            continue
        if sol.total_gain < min_gain:
            continue
        selected.append(sol)
        break
    if len(selected) >= 3:
        return selected[:3]
    log(f"pool2={len(pool2)} selected={len(selected)}")

    exclude3 = exclude2
    exclude3_outs = exclude2_outs
    if len(selected) > 1:
        exclude3 = exclude3 | set(top_in_names(selected[1], 4))
        exclude3_outs = exclude3_outs | set(top_out_names(selected[1], 3))
    pool3 = build_solutions(exclude3, exclude3_outs, relax_level=0)
    for sol in pool3:
        if sol in selected:
            continue
        if out_overlap(sol, selected[0]) > 3:
            continue
        if len(selected) > 1 and out_overlap(sol, selected[1]) > 3:
            continue
        if sol.total_gain < min_gain:
            continue
        selected.append(sol)
        break

    if len(selected) < 3:
        # Relax step 1
        pool = build_solutions(base_exclude, base_exclude_outs, relax_level=1)
        min_gain_relax = min_gain_threshold(best_gain, relax=1.0)
        for sol in pool:
            if sol in selected:
                continue
            if sol.total_gain < min_gain_relax:
                continue
            selected.append(sol)
            if len(selected) >= 3:
                break
        log(f"relax1 pool={len(pool)} selected={len(selected)}")
    if len(selected) < 3:
        # Relax step 2
        pool = build_solutions(base_exclude, base_exclude_outs, relax_level=2)
        min_gain_relax = min_gain_threshold(best_gain, relax=1.0)
        for sol in pool:
            if sol in selected:
                continue
            if sol.total_gain < min_gain_relax:
                continue
            selected.append(sol)
            if len(selected) >= 3:
                break
        log(f"relax2 pool={len(pool)} selected={len(selected)}")

    if not selected and not emergency_relax:
        log("no solutions, triggering emergency_relax")
        return suggest_transfers(
            user_squad=user_squad,
            credits_residui=credits_residui,
            players_pool=players_pool,
            teams_data=teams_data,
            fixtures=fixtures,
            current_round=current_round,
            max_changes=max_changes,
            k_pool=k_pool,
            m_out=m_out,
            beam_width=beam_width,
            seed=seed,
            allow_overbudget=allow_overbudget,
            max_negative_gain=max_negative_gain,
            max_negative_swaps=max_negative_swaps,
            max_negative_sum=max_negative_sum,
            require_roles=require_roles,
            required_outs=required_outs,
            exclude_ins=exclude_ins,
            fixed_swaps=fixed_swaps,
            include_outs_any=include_outs_any,
            emergency_relax=True,
            debug=debug,
        )

    if not selected:
        log("no solutions after relax, using greedy fallback")
        def greedy_solution(exclude_ins: set[str], exclude_outs: set[str]) -> Solution | None:
            swaps: List[Swap] = []
            used_out = set()
            used_in = set()
            spent = 0.0
            earned = 0.0
            neg_count = 0
            neg_sum = 0.0
            best_gain = 0.0

            team_counts: Dict[str, int] = {}
            for p in user_squad:
                club = club_of(p)
                if not club:
                    continue
                team_counts[club] = team_counts.get(club, 0) + 1

            for cand in sorted(candidates, key=lambda s: s.gain, reverse=True):
                name_out = norm_name(name_of(cand.out_player))
                name_in = norm_name(name_of(cand.in_player))
                if not name_out or not name_in:
                    continue
                if name_out in used_out or name_in in used_in:
                    continue
                if name_out in exclude_outs or name_in in exclude_ins:
                    continue
                if cand.gain < -3.0:
                    continue
                if cand.gain < 0 and neg_count + 1 > 2:
                    continue
                next_spent = spent + cand.qa_in
                next_earned = earned + cand.qa_out
                if next_spent > credits_residui + next_earned:
                    continue

                club_out = club_of(cand.out_player)
                club_in = club_of(cand.in_player)
                if club_out:
                    team_counts[club_out] = team_counts.get(club_out, 0) - 1
                if club_in:
                    team_counts[club_in] = team_counts.get(club_in, 0) + 1
                if any(v > 3 for v in team_counts.values()):
                    # revert and skip
                    if club_out:
                        team_counts[club_out] = team_counts.get(club_out, 0) + 1
                    if club_in:
                        team_counts[club_in] = team_counts.get(club_in, 0) - 1
                    continue

                swaps.append(cand)
                used_out.add(name_out)
                used_in.add(name_in)
                spent = next_spent
                earned = next_earned
                best_gain = max(best_gain, cand.gain)
                if cand.gain < 0:
                    neg_count += 1
                    neg_sum += abs(cand.gain)
                if len(swaps) >= max_changes:
                    break

            if not swaps:
                return None

            budget_final = credits_residui + earned - spent
            total_gain = sum(s.gain for s in swaps)
            warnings = []
            if neg_count > 0:
                warnings.append("Presente cambio negativo")
            for s in swaps:
                t = titolarita(s.in_player, players_pool)
            return Solution(
                swaps=swaps,
                budget_initial=credits_residui,
                budget_final=budget_final,
                total_gain=total_gain,
                warnings=sorted(set(warnings)),
            )

        greedy_selected: List[Solution] = []
        exclude_ins = set()
        exclude_outs = set()
        sol1 = greedy_solution(exclude_ins, exclude_outs)
        if sol1:
            greedy_selected.append(sol1)
            exclude_ins |= set(top_in_names(sol1, 4))
            exclude_outs |= set(top_out_names(sol1, 3))
        sol2 = greedy_solution(exclude_ins, exclude_outs)
        if sol2:
            greedy_selected.append(sol2)
            exclude_ins |= set(top_in_names(sol2, 4))
            exclude_outs |= set(top_out_names(sol2, 3))
        sol3 = greedy_solution(exclude_ins, exclude_outs)
        if sol3:
            greedy_selected.append(sol3)
        return greedy_selected[:3]

    return selected[:3]
