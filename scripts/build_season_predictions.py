from __future__ import annotations

import argparse
import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
REPORTS_DIR = DATA_DIR / "reports"


CLUB_ALIASES = {
    "juv": "juventus",
    "rom": "roma",
    "laz": "lazio",
    "tor": "torino",
    "par": "parma",
    "pis": "pisa",
    "ver": "verona",
    "cag": "cagliari",
    "hellasverona": "verona",
    "bolognaq": "bologna",
}


ROLE_ORDER = ["P", "D", "C", "A"]


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_key(value: Any) -> str:
    text = _normalize_text(value)
    return re.sub(r"[^a-z0-9]+", "", text)


def _canonical_club(value: Any) -> str:
    raw = _normalize_text(value)
    raw = raw.replace(".", "").replace("-", " ")
    raw = re.sub(r"\s+", " ", raw).strip()
    compact = raw.replace(" ", "")
    if compact in CLUB_ALIASES:
        return CLUB_ALIASES[compact]
    if raw in CLUB_ALIASES:
        return CLUB_ALIASES[raw]
    return raw


def _parse_module(module_raw: Any) -> Optional[Tuple[int, int, int]]:
    digits = "".join(ch for ch in str(module_raw or "") if ch.isdigit())
    if len(digits) != 3:
        return None
    d, c, a = int(digits[0]), int(digits[1]), int(digits[2])
    if d + c + a != 10:
        return None
    if min(d, c, a) <= 0:
        return None
    return d, c, a


def _format_module(d: int, c: int, a: int) -> str:
    return f"{d}-{c}-{a}"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            cleaned = value.replace(",", ".").strip()
            if cleaned == "":
                return default
            return float(cleaned)
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned == "":
                return default
            return int(float(cleaned))
        return int(value)
    except Exception:
        return default


def _poisson_pmf(lmbda: float, k: int) -> float:
    if lmbda <= 0:
        return 1.0 if k == 0 else 0.0
    return math.exp(-lmbda) * (lmbda**k) / math.factorial(k)


def _evaluate_bands(value: float, bands: List[Dict[str, Any]]) -> float:
    for band in bands:
        min_v = band.get("min")
        max_v = band.get("max")
        min_inc = bool(band.get("min_inclusive", True))
        max_inc = bool(band.get("max_inclusive", True))
        ok = True
        if min_v is not None:
            min_val = _safe_float(min_v)
            ok = ok and (value >= min_val if min_inc else value > min_val)
        if max_v is not None:
            max_val = _safe_float(max_v)
            ok = ok and (value <= max_val if max_inc else value < max_val)
        if ok:
            return _safe_float(band.get("value"), 0.0)
    return 0.0


def _load_regulation(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _get_allowed_modules(regulation: Dict[str, Any]) -> List[Tuple[int, int, int]]:
    formation_rules = regulation.get("formation_rules") if isinstance(regulation, dict) else {}
    raw = formation_rules.get("allowed_modules") if isinstance(formation_rules, dict) else []
    parsed: List[Tuple[int, int, int]] = []
    for value in raw if isinstance(raw, list) else []:
        tpl = _parse_module(value)
        if tpl is not None:
            parsed.append(tpl)
    if parsed:
        return parsed
    return [(3, 4, 3), (4, 3, 3), (4, 4, 2), (3, 5, 2)]


def _load_team_context() -> pd.DataFrame:
    base = pd.read_csv(DATA_DIR / "incoming" / "manual" / "seriea_context.csv")
    per90 = pd.read_csv(DATA_DIR / "incoming" / "manual" / "seriea_team_per90.csv")
    gk = pd.read_csv(DATA_DIR / "incoming" / "manual" / "seriea_goalkeeper_context.csv")
    disc = pd.read_csv(DATA_DIR / "incoming" / "manual" / "seriea_discipline_context.csv")

    for frame in (base, per90, gk, disc):
        frame["club"] = frame["Squad"].apply(_canonical_club)

    merged = (
        base.merge(per90.drop(columns=["Squad"]), on="club", how="left")
        .merge(gk.drop(columns=["Squad"]), on="club", how="left")
        .merge(disc.drop(columns=["Squad"]), on="club", how="left")
    )
    merged["gf_per_match"] = merged["GF"] / merged["MP"].clip(lower=1)
    merged["ga_per_match"] = merged["GA"] / merged["MP"].clip(lower=1)
    merged["pts_per_match"] = merged["Pts"] / merged["MP"].clip(lower=1)
    merged["crdy_per_match"] = merged["CrdY"] / merged["MP"].clip(lower=1)
    merged["crdr_per_match"] = merged["CrdR"] / merged["MP"].clip(lower=1)
    merged["fouls_per_match"] = merged["Fls"] / merged["MP"].clip(lower=1)
    return merged


def _build_team_metrics(ctx: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    avg_gf = float(ctx["gf_per_match"].mean())
    avg_ga90 = float(ctx["GA90"].mean())
    avg_pts = float(ctx["pts_per_match"].mean())
    avg_gls90 = float(ctx["Gls"].mean())

    metrics: Dict[str, Dict[str, float]] = {}
    for row in ctx.to_dict(orient="records"):
        club = str(row["club"])
        gfpm = _safe_float(row.get("gf_per_match"), avg_gf)
        gapm = _safe_float(row.get("ga_per_match"), avg_gf)
        gls90 = _safe_float(row.get("Gls"), avg_gls90)
        ga90 = _safe_float(row.get("GA90"), avg_ga90)
        ppm = _safe_float(row.get("pts_per_match"), avg_pts)

        attack_norm = max(0.45, min(1.85, (0.6 * (gfpm / max(avg_gf, 0.001))) + (0.4 * (gls90 / max(avg_gls90, 0.001)))))
        defense_weak_norm = max(0.45, min(1.85, (0.6 * (gapm / max(avg_gf, 0.001))) + (0.4 * (ga90 / max(avg_ga90, 0.001)))))
        ppm_norm = max(0.5, min(1.8, ppm / max(avg_pts, 0.001)))
        cs_pct = _safe_float(row.get("CS%"), 0.0) / 100.0
        save_pct = _safe_float(row.get("Save%"), 0.0) / 100.0
        yellow_pm = _safe_float(row.get("crdy_per_match"), 0.0)
        red_pm = _safe_float(row.get("crdr_per_match"), 0.0)

        metrics[club] = {
            "attack_norm": attack_norm,
            "defense_weak_norm": defense_weak_norm,
            "ppm_norm": ppm_norm,
            "ga90": ga90,
            "gfpm": gfpm,
            "gapm": gapm,
            "cs_pct": cs_pct,
            "save_pct": save_pct,
            "yellow_pm": yellow_pm,
            "red_pm": red_pm,
        }
    return metrics


def _load_fixtures(start_round: int, end_round: int) -> pd.DataFrame:
    fixtures = pd.read_csv(DATA_DIR / "db" / "fixtures.csv")
    fixtures["round"] = pd.to_numeric(fixtures["round"], errors="coerce").astype("Int64")
    fixtures["team"] = fixtures["team"].apply(_canonical_club)
    fixtures["opponent"] = fixtures["opponent"].apply(_canonical_club)
    fixtures["home_away"] = fixtures["home_away"].astype(str).str.upper()
    filt = fixtures[
        fixtures["round"].between(start_round, end_round)
        & (fixtures["home_away"] == "H")
    ].copy()
    return filt.sort_values(["round", "team"]).reset_index(drop=True)


def _load_player_stats() -> pd.DataFrame:
    stats = pd.read_csv(DATA_DIR / "statistiche_giocatori.csv")
    stats["name"] = stats["Giocatore"].astype(str).str.strip()
    stats["name_key"] = stats["name"].apply(_normalize_key)
    stats["club"] = stats["Squadra"].apply(_canonical_club)
    stats["games"] = pd.to_numeric(stats["Partite"], errors="coerce").fillna(0).clip(lower=0)
    stats["mv"] = pd.to_numeric(stats["Mediavoto"], errors="coerce").fillna(6.0)
    stats["fm"] = pd.to_numeric(stats["Fantamedia"], errors="coerce").fillna(stats["mv"])
    stats["goal_rate"] = pd.to_numeric(stats["Gol"], errors="coerce").fillna(0) / stats["games"].replace(0, 1)
    stats["assist_rate"] = pd.to_numeric(stats["Assist"], errors="coerce").fillna(0) / stats["games"].replace(0, 1)
    stats["yellow_rate"] = pd.to_numeric(stats["Ammonizioni"], errors="coerce").fillna(0) / stats["games"].replace(0, 1)
    stats["red_rate"] = pd.to_numeric(stats["Espulsioni"], errors="coerce").fillna(0) / stats["games"].replace(0, 1)
    stats["cs_rate"] = pd.to_numeric(stats["Cleansheet"], errors="coerce").fillna(0) / stats["games"].replace(0, 1)
    return stats


def _load_tiers() -> pd.DataFrame:
    tiers = pd.read_csv(DATA_DIR / "player_tiers.csv")
    tiers["name"] = tiers["name"].astype(str).str.strip()
    tiers["name_key"] = tiers["name"].apply(_normalize_key)
    tiers["tier_weight"] = pd.to_numeric(tiers.get("weight"), errors="coerce").fillna(0.5)
    tiers["role"] = tiers.get("role", "").astype(str).str.strip().str.upper()
    return tiers[["name_key", "tier_weight", "role", "tier"]].drop_duplicates(subset=["name_key"], keep="first")


def _load_rosters() -> pd.DataFrame:
    rose = pd.read_csv(DATA_DIR / "rose_fantaportoscuso.csv")
    rose["fantateam"] = rose["Team"].astype(str).str.strip()
    rose["fantateam_key"] = rose["fantateam"].apply(_normalize_key)
    rose["name"] = rose["Giocatore"].astype(str).str.strip()
    rose["name_key"] = rose["name"].apply(_normalize_key)
    rose["role"] = rose["Ruolo"].astype(str).str.strip().str.upper()
    rose["club"] = rose["Squadra"].apply(_canonical_club)
    return rose


def _load_classifica() -> pd.DataFrame:
    cls = pd.read_csv(DATA_DIR / "classifica.csv")
    cls["fantateam"] = cls["Squadra"].astype(str).str.strip()
    cls["fantateam_key"] = cls["fantateam"].apply(_normalize_key)
    cls["points_current"] = pd.to_numeric(cls["Pt. totali"], errors="coerce").fillna(0.0)
    return cls


def _load_captains() -> Dict[str, Dict[str, str]]:
    path = DATA_DIR / "incoming" / "formazioni" / "formazioni.csv"
    if not path.exists():
        return {}
    form = pd.read_csv(path)
    if form.empty:
        return {}
    form["giornata"] = pd.to_numeric(form["giornata"], errors="coerce").fillna(0)
    form["team_key"] = form["team"].astype(str).str.strip().apply(_normalize_key)
    form = form.sort_values(["team_key", "giornata"])
    latest = form.groupby("team_key", as_index=False).tail(1)
    out: Dict[str, Dict[str, str]] = {}
    for row in latest.to_dict(orient="records"):
        out[str(row["team_key"])] = {
            "capitano": str(row.get("capitano") or "").strip(),
            "vice_capitano": str(row.get("vice_capitano") or "").strip(),
        }
    return out


def _build_player_pool(stats: pd.DataFrame, tiers: pd.DataFrame) -> pd.DataFrame:
    pool = stats.merge(tiers, on="name_key", how="left")
    pool["tier_weight"] = pool["tier_weight"].fillna(0.5)
    pool["tier"] = pool["tier"].fillna("scommessa")
    return pool


def _match_probabilities(home_xg: float, away_xg: float, max_goals: int = 7) -> Dict[str, Any]:
    p_home = [_poisson_pmf(home_xg, k) for k in range(max_goals + 1)]
    p_away = [_poisson_pmf(away_xg, k) for k in range(max_goals + 1)]
    home_win = 0.0
    draw = 0.0
    away_win = 0.0
    best_score = (0, 0)
    best_p = -1.0
    for h in range(max_goals + 1):
        for a in range(max_goals + 1):
            p = p_home[h] * p_away[a]
            if h > a:
                home_win += p
            elif h == a:
                draw += p
            else:
                away_win += p
            if p > best_p:
                best_p = p
                best_score = (h, a)
    return {
        "home_win": home_win,
        "draw": draw,
        "away_win": away_win,
        "best_score_home": best_score[0],
        "best_score_away": best_score[1],
        "home_cs_prob": p_away[0],
        "away_cs_prob": p_home[0],
        "exp_home_goals": home_xg,
        "exp_away_goals": away_xg,
    }


def _format_event_counts(players: List[str]) -> str:
    if not players:
        return ""
    counts: Dict[str, int] = defaultdict(int)
    for name in players:
        counts[name] += 1
    parts = []
    for name, qty in counts.items():
        if qty <= 1:
            parts.append(name)
        else:
            parts.append(f"{name} x{qty}")
    return "; ".join(parts)


def _pick_players_for_event(
    team_players: pd.DataFrame,
    count: int,
    metric_col: str,
    fallback_col: str,
    exclude: Optional[set[str]] = None,
) -> List[str]:
    if count <= 0 or team_players.empty:
        return []
    exclude = exclude or set()
    frame = team_players.copy()
    frame = frame[~frame["name"].isin(exclude)]
    if frame.empty:
        return []
    frame["metric"] = pd.to_numeric(frame[metric_col], errors="coerce").fillna(0.0)
    frame["fallback"] = pd.to_numeric(frame[fallback_col], errors="coerce").fillna(0.0)
    frame = frame.sort_values(["metric", "fallback", "name"], ascending=[False, False, True])
    names = frame["name"].tolist()
    if not names:
        return []
    picked: List[str] = []
    idx = 0
    while len(picked) < count:
        picked.append(names[idx % len(names)])
        idx += 1
    return picked

def _expected_goals(
    home: str,
    away: str,
    metrics: Dict[str, Dict[str, float]],
    league_gf: float,
) -> Tuple[float, float]:
    home_m = metrics.get(home, {})
    away_m = metrics.get(away, {})
    home_attack = _safe_float(home_m.get("attack_norm"), 1.0)
    away_attack = _safe_float(away_m.get("attack_norm"), 1.0)
    home_def_weak = _safe_float(home_m.get("defense_weak_norm"), 1.0)
    away_def_weak = _safe_float(away_m.get("defense_weak_norm"), 1.0)

    home_xg = league_gf * 1.06 * home_attack * away_def_weak
    away_xg = league_gf * 0.94 * away_attack * home_def_weak
    home_xg = max(0.2, min(3.6, home_xg))
    away_xg = max(0.2, min(3.6, away_xg))
    return home_xg, away_xg


def _build_round_match_predictions(
    fixtures_round: pd.DataFrame,
    metrics: Dict[str, Dict[str, float]],
    players_by_club: Dict[str, pd.DataFrame],
    league_gf: float,
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    rows: List[Dict[str, Any]] = []
    round_ctx: Dict[str, Dict[str, Any]] = {}
    for match in fixtures_round.to_dict(orient="records"):
        rnd = _safe_int(match["round"])
        home = str(match["team"])
        away = str(match["opponent"])
        home_xg, away_xg = _expected_goals(home, away, metrics, league_gf)
        probs = _match_probabilities(home_xg, away_xg)
        sh = int(probs["best_score_home"])
        sa = int(probs["best_score_away"])

        home_players = players_by_club.get(home, pd.DataFrame())
        away_players = players_by_club.get(away, pd.DataFrame())

        home_scorers_list = _pick_players_for_event(home_players, sh, "goal_rate", "fm")
        away_scorers_list = _pick_players_for_event(away_players, sa, "goal_rate", "fm")

        home_ass_count = min(sh, max(0, int(round(sh * 0.8))))
        away_ass_count = min(sa, max(0, int(round(sa * 0.8))))
        home_assists_list = _pick_players_for_event(
            home_players, home_ass_count, "assist_rate", "fm", exclude=set(home_scorers_list)
        )
        away_assists_list = _pick_players_for_event(
            away_players, away_ass_count, "assist_rate", "fm", exclude=set(away_scorers_list)
        )

        home_y_rate = _safe_float(metrics.get(home, {}).get("yellow_pm"), 2.0)
        away_y_rate = _safe_float(metrics.get(away, {}).get("yellow_pm"), 2.0)
        home_r_rate = _safe_float(metrics.get(home, {}).get("red_pm"), 0.1)
        away_r_rate = _safe_float(metrics.get(away, {}).get("red_pm"), 0.1)

        home_yellow = max(0, int(round(home_y_rate)))
        away_yellow = max(0, int(round(away_y_rate)))
        home_red = 1 if home_r_rate >= 0.25 else 0
        away_red = 1 if away_r_rate >= 0.25 else 0

        home_cards = _pick_players_for_event(home_players, home_yellow, "yellow_rate", "games")
        away_cards = _pick_players_for_event(away_players, away_yellow, "yellow_rate", "games")
        home_reds = _pick_players_for_event(home_players, home_red, "red_rate", "yellow_rate")
        away_reds = _pick_players_for_event(away_players, away_red, "red_rate", "yellow_rate")

        rows.append(
            {
                "round": rnd,
                "home_team": home.title(),
                "away_team": away.title(),
                "pred_score": f"{sh}-{sa}",
                "home_win_prob": round(probs["home_win"], 4),
                "draw_prob": round(probs["draw"], 4),
                "away_win_prob": round(probs["away_win"], 4),
                "home_xg": round(home_xg, 3),
                "away_xg": round(away_xg, 3),
                "home_scorers": _format_event_counts(home_scorers_list),
                "away_scorers": _format_event_counts(away_scorers_list),
                "home_assists": _format_event_counts(home_assists_list),
                "away_assists": _format_event_counts(away_assists_list),
                "home_yellow_cards": _format_event_counts(home_cards),
                "away_yellow_cards": _format_event_counts(away_cards),
                "home_red_cards": _format_event_counts(home_reds),
                "away_red_cards": _format_event_counts(away_reds),
                "home_clean_sheet_prob": round(probs["home_cs_prob"], 4),
                "away_clean_sheet_prob": round(probs["away_cs_prob"], 4),
            }
        )

        round_ctx[home] = {
            "round": rnd,
            "home_away": "H",
            "opponent": away,
            "xg_for": home_xg,
            "xg_against": away_xg,
            "win_prob": probs["home_win"],
            "draw_prob": probs["draw"],
            "lose_prob": probs["away_win"],
            "cs_prob": probs["home_cs_prob"],
        }
        round_ctx[away] = {
            "round": rnd,
            "home_away": "A",
            "opponent": home,
            "xg_for": away_xg,
            "xg_against": home_xg,
            "win_prob": probs["away_win"],
            "draw_prob": probs["draw"],
            "lose_prob": probs["home_win"],
            "cs_prob": probs["away_cs_prob"],
        }
    return rows, round_ctx


def _fixture_multiplier_for_role(
    role: str,
    home_away: str,
    team_ppm_norm: float,
    opp_metric: float,
    cfg: Dict[str, Any],
) -> float:
    min_mul = _safe_float(cfg.get("min_multiplier"), 0.8)
    max_mul = _safe_float(cfg.get("max_multiplier"), 1.22)
    home_bonus = cfg.get("home_bonus") if isinstance(cfg.get("home_bonus"), dict) else {}
    away_penalty = cfg.get("away_penalty") if isinstance(cfg.get("away_penalty"), dict) else {}
    own_weight = cfg.get("own_weight") if isinstance(cfg.get("own_weight"), dict) else {}
    opp_weight = cfg.get("opp_weight") if isinstance(cfg.get("opp_weight"), dict) else {}

    mult = 1.0
    if home_away == "H":
        mult += _safe_float(home_bonus.get(role), 0.0)
    else:
        mult += _safe_float(away_penalty.get(role), 0.0)
    mult += _safe_float(own_weight.get(role), 0.0) * (team_ppm_norm - 1.0)
    mult += _safe_float(opp_weight.get(role), 0.0) * (opp_metric - 1.0)
    return max(min_mul, min(max_mul, mult))


def _build_player_round_projection(
    player_row: Dict[str, Any],
    role: str,
    match_ctx: Dict[str, Any],
    team_metrics: Dict[str, Dict[str, float]],
    fixture_cfg: Dict[str, Any],
) -> Tuple[float, float]:
    club = str(player_row.get("club") or "")
    opp = str(match_ctx.get("opponent") or "")
    home_away = str(match_ctx.get("home_away") or "H")

    team_ppm_norm = _safe_float(team_metrics.get(club, {}).get("ppm_norm"), 1.0)
    opp_attack_norm = _safe_float(team_metrics.get(opp, {}).get("attack_norm"), 1.0)
    opp_def_weak_norm = _safe_float(team_metrics.get(opp, {}).get("defense_weak_norm"), 1.0)

    if role in {"P", "D"}:
        opp_metric = max(0.55, min(1.45, 2.0 - opp_attack_norm))
    else:
        opp_metric = max(0.55, min(1.45, opp_def_weak_norm))

    mult = _fixture_multiplier_for_role(role, home_away, team_ppm_norm, opp_metric, fixture_cfg)
    fm = _safe_float(player_row.get("fm"), 6.0)
    mv = _safe_float(player_row.get("mv"), 6.0)
    tier_weight = _safe_float(player_row.get("tier_weight"), 0.5)

    quality_adj = 0.92 + (0.18 * tier_weight)
    expected_fv = fm * mult * quality_adj
    expected_vote = mv * (0.97 + 0.08 * (mult - 1.0))
    return round(expected_vote, 2), round(expected_fv, 2)


def _select_lineup_for_team(
    players: List[Dict[str, Any]],
    allowed_modules: List[Tuple[int, int, int]],
    captain_pref: Dict[str, str],
    regulation: Dict[str, Any],
) -> Dict[str, Any]:
    by_role: Dict[str, List[Dict[str, Any]]] = {r: [] for r in ROLE_ORDER}
    for p in players:
        role = str(p.get("role") or "")
        if role not in by_role:
            continue
        by_role[role].append(p)
    for role in ROLE_ORDER:
        by_role[role] = sorted(by_role[role], key=lambda x: (_safe_float(x.get("exp_fv"), 0.0), x.get("name", "")), reverse=True)

    best: Optional[Dict[str, Any]] = None
    for d, c, a in allowed_modules:
        if len(by_role["P"]) < 1 or len(by_role["D"]) < d or len(by_role["C"]) < c or len(by_role["A"]) < a:
            continue
        gk = by_role["P"][0]
        ds = by_role["D"][:d]
        cs = by_role["C"][:c]
        ats = by_role["A"][:a]
        score = sum(_safe_float(x.get("exp_fv"), 0.0) for x in [gk, *ds, *cs, *ats])
        candidate = {
            "module": _format_module(d, c, a),
            "module_tuple": (d, c, a),
            "portiere": gk,
            "difensori": ds,
            "centrocampisti": cs,
            "attaccanti": ats,
            "base_total": score,
        }
        if best is None or score > _safe_float(best.get("base_total"), 0.0):
            best = candidate

    if best is None:
        gk = by_role["P"][0] if by_role["P"] else None
        ds = by_role["D"][:3]
        cs = by_role["C"][:4]
        ats = by_role["A"][:3]
        best = {
            "module": "3-4-3",
            "module_tuple": (3, 4, 3),
            "portiere": gk,
            "difensori": ds,
            "centrocampisti": cs,
            "attaccanti": ats,
            "base_total": sum(_safe_float(x.get("exp_fv"), 0.0) for x in [*( [gk] if gk else [] ), *ds, *cs, *ats]),
        }

    starters: List[Dict[str, Any]] = []
    if best["portiere"] is not None:
        starters.append(best["portiere"])
    starters.extend(best["difensori"])
    starters.extend(best["centrocampisti"])
    starters.extend(best["attaccanti"])

    starter_keys = {_normalize_key(p.get("name")) for p in starters}
    remaining = [p for p in players if _normalize_key(p.get("name")) not in starter_keys]
    remain_by_role: Dict[str, List[Dict[str, Any]]] = {r: [] for r in ROLE_ORDER}
    for p in remaining:
        role = str(p.get("role") or "")
        if role in remain_by_role:
            remain_by_role[role].append(p)
    for role in ROLE_ORDER:
        remain_by_role[role] = sorted(
            remain_by_role[role],
            key=lambda x: (_safe_float(x.get("exp_fv"), 0.0), x.get("name", "")),
            reverse=True,
        )
    bench: List[Dict[str, Any]] = []
    if remain_by_role["P"]:
        bench.append(remain_by_role["P"][0])
    outfield = sorted(
        [*remain_by_role["D"], *remain_by_role["C"], *remain_by_role["A"]],
        key=lambda x: (_safe_float(x.get("exp_fv"), 0.0), x.get("name", "")),
        reverse=True,
    )
    bench.extend(outfield[:6])

    preferred_cap = str(captain_pref.get("capitano") or "").strip()
    preferred_vice = str(captain_pref.get("vice_capitano") or "").strip()
    starter_by_key = {_normalize_key(p.get("name")): p for p in starters}
    ranked_votes = sorted(starters, key=lambda p: (_safe_float(p.get("exp_vote"), 0.0), _safe_float(p.get("exp_fv"), 0.0)), reverse=True)

    cap = starter_by_key.get(_normalize_key(preferred_cap))
    if cap is None and ranked_votes:
        cap = ranked_votes[0]
    vice = starter_by_key.get(_normalize_key(preferred_vice))
    if vice is None:
        for cand in ranked_votes:
            if cap is None or _normalize_key(cand.get("name")) != _normalize_key(cap.get("name")):
                vice = cand
                break

    modifiers = regulation.get("modifiers") if isinstance(regulation, dict) else {}
    mod_d_cfg = modifiers.get("difesa") if isinstance(modifiers, dict) else {}
    mod_c_cfg = modifiers.get("capitano") if isinstance(modifiers, dict) else {}

    mod_d = 0.0
    if bool(mod_d_cfg.get("enabled")):
        defenders_votes = sorted([_safe_float(p.get("exp_vote"), 0.0) for p in best["difensori"]], reverse=True)
        requires = _safe_int(mod_d_cfg.get("requires_defenders_min"), 4)
        include_gk = bool(mod_d_cfg.get("include_goalkeeper_vote", True))
        if len(best["difensori"]) >= max(3, requires) and defenders_votes:
            sample = defenders_votes[:3]
            if include_gk and best["portiere"] is not None:
                sample = [_safe_float(best["portiere"].get("exp_vote"), 6.0), *sample]
            avg = sum(sample) / max(1, len(sample))
            bands = mod_d_cfg.get("bands") if isinstance(mod_d_cfg.get("bands"), list) else []
            mod_d = _evaluate_bands(avg, bands)

    mod_c = 0.0
    if bool(mod_c_cfg.get("enabled")) and cap is not None:
        cap_vote = _safe_float(cap.get("exp_vote"), 6.0)
        bands = mod_c_cfg.get("bands") if isinstance(mod_c_cfg.get("bands"), list) else []
        mod_c = _evaluate_bands(cap_vote, bands)

    base_total = _safe_float(best.get("base_total"), 0.0)
    live_total = round(base_total + mod_d + mod_c, 2)

    return {
        "module": best["module"],
        "portiere": best["portiere"],
        "difensori": best["difensori"],
        "centrocampisti": best["centrocampisti"],
        "attaccanti": best["attaccanti"],
        "panchina": bench,
        "capitano": cap,
        "vice_capitano": vice,
        "base_total": round(base_total, 2),
        "mod_difesa": round(mod_d, 2),
        "mod_capitano": round(mod_c, 2),
        "live_total": live_total,
    }


def _as_names(players: Iterable[Dict[str, Any]]) -> str:
    return ";".join(str(p.get("name") or "").strip() for p in players if str(p.get("name") or "").strip())


def _project_fantasy_round(
    round_no: int,
    roster_df: pd.DataFrame,
    classifica_df: pd.DataFrame,
    player_pool: pd.DataFrame,
    team_metrics: Dict[str, Dict[str, float]],
    round_ctx: Dict[str, Dict[str, Any]],
    regulation: Dict[str, Any],
    captains_pref: Dict[str, Dict[str, str]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    allowed_modules = _get_allowed_modules(regulation)
    optimizer = regulation.get("optimizer_context") if isinstance(regulation, dict) else {}
    fixture_cfg = optimizer.get("fixture_multiplier") if isinstance(optimizer, dict) else {}
    if not isinstance(fixture_cfg, dict):
        fixture_cfg = {}

    pool_by_name = {
        str(row["name_key"]): row
        for row in player_pool.to_dict(orient="records")
    }

    cls_subset = classifica_df.copy().sort_values("fantateam")

    lineup_rows: List[Dict[str, Any]] = []
    score_rows: List[Dict[str, Any]] = []

    for cls in cls_subset.to_dict(orient="records"):
        team_key = str(cls["fantateam_key"])
        team_name = str(cls["fantateam"])
        team_roster = roster_df[roster_df["fantateam_key"] == team_key]
        if team_roster.empty:
            continue

        projected_players: List[Dict[str, Any]] = []
        for row in team_roster.to_dict(orient="records"):
            name = str(row.get("name") or "").strip()
            name_key = str(row.get("name_key") or "")
            role = str(row.get("role") or "").strip().upper()
            if role not in ROLE_ORDER:
                continue
            club = str(row.get("club") or "")
            match = round_ctx.get(club)
            if not match:
                continue

            stats_row = pool_by_name.get(name_key, {})
            fallback = {
                "name": name,
                "club": club,
                "mv": 6.0,
                "fm": 6.0,
                "tier_weight": 0.5,
            }
            merged = {**fallback, **stats_row}
            exp_vote, exp_fv = _build_player_round_projection(
                merged,
                role,
                match,
                team_metrics,
                fixture_cfg,
            )
            projected_players.append(
                {
                    "name": name,
                    "role": role,
                    "club": club,
                    "exp_vote": exp_vote,
                    "exp_fv": exp_fv,
                }
            )

        if not projected_players:
            continue

        pref = captains_pref.get(team_key, {})
        lineup = _select_lineup_for_team(projected_players, allowed_modules, pref, regulation)

        line_row = {
            "round": round_no,
            "fantateam": team_name,
            "module": lineup["module"],
            "portiere": lineup["portiere"]["name"] if lineup["portiere"] else "",
            "difensori": _as_names(lineup["difensori"]),
            "centrocampisti": _as_names(lineup["centrocampisti"]),
            "attaccanti": _as_names(lineup["attaccanti"]),
            "panchina": _as_names(lineup["panchina"]),
            "capitano": lineup["capitano"]["name"] if lineup["capitano"] else "",
            "vice_capitano": lineup["vice_capitano"]["name"] if lineup["vice_capitano"] else "",
            "base_total": lineup["base_total"],
            "mod_difesa": lineup["mod_difesa"],
            "mod_capitano": lineup["mod_capitano"],
            "predicted_total": lineup["live_total"],
        }
        lineup_rows.append(line_row)
        score_rows.append(
            {
                "round": round_no,
                "fantateam": team_name,
                "predicted_total": lineup["live_total"],
            }
        )

    scores_df = pd.DataFrame(score_rows)
    if not scores_df.empty:
        scores_df = scores_df.sort_values(["predicted_total", "fantateam"], ascending=[False, True]).reset_index(drop=True)
        scores_df["position"] = scores_df.index + 1
        score_rows = scores_df[["round", "position", "fantateam", "predicted_total"]].to_dict(orient="records")
    return lineup_rows, score_rows


def _project_seriea_table(
    ctx_df: pd.DataFrame,
    match_rows: List[Dict[str, Any]],
) -> pd.DataFrame:
    base = ctx_df.copy()
    base["club"] = base["club"].astype(str)
    standings: Dict[str, Dict[str, float]] = {}
    for row in base.to_dict(orient="records"):
        club = str(row["club"])
        standings[club] = {
            "points": _safe_float(row.get("Pts"), 0.0),
            "gf": _safe_float(row.get("GF"), 0.0),
            "ga": _safe_float(row.get("GA"), 0.0),
        }

    for row in match_rows:
        home = _canonical_club(row["home_team"])
        away = _canonical_club(row["away_team"])
        hwin = _safe_float(row["home_win_prob"])
        draw = _safe_float(row["draw_prob"])
        awin = _safe_float(row["away_win_prob"])
        hxg = _safe_float(row["home_xg"])
        axg = _safe_float(row["away_xg"])
        if home not in standings:
            standings[home] = {"points": 0.0, "gf": 0.0, "ga": 0.0}
        if away not in standings:
            standings[away] = {"points": 0.0, "gf": 0.0, "ga": 0.0}
        standings[home]["points"] += 3.0 * hwin + draw
        standings[away]["points"] += 3.0 * awin + draw
        standings[home]["gf"] += hxg
        standings[home]["ga"] += axg
        standings[away]["gf"] += axg
        standings[away]["ga"] += hxg

    rows = []
    for club, vals in standings.items():
        gf = vals["gf"]
        ga = vals["ga"]
        pts = vals["points"]
        gd = gf - ga
        rows.append(
            {
                "squad": club.title(),
                "projected_pts": round(pts, 2),
                "projected_gf": round(gf, 2),
                "projected_ga": round(ga, 2),
                "projected_gd": round(gd, 2),
            }
        )
    df = pd.DataFrame(rows).sort_values(
        ["projected_pts", "projected_gd", "projected_gf", "squad"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    df.insert(0, "rank", df.index + 1)
    return df


def _project_fanta_final(
    classifica_df: pd.DataFrame,
    score_rows: List[Dict[str, Any]],
) -> pd.DataFrame:
    base = classifica_df[["fantateam", "fantateam_key", "points_current"]].copy()
    score_df = pd.DataFrame(score_rows)
    if score_df.empty:
        out = base.copy()
        out["predicted_gain"] = 0.0
        out["projected_total"] = out["points_current"]
        out = out.sort_values(["projected_total", "fantateam"], ascending=[False, True]).reset_index(drop=True)
        out.insert(0, "position", out.index + 1)
        return out

    score_df["team_key"] = score_df["fantateam"].apply(_normalize_key)
    gain = score_df.groupby("team_key", as_index=False)["predicted_total"].sum().rename(columns={"predicted_total": "predicted_gain"})
    out = base.merge(gain, left_on="fantateam_key", right_on="team_key", how="left")
    out["predicted_gain"] = out["predicted_gain"].fillna(0.0)
    out["projected_total"] = out["points_current"] + out["predicted_gain"]
    out = out.sort_values(["projected_total", "fantateam"], ascending=[False, True]).reset_index(drop=True)
    out.insert(0, "position", out.index + 1)
    out = out.drop(columns=["team_key"])
    return out


def run(start_round: int, end_round: int, outdir: Path) -> Dict[str, Path]:
    outdir.mkdir(parents=True, exist_ok=True)

    regulation = _load_regulation(DATA_DIR / "config" / "regolamento.json")
    ctx_df = _load_team_context()
    team_metrics = _build_team_metrics(ctx_df)
    league_gf = float(ctx_df["gf_per_match"].mean())
    fixtures = _load_fixtures(start_round, end_round)
    stats = _load_player_stats()
    tiers = _load_tiers()
    roster = _load_rosters()
    classifica = _load_classifica()
    captains_pref = _load_captains()
    player_pool = _build_player_pool(stats, tiers)

    players_by_club = {
        club: df.copy()
        for club, df in player_pool.groupby("club", dropna=False)
    }

    all_match_rows: List[Dict[str, Any]] = []
    all_lineup_rows: List[Dict[str, Any]] = []
    all_score_rows: List[Dict[str, Any]] = []

    rounds = sorted(fixtures["round"].dropna().astype(int).unique().tolist())
    for rnd in rounds:
        fixtures_round = fixtures[fixtures["round"] == rnd]
        match_rows, round_ctx = _build_round_match_predictions(fixtures_round, team_metrics, players_by_club, league_gf)
        all_match_rows.extend(match_rows)

        lineup_rows, score_rows = _project_fantasy_round(
            rnd,
            roster,
            classifica,
            player_pool,
            team_metrics,
            round_ctx,
            regulation,
            captains_pref,
        )
        all_lineup_rows.extend(lineup_rows)
        all_score_rows.extend(score_rows)

    match_df = pd.DataFrame(all_match_rows)
    seriea_final_df = _project_seriea_table(ctx_df, all_match_rows)
    lineup_df = pd.DataFrame(all_lineup_rows)
    score_df = pd.DataFrame(all_score_rows)
    fanta_final_df = _project_fanta_final(classifica, all_score_rows)

    match_path = outdir / f"seriea_predictions_round{start_round}_{end_round}.csv"
    seriea_table_path = outdir / f"seriea_final_table_projection_round{start_round}_{end_round}.csv"
    lineup_path = outdir / f"fantaportoscuso_lineups_projection_round{start_round}_{end_round}.csv"
    score_path = outdir / f"fantaportoscuso_round_scores_projection_round{start_round}_{end_round}.csv"
    final_path = outdir / f"fantaportoscuso_final_projection_round{start_round}_{end_round}.csv"
    seriea_xlsx_path = outdir / f"seriea_projection_round{start_round}_{end_round}.xlsx"
    fanta_xlsx_path = outdir / f"fantaportoscuso_projection_round{start_round}_{end_round}.xlsx"

    match_df.to_csv(match_path, index=False, encoding="utf-8")
    seriea_final_df.to_csv(seriea_table_path, index=False, encoding="utf-8")
    lineup_df.to_csv(lineup_path, index=False, encoding="utf-8")
    score_df.to_csv(score_path, index=False, encoding="utf-8")
    fanta_final_df.to_csv(final_path, index=False, encoding="utf-8")

    with pd.ExcelWriter(seriea_xlsx_path, engine="openpyxl") as writer:
        match_df.to_excel(writer, index=False, sheet_name="match_predictions")
        seriea_final_df.to_excel(writer, index=False, sheet_name="final_table")

    with pd.ExcelWriter(fanta_xlsx_path, engine="openpyxl") as writer:
        lineup_df.to_excel(writer, index=False, sheet_name="lineups")
        score_df.to_excel(writer, index=False, sheet_name="round_scores")
        fanta_final_df.to_excel(writer, index=False, sheet_name="final_table")

    return {
        "seriea_matches": match_path,
        "seriea_final_table": seriea_table_path,
        "seriea_workbook": seriea_xlsx_path,
        "fanta_lineups": lineup_path,
        "fanta_round_scores": score_path,
        "fanta_final_table": final_path,
        "fanta_workbook": fanta_xlsx_path,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Serie A + FantaPortoscuso projections by round window.")
    parser.add_argument("--start-round", type=int, default=25)
    parser.add_argument("--end-round", type=int, default=38)
    parser.add_argument("--outdir", default=str(REPORTS_DIR))
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    out = run(args.start_round, args.end_round, Path(args.outdir))
    print("Prediction reports generated:")
    for key, path in out.items():
        print(f"- {key}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
