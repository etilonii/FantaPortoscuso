from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.app.market_advisor.features import attach_features
from apps.api.app.market_advisor.io import build_player_universe
from apps.api.app.market_advisor.scoring import compute_scores
from apps.api.app.utils.names import normalize_name


DATA_DIR = ROOT / "data"
OUT_TIERS = DATA_DIR / "player_tiers.csv"
OUT_REPORT = DATA_DIR / "reports" / "player_tiers_build_report.json"
NEW_ARRIVALS_PATH = DATA_DIR / "nuovi_arrivi_weights.csv"
TEAM_CONTEXT_CANDIDATES = [
    DATA_DIR / "config" / "seriea_context.csv",
    DATA_DIR / "config" / "seriea_context.tsv",
    DATA_DIR / "incoming" / "manual" / "seriea_context.csv",
    DATA_DIR / "incoming" / "manual" / "seriea_context.tsv",
    DATA_DIR / "incoming" / "manual" / "seriea_table.csv",
    DATA_DIR / "incoming" / "manual" / "seriea_table.tsv",
]
TEAM_ATTACK_CANDIDATES = [
    DATA_DIR / "config" / "seriea_team_per90.csv",
    DATA_DIR / "config" / "seriea_team_per90.tsv",
    DATA_DIR / "incoming" / "manual" / "seriea_team_per90.csv",
    DATA_DIR / "incoming" / "manual" / "seriea_team_attack.csv",
]
TEAM_GK_CANDIDATES = [
    DATA_DIR / "config" / "seriea_goalkeeper_context.csv",
    DATA_DIR / "config" / "seriea_goalkeeper_context.tsv",
    DATA_DIR / "incoming" / "manual" / "seriea_goalkeeper_context.csv",
    DATA_DIR / "incoming" / "manual" / "seriea_team_goalkeepers.csv",
]
TEAM_DISCIPLINE_CANDIDATES = [
    DATA_DIR / "config" / "seriea_discipline_context.csv",
    DATA_DIR / "config" / "seriea_discipline_context.tsv",
    DATA_DIR / "incoming" / "manual" / "seriea_discipline_context.csv",
    DATA_DIR / "incoming" / "manual" / "seriea_team_discipline.csv",
]
PLAYER_CONTEXT_CANDIDATES = [
    DATA_DIR / "config" / "seriea_players_context.csv",
    DATA_DIR / "config" / "seriea_players_context.tsv",
    DATA_DIR / "incoming" / "manual" / "seriea_players_context.csv",
    DATA_DIR / "incoming" / "manual" / "seriea_players_context.tsv",
]

ROLE_DYNAMIC_WEIGHTS = {
    "P": {
        "real": 0.22,
        "potential": 0.10,
        "mv": 0.16,
        "fm": 0.08,
        "availability": 0.10,
        "starter": 0.10,
        "context": 0.16,
        "micro": 0.05,
        "discipline": 0.03,
    },
    "D": {
        "real": 0.22,
        "potential": 0.12,
        "mv": 0.16,
        "fm": 0.08,
        "availability": 0.10,
        "starter": 0.10,
        "context": 0.12,
        "micro": 0.08,
        "discipline": 0.02,
    },
    "C": {
        "real": 0.21,
        "potential": 0.14,
        "mv": 0.14,
        "fm": 0.09,
        "availability": 0.10,
        "starter": 0.10,
        "context": 0.11,
        "micro": 0.09,
        "discipline": 0.02,
    },
    "A": {
        "real": 0.21,
        "potential": 0.16,
        "mv": 0.12,
        "fm": 0.09,
        "availability": 0.10,
        "starter": 0.10,
        "context": 0.10,
        "micro": 0.10,
        "discipline": 0.02,
    },
}

TIER_PERCENTILE_CUTS = {
    "top": 94.0,
    "semitop": 80.0,
    "starter": 45.0,
    "scommessa": 22.0,
}
MIN_STARTER_MINUTES_RATIO = 0.51
LOW_MAX_CREDITS = 3.0
HIGH_PRICE_MIN_BY_ROLE = {
    "P": 10.0,
    "D": 10.0,
    "C": 13.0,
    "A": 15.0,
}


def safe_float(value: object, default: float = 0.0) -> float:
    raw = str(value or "").strip()
    if not raw:
        return default
    raw = raw.replace("%", "").replace(" ", "")
    # Normalize decimal/thousands separators:
    # - "1.234,56" -> "1234.56"
    # - "123,45"   -> "123.45"
    # - "1,234"    -> "1234" (if exactly 3 trailing digits)
    if "," in raw and "." in raw:
        if raw.rfind(",") > raw.rfind("."):
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(",", "")
    elif "," in raw:
        left, right = raw.rsplit(",", 1)
        if right.isdigit() and len(right) == 3 and left.replace("-", "").replace("+", "").isdigit():
            raw = left + right
        else:
            raw = raw.replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return default


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def club_token(value: str) -> str:
    token = normalize_name(value)
    token = re.sub(r"^(ac|fc|ssc|us|asd|calcio)+", "", token)
    return token


def macro_role(mantra_role_best: str) -> str:
    role = str(mantra_role_best or "").strip()
    if role == "Por":
        return "P"
    if role in {"Dc", "Dd", "Ds", "B"}:
        return "D"
    if role in {"E", "M", "C", "T", "W"}:
        return "C"
    if role in {"A", "Pc"}:
        return "A"
    return "C"


def normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def clean_context_squad(value: str) -> str:
    raw = str(value or "").strip()
    raw = re.sub(r"^\d+\s+", "", raw)
    raw = re.sub(r"^club\s+crest\s+", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"^(club|team)\s+", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def parse_table_rows(path: Path) -> List[Dict[str, str]]:
    text = path.read_text(encoding="utf-8", errors="ignore")
    if not text.strip():
        return []
    try:
        dialect = csv.Sniffer().sniff(text[:4096], delimiters=",;\t|")
        delim = dialect.delimiter
    except csv.Error:
        delim = "\t" if "\t" in text else ","
    reader = csv.reader(text.splitlines(), delimiter=delim)
    rows_raw = [row for row in reader if any(str(cell or "").strip() for cell in row)]
    if not rows_raw:
        return []

    base_headers = [str(h or "").strip() for h in rows_raw[0]]
    headers: List[str] = []
    seen: Dict[str, int] = {}
    for raw_h in base_headers:
        h = raw_h or "col"
        seen[h] = seen.get(h, 0) + 1
        headers.append(h if seen[h] == 1 else f"{h}_{seen[h]}")

    out: List[Dict[str, str]] = []
    for row in rows_raw[1:]:
        if len(row) < len(headers):
            row = row + ([""] * (len(headers) - len(row)))
        elif len(row) > len(headers):
            row = row[: len(headers)]
        item = {headers[idx]: str(row[idx] or "").strip() for idx in range(len(headers))}
        # Skip repeated in-file headers (common in copied tables split by chunks/pages).
        first = str(row[0] or "").strip().lower()
        if first in {"rk", "rank", "#", "number"}:
            continue
        player_guess = str(item.get("Player", "") or item.get("Giocatore", "")).strip().lower()
        squad_guess = str(item.get("Squad", "") or item.get("Team", "")).strip().lower()
        if player_guess == "player" or squad_guess in {"squad", "team"}:
            continue
        out.append(item)
    return out


def choose_col(headers: List[str], aliases: List[str]) -> str:
    normalized = {normalize_header(h): h for h in headers}
    for alias in aliases:
        key = normalize_header(alias)
        if key in normalized:
            return normalized[key]
    # Fuzzy fallback for slight header variations.
    keys = list(normalized.keys())
    for alias in aliases:
        key = normalize_header(alias)
        for header_key in keys:
            if key and (key in header_key or header_key in key):
                return normalized[header_key]
    return ""


def load_team_context_scores() -> Tuple[Dict[str, float], str]:
    source_path: Path | None = None
    for candidate in TEAM_CONTEXT_CANDIDATES:
        if candidate.exists():
            source_path = candidate
            break
    if source_path is None:
        return {}, "none"

    rows = parse_table_rows(source_path)
    if not rows:
        return {}, f"empty:{source_path}"

    headers = list(rows[0].keys())
    squad_col = choose_col(headers, ["Squad", "Team"])
    pts_mp_col = choose_col(
        headers,
        ["Pts/MP", "PtsPerMatch", "PointsPerMatch", "Points/Match", "PointsPerGame", "PPM"],
    )
    gd_col = choose_col(headers, ["GD", "GoalDifference", "GoalDiff", "+/-"])
    pts_col = choose_col(headers, ["Pts", "Points"])
    mp_col = choose_col(headers, ["MP", "Matches", "MatchesPlayed"])

    if not squad_col:
        return {}, f"invalid:{source_path}"

    raw: Dict[str, Dict[str, float]] = {}
    pts_vals: List[float] = []
    gd_vals: List[float] = []

    for row in rows:
        squad = clean_context_squad(row.get(squad_col, ""))
        club = club_token(squad)
        if not club:
            continue

        pts_mp = safe_float(row.get(pts_mp_col, ""), 0.0) if pts_mp_col else 0.0
        if pts_mp <= 0.0 and pts_col and mp_col:
            pts = safe_float(row.get(pts_col, ""), 0.0)
            mp = safe_float(row.get(mp_col, ""), 0.0)
            if mp > 0:
                pts_mp = pts / mp

        gd = safe_float(row.get(gd_col, ""), 0.0) if gd_col else 0.0

        raw[club] = {"pts_mp": pts_mp, "gd": gd}
        if pts_mp > 0:
            pts_vals.append(pts_mp)
        gd_vals.append(gd)

    if not raw:
        return {}, f"invalid:{source_path}"

    pts_min = min(pts_vals) if pts_vals else 0.5
    pts_max = max(pts_vals) if pts_vals else 2.5
    gd_min = min(gd_vals) if gd_vals else -30.0
    gd_max = max(gd_vals) if gd_vals else 30.0

    out: Dict[str, float] = {}
    for club, metrics in raw.items():
        pts_mp = metrics.get("pts_mp", 0.0)
        gd = metrics.get("gd", 0.0)
        pts_score = _scale_to_100(pts_mp, pts_min, pts_max) if pts_mp > 0 else 50.0
        gd_score = _scale_to_100(gd, gd_min, gd_max)
        out[club] = (0.75 * pts_score) + (0.25 * gd_score)

    return out, str(source_path)


def load_team_total_minutes() -> Tuple[Dict[str, float], str]:
    source_path: Path | None = None
    for candidate in TEAM_CONTEXT_CANDIDATES:
        if candidate.exists():
            source_path = candidate
            break
    if source_path is None:
        return {}, "none"

    rows = parse_table_rows(source_path)
    if not rows:
        return {}, f"empty:{source_path}"

    headers = list(rows[0].keys())
    squad_col = choose_col(headers, ["Squad", "Team"])
    mp_col = choose_col(headers, ["MP", "Matches", "MatchesPlayed"])
    if not squad_col or not mp_col:
        return {}, f"invalid:{source_path}"

    out: Dict[str, float] = {}
    for row in rows:
        squad = clean_context_squad(row.get(squad_col, ""))
        club = club_token(squad)
        if not club:
            continue
        mp = safe_float(row.get(mp_col, ""), 0.0)
        if mp <= 0:
            continue
        out[club] = mp * 90.0
    return out, str(source_path)


def pick_existing_path(candidates: List[Path]) -> Path | None:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def scale_metric_map(raw: Dict[str, float], invert: bool = False) -> Dict[str, float]:
    if not raw:
        return {}
    values = list(raw.values())
    lo = min(values)
    hi = max(values)
    out: Dict[str, float] = {}
    for club, value in raw.items():
        score = _scale_to_100(value, lo, hi)
        if invert:
            score = 100.0 - score
        out[club] = clamp(score, 0.0, 100.0)
    return out


def load_team_attack_scores() -> Tuple[Dict[str, float], str]:
    source_path = pick_existing_path(TEAM_ATTACK_CANDIDATES)
    if source_path is None:
        return {}, "none"
    rows = parse_table_rows(source_path)
    if not rows:
        return {}, f"empty:{source_path}"
    headers = list(rows[0].keys())
    squad_col = choose_col(headers, ["Squad", "Team"])
    gpk90_col = choose_col(
        headers,
        ["G+A-PK", "GAPK90", "GAPk90", "Non-Penalty Goals + Assists/90", "NPGAP90"],
    )
    gls90_col = choose_col(headers, ["Gls", "Gls/90", "GoalsPer90", "Gls90", "Goals/90"])
    ast90_col = choose_col(headers, ["Ast", "Ast/90", "AssistsPer90", "Ast90", "Assists/90"])
    if not squad_col:
        return {}, f"invalid:{source_path}"

    raw: Dict[str, float] = {}
    for row in rows:
        club = club_token(clean_context_squad(row.get(squad_col, "")))
        if not club:
            continue
        gpk90 = safe_float(row.get(gpk90_col, ""), 0.0) if gpk90_col else 0.0
        gls90 = safe_float(row.get(gls90_col, ""), 0.0) if gls90_col else 0.0
        ast90 = safe_float(row.get(ast90_col, ""), 0.0) if ast90_col else 0.0
        base = gpk90 if gpk90 > 0 else (gls90 + ast90)
        if base <= 0:
            continue
        raw[club] = base

    scaled = scale_metric_map(raw, invert=False)
    return scaled, str(source_path)


def load_team_goalkeeper_scores() -> Tuple[Dict[str, float], str]:
    source_path = pick_existing_path(TEAM_GK_CANDIDATES)
    if source_path is None:
        return {}, "none"
    rows = parse_table_rows(source_path)
    if not rows:
        return {}, f"empty:{source_path}"
    headers = list(rows[0].keys())
    squad_col = choose_col(headers, ["Squad", "Team"])
    ga90_col = choose_col(headers, ["GA90", "GoalsAgainst90", "Goals Against/90"])
    save_col = choose_col(headers, ["Save%", "SavePct", "SavePerc", "Save Percentage"])
    cs_col = choose_col(headers, ["CS%", "CleanSheetPct", "Clean Sheet Percentage"])
    if not squad_col:
        return {}, f"invalid:{source_path}"

    ga90_raw: Dict[str, float] = {}
    save_raw: Dict[str, float] = {}
    cs_raw: Dict[str, float] = {}
    clubs: set[str] = set()
    for row in rows:
        club = club_token(clean_context_squad(row.get(squad_col, "")))
        if not club:
            continue
        clubs.add(club)
        ga90_raw[club] = safe_float(row.get(ga90_col, ""), 0.0) if ga90_col else 0.0
        save_raw[club] = safe_float(row.get(save_col, ""), 0.0) if save_col else 0.0
        cs_raw[club] = safe_float(row.get(cs_col, ""), 0.0) if cs_col else 0.0

    ga90_scaled = scale_metric_map(ga90_raw, invert=True)
    save_scaled = scale_metric_map(save_raw, invert=False)
    cs_scaled = scale_metric_map(cs_raw, invert=False)

    out: Dict[str, float] = {}
    for club in clubs:
        out[club] = clamp(
            (0.45 * save_scaled.get(club, 50.0))
            + (0.35 * cs_scaled.get(club, 50.0))
            + (0.20 * ga90_scaled.get(club, 50.0)),
            0.0,
            100.0,
        )
    return out, str(source_path)


def load_team_discipline_scores() -> Tuple[Dict[str, float], str]:
    source_path = pick_existing_path(TEAM_DISCIPLINE_CANDIDATES)
    if source_path is None:
        return {}, "none"
    rows = parse_table_rows(source_path)
    if not rows:
        return {}, f"empty:{source_path}"
    headers = list(rows[0].keys())
    squad_col = choose_col(headers, ["Squad", "Team"])
    y_col = choose_col(headers, ["CrdY", "YellowCards", "Yellow Cards"])
    r_col = choose_col(headers, ["CrdR", "RedCards", "Red Cards"])
    if not squad_col:
        return {}, f"invalid:{source_path}"

    raw_index: Dict[str, float] = {}
    for row in rows:
        club = club_token(clean_context_squad(row.get(squad_col, "")))
        if not club:
            continue
        y = safe_float(row.get(y_col, ""), 0.0) if y_col else 0.0
        r = safe_float(row.get(r_col, ""), 0.0) if r_col else 0.0
        # Lower is better -> inverted after scaling.
        raw_index[club] = (1.0 * y) + (4.0 * r)

    scaled = scale_metric_map(raw_index, invert=True)
    return scaled, str(source_path)


def load_team_context_profiles() -> Tuple[Dict[str, Dict[str, float]], Dict[str, str]]:
    attack_scores, attack_source = load_team_attack_scores()
    gk_scores, gk_source = load_team_goalkeeper_scores()
    discipline_scores, discipline_source = load_team_discipline_scores()
    clubs = set(attack_scores.keys()) | set(gk_scores.keys()) | set(discipline_scores.keys())
    profiles: Dict[str, Dict[str, float]] = {}
    for club in clubs:
        profiles[club] = {
            "attack": attack_scores.get(club, 50.0),
            "defense": gk_scores.get(club, 50.0),
            "discipline": discipline_scores.get(club, 50.0),
        }
    sources = {
        "attack_source": attack_source,
        "goalkeeper_source": gk_source,
        "discipline_source": discipline_source,
    }
    return profiles, sources


def load_player_context_profiles() -> Tuple[Dict[Tuple[str, str], Dict[str, float]], str]:
    source_path = pick_existing_path(PLAYER_CONTEXT_CANDIDATES)
    if source_path is None:
        return {}, "none"
    rows = parse_table_rows(source_path)
    if not rows:
        return {}, f"empty:{source_path}"
    headers = list(rows[0].keys())
    player_col = choose_col(headers, ["Player", "Giocatore", "name"])
    squad_col = choose_col(headers, ["Squad", "Team"])
    mp_col = choose_col(headers, ["MP", "Matches Played", "Matches"])
    starts_col = choose_col(headers, ["Starts"])
    min_col = choose_col(headers, ["Min", "Minutes"])
    ninety_col = choose_col(headers, ["90s", "90s Played"])
    # In copied FBref tables duplicated headers become suffixed (_2):
    # first block totals, second block per-90 metrics.
    gls90_col = choose_col(headers, ["Gls_2", "Gls/90", "Goals/90", "GoalsPer90"])
    ast90_col = choose_col(headers, ["Ast_2", "Ast/90", "Assists/90", "AssistsPer90"])
    gapk90_col = choose_col(
        headers,
        ["G+A-PK", "G+A-PK_2", "Non-Penalty Goals + Assists/90", "GAPK90", "NPGAP90"],
    )
    y_col = choose_col(headers, ["CrdY", "Yellow Cards", "YellowCards"])
    r_col = choose_col(headers, ["CrdR", "Red Cards", "RedCards"])

    if not player_col:
        return {}, f"invalid:{source_path}"

    out: Dict[Tuple[str, str], Dict[str, float]] = {}

    def upsert(key: Tuple[str, str], payload: Dict[str, float]) -> None:
        existing = out.get(key)
        if existing is None or payload.get("min", 0.0) > existing.get("min", 0.0):
            out[key] = payload

    for row in rows:
        raw_name = str(row.get(player_col, "")).strip()
        if not raw_name:
            continue
        name_key = normalize_name(raw_name)
        if not name_key:
            continue
        team_key = club_token(clean_context_squad(row.get(squad_col, ""))) if squad_col else ""
        mp = safe_float(row.get(mp_col, ""), 0.0) if mp_col else 0.0
        starts = safe_float(row.get(starts_col, ""), 0.0) if starts_col else 0.0
        minutes = safe_float(row.get(min_col, ""), 0.0) if min_col else 0.0
        n90 = safe_float(row.get(ninety_col, ""), 0.0) if ninety_col else 0.0
        gls90 = safe_float(row.get(gls90_col, ""), 0.0) if gls90_col else 0.0
        ast90 = safe_float(row.get(ast90_col, ""), 0.0) if ast90_col else 0.0
        gapk90 = safe_float(row.get(gapk90_col, ""), 0.0) if gapk90_col else 0.0
        y = safe_float(row.get(y_col, ""), 0.0) if y_col else 0.0
        r = safe_float(row.get(r_col, ""), 0.0) if r_col else 0.0

        payload = {
            "mp": mp,
            "starts": starts,
            "min": minutes,
            "90s": n90,
            "gls90": gls90,
            "ast90": ast90,
            "gapk90": gapk90,
            "yellow": y,
            "red": r,
        }
        if team_key:
            upsert((name_key, team_key), payload)
        # Generic fallback by name only (for slight team naming mismatches).
        upsert((name_key, ""), payload)

    if not out:
        return {}, f"invalid:{source_path}"
    return out, str(source_path)


def load_new_arrivals_weights(path: Path) -> Dict[str, float]:
    if not path.exists():
        return {}
    out: Dict[str, float] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = normalize_name(str(row.get("name") or ""))
            if not key:
                continue
            out[key] = clamp(safe_float(row.get("weight"), 0.0), 0.0, 1.0)
    return out


def _scale_to_100(value: float, low: float, high: float) -> float:
    if high <= low:
        return 50.0
    return clamp(((value - low) / (high - low)) * 100.0, 0.0, 100.0)


def _percentile_rank(values: List[float], value: float) -> float:
    n = len(values)
    if n <= 1:
        return 50.0
    less = 0
    equal = 0
    for current in values:
        if current < value:
            less += 1
        elif current == value:
            equal += 1
    rank = (less + max(0, equal - 1) * 0.5) / float(n - 1)
    return clamp(rank * 100.0, 0.0, 100.0)


def _weighted_blend(values: Dict[str, float], weights: Dict[str, float]) -> float:
    total = 0.0
    total_w = 0.0
    for key, weight in weights.items():
        if key not in values:
            continue
        total += weight * safe_float(values.get(key), 0.0)
        total_w += weight
    if total_w <= 0:
        return 50.0
    return clamp(total / total_w, 0.0, 100.0)


def _tier_from_percentile(pct: float) -> str:
    if pct >= TIER_PERCENTILE_CUTS["top"]:
        return "top"
    if pct >= TIER_PERCENTILE_CUTS["semitop"]:
        return "semitop"
    if pct >= TIER_PERCENTILE_CUTS["starter"]:
        return "starter"
    if pct >= TIER_PERCENTILE_CUTS["scommessa"]:
        return "scommessa"
    return "low"


def _tier_top_to_starter_only(pct: float) -> str:
    if pct >= TIER_PERCENTILE_CUTS["top"]:
        return "top"
    if pct >= TIER_PERCENTILE_CUTS["semitop"]:
        return "semitop"
    return "starter"


def build_rows(
    players: List[Dict[str, object]],
    newcomers: Dict[str, float],
    games_min: int,
    team_context_scores: Dict[str, float] | None = None,
    team_total_minutes: Dict[str, float] | None = None,
    team_context_profiles: Dict[str, Dict[str, float]] | None = None,
    player_context_profiles: Dict[Tuple[str, str], Dict[str, float]] | None = None,
) -> List[Dict[str, object]]:
    max_games_all = 1.0
    role_team_max_games: Dict[Tuple[str, str], float] = {}
    club_real_scores: Dict[str, List[float]] = {}
    for player in players:
        features = dict(player.get("features") or {})
        games = max(
            safe_float(features.get("games"), 0.0),
            safe_float((player.get("stats") or {}).get("Partite"), 0.0),
        )
        if games > max_games_all:
            max_games_all = games
        role_key = (
            club_token(str(player.get("club") or "")),
            macro_role(str(player.get("mantra_role_best") or "")),
        )
        current = role_team_max_games.get(role_key, 0.0)
        if games > current:
            role_team_max_games[role_key] = games
        club_key = club_token(str(player.get("club") or ""))
        club_real_scores.setdefault(club_key, []).append(
            clamp(safe_float(player.get("RealScore"), 0.0), 0.0, 100.0)
        )

    club_strength: Dict[str, float] = {}
    for club_key, values in club_real_scores.items():
        top_values = sorted(values, reverse=True)[:7]
        if top_values:
            club_strength[club_key] = sum(top_values) / float(len(top_values))
        else:
            club_strength[club_key] = 50.0

    # Blend local team context (league table) with internal club strength.
    # This improves realism for players in stronger/weaker team environments.
    if team_context_scores:
        for club_key, ctx_score in team_context_scores.items():
            if club_key in club_strength:
                club_strength[club_key] = (0.75 * club_strength[club_key]) + (0.25 * ctx_score)
            else:
                club_strength[club_key] = ctx_score

    if club_strength:
        min_ctx = min(club_strength.values())
        max_ctx = max(club_strength.values())
    else:
        min_ctx = 50.0
        max_ctx = 50.0

    role_micro_values: Dict[str, List[float]] = {}
    role_discipline_values: Dict[str, List[float]] = {}
    scored: List[Dict[str, object]] = []

    for player in players:
        name = str(player.get("name") or "").strip()
        if not name:
            continue
        name_key = str(player.get("name_key") or "")
        stats = dict(player.get("stats") or {})
        features = dict(player.get("features") or {})
        games = max(
            safe_float(features.get("games"), 0.0),
            safe_float(stats.get("Partite"), 0.0),
        )
        role = macro_role(str(player.get("mantra_role_best") or ""))
        real_score_pct = clamp(safe_float(player.get("RealScore"), 0.0), 0.0, 100.0)
        potential_score_pct = clamp(safe_float(player.get("PotentialScore"), 0.0), 0.0, 100.0)
        efficiency_pct = clamp(safe_float(player.get("CreditEfficiencyScore"), 0.0), 0.0, 100.0)

        avail_global = clamp(games / max_games_all, 0.0, 1.0)
        role_key = (
            club_token(str(player.get("club") or "")),
            role,
        )
        role_team_cap = max(1.0, role_team_max_games.get(role_key, games if games > 0 else 1.0))
        avail_role_team = clamp(games / role_team_cap, 0.0, 1.0)
        availability = clamp((avail_global + avail_role_team) / 2.0, 0.0, 1.0)
        club_key = club_token(str(player.get("club") or ""))

        # Optional player-level context:
        # improve starter vs reserve discrimination using Starts/Min/90s.
        starter_index = availability
        context_minutes = 0.0
        if player_context_profiles:
            ctx = player_context_profiles.get((name_key, club_key)) or player_context_profiles.get((name_key, ""))
            if ctx:
                mp_ctx = max(1.0, safe_float(ctx.get("mp"), 0.0))
                starts_ctx = safe_float(ctx.get("starts"), 0.0)
                context_minutes = safe_float(ctx.get("min"), 0.0)
                n90_ctx = safe_float(ctx.get("90s"), 0.0)
                start_rate = clamp(starts_ctx / mp_ctx, 0.0, 1.0)
                minute_rate = clamp(context_minutes / 1500.0, 0.0, 1.0)
                n90_rate = clamp(n90_ctx / 18.0, 0.0, 1.0)
                starter_presence = clamp((0.55 * start_rate) + (0.30 * n90_rate) + (0.15 * minute_rate), 0.35, 1.0)
                availability = clamp((0.60 * availability) + (0.40 * starter_presence), 0.0, 1.0)
                starter_index = starter_presence
        else:
            starter_index = availability

        # Minutes ratio over team total minutes (season-to-date).
        # Fallback to global match span if team total is unavailable.
        total_minutes = 0.0
        if team_total_minutes:
            total_minutes = safe_float(team_total_minutes.get(club_key), 0.0)
        if total_minutes <= 0:
            total_minutes = max_games_all * 90.0
        if total_minutes <= 0:
            minutes_ratio_total = 0.0
        else:
            minutes_played = context_minutes if context_minutes > 0 else (games * 90.0)
            minutes_ratio_total = clamp(minutes_played / total_minutes, 0.0, 1.0)

        mv_raw = safe_float(stats.get("Mediavoto"), safe_float(features.get("mv"), 0.0))
        fm_raw = safe_float(stats.get("Fantamedia"), safe_float(features.get("fm"), 0.0))
        if role == "P":
            mv_score = _scale_to_100(mv_raw, 5.90, 6.60)
            fm_score = _scale_to_100(fm_raw, 4.50, 6.00)
        else:
            mv_score = _scale_to_100(mv_raw, 5.80, 6.80)
            fm_score = _scale_to_100(fm_raw, 6.00, 8.20)

        club_ctx_raw = club_strength.get(club_key, 50.0)
        club_ctx = _scale_to_100(club_ctx_raw, min_ctx, max_ctx)
        attack_ctx = 50.0
        defense_ctx = 50.0
        discipline_ctx = 50.0
        if team_context_profiles and club_key in team_context_profiles:
            profile = team_context_profiles[club_key]
            attack_ctx = safe_float(profile.get("attack"), 50.0)
            defense_ctx = safe_float(profile.get("defense"), 50.0)
            discipline_ctx = safe_float(profile.get("discipline"), 50.0)
        if role == "A":
            role_ctx = (0.55 * attack_ctx) + (0.20 * club_ctx) + (0.15 * defense_ctx) + (0.10 * discipline_ctx)
        elif role == "C":
            role_ctx = (0.35 * attack_ctx) + (0.35 * club_ctx) + (0.20 * defense_ctx) + (0.10 * discipline_ctx)
        elif role == "D":
            role_ctx = (0.20 * attack_ctx) + (0.25 * club_ctx) + (0.45 * defense_ctx) + (0.10 * discipline_ctx)
        else:
            role_ctx = (0.10 * attack_ctx) + (0.20 * club_ctx) + (0.60 * defense_ctx) + (0.10 * discipline_ctx)

        gol_pg = safe_float(features.get("gol_pg"), 0.0)
        assist_pg = safe_float(features.get("assist_pg"), 0.0)
        decisive_pg = safe_float(features.get("decisive_pg"), 0.0)
        clean_pg = safe_float(features.get("clean_pg"), 0.0)
        concede_pg = safe_float(features.get("concede_pg"), 0.0)
        rigori_parati_pg = safe_float(features.get("rigori_parati_pg"), 0.0)
        discipline_pg = safe_float(features.get("discipline_pg"), 0.0)

        # Raw role-dependent micro-impact index, then converted to role percentiles.
        if role == "P":
            micro_raw = (2.2 * clean_pg) + (2.5 * rigori_parati_pg) - (0.70 * concede_pg)
        elif role == "D":
            micro_raw = (2.3 * gol_pg) + (2.1 * assist_pg) + (1.2 * clean_pg) + (1.2 * decisive_pg) - (0.55 * discipline_pg)
        elif role == "C":
            micro_raw = (2.1 * gol_pg) + (2.0 * assist_pg) + (1.4 * decisive_pg) - (0.50 * discipline_pg)
        else:
            micro_raw = (2.8 * gol_pg) + (1.9 * assist_pg) + (1.6 * decisive_pg) - (0.45 * discipline_pg)

        role_micro_values.setdefault(role, []).append(micro_raw)
        role_discipline_values.setdefault(role, []).append(discipline_pg)

        metric_values = {
            "real": real_score_pct,
            "potential": potential_score_pct,
            "mv": mv_score,
            "fm": fm_score,
            "availability": availability * 100.0,
            "starter": starter_index * 100.0,
            "context": clamp(role_ctx, 0.0, 100.0),
            # Filled on second pass via role percentiles:
            # "micro": ...,
            # "discipline": ...,
            # Optional fallback metric if needed.
            "efficiency": efficiency_pct,
        }

        newcomer_weight = newcomers.get(name_key)
        is_new = 1 if newcomer_weight is not None else 0
        prezzo_attuale = clamp(safe_float(player.get("prezzo_attuale"), 0.0), 0.0, 999.0)

        scored.append(
            {
                "name": name,
                "name_key": name_key,
                "team": str(player.get("club") or "").strip(),
                "role": role,
                "partite": str(int(round(safe_float(stats.get("Partite"), 0.0)))),
                "mv": f"{safe_float(stats.get('Mediavoto'), 0.0):.2f}".rstrip("0").rstrip("."),
                "fm": f"{safe_float(stats.get('Fantamedia'), 0.0):.2f}".rstrip("0").rstrip("."),
                "is_new_arrival": str(is_new),
                "_metrics": metric_values,
                "_micro_raw": micro_raw,
                "_discipline_pg": discipline_pg,
                "_newcomer_weight": newcomer_weight,
                "_games": games,
                "_games_min": float(max(1, games_min)),
                "_minutes_ratio_total": minutes_ratio_total,
                "_prezzo_attuale": prezzo_attuale,
            }
        )

    # Second pass: role-relative percentiles for impact/discipline and final strength.
    role_strength_values: Dict[str, List[float]] = {}
    for row in scored:
        role = str(row.get("role") or "C")
        micro_values = role_micro_values.get(role, [])
        disc_values = role_discipline_values.get(role, [])
        micro_pct = _percentile_rank(micro_values, safe_float(row.get("_micro_raw"), 0.0)) if micro_values else 50.0
        disc_pct = 100.0 - _percentile_rank(disc_values, safe_float(row.get("_discipline_pg"), 0.0)) if disc_values else 50.0

        metrics = dict(row.get("_metrics") or {})
        metrics["micro"] = micro_pct
        metrics["discipline"] = clamp(disc_pct, 0.0, 100.0)

        role_weights = ROLE_DYNAMIC_WEIGHTS.get(role, ROLE_DYNAMIC_WEIGHTS["C"])
        strength_score = _weighted_blend(metrics, role_weights)

        # If availability is low, cap unrealistic spikes but keep it data-driven.
        games = safe_float(row.get("_games"), 0.0)
        games_min_local = safe_float(row.get("_games_min"), 10.0)
        if games < games_min_local:
            damp = clamp(0.70 + (0.30 * (games / max(1.0, games_min_local))), 0.70, 1.0)
            strength_score *= damp

        row["_metrics"] = metrics
        row["_strength_score"] = clamp(strength_score, 0.0, 100.0)
        role_strength_values.setdefault(role, []).append(float(row["_strength_score"]))

    out: List[Dict[str, object]] = []
    for row in scored:
        role = str(row.get("role") or "C")
        role_values = role_strength_values.get(role, [])
        strength = safe_float(row.get("_strength_score"), 50.0)
        strength_pct = _percentile_rank(role_values, strength) if role_values else 50.0
        tier = _tier_from_percentile(strength_pct)
        minutes_ratio_total = safe_float(row.get("_minutes_ratio_total"), 0.0)
        prezzo_attuale = safe_float(row.get("_prezzo_attuale"), 0.0)
        high_price_min = safe_float(HIGH_PRICE_MIN_BY_ROLE.get(role, 999.0), 999.0)
        is_high_price_exception = prezzo_attuale >= high_price_min

        # Rule 1: to be at least starter, require >=51% played minutes.
        # Exception: under 51% minutes but high current price by role
        # should never be "scommessa"; force one among starter/semitop/top
        # using the same percentile logic.
        if minutes_ratio_total < MIN_STARTER_MINUTES_RATIO:
            if is_high_price_exception:
                tier = _tier_top_to_starter_only(strength_pct)
            elif tier in {"top", "semitop", "starter"}:
                tier = "scommessa"

        # Rule 2: low tier only for very low price (<=3 credits).
        # Everyone else previously in low moves to scommessa.
        if tier == "low" and prezzo_attuale > LOW_MAX_CREDITS:
            tier = "scommessa"

        metrics = dict(row.get("_metrics") or {})
        real_score_pct = safe_float(metrics.get("real"), 50.0)
        availability_pct = safe_float(metrics.get("availability"), 50.0)

        # Keep output compatible with existing downstream scripts (0..0.99 fields).
        score_auto = clamp(((0.88 * strength) + (0.12 * real_score_pct)) / 100.0, 0.0, 0.99)
        weight = clamp((0.72 * score_auto) + (0.28 * (availability_pct / 100.0)), 0.0, 0.99)

        newcomer_weight = row.get("_newcomer_weight")
        if newcomer_weight is not None:
            weight = clamp(max(weight, safe_float(newcomer_weight, 0.0)), 0.0, 0.99)

        out.append(
            {
                "name": str(row.get("name") or ""),
                "team": str(row.get("team") or ""),
                "role": role,
                "tier": tier,
                "weight": f"{weight:.3f}",
                "score_auto": f"{score_auto:.3f}",
                "partite": str(row.get("partite") or "0"),
                "mv": str(row.get("mv") or ""),
                "fm": str(row.get("fm") or ""),
                "is_new_arrival": str(row.get("is_new_arrival") or "0"),
            }
        )

    out.sort(
        key=lambda row: (
            -safe_float(row.get("score_auto"), 0.0),
            -safe_float(row.get("weight"), 0.0),
            str(row.get("name") or "").lower(),
        )
    )
    return out


def write_tiers(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "name",
        "team",
        "role",
        "tier",
        "weight",
        "score_auto",
        "partite",
        "mv",
        "fm",
        "is_new_arrival",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_report(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rigenera data/player_tiers.csv usando solo dati locali."
    )
    parser.add_argument("--games-min", type=int, default=10)
    parser.add_argument("--out", default=str(OUT_TIERS))
    parser.add_argument("--report", default=str(OUT_REPORT))
    args = parser.parse_args()

    players = build_player_universe()
    if not players:
        raise RuntimeError("Universo giocatori vuoto: controlla data/quotazioni.csv")

    attach_features(players, games_min=max(1, int(args.games_min)))
    compute_scores(players, in_cost_source="current")

    newcomers = load_new_arrivals_weights(NEW_ARRIVALS_PATH)
    team_context_scores, team_context_source = load_team_context_scores()
    team_total_minutes, team_total_minutes_source = load_team_total_minutes()
    team_context_profiles, team_context_profile_sources = load_team_context_profiles()
    player_context_profiles, player_context_source = load_player_context_profiles()
    output_rows = build_rows(
        players,
        newcomers,
        games_min=max(1, int(args.games_min)),
        team_context_scores=team_context_scores,
        team_total_minutes=team_total_minutes,
        team_context_profiles=team_context_profiles,
        player_context_profiles=player_context_profiles,
    )

    out_path = Path(args.out)
    write_tiers(out_path, output_rows)

    report_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_mode": "local_only",
        "team_context_source": team_context_source,
        "team_context_teams": len(team_context_scores),
        "team_total_minutes_source": team_total_minutes_source,
        "team_total_minutes_teams": len(team_total_minutes),
        "team_context_profile_teams": len(team_context_profiles),
        "team_context_profile_sources": team_context_profile_sources,
        "player_context_source": player_context_source,
        "player_context_rows": len(player_context_profiles),
        "players_total": len(players),
        "rows_written": len(output_rows),
        "output": str(out_path),
    }
    write_report(Path(args.report), report_payload)

    print(f"[tiers] written: {out_path}")
    print(f"[tiers] rows: {len(output_rows)}")
    print("[tiers] source_mode: local_only")
    print(f"[tiers] team_context_source: {team_context_source}")
    print(f"[tiers] team_context_teams: {len(team_context_scores)}")
    print(f"[tiers] team_total_minutes_source: {team_total_minutes_source}")
    print(f"[tiers] team_total_minutes_teams: {len(team_total_minutes)}")
    print(f"[tiers] team_context_profile_teams: {len(team_context_profiles)}")
    print(f"[tiers] player_context_source: {player_context_source}")
    print(f"[tiers] player_context_rows: {len(player_context_profiles)}")


if __name__ == "__main__":
    main()

