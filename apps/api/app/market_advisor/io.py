from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from apps.api.app.utils.names import normalize_name

from .roles import best_role_from_set, normalize_role_candidates


def repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def data_dir() -> Path:
    return repo_root() / "data"


def reports_dir() -> Path:
    return data_dir() / "reports"


def history_dir() -> Path:
    return data_dir() / "history"


def runtime_data_dir() -> Path:
    return data_dir() / "runtime"


def _first_existing(candidates: Tuple[Path, ...]) -> Path:
    for path in candidates:
        if path.exists():
            return path
    return candidates[0]


def rose_path() -> Path:
    return data_dir() / "rose_fantaportoscuso.csv"


def quotazioni_path() -> Path:
    return data_dir() / "quotazioni.csv"


def stats_master_path() -> Path:
    return _first_existing(
        (
            runtime_data_dir() / "statistiche_giocatori.csv",
            data_dir() / "statistiche_giocatori.csv",
        )
    )


def stats_dir() -> Path:
    runtime_stats = runtime_data_dir() / "stats"
    if runtime_stats.exists():
        return runtime_stats
    return data_dir() / "stats"


def credits_path() -> Path:
    return data_dir() / "rose_nuovo_credits.csv"


TEAM_CONTEXT_CANDIDATES = (
    ("incoming", "manual", "seriea_context.csv"),
    ("config", "seriea_context.csv"),
)


def safe_float(value: object, default: float = 0.0) -> float:
    raw = str(value or "").strip()
    if not raw:
        return default
    raw = raw.replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return default


def safe_int(value: object, default: int = 0) -> int:
    return int(round(safe_float(value, float(default))))


def _team_token(value: str) -> str:
    token = normalize_name(value)
    token = token.replace("clubcrest", "")
    return token


def _first_existing_context_path() -> Path | None:
    base = data_dir()
    for parts in TEAM_CONTEXT_CANDIDATES:
        path = base.joinpath(*parts)
        if path.exists():
            return path
    return None


def load_team_context_scores() -> Dict[str, float]:
    """
    Returns a club-context score in [0, 100], built from table strength:
    75% Points-per-match + 25% Goal-difference normalization.
    """
    path = _first_existing_context_path()
    if path is None:
        return {}

    rows = read_csv_rows(path)
    if not rows:
        return {}

    def pick_col(candidates: List[str]) -> str:
        for candidate in candidates:
            if candidate in rows[0]:
                return candidate
        return ""

    squad_col = pick_col(["Squad", "Team", "squad", "team"])
    pts_mp_col = pick_col(["Pts/MP", "PtsPerMatch", "PointsPerMatch", "PPM"])
    gd_col = pick_col(["GD", "GoalDifference", "GoalDiff"])
    pts_col = pick_col(["Pts", "Points"])
    mp_col = pick_col(["MP", "Matches", "MatchesPlayed"])
    if not squad_col:
        return {}

    raw: Dict[str, Dict[str, float]] = {}
    pts_values: List[float] = []
    gd_values: List[float] = []
    for row in rows:
        club = _team_token(str(row.get(squad_col) or ""))
        if not club:
            continue
        pts_mp = safe_float(row.get(pts_mp_col), 0.0) if pts_mp_col else 0.0
        if pts_mp <= 0.0 and pts_col and mp_col:
            pts = safe_float(row.get(pts_col), 0.0)
            mp = safe_float(row.get(mp_col), 0.0)
            if mp > 0:
                pts_mp = pts / mp
        gd = safe_float(row.get(gd_col), 0.0) if gd_col else 0.0
        raw[club] = {"pts_mp": pts_mp, "gd": gd}
        if pts_mp > 0:
            pts_values.append(pts_mp)
        gd_values.append(gd)

    if not raw:
        return {}

    pts_min = min(pts_values) if pts_values else 0.5
    pts_max = max(pts_values) if pts_values else 2.5
    gd_min = min(gd_values) if gd_values else -25.0
    gd_max = max(gd_values) if gd_values else 25.0

    def scale(value: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 50.0
        pct = ((value - lo) / (hi - lo)) * 100.0
        return max(0.0, min(100.0, pct))

    out: Dict[str, float] = {}
    for club, metrics in raw.items():
        pts_score = scale(metrics.get("pts_mp", 0.0), pts_min, pts_max)
        gd_score = scale(metrics.get("gd", 0.0), gd_min, gd_max)
        out[club] = round((0.75 * pts_score) + (0.25 * gd_score), 2)
    return out


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: List[Dict[str, str]] = []
        for row in reader:
            clean: Dict[str, str] = {}
            for key, value in (row or {}).items():
                if key is None:
                    continue
                clean_key = str(key).strip().lstrip("\ufeff")
                clean[clean_key] = "" if value is None else str(value).strip()
            if clean:
                rows.append(clean)
        return rows


def write_csv_rows(path: Path, headers: Iterable[str], rows: Iterable[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(headers))
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in writer.fieldnames})


def load_roster_rows() -> List[Dict[str, str]]:
    return read_csv_rows(rose_path())


def load_quotazioni_rows() -> List[Dict[str, str]]:
    return read_csv_rows(quotazioni_path())


def load_stats_master_rows() -> List[Dict[str, str]]:
    return read_csv_rows(stats_master_path())


def load_stats_position_map() -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    folder = stats_dir()
    if not folder.exists():
        return mapping
    for path in folder.glob("*.csv"):
        for row in read_csv_rows(path):
            player = str(row.get("Giocatore") or "").strip()
            posizione = str(row.get("Posizione") or "").strip()
            if not player or not posizione:
                continue
            key = normalize_name(player)
            if key and key not in mapping:
                mapping[key] = posizione
    return mapping


def rows_by_normalized_name(rows: List[Dict[str, str]], key_col: str = "Giocatore") -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in rows:
        name = str(row.get(key_col) or "").strip()
        if not name:
            continue
        out[normalize_name(name)] = row
    return out


def build_player_universe() -> List[Dict[str, object]]:
    quot_rows = load_quotazioni_rows()
    stats_rows = load_stats_master_rows()
    stats_map = rows_by_normalized_name(stats_rows)
    pos_map = load_stats_position_map()
    team_context_scores = load_team_context_scores()

    players: List[Dict[str, object]] = []
    for row in quot_rows:
        name = str(row.get("Giocatore") or "").strip()
        club = str(row.get("Squadra") or "").strip()
        if not name or not club:
            continue
        key = normalize_name(name)
        stats = stats_map.get(key, {})
        club_key = _team_token(club)
        role_candidates = normalize_role_candidates(
            str(row.get("RuoloMantra") or ""),
            str(row.get("Ruolo") or ""),
            str(pos_map.get(key) or ""),
        )
        best_role = best_role_from_set(role_candidates)
        players.append(
            {
                "id": str(row.get("Id") or "").strip(),
                "name": name,
                "name_key": key,
                "club": club,
                "roles_all": sorted(role_candidates),
                "mantra_role_best": best_role,
                "quot_ruolo": str(row.get("Ruolo") or "").strip(),
                "quot_ruolo_mantra": str(row.get("RuoloMantra") or "").strip(),
                "prezzo_attuale": safe_float(row.get("PrezzoAttuale"), 0.0),
                "prezzo_iniziale": safe_float(row.get("PrezzoIniziale"), 0.0),
                "fvm": safe_float(row.get("FVM"), 0.0),
                "team_context": safe_float(team_context_scores.get(club_key), 50.0),
                "stats": stats,
            }
        )
    return players


def normalize_team_name(team_name: str) -> str:
    return str(team_name or "").strip()


def roster_for_team(team_name: str) -> List[Dict[str, str]]:
    target = normalize_name(normalize_team_name(team_name))
    return [
        row
        for row in load_roster_rows()
        if normalize_name(str(row.get("Team") or "")) == target
    ]


def market_snapshot_stamp(window_key: str, team_name: str) -> str:
    team_token = normalize_name(team_name) or "team"
    window_token = str(window_key).replace("/", "-").replace(" ", "_")
    return f"{team_token}_{window_token}"


def merge_roster_with_universe(team_rows: List[Dict[str, str]], universe: List[Dict[str, object]]) -> List[Dict[str, object]]:
    by_key: Dict[Tuple[str, str], Dict[str, object]] = {}
    by_name: Dict[str, Dict[str, object]] = {}
    for player in universe:
        name_key = str(player.get("name_key") or "")
        club = str(player.get("club") or "")
        by_name[name_key] = player
        by_key[(name_key, normalize_name(club))] = player

    merged: List[Dict[str, object]] = []
    for row in team_rows:
        name = str(row.get("Giocatore") or "").strip()
        if not name:
            continue
        key = normalize_name(name)
        club = str(row.get("Squadra") or "").strip()
        player = by_key.get((key, normalize_name(club))) or by_name.get(key)
        role_candidates = normalize_role_candidates(
            str(row.get("Ruolo") or ""),
            str((player or {}).get("quot_ruolo_mantra") or ""),
            str((player or {}).get("quot_ruolo") or ""),
        )
        merged.append(
            {
                "name": name,
                "name_key": key,
                "club": club,
                "roles_all": sorted(role_candidates),
                "mantra_role_best": best_role_from_set(role_candidates),
                "prezzo_acquisto": safe_float(row.get("PrezzoAcquisto"), 0.0),
                "prezzo_attuale_rosa": safe_float(row.get("PrezzoAttuale"), 0.0),
                "team": str(row.get("Team") or "").strip(),
                "stats": dict((player or {}).get("stats") or {}),
                "fvm": safe_float((player or {}).get("fvm"), 0.0),
                "prezzo_attuale": safe_float((player or {}).get("prezzo_attuale"), 0.0),
                "prezzo_iniziale": safe_float((player or {}).get("prezzo_iniziale"), 0.0),
                "team_context": safe_float((player or {}).get("team_context"), 50.0),
            }
        )
    return merged
