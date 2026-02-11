import csv
import json
import re
from collections import defaultdict
from datetime import datetime
from html import unescape as html_unescape
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

from fastapi import APIRouter, Query, Body, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from apps.api.app.backup import run_backup_fail_fast
from apps.api.app.auth_utils import access_key_from_bearer
from apps.api.app.config import BACKUP_DIR, BACKUP_KEEP_LAST, DATABASE_URL
from apps.api.app.deps import get_db
from apps.api.app.models import (
    AccessKey,
    Fixture,
    LiveFixtureFlag,
    LivePlayerVote,
    Player,
    PlayerStats,
    ScheduledJobState,
    Team,
    TeamKey,
)
from apps.api.app.utils.names import normalize_name, strip_star, is_starred


router = APIRouter(prefix="/data", tags=["data"])

DATA_DIR = Path(__file__).resolve().parents[4] / "data"
ROSE_PATH = DATA_DIR / "rose_fantaportoscuso.csv"
QUOT_PATH = DATA_DIR / "quotazioni.csv"
RESIDUAL_CREDITS_PATH = DATA_DIR / "rose_nuovo_credits.csv"
STATS_PATH = DATA_DIR / "statistiche_giocatori.csv"
MARKET_PATH = DATA_DIR / "market_latest.json"
STARTING_XI_REPORT_PATH = DATA_DIR / "reports" / "team_starting_xi.csv"
PLAYER_STRENGTH_REPORT_PATH = DATA_DIR / "reports" / "team_strength_players.csv"
REAL_FORMATIONS_FILE_CANDIDATES = [
    DATA_DIR / "reports" / "formazioni_giornata.csv",
    DATA_DIR / "reports" / "formazioni_giornata.xlsx",
    DATA_DIR / "reports" / "formazioni_reali.csv",
    DATA_DIR / "reports" / "formazioni_reali.xlsx",
    DATA_DIR / "db" / "formazioni_giornata.csv",
]
REAL_FORMATIONS_DIR_CANDIDATES = [
    DATA_DIR / "incoming" / "formazioni",
    DATA_DIR / "incoming" / "lineups",
]
REAL_FORMATIONS_TMP_DIR = DATA_DIR / "tmp"
REAL_FORMATIONS_APPKEY_GLOB = "formazioni*_appkey.json"
REAL_FORMATIONS_CONTEXT_HTML_CANDIDATES = [
    REAL_FORMATIONS_TMP_DIR / "formazioni_page.html",
]
VOTI_PAGE_CACHE_PATH = DATA_DIR / "tmp" / "voti_page.html"
VOTI_BASE_URL = "https://www.fantacalcio.it/voti-fantacalcio-serie-a"
STATUS_PATH = DATA_DIR / "status.json"
MARKET_REPORT_GLOB = "rose_changes_*.csv"
ROSE_DIFF_GLOB = "diff_rose_*.txt"
STATS_DIR = DATA_DIR / "stats"
STATS_MASTER_HEADERS: Tuple[str, ...] = (
    "Giocatore",
    "Squadra",
    "Gol",
    "Autogol",
    "RigoriParati",
    "RigoriSegnati",
    "RigoriSbagliati",
    "Assist",
    "Ammonizioni",
    "Espulsioni",
    "Cleansheet",
    "Partite",
    "Mediavoto",
    "Fantamedia",
    "GolVittoria",
    "GolPareggio",
    "GolSubiti",
)
STATS_RANK_FILE_MAP: Tuple[Tuple[str, str], ...] = (
    ("Gol", "gol.csv"),
    ("Assist", "assist.csv"),
    ("Ammonizioni", "ammonizioni.csv"),
    ("Espulsioni", "espulsioni.csv"),
    ("Cleansheet", "cleansheet.csv"),
    ("Autogol", "autogol.csv"),
)
LIVE_EVENT_TO_STATS_COLUMN: Dict[str, str] = {
    "goal": "Gol",
    "assist": "Assist",
    "assist_da_fermo": "Assist",
    "rigore_segnato": "RigoriSegnati",
    "rigore_parato": "RigoriParati",
    "rigore_sbagliato": "RigoriSbagliati",
    "autogol": "Autogol",
    "gol_subito_portiere": "GolSubiti",
    "ammonizione": "Ammonizioni",
    "espulsione": "Espulsioni",
    "gol_vittoria": "GolVittoria",
    "gol_pareggio": "GolPareggio",
}
PLAYER_CARDS_PATH = DATA_DIR / "db" / "quotazioni_master.csv"
PLAYER_STATS_PATH = DATA_DIR / "db" / "player_stats.csv"
TEAMS_PATH = DATA_DIR / "db" / "teams.csv"
FIXTURES_PATH = DATA_DIR / "db" / "fixtures.csv"
REGULATION_PATH = DATA_DIR / "config" / "regolamento.json"
SEED_DB_DIR = Path("/app/seed/db")
ROSE_XLSX_DIR = DATA_DIR / "archive" / "incoming" / "rose"
TABULAR_EXTENSIONS = {".csv", ".xlsx", ".xls"}
_RESIDUAL_CREDITS_CACHE: Dict[str, object] = {}
_NAME_LIST_CACHE: Dict[str, object] = {}
_LISTONE_NAME_CACHE: Dict[str, object] = {}
_PLAYER_FORCE_CACHE: Dict[str, object] = {}
_REGULATION_CACHE: Dict[str, object] = {}

LIVE_EVENT_FIELDS: Tuple[str, ...] = (
    "goal",
    "assist",
    "assist_da_fermo",
    "rigore_segnato",
    "rigore_parato",
    "rigore_sbagliato",
    "autogol",
    "gol_subito_portiere",
    "ammonizione",
    "espulsione",
    "gol_vittoria",
    "gol_pareggio",
)

FORMATION_ROLE_ORDER: Tuple[str, ...] = ("P", "D", "C", "A")
FORMATION_OUTFIELD_ROLES: Tuple[str, ...] = ("D", "C", "A")
RESERVE_GENERIC_COLUMNS: Tuple[str, ...] = (
    "panchina",
    "panchinari",
    "panchina_ordine",
    "riserve",
    "riserva",
    "bench",
    "reserves",
)
RESERVE_ROLE_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "P": (
        "panchina_portieri",
        "panchina_portiere",
        "panchina_p",
        "riserve_portieri",
        "riserva_portieri",
        "bench_gk",
        "bench_goalkeepers",
    ),
    "D": (
        "panchina_difensori",
        "panchina_difensore",
        "panchina_d",
        "riserve_difensori",
        "riserva_difensori",
        "bench_defenders",
        "bench_d",
    ),
    "C": (
        "panchina_centrocampisti",
        "panchina_centrocampista",
        "panchina_c",
        "riserve_centrocampisti",
        "riserva_centrocampisti",
        "bench_midfielders",
        "bench_c",
    ),
    "A": (
        "panchina_attaccanti",
        "panchina_attaccante",
        "panchina_a",
        "riserve_attaccanti",
        "riserva_attaccanti",
        "bench_forwards",
        "bench_attackers",
        "bench_a",
    ),
}


def _default_regulation() -> Dict[str, object]:
    return {
        "scoring": {
            "default_vote": 6.0,
            "default_fantavote": 6.0,
            "six_politico": {"vote": 6.0, "fantavote": 6.0},
            "bonus_malus": {
                "goal": 3.0,
                "assist": 1.0,
                "assist_da_fermo": 1.0,
                "rigore_segnato": 3.0,
                "rigore_parato": 3.0,
                "rigore_sbagliato": -3.0,
                "autogol": -2.0,
                "gol_subito_portiere": -1.0,
                "ammonizione": -0.5,
                "espulsione": -1.0,
                "gol_vittoria": 1.0,
                "gol_pareggio": 0.5,
            },
        },
        "modifiers": {
            "difesa": {"enabled": False, "use_goalkeeper": True, "bands": []},
            "capitano": {"enabled": False, "bands": []},
        },
        "ordering": {"default": "classifica", "allowed": ["classifica", "live_total"]},
    }


def _load_regulation() -> Dict[str, object]:
    if not REGULATION_PATH.exists():
        return _default_regulation()

    mtime = REGULATION_PATH.stat().st_mtime
    cached = _REGULATION_CACHE.get(str(REGULATION_PATH))
    if cached and cached.get("mtime") == mtime:
        return cached.get("data", _default_regulation())

    try:
        parsed = json.loads(REGULATION_PATH.read_text(encoding="utf-8"))
        if not isinstance(parsed, dict):
            parsed = _default_regulation()
    except Exception:
        parsed = _default_regulation()

    _REGULATION_CACHE[str(REGULATION_PATH)] = {"mtime": mtime, "data": parsed}
    return parsed


class LiveMatchToggleRequest(BaseModel):
    round: int = Field(ge=1, le=99)
    home_team: str = Field(min_length=1, max_length=64)
    away_team: str = Field(min_length=1, max_length=64)
    six_politico: bool = False


class LivePlayerVoteRequest(BaseModel):
    round: int = Field(ge=1, le=99)
    team: str = Field(min_length=1, max_length=64)
    player: str = Field(min_length=1, max_length=128)
    role: Optional[str] = Field(default=None, max_length=8)
    vote: Optional[str] = None
    fantavote: Optional[str] = None
    goal: Optional[int] = Field(default=0, ge=0, le=20)
    assist: Optional[int] = Field(default=0, ge=0, le=20)
    assist_da_fermo: Optional[int] = Field(default=0, ge=0, le=20)
    rigore_segnato: Optional[int] = Field(default=0, ge=0, le=20)
    rigore_parato: Optional[int] = Field(default=0, ge=0, le=20)
    rigore_sbagliato: Optional[int] = Field(default=0, ge=0, le=20)
    autogol: Optional[int] = Field(default=0, ge=0, le=20)
    gol_subito_portiere: Optional[int] = Field(default=0, ge=0, le=20)
    ammonizione: Optional[int] = Field(default=0, ge=0, le=20)
    espulsione: Optional[int] = Field(default=0, ge=0, le=20)
    gol_vittoria: Optional[int] = Field(default=0, ge=0, le=20)
    gol_pareggio: Optional[int] = Field(default=0, ge=0, le=20)
    is_sv: bool = False
    is_absent: bool = False


class LiveImportVotesRequest(BaseModel):
    round: int = Field(ge=1, le=99)
    season: Optional[str] = Field(default=None, max_length=16)
    source_url: Optional[str] = Field(default=None, max_length=512)
    source_html: Optional[str] = Field(default=None, max_length=3_000_000)


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            if not row:
                continue
            cleaned = {}
            for key, value in row.items():
                if key is None:
                    continue
                clean_key = key.strip().lstrip("\ufeff")
                cleaned[clean_key] = value
            rows.append(cleaned)
        return rows


def _clean_row_keys(row: Dict[object, object]) -> Dict[str, str]:
    cleaned: Dict[str, str] = {}
    for key, value in row.items():
        if key is None:
            continue
        clean_key = str(key).strip().lstrip("\ufeff")
        cleaned[clean_key] = "" if value is None else str(value)
    return cleaned


def _read_tabular_rows(path: Path) -> List[Dict[str, str]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _read_csv(path)
    if suffix not in {".xlsx", ".xls"}:
        return []

    try:
        import pandas as pd

        rows: List[Dict[str, str]] = []
        sheets = pd.read_excel(path, sheet_name=None)
        if not isinstance(sheets, dict):
            return []
        for _, frame in sheets.items():
            if frame is None or frame.empty:
                continue
            frame = frame.fillna("")
            for _, row in frame.iterrows():
                cleaned = _clean_row_keys(row.to_dict())
                if any(str(v).strip() for v in cleaned.values()):
                    rows.append(cleaned)
        return rows
    except Exception:
        return []


def _latest_supported_file(folder: Path) -> Optional[Path]:
    if not folder.exists() or not folder.is_dir():
        return None
    files = [
        p
        for p in folder.iterdir()
        if p.is_file() and p.suffix.lower() in TABULAR_EXTENSIONS
    ]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def _count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            return sum(1 for _ in handle)
    except Exception:
        return 0


def _split_players_cell(value: str | None) -> List[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    return [item.strip() for item in re.split(r"[;\n|]+", raw) if item and item.strip()]


def _normalize_module(value: object) -> str:
    raw = re.sub(r"[^0-9]", "", str(value or ""))
    if len(raw) != 3:
        return ""
    try:
        if sum(int(ch) for ch in raw) != 10:
            return ""
    except ValueError:
        return ""
    return raw


def _format_module(value: object) -> str:
    normalized = _normalize_module(value)
    if not normalized:
        return str(value or "").strip()
    return f"{normalized[0]}-{normalized[1]}-{normalized[2]}"


def _role_from_text(value: object) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    if raw in FORMATION_ROLE_ORDER:
        return raw
    if "POR" in raw or "GK" in raw:
        return "P"
    if "DIF" in raw or "DEF" in raw:
        return "D"
    if "CEN" in raw or "MID" in raw:
        return "C"
    if "ATT" in raw or "FWD" in raw or "ST" in raw:
        return "A"

    first_hits: List[Tuple[int, str]] = []
    for role in FORMATION_ROLE_ORDER:
        idx = raw.find(role)
        if idx >= 0:
            first_hits.append((idx, role))
    if first_hits:
        first_hits.sort(key=lambda item: item[0])
        return first_hits[0][1]
    return ""


def _module_counts_from_str(module_value: object) -> Optional[Dict[str, int]]:
    module = _normalize_module(module_value)
    if not module:
        return None
    return {
        "P": 1,
        "D": int(module[0]),
        "C": int(module[1]),
        "A": int(module[2]),
    }


def _module_from_role_counts(counts: Dict[str, int]) -> str:
    p_count = int(counts.get("P", 0))
    d_count = int(counts.get("D", 0))
    c_count = int(counts.get("C", 0))
    a_count = int(counts.get("A", 0))
    if p_count != 1:
        return ""
    if d_count + c_count + a_count != 10:
        return ""
    return f"{d_count}{c_count}{a_count}"


def _allowed_modules_from_regulation(regulation: Dict[str, object]) -> List[str]:
    formation_rules = regulation.get("formation_rules") if isinstance(regulation, dict) else {}
    formation_rules = formation_rules if isinstance(formation_rules, dict) else {}
    allowed_raw = formation_rules.get("allowed_modules")
    if not isinstance(allowed_raw, list):
        allowed_raw = []
    allowed: List[str] = []
    for value in allowed_raw:
        normalized = _normalize_module(value)
        if normalized and normalized not in allowed:
            allowed.append(normalized)
    return allowed


def _lineup_role_counts(entries: List[Dict[str, str]]) -> Dict[str, int]:
    counts = {role: 0 for role in FORMATION_ROLE_ORDER}
    for entry in entries:
        role = _role_from_text(entry.get("role"))
        if role:
            counts[role] = int(counts.get(role, 0)) + 1
    return counts


def _extract_reserve_players(
    normalized_row: Dict[str, str],
    role_map: Dict[str, str],
) -> List[Dict[str, str]]:
    entries: List[Dict[str, str]] = []
    by_key: Dict[str, Dict[str, str]] = {}
    order = 0

    def add_names(raw_value: str, role_hint: str = "") -> None:
        nonlocal order
        for raw_name in _split_players_cell(raw_value):
            player_name = _canonicalize_name(raw_name)
            if not player_name:
                continue
            player_key = normalize_name(player_name)
            role = _role_from_text(role_hint) or _role_from_text(role_map.get(player_key, ""))
            existing = by_key.get(player_key)
            if existing is not None:
                if not existing.get("role") and role:
                    existing["role"] = role
                continue

            payload = {
                "name": player_name,
                "role": role,
                "order": str(order),
            }
            entries.append(payload)
            by_key[player_key] = payload
            order += 1

    indexed_columns: List[Tuple[int, str, str]] = []
    for key, value in normalized_row.items():
        current_value = str(value or "").strip()
        if not current_value:
            continue
        has_bench_token = any(token in key for token in ("panchina", "riserva", "riserve", "bench", "reserve"))
        simple_index = re.match(r"^(?:p|r|b)(\d{1,2})$", key)
        if not has_bench_token and simple_index is None:
            continue
        index_match = re.search(r"(\d{1,2})$", key)
        if index_match is None:
            continue
        indexed_columns.append((int(index_match.group(1)), key, current_value))
    indexed_columns.sort(key=lambda item: (item[0], item[1]))
    for _, _, value in indexed_columns:
        add_names(value)

    for candidate in RESERVE_GENERIC_COLUMNS:
        value = normalized_row.get(normalize_name(candidate), "")
        if value:
            add_names(value)

    for role, candidates in RESERVE_ROLE_COLUMNS.items():
        for candidate in candidates:
            value = normalized_row.get(normalize_name(candidate), "")
            if value:
                add_names(value, role)

    entries.sort(key=lambda item: _parse_int(item.get("order")) or 0)
    return entries


def _read_csv_fallback(path: Path, fallback: Path) -> List[Dict[str, str]]:
    rows = _read_csv(path)
    if rows:
        return rows
    if fallback.exists():
        return _read_csv(fallback)
    return []


def _load_name_list(path: Path) -> List[str]:
    if not path.exists():
        return []
    mtime = path.stat().st_mtime
    cached = _NAME_LIST_CACHE.get(str(path))
    if cached and cached.get("mtime") == mtime:
        return cached.get("data", [])
    data = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        name = line.strip()
        if not name or name.startswith("#"):
            continue
        data.append(name)
    _NAME_LIST_CACHE[str(path)] = {"mtime": mtime, "data": data}
    return data


def _matches(text: str, query: str) -> bool:
    return query.lower() in text.lower()


def _require_admin_key(
    x_admin_key: str | None,
    db: Session,
    authorization: str | None = None,
) -> None:
    bearer_record = access_key_from_bearer(authorization, db)
    if bearer_record is not None:
        if not bearer_record.is_admin:
            raise HTTPException(status_code=403, detail="Permessi admin richiesti")
        return

    if not x_admin_key:
        raise HTTPException(status_code=401, detail="Admin key richiesta")
    key_value = x_admin_key.strip().lower()
    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record or not record.is_admin:
        raise HTTPException(status_code=403, detail="Admin key non valida")
    if not record.used:
        raise HTTPException(status_code=403, detail="Admin key non ancora attivata")


def _backup_or_500(prefix: str) -> None:
    try:
        run_backup_fail_fast(
            DATABASE_URL,
            BACKUP_DIR,
            BACKUP_KEEP_LAST,
            prefix=prefix,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=f"Backup failed: {exc}") from exc


def _strip_leading_initial(value: str) -> str:
    return re.sub(r"^[A-Za-z]\.?\s+", "", value or "")


def _repair_mojibake(value: str) -> str:
    text = str(value or "")
    if not text:
        return text

    repaired = text
    for _ in range(2):
        if not any(token in repaired for token in ("Ã", "Â", "Ð", "Ñ")):
            break
        try:
            candidate = repaired.encode("latin-1").decode("utf-8")
        except Exception:
            break
        if candidate == repaired:
            break
        repaired = candidate
    return repaired


def _load_listone_name_map() -> Dict[str, str]:
    if not QUOT_PATH.exists():
        return {}
    mtime = QUOT_PATH.stat().st_mtime
    cached = _LISTONE_NAME_CACHE.get(str(QUOT_PATH))
    if cached and cached.get("mtime") == mtime:
        return cached.get("data", {})
    mapping: Dict[str, str] = {}
    for row in _read_csv(QUOT_PATH):
        name = (row.get("Giocatore") or "").strip()
        if not name:
            continue
        key = normalize_name(name)
        base_name = strip_star(name)
        base_key = normalize_name(base_name)
        # Prefer non-starred version if both exist
        if "*" not in name:
            mapping[key] = name
            mapping[base_key] = name
        else:
            if key not in mapping:
                mapping[key] = base_name
            if base_key not in mapping:
                mapping[base_key] = base_name
    _LISTONE_NAME_CACHE[str(QUOT_PATH)] = {"mtime": mtime, "data": mapping}
    return mapping


def _canonicalize_name(value: str) -> str:
    raw = _repair_mojibake((value or "").strip()).strip()
    if not raw:
        return raw
    mapping = _load_listone_name_map()
    direct = mapping.get(normalize_name(raw))
    if direct:
        return direct
    stripped = _strip_leading_initial(raw)
    if stripped:
        mapped = mapping.get(normalize_name(stripped))
        if mapped:
            return mapped
    return raw


def _load_role_map() -> Dict[str, str]:
    roles = {}
    for row in _read_csv(ROSE_PATH):
        name = row.get("Giocatore", "")
        role = row.get("Ruolo", "")
        if not name or not role:
            continue
        roles[normalize_name(name)] = role.strip().upper()
    return roles


def _load_qa_map() -> Dict[str, float]:
    qa_map: Dict[str, float] = {}
    for row in _read_csv(QUOT_PATH):
        name = (row.get("Giocatore") or "").strip()
        if not name:
            continue
        try:
            qa = float(row.get("PrezzoAttuale", 0) or 0)
        except ValueError:
            qa = 0.0
        if qa <= 0:
            continue
        qa_map[normalize_name(name)] = qa
    return qa_map


def _load_player_force_map() -> Dict[str, float]:
    if not PLAYER_STRENGTH_REPORT_PATH.exists():
        return {}
    mtime = PLAYER_STRENGTH_REPORT_PATH.stat().st_mtime
    cache_key = str(PLAYER_STRENGTH_REPORT_PATH)
    cached = _PLAYER_FORCE_CACHE.get(cache_key)
    if cached and cached.get("mtime") == mtime:
        return cached.get("data", {})

    force_map: Dict[str, float] = {}
    for row in _read_csv(PLAYER_STRENGTH_REPORT_PATH):
        raw_name = str(row.get("Giocatore") or "").strip()
        if not raw_name:
            continue
        force = _parse_float(row.get("ForzaGiocatore"))
        if force is None:
            continue
        canonical = _canonicalize_name(raw_name)
        keys = {
            normalize_name(strip_star(raw_name)),
            normalize_name(strip_star(canonical)),
            normalize_name(raw_name),
            normalize_name(canonical),
        }
        for key in keys:
            if not key:
                continue
            existing = force_map.get(key)
            if existing is None or force > existing:
                force_map[key] = force

    _PLAYER_FORCE_CACHE[cache_key] = {"mtime": mtime, "data": force_map}
    return force_map


def _lineup_player_names(item: Dict[str, object]) -> List[str]:
    players: List[str] = []
    goalkeeper = str(item.get("portiere") or "").strip()
    if goalkeeper:
        players.append(goalkeeper)
    for field in ("difensori", "centrocampisti", "attaccanti"):
        values = item.get(field) if isinstance(item.get(field), list) else []
        for value in values:
            name = str(value or "").strip()
            if name:
                players.append(name)
    return players


def _recompute_forza_titolari(items: List[Dict[str, object]]) -> None:
    force_map = _load_player_force_map()
    if not force_map:
        return

    for item in items:
        total = 0.0
        hits = 0
        for raw_name in _lineup_player_names(item):
            canonical = _canonicalize_name(raw_name)
            key = normalize_name(strip_star(canonical))
            if not key:
                key = normalize_name(strip_star(raw_name))
            force = force_map.get(key)
            if force is None and canonical != raw_name:
                force = force_map.get(normalize_name(strip_star(raw_name)))
            if force is None:
                continue
            total += float(force)
            hits += 1
        if hits > 0:
            item["forza_titolari"] = round(total, 2)


def _load_last_quotazioni_map() -> Dict[str, Dict[str, str]]:
    """Return last-seen quotazione rows from history/quotazioni (CSV).

    For players that disappear in a newer file, keep the last value from the
    most recent file where they still appeared.
    """
    hist_dir = DATA_DIR / "history" / "quotazioni"
    if not hist_dir.exists():
        return {}
    files = list(hist_dir.glob("quotazioni_*.csv"))
    if not files:
        return {}
    def _date_key(p: Path):
        try:
            return datetime.strptime(p.stem.replace("quotazioni_", ""), "%Y-%m-%d")
        except Exception:
            return datetime.fromtimestamp(p.stat().st_mtime)

    files = sorted(files, key=_date_key)

    last_seen: Dict[str, Dict[str, str]] = {}
    closed: Dict[str, Dict[str, str]] = {}
    prev_names: set[str] = set()

    for path in files:
        rows = _read_csv(path)
        current_names: set[str] = set()
        for row in rows:
            name = (row.get("Giocatore") or "").strip()
            if not name:
                continue
            key = normalize_name(strip_star(name))
            current_names.add(key)
            qa_val = (
                row.get("PrezzoAttuale")
                or row.get("QuotazioneAttuale")
                or row.get("QA")
                or 0
            )
            last_seen[key] = {
                "Squadra": row.get("Squadra", ""),
                "PrezzoAttuale": qa_val,
                "Ruolo": row.get("Ruolo", ""),
            }

        # Players present before but missing now -> freeze last_seen.
        if prev_names:
            disappeared = prev_names - current_names
            for key in disappeared:
                if key in closed:
                    continue
                if key in last_seen:
                    closed[key] = last_seen[key]
        prev_names = current_names

    # For players never disappeared, keep latest seen.
    for key, row in last_seen.items():
        if key not in closed:
            closed[key] = row
    return closed


def _apply_qa_from_quot(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    qa_map = _load_qa_map()
    if not qa_map:
        return rows
    out = []
    for row in rows:
        name_key = normalize_name(row.get("Giocatore", ""))
        qa = qa_map.get(name_key)
        if qa is not None:
            row = dict(row)
            row["PrezzoAttuale"] = qa
        out.append(row)
    return out



def _latest_old_quotazioni_file() -> Optional[Path]:
    quot_dir = DATA_DIR / "Quotazioni"
    if not quot_dir.exists():
        return None
    candidates = sorted(quot_dir.glob("Quotazioni_Fantacalcio_Stagione_2025_26*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _load_old_quotazioni_map() -> Dict[str, Dict[str, str]]:
    path = _latest_old_quotazioni_file()
    if not path:
        return {}
    try:
        import pandas as pd
    except Exception:
        return {}
    raw = pd.read_excel(path, header=None)
    header_row = None
    for i in range(min(10, len(raw))):
        row = raw.iloc[i].astype(str).str.strip().tolist()
        if "Nome" in row and "Qt.A" in row:
            header_row = i
            break
    if header_row is None:
        return {}
    df = pd.read_excel(path, header=header_row)
    col_map = {
        "Nome": "Giocatore",
        "Squadra": "Squadra",
        "Qt.A": "PrezzoAttuale",
        "R": "Ruolo",
    }
    df = df.rename(columns=col_map)
    out = {}
    for _, r in df.iterrows():
        name = str(r.get("Giocatore", "")).strip()
        if not name:
            continue
        out[normalize_name(name)] = {
            "Squadra": r.get("Squadra", ""),
            "PrezzoAttuale": r.get("PrezzoAttuale", 0),
            "Ruolo": r.get("Ruolo", ""),
        }
    return out


def _load_player_cards_map() -> Dict[str, Dict[str, str]]:
    rows = _read_csv(PLAYER_CARDS_PATH)
    out = {}
    for row in rows:
        name = (row.get("nome") or "").strip()
        if not name:
            continue
        out[normalize_name(name)] = {
            "Squadra": row.get("club", ""),
            "PrezzoAttuale": row.get("QA", 0),
            "Ruolo": row.get("R", row.get("ruolo", "")),
        }
    return out


def _load_stats_map() -> Dict[str, Dict[str, str]]:
    rows = _read_csv_fallback(PLAYER_STATS_PATH, SEED_DB_DIR / "player_stats.csv")
    out = {}
    for row in rows:
        name = (row.get("Giocatore") or "").strip()
        if not name:
            continue
        out[normalize_name(name)] = row
    return out


def _build_players_pool_from_csv() -> List[Dict[str, object]]:
    cards = _read_csv_fallback(PLAYER_CARDS_PATH, SEED_DB_DIR / "quotazioni_master.csv")
    stats_map = _load_stats_map()
    players_pool = []
    for row in cards:
        name = (row.get("nome") or "").strip()
        if not name:
            continue
        key = normalize_name(name)
        stats = stats_map.get(key, {})
        try:
            qa = float(row.get("QA", 0) or 0)
        except Exception:
            qa = 0.0
        try:
            pk_role = float(stats.get("PKRole", 0) or 0)
        except Exception:
            pk_role = 0.0
        players_pool.append(
            {
                "nome": name,
                "ruolo_base": (row.get("R") or "").strip(),
                "club": (row.get("club") or "").strip(),
                "QA": qa,
                "PV_S": float(stats.get("PV_S", 0) or 0),
                "PV_R8": float(stats.get("PV_R8", 0) or 0),
                "PT_S": float(stats.get("PT_S", 0) or 0),
                "PT_R8": float(stats.get("PT_R8", 0) or 0),
                "MIN_S": float(stats.get("MIN_S", 0) or 0),
                "MIN_R8": float(stats.get("MIN_R8", 0) or 0),
                "G_S": float(stats.get("G_S", 0) or 0),
                "G_R8": float(stats.get("G_R8", 0) or 0),
                "A_S": float(stats.get("A_S", 0) or 0),
                "A_R8": float(stats.get("A_R8", 0) or 0),
                "xG_S": float(stats.get("xG_S", 0) or 0),
                "xG_R8": float(stats.get("xG_R8", 0) or 0),
                "xA_S": float(stats.get("xA_S", 0) or 0),
                "xA_R8": float(stats.get("xA_R8", 0) or 0),
                "AMM_S": float(stats.get("AMM_S", 0) or 0),
                "AMM_R8": float(stats.get("AMM_R8", 0) or 0),
                "ESP_S": float(stats.get("ESP_S", 0) or 0),
                "ESP_R8": float(stats.get("ESP_R8", 0) or 0),
                "AUTOGOL_S": float(stats.get("AUTOGOL_S", 0) or 0),
                "AUTOGOL_R8": float(stats.get("AUTOGOL_R8", 0) or 0),
                "RIGSEG_S": float(stats.get("RIGSEG_S", 0) or 0),
                "RIGSEG_R8": float(stats.get("RIGSEG_R8", 0) or 0),
                "RIGSBAGL_S": float(stats.get("RIGSBAGL_S", 0) or 0),
                "RIGSBAGL_R8": float(stats.get("RIGSBAGL_R8", 0) or 0),
                "GDECWIN_S": float(stats.get("GDECWIN_S", 0) or 0),
                "GDECPAR_S": float(stats.get("GDECPAR_S", 0) or 0),
                "GOLS_S": float(stats.get("GOLS_S", 0) or 0),
                "GOLS_R8": float(stats.get("GOLS_R8", 0) or 0),
                "RIGPAR_S": float(stats.get("RIGPAR_S", 0) or 0),
                "RIGPAR_R8": float(stats.get("RIGPAR_R8", 0) or 0),
                "CS_S": float(stats.get("CS_S", 0) or 0),
                "CS_R8": float(stats.get("CS_R8", 0) or 0),
                "PKRole": pk_role,
            }
        )
    return players_pool


def _build_teams_data_from_csv() -> Dict[str, Dict[str, object]]:
    rows = _read_csv_fallback(TEAMS_PATH, SEED_DB_DIR / "teams.csv")
    out = {}
    for row in rows:
        name = (row.get("name") or "").strip()
        if not name:
            continue
        out[name] = {
            "PPG_S": float(row.get("PPG_S", 0) or 0),
            "PPG_R8": float(row.get("PPG_R8", 0) or 0),
            "GFpg_S": float(row.get("GFpg_S", 0) or 0),
            "GFpg_R8": float(row.get("GFpg_R8", 0) or 0),
            "GApg_S": float(row.get("GApg_S", 0) or 0),
            "GApg_R8": float(row.get("GApg_R8", 0) or 0),
            "MoodTeam": float(row.get("MoodTeam", 0.5) or 0.5),
            "CoachStyle_P": float(row.get("CoachStyle_P", 0.5) or 0.5),
            "CoachStyle_D": float(row.get("CoachStyle_D", 0.5) or 0.5),
            "CoachStyle_C": float(row.get("CoachStyle_C", 0.5) or 0.5),
            "CoachStyle_A": float(row.get("CoachStyle_A", 0.5) or 0.5),
            "CoachStability": float(row.get("CoachStability", 0.5) or 0.5),
            "CoachBoost": float(row.get("CoachBoost", 0.5) or 0.5),
            "GamesRemaining": int(float(row.get("GamesRemaining", 0) or 0)),
        }
    return out


def _build_teams_data_from_roster() -> Dict[str, Dict[str, object]]:
    rose_rows = _read_csv(ROSE_PATH)
    clubs = set()
    for row in rose_rows:
        club = (row.get("Squadra") or "").strip()
        if club:
            clubs.add(club)
    out = {}
    for club in sorted(clubs):
        out[club] = {
            "PPG_S": 0.0,
            "PPG_R8": 0.0,
            "GFpg_S": 0.0,
            "GFpg_R8": 0.0,
            "GApg_S": 0.0,
            "GApg_R8": 0.0,
            "MoodTeam": 0.5,
            "CoachStyle_P": 0.5,
            "CoachStyle_D": 0.5,
            "CoachStyle_C": 0.5,
            "CoachStyle_A": 0.5,
            "CoachStability": 0.5,
            "CoachBoost": 0.5,
            "GamesRemaining": 0,
        }
    return out


def _build_teams_data_from_user_squad(user_squad: List[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    clubs = set()
    for row in user_squad:
        club = (row.get("Squadra") or "").strip()
        if club:
            clubs.add(club)
    out = {}
    for club in sorted(clubs):
        out[club] = {
            "PPG_S": 0.0,
            "PPG_R8": 0.0,
            "GFpg_S": 0.0,
            "GFpg_R8": 0.0,
            "GApg_S": 0.0,
            "GApg_R8": 0.0,
            "MoodTeam": 0.5,
            "CoachStyle_P": 0.5,
            "CoachStyle_D": 0.5,
            "CoachStyle_C": 0.5,
            "CoachStyle_A": 0.5,
            "CoachStability": 0.5,
            "CoachBoost": 0.5,
            "GamesRemaining": 0,
        }
    return out


def _build_fixtures_from_csv(teams_data: Dict[str, Dict[str, object]]) -> List[Dict[str, object]]:
    rows = _read_csv_fallback(FIXTURES_PATH, SEED_DB_DIR / "fixtures.csv")
    team_map = {name.lower(): name for name in teams_data.keys()}
    fixtures = []
    rounds = []
    for row in rows:
        team = (row.get("team") or "").strip()
        opponent = (row.get("opponent") or "").strip()
        if not team or not opponent:
            continue
        team = team_map.get(team.lower(), team)
        opponent = team_map.get(opponent.lower(), opponent)
        fixtures.append(
            {
                "round": int(float(row.get("round", 0) or 0)),
                "team": team,
                "opponent": opponent,
                "home_away": row.get("home_away") or row.get("home_away".upper()),
            }
        )
        try:
            rounds.append(int(float(row.get("round", 0) or 0)))
        except Exception:
            pass
    return fixtures


def _resolve_current_round(rounds: List[int]) -> int:
    status_matchday = _load_status_matchday()
    if status_matchday is not None:
        return status_matchday

    inferred_from_fixtures = _infer_matchday_from_fixtures()
    if inferred_from_fixtures is not None:
        return inferred_from_fixtures

    inferred_from_stats = _infer_matchday_from_stats()
    if inferred_from_stats is not None:
        return inferred_from_stats

    valid_rounds = [int(r) for r in rounds if isinstance(r, int) and r > 0]
    return min(valid_rounds) if valid_rounds else 1


def _latest_market_report() -> Optional[Path]:
    reports_dir = DATA_DIR / "reports"
    if not reports_dir.exists():
        return None
    candidates = sorted(reports_dir.glob(MARKET_REPORT_GLOB), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _latest_rose_diff() -> Optional[Path]:
    diffs_dir = DATA_DIR / "history" / "diffs"
    if not diffs_dir.exists():
        return None
    candidates = sorted(
        diffs_dir.glob(ROSE_DIFF_GLOB),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _build_market_from_rose_diff(path: Path) -> Dict[str, List[Dict[str, str]]]:
    stamp = path.stem.replace("diff_rose_", "")
    items: List[Dict[str, str]] = []
    team_rows: Dict[str, Dict[str, object]] = {}
    seen = set()

    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for raw_line in lines:
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        team, payload = line.split(":", 1)
        team = team.strip()
        payload = payload.strip()
        if not team or not payload:
            continue

        swaps = [part.strip() for part in payload.split(";") if part.strip()]
        for swap in swaps:
            if "->" not in swap:
                continue
            left, right = [part.strip() for part in swap.split("->", 1)]

            out_parts = [part.strip() for part in left.split(",")]
            in_parts = [part.strip() for part in right.split(",")]

            out_name = out_parts[0] if len(out_parts) >= 1 else ""
            out_value = out_parts[1] if len(out_parts) >= 2 else "0"
            out_role = out_parts[2] if len(out_parts) >= 3 else ""
            out_team = out_parts[3] if len(out_parts) >= 4 else ""

            in_name = in_parts[0] if len(in_parts) >= 1 else ""
            in_value = in_parts[1] if len(in_parts) >= 2 else "0"
            in_role = in_parts[2] if len(in_parts) >= 3 else ""
            in_team = in_parts[3] if len(in_parts) >= 4 else ""

            # Keep one logical swap only once per team/date.
            dedupe_key = (
                team.lower(),
                stamp,
                normalize_name(out_name),
                normalize_name(in_name),
                (out_role or "").upper(),
                (in_role or "").upper(),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)

            try:
                out_value_num = float(str(out_value).replace(",", "."))
            except ValueError:
                out_value_num = 0.0
            try:
                in_value_num = float(str(in_value).replace(",", "."))
            except ValueError:
                in_value_num = 0.0

            items.append(
                {
                    "team": team,
                    "date": stamp,
                    "out": out_name,
                    "out_missing": out_name.endswith("*"),
                    "out_squadra": out_team,
                    "out_ruolo": (out_role or "").upper(),
                    "out_value": out_value_num,
                    "in": in_name,
                    "in_missing": in_name.endswith("*"),
                    "in_squadra": in_team,
                    "in_ruolo": (in_role or "").upper(),
                    "in_value": in_value_num,
                    "delta": out_value_num - in_value_num,
                }
            )

            row = team_rows.get(team) or {
                "team": team,
                "delta": 0.0,
                "changed_count": 0,
                "last_date": stamp,
            }
            row["delta"] = float(row["delta"]) + (out_value_num - in_value_num)
            row["changed_count"] = int(row["changed_count"]) + 1
            row["last_date"] = stamp
            team_rows[team] = row

    teams = list(team_rows.values())
    teams.sort(key=lambda r: str(r.get("team", "")).lower())
    return {"items": items, "teams": teams}


def _latest_rose_xlsx() -> Optional[Path]:
    if not ROSE_XLSX_DIR.exists():
        return None
    candidates = sorted(
        ROSE_XLSX_DIR.glob("rose_nuovo_*.xlsx"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _load_residual_credits_map() -> Dict[str, float]:
    path = _latest_rose_xlsx()
    if not path:
        return {}
    mtime = path.stat().st_mtime
    cached_path = _RESIDUAL_CREDITS_CACHE.get("path")
    cached_mtime = _RESIDUAL_CREDITS_CACHE.get("mtime")
    if cached_path == str(path) and cached_mtime == mtime:
        return _RESIDUAL_CREDITS_CACHE.get("data", {})
    if RESIDUAL_CREDITS_PATH.exists():
        try:
            if RESIDUAL_CREDITS_PATH.stat().st_mtime >= mtime:
                rows = _read_csv(RESIDUAL_CREDITS_PATH)
                credits = {}
                for row in rows:
                    team = (row.get("Team") or "").strip()
                    value = row.get("CreditiResidui")
                    if not team:
                        continue
                    try:
                        credits[normalize_name(team)] = float(str(value).replace(",", "."))
                    except Exception:
                        continue
                if credits:
                    _RESIDUAL_CREDITS_CACHE["path"] = str(path)
                    _RESIDUAL_CREDITS_CACHE["mtime"] = mtime
                    _RESIDUAL_CREDITS_CACHE["data"] = credits
                    return credits
        except Exception:
            pass

    try:
        import pandas as pd
    except Exception:
        pd = None

    credits: Dict[str, float] = {}
    left_team = ""
    right_team = ""
    pending_left: Optional[float] = None
    pending_right: Optional[float] = None
    header_tokens = {"Ruolo", "Calciatore", "Squadra", "Costo", "P", "D", "C", "A"}

    def _extract_credit(text: str) -> Optional[float]:
        match = re.search(r"Crediti\s+Residui:\s*(\d+(?:[.,]\d+)?)", text)
        if not match:
            return None
        return float(match.group(1).replace(",", "."))

    if pd is not None:
        df = pd.read_excel(path, header=None)
        for _, row in df.iterrows():
            left_cell = row.iloc[0]
            right_cell = row.iloc[5] if len(row) > 5 else None

            if isinstance(left_cell, str):
                value = left_cell.strip()
                if value and value not in header_tokens and "Crediti Residui" not in value:
                    left_team = value
                    if pending_left is not None:
                        credits[normalize_name(left_team)] = pending_left
                        pending_left = None
                elif "Crediti Residui" in value and left_team:
                    credit = _extract_credit(value)
                    if credit is not None:
                        credits[normalize_name(left_team)] = credit
                elif "Crediti Residui" in value and not left_team:
                    credit = _extract_credit(value)
                    if credit is not None:
                        pending_left = credit

            if isinstance(right_cell, str):
                value = right_cell.strip()
                if value and value not in header_tokens and "Crediti Residui" not in value:
                    right_team = value
                    if pending_right is not None:
                        credits[normalize_name(right_team)] = pending_right
                        pending_right = None
                elif "Crediti Residui" in value and right_team:
                    credit = _extract_credit(value)
                    if credit is not None:
                        credits[normalize_name(right_team)] = credit
                elif "Crediti Residui" in value and not right_team:
                    credit = _extract_credit(value)
                    if credit is not None:
                        pending_right = credit
    else:
        try:
            import openpyxl
            wb = openpyxl.load_workbook(path, data_only=True)
            ws = wb.active
            for row in ws.iter_rows(values_only=True):
                left_cell = row[0] if len(row) > 0 else None
                right_cell = row[5] if len(row) > 5 else None

                if isinstance(left_cell, str):
                    value = left_cell.strip()
                    if value and value not in header_tokens and "Crediti Residui" not in value:
                        left_team = value
                        if pending_left is not None:
                            credits[normalize_name(left_team)] = pending_left
                            pending_left = None
                    elif "Crediti Residui" in value and left_team:
                        credit = _extract_credit(value)
                        if credit is None and len(row) > 1 and isinstance(row[1], (int, float)):
                            credit = float(row[1])
                        if credit is not None:
                            credits[normalize_name(left_team)] = credit
                    elif "Crediti Residui" in value and not left_team:
                        credit = _extract_credit(value)
                        if credit is None and len(row) > 1 and isinstance(row[1], (int, float)):
                            credit = float(row[1])
                        if credit is not None:
                            pending_left = credit

                if isinstance(right_cell, str):
                    value = right_cell.strip()
                    if value and value not in header_tokens and "Crediti Residui" not in value:
                        right_team = value
                        if pending_right is not None:
                            credits[normalize_name(right_team)] = pending_right
                            pending_right = None
                    elif "Crediti Residui" in value and right_team:
                        credit = _extract_credit(value)
                        if credit is None and len(row) > 6 and isinstance(row[6], (int, float)):
                            credit = float(row[6])
                        if credit is not None:
                            credits[normalize_name(right_team)] = credit
                    elif "Crediti Residui" in value and not right_team:
                        credit = _extract_credit(value)
                        if credit is None and len(row) > 6 and isinstance(row[6], (int, float)):
                            credit = float(row[6])
                        if credit is not None:
                            pending_right = credit
        except Exception:
            credits = {}

    _RESIDUAL_CREDITS_CACHE["path"] = str(path)
    _RESIDUAL_CREDITS_CACHE["mtime"] = mtime
    _RESIDUAL_CREDITS_CACHE["data"] = credits
    return credits


def _build_market_placeholder() -> Dict[str, List[Dict[str, str]]]:
    diff_path = _latest_rose_diff()
    if diff_path:
        data = _build_market_from_rose_diff(diff_path)
        if data.get("items"):
            return data

    report_path = _latest_market_report()
    if not report_path:
        if MARKET_PATH.exists():
            try:
                data = json.loads(MARKET_PATH.read_text(encoding="utf-8"))
                return {
                    "items": data.get("items", []) or [],
                    "teams": data.get("teams", []) or [],
                }
            except Exception:
                pass
        return {"items": [], "teams": []}
    rose_rows = _read_csv(ROSE_PATH)
    quot_rows = _read_csv(QUOT_PATH)
    old_quot_map = _load_old_quotazioni_map()
    last_quot_map = _load_last_quotazioni_map()
    player_cards_map = _load_player_cards_map()
    quot_map = {}
    for row in quot_rows:
        name = (row.get("Giocatore") or "").strip()
        if not name:
            continue
        quot_map[normalize_name(name)] = {
            "Squadra": row.get("Squadra", ""),
            "PrezzoAttuale": row.get("PrezzoAttuale", 0),
            "Ruolo": row.get("Ruolo", ""),
        }
    qa_map = _load_qa_map()
    rose_team_map: Dict[str, Dict[str, Dict[str, str]]] = defaultdict(dict)
    for row in rose_rows:
        team = (row.get("Team") or "").strip()
        name = (row.get("Giocatore") or "").strip()
        if not team or not name:
            continue
        name_key = normalize_name(name)
        qa = qa_map.get(name_key, row.get("PrezzoAttuale", 0))
        rose_team_map[team.lower()][normalize_name(name)] = {
            "Nome": name,
            "Squadra": row.get("Squadra", ""),
            "PrezzoAttuale": qa,
            "Ruolo": row.get("Ruolo", ""),
        }
    role_map = _load_role_map()
    rows = _read_csv(report_path)
    stamp = report_path.stem.replace("rose_changes_", "").replace("_", "-")
    items = []
    teams = []
    for row in rows:
        team = row.get("Team", "").strip()
        if not team:
            continue
        added = [x.strip() for x in (row.get("Added") or "").split(";") if x.strip()]
        removed = [x.strip() for x in (row.get("Removed") or "").split(";") if x.strip()]
        if not added and not removed:
            continue
        changed_names = set([x.lower() for x in added + removed if x])
        teams.append(
            {
                "team": team,
                "delta": 0,
                "changed_count": len(changed_names),
                "last_date": stamp,
            }
        )
        team_map = rose_team_map.get(team.lower(), {})
        def _role_for(name: str) -> str:
            if not name:
                return ""
            key = normalize_name(name)
            info = (
                (player_cards_map.get(key) if name.strip().endswith("*") else None)
                or (last_quot_map.get(key) if name.strip().endswith("*") else None)
                or (old_quot_map.get(key) if name.strip().endswith("*") else None)
                or team_map.get(key)
                or quot_map.get(key)
            )
            role = (info or {}).get("Ruolo", "")
            if not role:
                role = (player_cards_map.get(key) or {}).get("Ruolo", "")
            if not role:
                role = role_map.get(key, "")
            return role or ""

        def _role_key(name: str) -> str:
            role = _role_for(name)
            if role:
                return role
            return f"__{normalize_name(name)}"

        removed_by_role: Dict[str, List[str]] = defaultdict(list)
        added_by_role: Dict[str, List[str]] = defaultdict(list)
        for name in removed:
            removed_by_role[_role_key(name)].append(name)
        for name in added:
            added_by_role[_role_key(name)].append(name)

        roles = sorted(set(removed_by_role.keys()) | set(added_by_role.keys()))
        for role in roles:
            outs = removed_by_role.get(role, [])
            ins = added_by_role.get(role, [])
            for i in range(max(len(outs), len(ins))):
                out_name = outs[i] if i < len(outs) else ""
                in_name = ins[i] if i < len(ins) else ""
            out_key = normalize_name(out_name)
            in_key = normalize_name(in_name)
            if out_key and out_key == in_key:
                continue
            out_info = (
                (player_cards_map.get(out_key) if out_name.strip().endswith("*") else None)
                or (last_quot_map.get(out_key) if out_name.strip().endswith("*") else None)
                or (old_quot_map.get(out_key) if out_name.strip().endswith("*") else None)
                or team_map.get(out_key)
                or quot_map.get(out_key)
            )
            in_info = (
                (player_cards_map.get(in_key) if in_name.strip().endswith("*") else None)
                or (last_quot_map.get(in_key) if in_name.strip().endswith("*") else None)
                or (old_quot_map.get(in_key) if in_name.strip().endswith("*") else None)
                or team_map.get(in_key)
                or quot_map.get(in_key)
            )
            if not out_name.strip().endswith("*"):
                alt_out = team_map.get(out_key, {}).get("Nome")
                if alt_out and alt_out.strip().endswith("*"):
                    out_name = alt_out
            if not in_name.strip().endswith("*"):
                alt_in = team_map.get(in_key, {}).get("Nome")
                if alt_in and alt_in.strip().endswith("*"):
                    in_name = alt_in
            out_value = float((out_info or {}).get("PrezzoAttuale", 0) or 0)
            in_value = float((in_info or {}).get("PrezzoAttuale", 0) or 0)
            if out_name.strip().endswith("*"):
                out_value = float((last_quot_map.get(out_key) or {}).get("PrezzoAttuale", out_value) or out_value)
            elif out_key in qa_map:
                out_value = float(qa_map.get(out_key) or 0)
            if in_name.strip().endswith("*"):
                in_value = float((last_quot_map.get(in_key) or {}).get("PrezzoAttuale", in_value) or in_value)
            elif in_key in qa_map:
                in_value = float(qa_map.get(in_key) or 0)
            out_role = (
                (out_info or {}).get("Ruolo")
                or (team_map.get(out_key) or {}).get("Ruolo")
                or (quot_map.get(out_key) or {}).get("Ruolo")
                or ""
            )
            in_role = (
                (in_info or {}).get("Ruolo")
                or (team_map.get(in_key) or {}).get("Ruolo")
                or (quot_map.get(in_key) or {}).get("Ruolo")
                or ""
            )
            out_team = (
                (out_info or {}).get("Squadra")
                or (team_map.get(out_key) or {}).get("Squadra")
                or (quot_map.get(out_key) or {}).get("Squadra")
                or ""
            )
            in_team = (
                (in_info or {}).get("Squadra")
                or (team_map.get(in_key) or {}).get("Squadra")
                or (quot_map.get(in_key) or {}).get("Squadra")
                or ""
            )
            if out_role and in_role and out_role != in_role:
                in_name = ""
                in_role = ""
                in_team = ""
                in_value = 0
            items.append(
                {
                    "team": team,
                    "date": stamp,
                    "out": out_name,
                    "out_missing": out_name.strip().endswith("*"),
                    "out_squadra": out_team,
                    "out_ruolo": out_role,
                    "out_value": out_value,
                    "in": in_name,
                    "in_missing": in_name.strip().endswith("*"),
                    "in_squadra": in_team,
                    "in_ruolo": in_role,
                    "in_value": in_value,
                    "delta": out_value - in_value,
                }
            )
    return {"items": items, "teams": teams}


def _enrich_market_items(items: List[Dict[str, str]]) -> List[Dict[str, str]]:
    if not items:
        return items
    rose_rows = _read_csv(ROSE_PATH)
    quot_rows = _read_csv(QUOT_PATH)
    old_quot_map = _load_old_quotazioni_map()
    last_quot_map = _load_last_quotazioni_map()
    player_cards_map = _load_player_cards_map()

    quot_map = {}
    for row in quot_rows:
        name = (row.get("Giocatore") or "").strip()
        if not name:
            continue
        quot_map[normalize_name(name)] = {
            "Squadra": row.get("Squadra", ""),
            "PrezzoAttuale": row.get("PrezzoAttuale", 0),
            "Ruolo": row.get("Ruolo", ""),
        }

    qa_map = _load_qa_map()
    rose_team_map: Dict[str, Dict[str, Dict[str, str]]] = defaultdict(dict)
    starred_players = set()
    for row in rose_rows:
        team = (row.get("Team") or "").strip()
        name = (row.get("Giocatore") or "").strip()
        if not team or not name:
            continue
        key = normalize_name(name)
        qa = qa_map.get(key, row.get("PrezzoAttuale", 0))
        rose_team_map[team.lower()][key] = {
            "Nome": name,
            "Squadra": row.get("Squadra", ""),
            "PrezzoAttuale": qa,
            "Ruolo": row.get("Ruolo", ""),
        }
        if name.strip().endswith("*"):
            starred_players.add(key)

    def _lookup_info(name: str, team: str) -> Dict[str, str]:
        key = normalize_name(name)
        if not key:
            return {}
        if name.strip().endswith("*"):
            return (
                player_cards_map.get(key)
                or last_quot_map.get(key)
                or old_quot_map.get(key)
                or rose_team_map.get(team.lower(), {}).get(key, {})
                or quot_map.get(key, {})
            )
        return (
            rose_team_map.get(team.lower(), {}).get(key, {})
            or quot_map.get(key, {})
            or player_cards_map.get(key)
            or old_quot_map.get(key)
            or {}
        )

    enriched = []
    for item in items:
        out_name = (item.get("out") or "").strip()
        in_name = (item.get("in") or "").strip()
        team = (item.get("team") or "").strip()
        out_key = normalize_name(out_name)
        in_key = normalize_name(in_name)

        out_info = _lookup_info(out_name, team)
        in_info = _lookup_info(in_name, team)

        if out_key and out_key in starred_players and not out_name.endswith("*"):
            out_name = f"{out_name} *"
        if in_key and in_key in starred_players and not in_name.endswith("*"):
            in_name = f"{in_name} *"

        item = dict(item)
        item["out"] = out_name
        item["in"] = in_name
        item["out_ruolo"] = item.get("out_ruolo") or out_info.get("Ruolo", "")
        item["in_ruolo"] = item.get("in_ruolo") or in_info.get("Ruolo", "")
        item["out_squadra"] = out_info.get("Squadra", "") or item.get("out_squadra", "")
        item["in_squadra"] = in_info.get("Squadra", "") or item.get("in_squadra", "")
        out_val = item.get("out_value")
        in_val = item.get("in_value")
        if out_val in ("", None):
            out_val = out_info.get("PrezzoAttuale", 0)
        if in_val in ("", None):
            in_val = in_info.get("PrezzoAttuale", 0)
        if out_name.strip().endswith("*"):
            out_val = (last_quot_map.get(out_key) or {}).get("PrezzoAttuale", out_val)
        elif out_key in qa_map:
            out_val = qa_map.get(out_key)
        if in_name.strip().endswith("*"):
            in_val = (last_quot_map.get(in_key) or {}).get("PrezzoAttuale", in_val)
        elif in_key in qa_map:
            in_val = qa_map.get(in_key)
        if item["out_ruolo"] and item["in_ruolo"] and item["out_ruolo"] != item["in_ruolo"]:
            item["in"] = ""
            item["in_ruolo"] = ""
            item["in_squadra"] = ""
            in_val = 0
        item["out_value"] = float(out_val or 0)
        item["in_value"] = float(in_val or 0)
        item["delta"] = item["out_value"] - item["in_value"]
        enriched.append(item)
    return enriched


def _build_market_suggest_payload(team_name: str, db: Session) -> Dict[str, object]:
    rose_rows = _read_csv(ROSE_PATH)
    qa_map = _load_qa_map()
    team_key = normalize_name(team_name)
    residual_map = _load_residual_credits_map()
    credits_residui = float(residual_map.get(team_key, 0) or 0)
    user_squad = []
    for row in rose_rows:
        if normalize_name(row.get("Team", "")) != team_key:
            continue
        name_key = normalize_name(row.get("Giocatore", ""))
        qa = qa_map.get(name_key, row.get("PrezzoAttuale", 0))
        user_squad.append(
            {
                "Giocatore": row.get("Giocatore", ""),
                "Ruolo": row.get("Ruolo", ""),
                "Squadra": row.get("Squadra", ""),
                "PrezzoAttuale": qa,
            }
        )

    players_pool = []
    players = db.query(Player, PlayerStats).outerjoin(
        PlayerStats, PlayerStats.player_id == Player.id
    ).all()
    for player, stats in players:
        players_pool.append(
            {
                "nome": player.name,
                "ruolo_base": player.role,
                "club": player.club or "",
                "QA": player.qa,
                "PV_S": stats.pv_s if stats else 0,
                "PV_R8": stats.pv_r8 if stats else 0,
                "PT_S": stats.pt_s if stats else 0,
                "PT_R8": stats.pt_r8 if stats else 0,
                "MIN_S": stats.min_s if stats else 0,
                "MIN_R8": stats.min_r8 if stats else 0,
                "G_S": stats.g_s if stats else 0,
                "G_R8": stats.g_r8 if stats else 0,
                "A_S": stats.a_s if stats else 0,
                "A_R8": stats.a_r8 if stats else 0,
                "xG_S": stats.xg_s if stats else 0,
                "xG_R8": stats.xg_r8 if stats else 0,
                "xA_S": stats.xa_s if stats else 0,
                "xA_R8": stats.xa_r8 if stats else 0,
                "AMM_S": stats.amm_s if stats else 0,
                "AMM_R8": stats.amm_r8 if stats else 0,
                "ESP_S": stats.esp_s if stats else 0,
                "ESP_R8": stats.esp_r8 if stats else 0,
                "AUTOGOL_S": stats.autogol_s if stats else 0,
                "AUTOGOL_R8": stats.autogol_r8 if stats else 0,
                "RIGSEG_S": stats.rigseg_s if stats else 0,
                "RIGSEG_R8": stats.rigseg_r8 if stats else 0,
                "RIGSBAGL_S": stats.rig_sbagl_s if stats else 0,
                "RIGSBAGL_R8": stats.rig_sbagl_r8 if stats else 0,
                "GDECWIN_S": stats.gdecwin_s if stats else 0,
                "GDECPAR_S": stats.gdecpar_s if stats else 0,
                "GOLS_S": stats.gols_s if stats else 0,
                "GOLS_R8": stats.gols_r8 if stats else 0,
                "RIGPAR_S": stats.rigpar_s if stats else 0,
                "RIGPAR_R8": stats.rigpar_r8 if stats else 0,
                "CS_S": stats.cs_s if stats else 0,
                "CS_R8": stats.cs_r8 if stats else 0,
                "PKRole": player.pk_role,
            }
        )
    if not players_pool:
        players_pool = _build_players_pool_from_csv()

    teams_data = {}
    teams = db.query(Team).all()
    team_map = {t.name.lower(): t.name for t in teams if t.name}
    for team in teams:
        if not team.name:
            continue
        teams_data[team.name] = {
            "PPG_S": team.ppg_s,
            "PPG_R8": team.ppg_r8,
            "GFpg_S": team.gfpg_s,
            "GFpg_R8": team.gfpg_r8,
            "GApg_S": team.gapg_s,
            "GApg_R8": team.gapg_r8,
            "MoodTeam": team.mood_team,
            "CoachStyle_P": team.coach_style_p,
            "CoachStyle_D": team.coach_style_d,
            "CoachStyle_C": team.coach_style_c,
            "CoachStyle_A": team.coach_style_a,
            "CoachStability": team.coach_stability,
            "CoachBoost": team.coach_boost,
            "GamesRemaining": team.games_remaining,
        }
    if not teams_data:
        teams_data = _build_teams_data_from_csv()
        team_map = {name.lower(): name for name in teams_data.keys()}
    if not teams_data:
        teams_data = _build_teams_data_from_roster()
        team_map = {name.lower(): name for name in teams_data.keys()}
    if not teams_data:
        teams_data = _build_teams_data_from_user_squad(user_squad)
        team_map = {name.lower(): name for name in teams_data.keys()}

    fixtures = []
    fixture_rows = db.query(Fixture).all()
    rounds = []
    for row in fixture_rows:
        team = row.team.strip() if row.team else ""
        opponent = row.opponent.strip() if row.opponent else ""
        team = team_map.get(team.lower(), team)
        opponent = team_map.get(opponent.lower(), opponent)
        if not team or not opponent:
            continue
        fixtures.append(
            {
                "round": row.round,
                "team": team,
                "opponent": opponent,
                "home_away": row.home_away,
            }
        )
        rounds.append(row.round)
    if not fixtures:
        fixtures = _build_fixtures_from_csv(teams_data)
        rounds = [f.get("round") for f in fixtures if f.get("round")]

    current_round = _resolve_current_round(rounds)

    return {
        "user_squad": user_squad,
        "credits_residui": credits_residui,
        "players_pool": players_pool,
        "teams_data": teams_data,
        "fixtures": fixtures,
        "currentRound": current_round,
        "injured_list": _load_name_list(DATA_DIR / "infortunati_clean.txt"),
        "injured_whitelist": _load_name_list(DATA_DIR / "infortunati_whitelist.txt"),
        "params": {
            "max_changes": 5,
            "k_pool": 60,
            "m_out": 8,
            "beam_width": 200,
        },
    }




@router.get("/summary")
def summary():
    rose = _read_csv(ROSE_PATH)
    teams = {row.get("Team", "") for row in rose if row.get("Team")}
    players = {row.get("Giocatore", "") for row in rose if row.get("Giocatore")}
    return {
        "teams": len(teams),
        "players": len(players),
    }


@router.get("/players")
def players(
    q: Optional[str] = Query(default=None),
    team: Optional[str] = Query(default=None),
    ruolo: Optional[str] = Query(default=None),
    squadra: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
):
    rose = _apply_qa_from_quot(_read_csv(ROSE_PATH))
    results = []
    for row in rose:
        if q and not _matches(row.get("Giocatore", ""), q):
            continue
        if team and not _matches(row.get("Team", ""), team):
            continue
        if ruolo and row.get("Ruolo", "").upper() != ruolo.upper():
            continue
        if squadra and not _matches(row.get("Squadra", ""), squadra):
            continue
        results.append(row)
        if len(results) >= limit:
            break
    return {"items": results}


@router.get("/quotazioni")
def quotazioni(q: Optional[str] = Query(default=None), limit: int = Query(default=50, ge=1, le=200)):
    quot = _read_csv(QUOT_PATH)
    results = []
    for row in quot:
        if q and not _matches(row.get("Giocatore", ""), q):
            continue
        results.append(row)
        if len(results) >= limit:
            break
    return {"items": results}


@router.get("/listone")
def listone(
    ruolo: str = Query(..., min_length=1, max_length=1),
    order: str = Query(default="price_desc"),
    limit: int = Query(default=200, ge=1, le=1000),
):
    quot = _read_csv(QUOT_PATH)
    ruolo = ruolo.upper()
    order = order.strip().lower()
    items_map: Dict[str, Dict[str, str]] = {}
    for row in quot:
        if row.get("Ruolo", "").upper() != ruolo:
            continue
        name = row.get("Giocatore", "")
        if not name:
            continue
        try:
            price = float(row.get("PrezzoAttuale", 0) or 0)
        except ValueError:
            price = 0.0
        current = items_map.get(name)
        if not current:
            items_map[name] = {
                "Giocatore": name,
                "Squadra": row.get("Squadra", ""),
                "Ruolo": ruolo,
                "PrezzoAttuale": price,
            }

    items = list(items_map.values())
    if order == "alpha":
        items.sort(key=lambda x: x["Giocatore"])
    elif order == "alpha_desc":
        items.sort(key=lambda x: x["Giocatore"], reverse=True)
    elif order == "price_asc":
        items.sort(key=lambda x: x["PrezzoAttuale"])
    elif order == "price_desc":
        items.sort(key=lambda x: x["PrezzoAttuale"], reverse=True)
    else:
        items.sort(key=lambda x: x["PrezzoAttuale"], reverse=True)
    return {"items": items[:limit]}


@router.get("/teams")
def teams():
    rose = _read_csv(ROSE_PATH)
    team_set = sorted({row.get("Team", "") for row in rose if row.get("Team")})
    return {"items": team_set}


def _load_standings_rows() -> List[Dict[str, object]]:
    base_dir = Path(__file__).resolve().parents[4]
    candidates = [
        base_dir / "Classifica_FantaPortoscuso-25.xlsx",
        DATA_DIR / "classifica.xlsx",
        DATA_DIR / "classifica.csv",
    ]
    source = next((p for p in candidates if p.exists()), None)
    if source is None:
        return []

    try:
        if source.suffix.lower() == ".csv":
            rows = _read_csv(source)
            out = []
            for idx, row in enumerate(rows):
                team = str(row.get("Squadra") or row.get("Team") or "").strip()
                if not team:
                    continue
                pos_raw = str(row.get("Pos") or row.get("Posizione") or "").strip()
                pts_raw = str(row.get("Pt. totali") or row.get("Punti") or "").strip()
                played_raw = str(row.get("Partite Giocate") or row.get("PG") or "").strip()
                try:
                    pos = int(float(pos_raw.replace(",", "."))) if pos_raw else idx + 1
                except ValueError:
                    pos = idx + 1
                try:
                    points = float(pts_raw.replace(",", ".")) if pts_raw else 0.0
                except ValueError:
                    points = 0.0
                try:
                    played = int(float(played_raw.replace(",", "."))) if played_raw else 0
                except ValueError:
                    played = 0
                out.append({"pos": pos, "team": team, "played": played, "points": points})
            out.sort(key=lambda x: x["pos"])
            return out

        import pandas as pd

        raw = pd.read_excel(source, header=None)
        header_row = 0
        for i in range(min(len(raw), 20)):
            values = [
                str(v).strip().lower()
                for v in raw.iloc[i].tolist()
                if str(v).strip() and str(v).strip().lower() != "nan"
            ]
            if not values:
                continue
            has_pos = any(v in {"pos", "posizione"} for v in values)
            has_team = any("squadra" in v or "team" in v for v in values)
            if has_pos and has_team:
                header_row = i
                break

        df = pd.read_excel(source, header=header_row)
        col_map = {str(c).strip().lower(): c for c in df.columns}

        pos_col = None
        team_col = None
        points_col = None
        played_col = None

        for k, v in col_map.items():
            if pos_col is None and (k == "pos" or "posizione" in k):
                pos_col = v
            if team_col is None and ("squadra" in k or k == "team"):
                team_col = v
            if points_col is None and ("pt" in k and "tot" in k):
                points_col = v
            if played_col is None and ("partite" in k or k in {"pg", "23"}):
                played_col = v

        if team_col is None:
            return []

        out = []
        for idx, row in df.iterrows():
            team = str(row.get(team_col, "")).strip()
            if not team or team.lower() == "nan":
                continue
            pos_val = row.get(pos_col) if pos_col is not None else (idx + 1)
            points_val = row.get(points_col) if points_col is not None else 0
            played_val = row.get(played_col) if played_col is not None else 0
            try:
                pos = int(float(pos_val))
            except (TypeError, ValueError):
                pos = idx + 1
            try:
                points = float(points_val)
            except (TypeError, ValueError):
                points = 0.0
            try:
                played = int(float(played_val))
            except (TypeError, ValueError):
                played = 0
            out.append({"pos": pos, "team": team, "played": played, "points": points})

        out.sort(key=lambda x: x["pos"])
        return out
    except Exception:
        return []


@router.get("/standings")
def standings():
    return {"items": _load_standings_rows()}


def _parse_int(value: object) -> Optional[int]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return int(float(raw.replace(",", ".")))
    except ValueError:
        return None


def _parse_float(value: object) -> Optional[float]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return float(raw.replace(",", "."))
    except ValueError:
        return None


def _normalize_row(row: Dict[str, str]) -> Dict[str, str]:
    normalized: Dict[str, str] = {}
    for key, value in row.items():
        if key is None:
            continue
        normalized[normalize_name(key)] = str(value or "").strip()
    return normalized


def _pick_row_value(normalized_row: Dict[str, str], candidates: List[str]) -> str:
    for candidate in candidates:
        value = normalized_row.get(normalize_name(candidate), "")
        if value:
            return value
    return ""


def _extract_starters_from_titolari_columns(
    normalized_row: Dict[str, str],
    module_raw: str,
) -> Tuple[str, List[str], List[str], List[str]]:
    titolari: List[str] = []
    for idx in range(1, 12):
        raw_value = _pick_row_value(
            normalized_row,
            [f"titolare_{idx}", f"titolare{idx}", f"starter_{idx}", f"starter{idx}"],
        )
        for name in _split_players_cell(raw_value):
            normalized_name = _canonicalize_name(name)
            if normalized_name:
                titolari.append(normalized_name)

    if not titolari:
        return "", [], [], []

    module_counts = _module_counts_from_str(module_raw)
    if module_counts is None:
        module_counts = {"P": 1, "D": 3, "C": 4, "A": 3}

    portiere = titolari[0] if titolari else ""
    outfield = titolari[1:]
    d_count = int(module_counts.get("D", 0))
    c_count = int(module_counts.get("C", 0))
    a_count = int(module_counts.get("A", 0))

    difensori = outfield[:d_count]
    centrocampisti = outfield[d_count : d_count + c_count]
    attaccanti = outfield[d_count + c_count : d_count + c_count + a_count]
    return portiere, difensori, centrocampisti, attaccanti


def _load_status_matchday() -> Optional[int]:
    if not STATUS_PATH.exists():
        return None
    try:
        raw = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return None
        return _parse_int(raw.get("matchday"))
    except Exception:
        return None


def _infer_matchday_from_fixtures() -> Optional[int]:
    rows = _read_csv_fallback(FIXTURES_PATH, SEED_DB_DIR / "fixtures.csv")
    if not rows:
        return None

    by_round: Dict[int, Dict[str, bool]] = {}
    any_score_found = False

    for row in rows:
        round_value = _parse_int(row.get("round"))
        if round_value is None:
            continue

        home_away = str(row.get("home_away") or row.get("HOME_AWAY") or "").strip().upper()
        if home_away and home_away != "H":
            continue

        home_score = _parse_int(
            row.get("home_score")
            or row.get("goals_home")
            or row.get("gol_casa")
            or row.get("home_goals")
            or row.get("score_home")
        )
        away_score = _parse_int(
            row.get("away_score")
            or row.get("goals_away")
            or row.get("gol_trasferta")
            or row.get("away_goals")
            or row.get("score_away")
        )
        is_played = home_score is not None and away_score is not None
        any_score_found = any_score_found or is_played

        current = by_round.setdefault(round_value, {"matches": False, "all_played": True, "any_played": False})
        current["matches"] = True
        current["all_played"] = bool(current["all_played"] and is_played)
        current["any_played"] = bool(current["any_played"] or is_played)

    if not by_round or not any_score_found:
        return None

    rounds_sorted = sorted(by_round.keys())
    for round_value in rounds_sorted:
        state = by_round[round_value]
        if not state["matches"]:
            continue
        if state["all_played"]:
            continue
        return round_value

    return rounds_sorted[-1]


def _infer_matchday_from_stats() -> Optional[int]:
    rows = _read_csv(STATS_DIR / "partite.csv")
    if not rows:
        return None

    max_played = 0
    for row in rows:
        played_value = _parse_int(
            row.get("Partite")
            or row.get("partite")
            or row.get("Presenze")
            or row.get("presenze")
            or row.get("PG")
        )
        if played_value is None:
            continue
        if played_value > max_played:
            max_played = played_value

    return (max_played + 1) if max_played > 0 else None


def _build_standings_index() -> Dict[str, Dict[str, object]]:
    index: Dict[str, Dict[str, object]] = {}
    for row in _load_standings_rows():
        team = str(row.get("team") or "").strip()
        if not team:
            continue
        index[normalize_name(team)] = {
            "team": team,
            "pos": _parse_int(row.get("pos")),
        }
    return index


def _resolve_team_name_with_standings(
    team_name: str,
    standings_index: Dict[str, Dict[str, object]],
) -> tuple[str, Optional[int]]:
    team = str(team_name or "").strip()
    if not team:
        return "", None

    team_key = normalize_name(team)
    direct = standings_index.get(team_key)
    if direct:
        return str(direct.get("team") or team), _parse_int(direct.get("pos"))

    if team_key and len(team_key) <= 4:
        matches = [item for key, item in standings_index.items() if key.startswith(team_key)]
        if len(matches) == 1:
            picked = matches[0]
            return str(picked.get("team") or team), _parse_int(picked.get("pos"))

    return team, None


def _formations_sort_key(item: Dict[str, object]) -> tuple[int, str]:
    standing_pos = _parse_int(item.get("standing_pos"))
    fallback_pos = _parse_int(item.get("pos"))
    pos = standing_pos if standing_pos is not None else (fallback_pos if fallback_pos is not None else 9999)
    return pos, normalize_name(str(item.get("team") or ""))


def _formations_sort_live_key(item: Dict[str, object]) -> tuple[int, float, int, str]:
    total = item.get("totale_live")
    numeric_total: Optional[float]
    if isinstance(total, (int, float)):
        numeric_total = float(total)
    else:
        numeric_total = _parse_float(total)
    if numeric_total is not None:
        base = _formations_sort_key(item)
        return (0, -numeric_total, base[0], base[1])
    base = _formations_sort_key(item)
    return (1, 0.0, base[0], base[1])


def _display_team_name(value: str, club_index: Dict[str, str]) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    key = normalize_name(raw)
    if key in club_index:
        return club_index[key]
    if key and len(key) <= 4:
        matches = [name for k, name in club_index.items() if k.startswith(key)]
        if len(matches) == 1:
            return matches[0]
    return raw.title() if raw.islower() else raw


def _load_club_name_index() -> Dict[str, str]:
    index: Dict[str, str] = {}
    for row in _read_csv(QUOT_PATH):
        team = str(row.get("Squadra") or row.get("Team") or "").strip()
        if not team:
            continue
        index.setdefault(normalize_name(team), team)

    fixture_rows = _read_csv_fallback(FIXTURES_PATH, SEED_DB_DIR / "fixtures.csv")
    for row in fixture_rows:
        for field in ("team", "opponent"):
            raw = str(row.get(field) or "").strip()
            if not raw:
                continue
            key = normalize_name(raw)
            if key not in index:
                index[key] = raw.title() if raw.islower() else raw
    return index


def _load_fixture_rows_for_live(db: Session, club_index: Dict[str, str]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []

    fixture_rows = db.query(Fixture).all()
    for row in fixture_rows:
        round_value = _parse_int(row.round)
        if round_value is None:
            continue
        team = _display_team_name(str(row.team or ""), club_index)
        opponent = _display_team_name(str(row.opponent or ""), club_index)
        if not team or not opponent:
            continue
        rows.append(
            {
                "round": round_value,
                "team": team,
                "opponent": opponent,
                "home_away": str(row.home_away or "").strip().upper(),
            }
        )

    if rows:
        return rows

    csv_rows = _read_csv_fallback(FIXTURES_PATH, SEED_DB_DIR / "fixtures.csv")
    for row in csv_rows:
        round_value = _parse_int(row.get("round"))
        if round_value is None:
            continue
        team = _display_team_name(str(row.get("team") or ""), club_index)
        opponent = _display_team_name(str(row.get("opponent") or ""), club_index)
        if not team or not opponent:
            continue
        rows.append(
            {
                "round": round_value,
                "team": team,
                "opponent": opponent,
                "home_away": str(row.get("home_away") or "").strip().upper(),
            }
        )
    return rows


def _rounds_from_fixture_rows(rows: List[Dict[str, object]]) -> List[int]:
    rounds = {
        int(round_value)
        for round_value in (_parse_int(row.get("round")) for row in rows)
        if isinstance(round_value, int) and round_value > 0
    }
    return sorted(rounds)


def _build_round_matches(rows: List[Dict[str, object]], round_value: int) -> List[Dict[str, object]]:
    grouped: Dict[Tuple[str, str], Dict[str, object]] = {}

    for row in rows:
        current_round = _parse_int(row.get("round"))
        if current_round != round_value:
            continue

        team = str(row.get("team") or "").strip()
        opponent = str(row.get("opponent") or "").strip()
        team_key = normalize_name(team)
        opponent_key = normalize_name(opponent)
        if not team_key or not opponent_key or team_key == opponent_key:
            continue

        pair_key = tuple(sorted((team_key, opponent_key)))
        home_away = str(row.get("home_away") or "").strip().upper()
        entry = grouped.setdefault(
            pair_key,
            {
                "round": round_value,
                "home_team": "",
                "away_team": "",
                "pair_key": pair_key,
            },
        )

        if home_away == "H":
            entry["home_team"] = team
            entry["away_team"] = opponent
        elif home_away == "A":
            entry["home_team"] = opponent
            entry["away_team"] = team
        else:
            if not entry["home_team"] and not entry["away_team"]:
                entry["home_team"] = team
                entry["away_team"] = opponent

    matches: List[Dict[str, object]] = []
    for entry in grouped.values():
        home_team = str(entry.get("home_team") or "").strip()
        away_team = str(entry.get("away_team") or "").strip()
        pair_key = entry.get("pair_key")
        if (not home_team or not away_team) and isinstance(pair_key, tuple) and len(pair_key) == 2:
            left, right = pair_key
            home_team = home_team or str(left)
            away_team = away_team or str(right)

        match_id = f"{round_value}:{normalize_name(home_team)}:{normalize_name(away_team)}"
        matches.append(
            {
                "round": round_value,
                "match_id": match_id,
                "home_team": home_team,
                "away_team": away_team,
                "pair_key": tuple(sorted((normalize_name(home_team), normalize_name(away_team)))),
            }
        )

    matches.sort(
        key=lambda item: (
            normalize_name(str(item.get("home_team") or "")),
            normalize_name(str(item.get("away_team") or "")),
        )
    )
    return matches


def _load_player_catalog_for_teams(
    team_names: Set[str],
    club_index: Dict[str, str],
) -> Dict[str, List[Dict[str, str]]]:
    catalog: Dict[str, List[Dict[str, str]]] = {team: [] for team in team_names}
    seen: Set[Tuple[str, str]] = set()

    for row in _read_csv(QUOT_PATH):
        team_name = _display_team_name(str(row.get("Squadra") or ""), club_index)
        if team_names and team_name not in team_names:
            continue
        player_name = _canonicalize_name(str(row.get("Giocatore") or ""))
        if not team_name or not player_name:
            continue

        item_key = (normalize_name(team_name), normalize_name(player_name))
        if item_key in seen:
            continue
        seen.add(item_key)

        catalog.setdefault(team_name, []).append(
            {
                "name": player_name,
                "role": str(row.get("Ruolo") or "").strip().upper(),
            }
        )

    for team in catalog:
        catalog[team].sort(key=lambda item: normalize_name(item.get("name", "")))
    return catalog


def _build_player_team_map(club_index: Dict[str, str]) -> Dict[str, str]:
    player_map: Dict[str, str] = {}
    for row in _read_csv(QUOT_PATH):
        player_name = _canonicalize_name(str(row.get("Giocatore") or ""))
        team_name = _display_team_name(str(row.get("Squadra") or ""), club_index)
        if not player_name or not team_name:
            continue
        player_map.setdefault(normalize_name(player_name), team_name)
    return player_map


def _infer_current_season_slug(reference: Optional[datetime] = None) -> str:
    current = reference or datetime.utcnow()
    start_year = current.year if current.month >= 7 else current.year - 1
    return f"{start_year}-{str(start_year + 1)[-2:]}"


def _normalize_season_slug(value: Optional[str]) -> str:
    raw = str(value or "").strip()
    if not raw:
        return _infer_current_season_slug()

    match = re.search(r"(20\d{2})\s*[-/]\s*(\d{2,4})", raw)
    if not match:
        return _infer_current_season_slug()

    start_year = int(match.group(1))
    end_part = match.group(2)
    if len(end_part) == 4:
        end_year = int(end_part)
        suffix = str(end_year)[-2:]
    else:
        suffix = end_part[-2:]
    return f"{start_year}-{suffix}"


def _build_default_voti_url(round_value: int, season_slug: str) -> str:
    return f"{VOTI_BASE_URL}/{season_slug}/{int(round_value)}"


def _fetch_text_url(url: str, timeout_seconds: float = 20.0) -> str:
    request = Request(
        str(url),
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.7,en;q=0.6",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read()
            encoding = response.headers.get_content_charset() or "utf-8"
            try:
                return raw.decode(encoding, errors="replace")
            except LookupError:
                return raw.decode("utf-8", errors="replace")
    except HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Import voti non riuscito (HTTP {exc.code}) da {url}",
        ) from exc
    except URLError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Import voti non riuscito: {exc.reason}",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Import voti non riuscito: {exc}",
        ) from exc


def _strip_html_tags(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", str(value or ""), flags=re.DOTALL)
    decoded = html_unescape(without_tags)
    return re.sub(r"\s+", " ", decoded).strip()


def _parse_fc_grade_value(raw_value: str) -> Tuple[Optional[float], bool]:
    raw = html_unescape(str(raw_value or "")).strip()
    if not raw:
        return None, False

    compact = raw.upper().replace(" ", "")
    if compact in {"SV", "S.V.", "S/V"}:
        return None, True
    if compact in {"-", "--", "N/A", "N.A."}:
        return None, False

    normalized = raw.replace(",", ".")
    parsed = _parse_float(normalized)
    if parsed is None:
        return None, False

    if parsed > 20 and parsed <= 100:
        rounded = round(parsed)
        if abs(parsed - rounded) < 0.0001 and rounded % 5 == 0:
            parsed = parsed / 10.0

    if parsed < 0 or parsed > 10:
        return None, False

    return round(parsed, 2), False


def _event_key_from_bonus_title(title: str, role: str) -> str:
    cleaned = _strip_html_tags(title).lower()
    if not cleaned:
        return ""
    # "Player of the match" is informational and must not affect fantasy scoring.
    if "player of the match" in cleaned or "man of the match" in cleaned:
        return ""
    if "decisiv" in cleaned:
        if "vittori" in cleaned:
            return "gol_vittoria"
        if "pareggi" in cleaned:
            return "gol_pareggio"
    if "gol vittoria" in cleaned:
        return "gol_vittoria"
    if "gol pareggio" in cleaned or "gol del pareggio" in cleaned:
        return "gol_pareggio"
    if "gol segn" in cleaned:
        return "goal"
    if "gol subit" in cleaned:
        return "gol_subito_portiere" if str(role or "").upper() == "P" else ""
    if "autore" in cleaned or "autogol" in cleaned:
        return "autogol"
    if "rigori segnat" in cleaned:
        return "rigore_segnato"
    if "rigori sbagliat" in cleaned:
        return "rigore_sbagliato"
    if "rigori parat" in cleaned:
        return "rigore_parato"
    if "assist" in cleaned:
        return "assist"
    return ""


def _extract_fantacalcio_voti_rows(
    html_text: str,
    club_index: Dict[str, str],
) -> Dict[str, object]:
    team_blocks = re.findall(
        r'<li\s+id="team-\d+"\s+class="team-table">\s*(.*?)</li>',
        str(html_text or ""),
        flags=re.IGNORECASE | re.DOTALL,
    )
    rows: List[Dict[str, object]] = []
    skipped_rows = 0
    teams_seen: Set[str] = set()

    for block in team_blocks:
        team_name_match = re.search(
            r'<div class="team-info">.*?<a class="team-name team-link[^"]*"[^>]*>(.*?)</a>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not team_name_match:
            continue

        team_name = _display_team_name(_strip_html_tags(team_name_match.group(1)), club_index)
        if not team_name:
            continue
        teams_seen.add(team_name)

        for row_html in re.findall(r"<tr>(.*?)</tr>", block, flags=re.IGNORECASE | re.DOTALL):
            role_match = re.search(
                r'<span class="role" data-value="([^"]+)"',
                row_html,
                flags=re.IGNORECASE,
            )
            role = str(role_match.group(1)).strip().upper()[:1] if role_match else ""
            if role not in FORMATION_ROLE_ORDER:
                continue

            player_name_match = re.search(
                r'<a class="player-name player-link[^"]*"[^>]*>(.*?)</a>',
                row_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if player_name_match:
                player_name = _canonicalize_name(_strip_html_tags(player_name_match.group(1)))
            else:
                fallback_name = re.search(
                    r'<span class="player-name">([^<]+)</span>',
                    row_html,
                    flags=re.IGNORECASE | re.DOTALL,
                )
                player_name = _canonicalize_name(_strip_html_tags(fallback_name.group(1))) if fallback_name else ""

            if not player_name:
                continue

            grade_match = re.search(
                r'<span class="([^"]*player-grade[^"]*)"[^>]*data-value="([^"]*)"',
                row_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            fanta_match = re.search(
                r'<span class="[^"]*player-fanta-grade[^"]*"[^>]*data-value="([^"]*)"',
                row_html,
                flags=re.IGNORECASE | re.DOTALL,
            )

            grade_classes = str(grade_match.group(1) if grade_match else "")
            raw_vote = str(grade_match.group(2) if grade_match else "")
            raw_fantavote = str(fanta_match.group(1) if fanta_match else "")
            vote_value, vote_is_sv = _parse_fc_grade_value(raw_vote)
            fantavote_value, fantavote_is_sv = _parse_fc_grade_value(raw_fantavote)
            is_sv = bool(vote_is_sv or fantavote_is_sv)

            events = {field: 0 for field in LIVE_EVENT_FIELDS}
            grade_class_normalized = str(grade_classes or "").lower()
            if "red-card" in grade_class_normalized:
                events["espulsione"] = 1
            elif "yellow-card" in grade_class_normalized:
                events["ammonizione"] = 1

            bonus_matches = re.findall(
                r'<span class="[^"]*player-bonus[^"]*"[^>]*data-value="([^"]*)"[^>]*title="([^"]*)"',
                row_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            for raw_count, raw_title in bonus_matches:
                event_key = _event_key_from_bonus_title(raw_title, role)
                if not event_key:
                    continue
                parsed_count = _parse_int(_strip_html_tags(raw_count))
                if parsed_count is None:
                    continue
                events[event_key] = max(0, int(parsed_count))

            has_events = any(int(events.get(field, 0)) > 0 for field in LIVE_EVENT_FIELDS)
            if vote_value is None and not is_sv and not has_events:
                skipped_rows += 1
                rows.append(
                    {
                        "team": team_name,
                        "player": player_name,
                        "role": role,
                        "vote": None,
                        "fantavote": None,
                        "is_sv": False,
                        "is_absent": True,
                        **events,
                    }
                )
                continue

            rows.append(
                {
                    "team": team_name,
                    "player": player_name,
                    "role": role,
                    "vote": vote_value,
                    "fantavote": fantavote_value,
                    "is_sv": is_sv,
                    "is_absent": False,
                    **events,
                }
            )

    deduped: Dict[Tuple[str, str], Dict[str, object]] = {}
    for item in rows:
        deduped[
            (
                normalize_name(str(item.get("team") or "")),
                normalize_name(str(item.get("player") or "")),
            )
        ] = item

    return {
        "rows": list(deduped.values()),
        "team_count": len(teams_seen),
        "raw_row_count": len(rows),
        "row_count": len(deduped),
        "skipped_rows": skipped_rows,
    }


def _parse_live_value(value: Optional[str]) -> Optional[float]:
    raw = str(value or "").strip()
    if not raw or raw.upper() == "SV":
        return None
    return _parse_float(raw)


def _format_live_number(value: Optional[float]) -> str:
    if value is None:
        return "-"
    rounded = round(float(value), 2)
    if abs(rounded - int(rounded)) < 0.0001:
        return str(int(rounded))
    return f"{rounded:.2f}".rstrip("0").rstrip(".").replace(".", ",")


def _safe_float_value(value: object, default: float) -> float:
    parsed = _parse_float(value)
    return float(default if parsed is None else parsed)


def _reg_scoring_defaults(regulation: Dict[str, object]) -> Dict[str, float]:
    scoring = regulation.get("scoring") if isinstance(regulation, dict) else {}
    scoring = scoring if isinstance(scoring, dict) else {}
    six_cfg = scoring.get("six_politico") if isinstance(scoring.get("six_politico"), dict) else {}

    default_vote = _safe_float_value(scoring.get("default_vote"), 6.0)
    default_fantavote = _safe_float_value(scoring.get("default_fantavote"), default_vote)
    six_vote = _safe_float_value(six_cfg.get("vote"), default_vote)
    six_fantavote = _safe_float_value(six_cfg.get("fantavote"), default_fantavote)

    return {
        "default_vote": default_vote,
        "default_fantavote": default_fantavote,
        "six_vote": six_vote,
        "six_fantavote": six_fantavote,
    }


def _reg_bonus_map(regulation: Dict[str, object]) -> Dict[str, float]:
    defaults = _default_regulation()
    default_bonus = defaults.get("scoring", {}).get("bonus_malus", {})

    scoring = regulation.get("scoring") if isinstance(regulation, dict) else {}
    scoring = scoring if isinstance(scoring, dict) else {}
    raw_bonus = scoring.get("bonus_malus") if isinstance(scoring.get("bonus_malus"), dict) else {}

    bonus_map: Dict[str, float] = {}
    for field in LIVE_EVENT_FIELDS:
        fallback = _safe_float_value(default_bonus.get(field), 0.0)
        bonus_map[field] = _safe_float_value(raw_bonus.get(field), fallback)
    return bonus_map


def _live_event_counts(raw: Dict[str, object]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for field in LIVE_EVENT_FIELDS:
        parsed = _parse_int(raw.get(field))
        counts[field] = max(0, parsed or 0)
    return counts


def _stats_counts_from_live_events(event_counts: Dict[str, int]) -> Dict[str, int]:
    counters: Dict[str, int] = {column: 0 for column in set(LIVE_EVENT_TO_STATS_COLUMN.values())}
    for event_key, column_name in LIVE_EVENT_TO_STATS_COLUMN.items():
        counters[column_name] = int(counters.get(column_name, 0)) + int(event_counts.get(event_key, 0) or 0)
    return counters


def _stats_delta_from_live_events(
    old_event_counts: Dict[str, int],
    new_event_counts: Dict[str, int],
) -> Dict[str, int]:
    old_counts = _stats_counts_from_live_events(old_event_counts)
    new_counts = _stats_counts_from_live_events(new_event_counts)
    deltas: Dict[str, int] = {}
    for column_name in set(old_counts.keys()) | set(new_counts.keys()):
        old_value = int(old_counts.get(column_name, 0))
        new_value = int(new_counts.get(column_name, 0))
        if new_value != old_value:
            deltas[column_name] = new_value - old_value
    return deltas


def _live_has_appearance(
    vote_value: Optional[float],
    fantavote_value: Optional[float],
    is_sv: bool,
    is_absent: bool,
    event_counts: Dict[str, int],
) -> bool:
    if bool(is_absent):
        return False
    if bool(is_sv):
        return True
    if vote_value is not None or fantavote_value is not None:
        return True
    return any(int(event_counts.get(field, 0)) > 0 for field in LIVE_EVENT_FIELDS)


def _is_nonzero_stats_delta(delta: Dict[str, int]) -> bool:
    return any(int(value or 0) != 0 for value in delta.values())


def _write_csv_rows(path: Path, fieldnames: List[str], rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _normalize_stat_counter(value: object) -> int:
    parsed_int = _parse_int(value)
    if parsed_int is not None:
        return max(0, parsed_int)
    parsed_float = _parse_float(value)
    if parsed_float is None:
        return 0
    return max(0, int(round(parsed_float)))


def _build_default_stats_row(player_name: str, team_name: str) -> Dict[str, object]:
    row = {field: "0" for field in STATS_MASTER_HEADERS}
    row["Giocatore"] = player_name
    row["Squadra"] = team_name
    row["Mediavoto"] = "0.0"
    row["Fantamedia"] = "0.0"
    return row


def _rebuild_rank_stats_files(stats_rows: List[Dict[str, object]], fallback_roles: Dict[str, str]) -> None:
    role_map = _load_role_map()
    STATS_DIR.mkdir(parents=True, exist_ok=True)

    for stat_column, filename in STATS_RANK_FILE_MAP:
        ranking_rows: List[Dict[str, object]] = []
        for row in stats_rows:
            player_name = _canonicalize_name(str(row.get("Giocatore") or ""))
            if not player_name:
                continue
            stat_value = _normalize_stat_counter(row.get(stat_column))
            if stat_value <= 0:
                continue
            key = normalize_name(player_name)
            role = role_map.get(key) or fallback_roles.get(key) or ""
            ranking_rows.append(
                {
                    "Giocatore": player_name,
                    "Posizione": role,
                    "Squadra": str(row.get("Squadra") or "").strip(),
                    stat_column: stat_value,
                }
            )

        ranking_rows.sort(
            key=lambda item: (
                -_normalize_stat_counter(item.get(stat_column)),
                normalize_name(str(item.get("Giocatore") or "")),
            )
        )
        _write_csv_rows(
            STATS_DIR / filename,
            ["Giocatore", "Posizione", "Squadra", stat_column],
            ranking_rows,
        )


def _sync_live_stats_for_player(
    player_name: str,
    team_name: str,
    role_value: Optional[str],
    stats_delta: Dict[str, int],
) -> None:
    if not _is_nonzero_stats_delta(stats_delta):
        return

    stats_rows = _read_csv(STATS_PATH)
    player_key = normalize_name(_canonicalize_name(player_name))

    headers = list(stats_rows[0].keys()) if stats_rows else []
    if not headers:
        headers = list(STATS_MASTER_HEADERS)
    for required in STATS_MASTER_HEADERS:
        if required not in headers:
            headers.append(required)

    target_row = None
    for row in stats_rows:
        row_name = _canonicalize_name(str(row.get("Giocatore") or ""))
        if normalize_name(row_name) == player_key:
            target_row = row
            break

    if target_row is None:
        target_row = _build_default_stats_row(_canonicalize_name(player_name), team_name)
        stats_rows.append(target_row)

    target_row["Giocatore"] = _canonicalize_name(player_name)
    target_row["Squadra"] = team_name

    for counter_name, delta_value in stats_delta.items():
        if counter_name not in headers:
            headers.append(counter_name)
        previous = _normalize_stat_counter(target_row.get(counter_name))
        updated = max(0, previous + int(delta_value))
        target_row[counter_name] = str(updated)

    stats_rows.sort(key=lambda row: normalize_name(str(row.get("Giocatore") or "")))
    _write_csv_rows(STATS_PATH, headers, stats_rows)

    fallback_roles: Dict[str, str] = {}
    if role_value:
        fallback_roles[player_key] = str(role_value).strip().upper()[:1]
    _rebuild_rank_stats_files(stats_rows, fallback_roles)


def _compute_live_fantavote(
    vote_value: Optional[float],
    event_counts: Dict[str, int],
    bonus_map: Dict[str, float],
    fantavote_override: Optional[float] = None,
) -> Optional[float]:
    if vote_value is None:
        return None

    has_events = any(int(event_counts.get(field, 0)) > 0 for field in LIVE_EVENT_FIELDS)
    if fantavote_override is not None and not has_events:
        return round(float(fantavote_override), 2)

    total = float(vote_value)
    for field in LIVE_EVENT_FIELDS:
        count = int(event_counts.get(field, 0))
        if count <= 0:
            continue
        total += float(bonus_map.get(field, 0.0)) * count
    return round(total, 2)


def _evaluate_bands(value: float, bands: List[Dict[str, object]]) -> float:
    for band in bands:
        if not isinstance(band, dict):
            continue

        min_value = _parse_float(band.get("min"))
        max_value = _parse_float(band.get("max"))
        min_inclusive = bool(band.get("min_inclusive", True))
        max_inclusive = bool(band.get("max_inclusive", True))

        if min_value is not None:
            if min_inclusive:
                if value < min_value:
                    continue
            else:
                if value <= min_value:
                    continue
        if max_value is not None:
            if max_inclusive:
                if value > max_value:
                    continue
            else:
                if value >= max_value:
                    continue

        return _safe_float_value(band.get("value"), 0.0)
    return 0.0


def _reg_ordering(regulation: Dict[str, object]) -> Tuple[str, List[str]]:
    raw_ordering = regulation.get("ordering") if isinstance(regulation, dict) else {}
    ordering = raw_ordering if isinstance(raw_ordering, dict) else {}
    default_value = str(ordering.get("default") or "classifica").strip().lower()
    allowed_values = ordering.get("allowed")
    if not isinstance(allowed_values, list):
        allowed_values = ["classifica", "live_total"]
    allowed = []
    for value in allowed_values:
        key = str(value or "").strip().lower()
        if key in {"classifica", "live_total"} and key not in allowed:
            allowed.append(key)
    if not allowed:
        allowed = ["classifica", "live_total"]
    if default_value not in allowed:
        default_value = allowed[0]
    return default_value, allowed


def _load_live_round_context(db: Session, round_value: Optional[int]) -> Dict[str, object]:
    regulation = _load_regulation()
    scoring_defaults = _reg_scoring_defaults(regulation)
    bonus_map = _reg_bonus_map(regulation)

    club_index = _load_club_name_index()
    fixture_rows = _load_fixture_rows_for_live(db, club_index)
    available_rounds = _rounds_from_fixture_rows(fixture_rows)

    target_round = round_value
    if target_round is None:
        target_round = _resolve_current_round(available_rounds)
    if not target_round and available_rounds:
        target_round = available_rounds[-1]

    target_round = int(target_round) if target_round else 1
    matches = _build_round_matches(fixture_rows, target_round)

    try:
        fixture_flags = (
            db.query(LiveFixtureFlag).filter(LiveFixtureFlag.round == target_round).all()
            if target_round
            else []
        )
    except OperationalError:
        fixture_flags = []
    flag_map: Dict[Tuple[str, str], bool] = {}
    for row in fixture_flags:
        pair_key = tuple(
            sorted(
                (
                    normalize_name(_display_team_name(str(row.home_team or ""), club_index)),
                    normalize_name(_display_team_name(str(row.away_team or ""), club_index)),
                )
            )
        )
        if pair_key[0] and pair_key[1] and row.six_politico:
            flag_map[pair_key] = True

    six_team_keys: Set[str] = set()
    for match in matches:
        pair_key = match.get("pair_key")
        six_politico = bool(flag_map.get(pair_key, False))
        match["six_politico"] = six_politico
        if six_politico:
            six_team_keys.add(normalize_name(str(match.get("home_team") or "")))
            six_team_keys.add(normalize_name(str(match.get("away_team") or "")))

    team_names: Set[str] = set()
    for match in matches:
        home_team = str(match.get("home_team") or "").strip()
        away_team = str(match.get("away_team") or "").strip()
        if home_team:
            team_names.add(home_team)
        if away_team:
            team_names.add(away_team)

    catalog = _load_player_catalog_for_teams(team_names, club_index)
    player_team_map = _build_player_team_map(club_index)

    try:
        vote_rows = (
            db.query(LivePlayerVote).filter(LivePlayerVote.round == target_round).all()
            if target_round
            else []
        )
    except OperationalError:
        vote_rows = []
    votes_by_team_player: Dict[Tuple[str, str], Dict[str, object]] = {}
    votes_by_player: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    teams_with_votes: Set[str] = set()
    for row in vote_rows:
        team_name = _display_team_name(str(row.team or ""), club_index)
        player_name = _canonicalize_name(str(row.player_name or ""))
        team_key = normalize_name(team_name)
        player_key = normalize_name(player_name)
        if not team_key or not player_key:
            continue
        payload = {
            "team": team_name,
            "team_key": team_key,
            "player_name": player_name,
            "player_key": player_key,
            "role": str(row.role or "").strip().upper(),
            "vote": row.vote,
            "fantavote": row.fantavote,
            **_live_event_counts(
                {
                    field: getattr(row, field, 0)
                    for field in LIVE_EVENT_FIELDS
                }
            ),
            "is_sv": bool(row.is_sv),
            "is_absent": bool(getattr(row, "is_absent", False)),
            "updated_at": row.updated_at.isoformat() if row.updated_at else "",
        }
        votes_by_team_player[(team_key, player_key)] = payload
        votes_by_player[player_key].append(payload)
        teams_with_votes.add(team_key)

    return {
        "round": target_round,
        "available_rounds": available_rounds,
        "matches": matches,
        "catalog": catalog,
        "player_team_map": player_team_map,
        "votes_by_team_player": votes_by_team_player,
        "votes_by_player": votes_by_player,
        "teams_with_votes": teams_with_votes,
        "six_team_keys": six_team_keys,
        "regulation": regulation,
        "scoring_defaults": scoring_defaults,
        "bonus_map": bonus_map,
    }


def _resolve_live_player_score(player_name: str, context: Dict[str, object]) -> Dict[str, object]:
    canonical_player = _canonicalize_name(player_name)
    player_key = normalize_name(canonical_player)
    scoring_defaults = context.get("scoring_defaults", {})
    default_vote = _safe_float_value(scoring_defaults.get("default_vote"), 6.0)
    default_fantavote = _safe_float_value(scoring_defaults.get("default_fantavote"), default_vote)
    six_vote = _safe_float_value(scoring_defaults.get("six_vote"), default_vote)
    six_fantavote = _safe_float_value(scoring_defaults.get("six_fantavote"), default_fantavote)
    bonus_map = context.get("bonus_map", {})
    bonus_map = bonus_map if isinstance(bonus_map, dict) else _reg_bonus_map(_default_regulation())

    player_team_map: Dict[str, str] = context.get("player_team_map", {})
    club_name = player_team_map.get(player_key, "")
    club_key = normalize_name(club_name)
    six_team_keys: Set[str] = context.get("six_team_keys", set())

    if club_key and club_key in six_team_keys:
        return {
            "vote": six_vote,
            "fantavote": six_fantavote,
            "vote_label": _format_live_number(six_vote),
            "fantavote_label": _format_live_number(six_fantavote),
            "events": {field: 0 for field in LIVE_EVENT_FIELDS},
            "bonus_total": round(six_fantavote - six_vote, 2),
            "is_sv": False,
            "is_absent": False,
            "source": "six_politico",
            "manual": False,
        }

    votes_by_team_player: Dict[Tuple[str, str], Dict[str, object]] = context.get(
        "votes_by_team_player", {}
    )
    votes_by_player: Dict[str, List[Dict[str, object]]] = context.get("votes_by_player", {})

    vote_row = votes_by_team_player.get((club_key, player_key)) if club_key else None
    if vote_row is None:
        player_rows = votes_by_player.get(player_key, [])
        if len(player_rows) == 1:
            vote_row = player_rows[0]

    if vote_row is not None:
        event_counts = _live_event_counts(vote_row)
        is_absent = bool(vote_row.get("is_absent"))
        if is_absent:
            return {
                "vote": None,
                "fantavote": None,
                "vote_label": "X",
                "fantavote_label": "X",
                "events": {field: 0 for field in LIVE_EVENT_FIELDS},
                "bonus_total": None,
                "is_sv": False,
                "is_absent": True,
                "source": "manual",
                "manual": True,
            }
        is_sv = bool(vote_row.get("is_sv"))
        if is_sv:
            return {
                "vote": None,
                "fantavote": None,
                "vote_label": "SV",
                "fantavote_label": "SV",
                "events": event_counts,
                "bonus_total": None,
                "is_sv": True,
                "is_absent": False,
                "source": "manual",
                "manual": True,
            }

        vote_value = vote_row.get("vote")
        fantavote_value = vote_row.get("fantavote")
        vote_number = default_vote if vote_value is None else float(vote_value)
        fantavote_override = float(fantavote_value) if fantavote_value is not None else None
        fantavote_number = _compute_live_fantavote(
            vote_number,
            event_counts,
            bonus_map,
            fantavote_override=fantavote_override,
        )
        if fantavote_number is None:
            fantavote_number = default_fantavote
        return {
            "vote": vote_number,
            "fantavote": fantavote_number,
            "vote_label": _format_live_number(vote_number),
            "fantavote_label": _format_live_number(fantavote_number),
            "events": event_counts,
            "bonus_total": round(fantavote_number - vote_number, 2),
            "is_sv": False,
            "is_absent": False,
            "source": "manual",
            "manual": True,
        }

    teams_with_votes_raw = context.get("teams_with_votes", set())
    teams_with_votes = (
        teams_with_votes_raw
        if isinstance(teams_with_votes_raw, set)
        else {normalize_name(str(value or "")) for value in (teams_with_votes_raw or [])}
    )
    if club_key and club_key in teams_with_votes:
        return {
            "vote": None,
            "fantavote": None,
            "vote_label": "X",
            "fantavote_label": "X",
            "events": {field: 0 for field in LIVE_EVENT_FIELDS},
            "bonus_total": None,
            "is_sv": False,
            "is_absent": True,
            "source": "import_absent",
            "manual": False,
        }

    return {
        "vote": default_vote,
        "fantavote": default_fantavote,
        "vote_label": _format_live_number(default_vote),
        "fantavote_label": _format_live_number(default_fantavote),
        "events": {field: 0 for field in LIVE_EVENT_FIELDS},
        "bonus_total": round(default_fantavote - default_vote, 2),
        "is_sv": False,
        "is_absent": False,
        "source": "default",
        "manual": False,
    }


def _safe_number(value: object) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _player_score_lookup(
    player_scores: Dict[str, Dict[str, object]],
    player_name: str,
) -> Optional[Dict[str, object]]:
    direct = player_scores.get(player_name)
    if direct is not None:
        return direct
    key = normalize_name(player_name)
    for candidate_name, payload in player_scores.items():
        if normalize_name(candidate_name) == key:
            return payload
    return None


def _score_has_live_vote(score: Optional[Dict[str, object]]) -> bool:
    if not isinstance(score, dict):
        return False
    return _safe_number(score.get("fantavote")) is not None


def _lineup_starters(item: Dict[str, object]) -> List[Dict[str, str]]:
    starters: List[Dict[str, str]] = []
    goalkeeper = str(item.get("portiere") or "").strip()
    if goalkeeper:
        starters.append({"name": goalkeeper, "role": "P"})

    role_fields = [
        ("difensori", "D"),
        ("centrocampisti", "C"),
        ("attaccanti", "A"),
    ]
    for field, role in role_fields:
        values = item.get(field) if isinstance(item.get(field), list) else []
        for value in values:
            player_name = str(value or "").strip()
            if not player_name:
                continue
            starters.append({"name": player_name, "role": role})
    return starters


def _lineup_reserves(item: Dict[str, object], role_map: Dict[str, str]) -> List[Dict[str, str]]:
    reserves: List[Dict[str, str]] = []
    raw_details = item.get("panchina_details")
    if isinstance(raw_details, list):
        for idx, reserve in enumerate(raw_details):
            if not isinstance(reserve, dict):
                continue
            player_name = _canonicalize_name(str(reserve.get("name") or ""))
            if not player_name:
                continue
            role = _role_from_text(reserve.get("role"))
            if not role:
                role = _role_from_text(role_map.get(normalize_name(player_name), ""))
            reserve_id = str(reserve.get("id") or f"r{idx}")
            reserves.append(
                {
                    "id": reserve_id,
                    "name": player_name,
                    "role": role,
                }
            )
    if reserves:
        return reserves

    fallback = item.get("panchina")
    if isinstance(fallback, list):
        for idx, value in enumerate(fallback):
            player_name = _canonicalize_name(str(value or ""))
            if not player_name:
                continue
            role = _role_from_text(role_map.get(normalize_name(player_name), ""))
            reserves.append(
                {
                    "id": f"r{idx}",
                    "name": player_name,
                    "role": role,
                }
            )
    return reserves


def _is_valid_module_counts(counts: Dict[str, int], allowed_modules: List[str]) -> bool:
    module = _module_from_role_counts(counts)
    if not module:
        return False
    if not allowed_modules:
        return True
    return module in allowed_modules


def _pick_reserve_same_role(
    target_role: str,
    reserves: List[Dict[str, str]],
    used_reserve_ids: Set[str],
    player_scores: Dict[str, Dict[str, object]],
) -> Optional[Dict[str, str]]:
    for reserve in reserves:
        reserve_id = str(reserve.get("id") or "")
        if reserve_id in used_reserve_ids:
            continue
        reserve_role = _role_from_text(reserve.get("role"))
        if reserve_role != target_role:
            continue
        reserve_name = str(reserve.get("name") or "").strip()
        if not reserve_name:
            continue
        if not _score_has_live_vote(_player_score_lookup(player_scores, reserve_name)):
            continue
        return reserve
    return None


def _pick_reserve_flexible(
    target_role: str,
    target_index: int,
    effective_entries: List[Dict[str, str]],
    reserves: List[Dict[str, str]],
    used_reserve_ids: Set[str],
    player_scores: Dict[str, Dict[str, object]],
    allowed_modules: List[str],
) -> Optional[Dict[str, str]]:
    for reserve in reserves:
        reserve_id = str(reserve.get("id") or "")
        if reserve_id in used_reserve_ids:
            continue

        reserve_name = str(reserve.get("name") or "").strip()
        reserve_role = _role_from_text(reserve.get("role"))
        if not reserve_name or not reserve_role:
            continue
        if not _score_has_live_vote(_player_score_lookup(player_scores, reserve_name)):
            continue

        if target_role == "P" and reserve_role != "P":
            continue

        simulated = [dict(entry) for entry in effective_entries]
        if target_index < 0 or target_index >= len(simulated):
            continue
        simulated[target_index]["role"] = reserve_role

        counts = _lineup_role_counts(simulated)
        if not _is_valid_module_counts(counts, allowed_modules):
            continue
        return reserve
    return None


def _apply_live_substitutions(
    item: Dict[str, object],
    player_scores: Dict[str, Dict[str, object]],
    regulation: Dict[str, object],
    role_map: Dict[str, str],
) -> Dict[str, object]:
    starters = _lineup_starters(item)
    reserves = _lineup_reserves(item, role_map)

    formation_rules = regulation.get("formation_rules") if isinstance(regulation, dict) else {}
    formation_rules = formation_rules if isinstance(formation_rules, dict) else {}
    max_substitutions = _parse_int(formation_rules.get("max_substitutions"))
    if max_substitutions is None:
        max_substitutions = 4
    max_substitutions = max(0, max_substitutions)

    allowed_modules = _allowed_modules_from_regulation(regulation)
    if not allowed_modules:
        fallback_module = _normalize_module(item.get("modulo"))
        if fallback_module:
            allowed_modules = [fallback_module]

    effective_entries = [dict(entry) for entry in starters]
    substitutions: List[Dict[str, str]] = []
    missing_players: List[str] = []
    used_reserve_ids: Set[str] = set()

    substitutions_used = 0
    for idx, starter in enumerate(starters):
        starter_name = str(starter.get("name") or "").strip()
        starter_role = _role_from_text(starter.get("role"))
        if not starter_name or not starter_role:
            continue

        starter_score = _player_score_lookup(player_scores, starter_name)
        if _score_has_live_vote(starter_score):
            continue

        if substitutions_used >= max_substitutions:
            missing_players.append(starter_name)
            continue

        reserve = _pick_reserve_same_role(
            starter_role,
            reserves,
            used_reserve_ids,
            player_scores,
        )

        source = "same_role"
        if reserve is None:
            reserve = _pick_reserve_flexible(
                starter_role,
                idx,
                effective_entries,
                reserves,
                used_reserve_ids,
                player_scores,
                allowed_modules,
            )
            source = "flex"

        if reserve is None:
            missing_players.append(starter_name)
            continue

        reserve_name = str(reserve.get("name") or "").strip()
        reserve_role = _role_from_text(reserve.get("role"))
        reserve_id = str(reserve.get("id") or "")
        if not reserve_name or not reserve_role or not reserve_id:
            missing_players.append(starter_name)
            continue

        effective_entries[idx] = {"name": reserve_name, "role": reserve_role}
        used_reserve_ids.add(reserve_id)
        substitutions_used += 1
        substitutions.append(
            {
                "out": starter_name,
                "out_role": starter_role,
                "in": reserve_name,
                "in_role": reserve_role,
                "source": source,
            }
        )

    by_role = {role: [] for role in FORMATION_ROLE_ORDER}
    for entry in effective_entries:
        player_name = str(entry.get("name") or "").strip()
        role = _role_from_text(entry.get("role"))
        if not player_name or not role:
            continue
        by_role[role].append(player_name)

    role_counts = _lineup_role_counts(effective_entries)
    effective_module = _module_from_role_counts(role_counts)
    players_with_vote = 0
    for entry in effective_entries:
        player_name = str(entry.get("name") or "").strip()
        if not player_name:
            continue
        score = _player_score_lookup(player_scores, player_name)
        if _score_has_live_vote(score):
            players_with_vote += 1

    return {
        "portiere": by_role["P"][0] if by_role["P"] else "",
        "difensori": by_role["D"],
        "centrocampisti": by_role["C"],
        "attaccanti": by_role["A"],
        "module": _format_module(effective_module),
        "module_raw": effective_module,
        "substitutions": substitutions,
        "missing_players": missing_players,
        "players_with_vote": players_with_vote,
    }


def _compute_defense_modifier(
    item: Dict[str, object],
    player_scores: Dict[str, Dict[str, object]],
    regulation: Dict[str, object],
) -> Dict[str, object]:
    modifiers = regulation.get("modifiers") if isinstance(regulation, dict) else {}
    difesa_cfg = modifiers.get("difesa") if isinstance(modifiers, dict) else {}
    difesa_cfg = difesa_cfg if isinstance(difesa_cfg, dict) else {}
    if not bool(difesa_cfg.get("enabled")):
        return {"value": 0.0, "average_vote": None}

    requires_def = _parse_int(difesa_cfg.get("requires_defenders_min")) or 4
    defenders = item.get("difensori") if isinstance(item.get("difensori"), list) else []
    defenders = [str(value).strip() for value in defenders if str(value).strip()]
    if len(defenders) < requires_def:
        return {"value": 0.0, "average_vote": None}

    defender_votes: List[float] = []
    for defender in defenders:
        payload = _player_score_lookup(player_scores, defender)
        vote_value = _safe_number(payload.get("vote")) if isinstance(payload, dict) else None
        if vote_value is not None:
            defender_votes.append(vote_value)
    if len(defender_votes) < 3:
        return {"value": 0.0, "average_vote": None}

    defender_votes.sort(reverse=True)
    average_values = defender_votes[:3]

    include_goalkeeper = bool(difesa_cfg.get("include_goalkeeper_vote", difesa_cfg.get("use_goalkeeper", True)))
    if include_goalkeeper:
        goalkeeper = str(item.get("portiere") or "").strip()
        if not goalkeeper:
            return {"value": 0.0, "average_vote": None}
        gk_payload = _player_score_lookup(player_scores, goalkeeper)
        gk_vote = _safe_number(gk_payload.get("vote")) if isinstance(gk_payload, dict) else None
        if gk_vote is None:
            return {"value": 0.0, "average_vote": None}
        average_values = [gk_vote] + average_values

    avg_vote = sum(average_values) / len(average_values)
    bands = difesa_cfg.get("bands")
    value = _evaluate_bands(avg_vote, bands if isinstance(bands, list) else [])
    return {"value": round(value, 2), "average_vote": round(avg_vote, 3)}


def _compute_captain_modifier(
    item: Dict[str, object],
    player_scores: Dict[str, Dict[str, object]],
    regulation: Dict[str, object],
) -> Dict[str, object]:
    modifiers = regulation.get("modifiers") if isinstance(regulation, dict) else {}
    captain_cfg = modifiers.get("capitano") if isinstance(modifiers, dict) else {}
    captain_cfg = captain_cfg if isinstance(captain_cfg, dict) else {}
    if not bool(captain_cfg.get("enabled")):
        return {"value": 0.0, "captain_player": "", "captain_vote": None}

    captain_name = str(item.get("capitano") or item.get("captain") or "").strip()
    vice_name = str(item.get("vice_capitano") or item.get("vicecaptain") or "").strip()
    selected_player = ""
    selected_vote = None

    for candidate in (captain_name, vice_name):
        if not candidate:
            continue
        payload = _player_score_lookup(player_scores, candidate)
        vote_value = _safe_number(payload.get("vote")) if isinstance(payload, dict) else None
        if vote_value is not None:
            selected_player = candidate
            selected_vote = vote_value
            break

    if selected_vote is None:
        return {"value": 0.0, "captain_player": "", "captain_vote": None}

    bands = captain_cfg.get("bands")
    value = _evaluate_bands(float(selected_vote), bands if isinstance(bands, list) else [])
    return {
        "value": round(value, 2),
        "captain_player": selected_player,
        "captain_vote": round(float(selected_vote), 2),
    }


def _attach_live_scores_to_formations(
    items: List[Dict[str, object]],
    context: Dict[str, object],
) -> None:
    regulation = context.get("regulation")
    if not isinstance(regulation, dict):
        regulation = _default_regulation()

    role_map = _load_role_map()
    for item in items:
        original_lineup = {
            "modulo": _format_module(item.get("modulo")),
            "portiere": str(item.get("portiere") or "").strip(),
            "difensori": [str(value).strip() for value in (item.get("difensori") or []) if str(value).strip()],
            "centrocampisti": [
                str(value).strip() for value in (item.get("centrocampisti") or []) if str(value).strip()
            ],
            "attaccanti": [str(value).strip() for value in (item.get("attaccanti") or []) if str(value).strip()],
        }

        players_set: Set[str] = set()
        for value in (
            original_lineup["portiere"],
            str(item.get("capitano") or "").strip(),
            str(item.get("vice_capitano") or "").strip(),
        ):
            if value:
                players_set.add(value)
        for field in ("difensori", "centrocampisti", "attaccanti", "panchina"):
            values = item.get(field) or []
            if isinstance(values, list):
                for value in values:
                    player_name = str(value).strip()
                    if player_name:
                        players_set.add(player_name)
        if isinstance(item.get("panchina_details"), list):
            for reserve in item.get("panchina_details") or []:
                if isinstance(reserve, dict):
                    player_name = str(reserve.get("name") or "").strip()
                    if player_name:
                        players_set.add(player_name)

        player_scores: Dict[str, Dict[str, object]] = {}
        for player_name in sorted(players_set, key=lambda value: normalize_name(value)):
            player_scores[player_name] = _resolve_live_player_score(player_name, context)

        effective_lineup = _apply_live_substitutions(item, player_scores, regulation, role_map)
        effective_item = {
            **item,
            "portiere": effective_lineup.get("portiere") or "",
            "difensori": effective_lineup.get("difensori") or [],
            "centrocampisti": effective_lineup.get("centrocampisti") or [],
            "attaccanti": effective_lineup.get("attaccanti") or [],
        }

        base_total = 0.0
        base_count = 0
        effective_players: List[str] = []
        for value in (
            str(effective_item.get("portiere") or "").strip(),
            *[str(x).strip() for x in effective_item.get("difensori", [])],
            *[str(x).strip() for x in effective_item.get("centrocampisti", [])],
            *[str(x).strip() for x in effective_item.get("attaccanti", [])],
        ):
            if value:
                effective_players.append(value)

        for player_name in effective_players:
            score = _player_score_lookup(player_scores, player_name)
            fantavote_value = _safe_number(score.get("fantavote")) if isinstance(score, dict) else None
            if fantavote_value is not None:
                base_total += fantavote_value
                base_count += 1

        defense_modifier = _compute_defense_modifier(effective_item, player_scores, regulation)
        captain_modifier = _compute_captain_modifier(effective_item, player_scores, regulation)
        mod_difesa = float(defense_modifier.get("value") or 0.0)
        mod_capitano = float(captain_modifier.get("value") or 0.0)
        live_total = round(base_total + mod_difesa + mod_capitano, 2) if base_count else None
        base_total_value = round(base_total, 2) if base_count else None

        effective_module = str(effective_lineup.get("module") or "").strip()
        if effective_module:
            item["modulo"] = effective_module
        else:
            item["modulo"] = original_lineup["modulo"]
        item["modulo_originale"] = original_lineup["modulo"]
        item["original_lineup"] = original_lineup
        item["portiere"] = effective_item["portiere"]
        item["difensori"] = effective_item["difensori"]
        item["centrocampisti"] = effective_item["centrocampisti"]
        item["attaccanti"] = effective_item["attaccanti"]
        item["substitutions"] = effective_lineup.get("substitutions", [])
        item["missing_players"] = effective_lineup.get("missing_players", [])
        item["players_with_vote"] = int(effective_lineup.get("players_with_vote") or 0)
        item["player_scores"] = player_scores
        item["fantavote_total"] = base_total_value
        item["totale_live_base"] = base_total_value
        item["mod_difesa"] = round(mod_difesa, 2)
        item["mod_capitano"] = round(mod_capitano, 2)
        item["totale_live"] = live_total
        item["totale_live_label"] = _format_live_number(live_total)
        item["live_components"] = {
            "base": base_total_value,
            "mod_difesa": round(mod_difesa, 2),
            "mod_capitano": round(mod_capitano, 2),
            "totale_live": live_total,
            "difesa_average_vote": defense_modifier.get("average_vote"),
            "captain_player": captain_modifier.get("captain_player"),
            "captain_vote": captain_modifier.get("captain_vote"),
        }


def _load_projected_formazioni_rows(
    team_key: str,
    standings_index: Dict[str, Dict[str, object]],
) -> List[Dict[str, object]]:
    rows = _read_csv(STARTING_XI_REPORT_PATH)
    items: List[Dict[str, object]] = []

    for idx, row in enumerate(rows):
        team_name_raw = str(row.get("Team") or row.get("Squadra") or "").strip()
        if not team_name_raw:
            continue

        resolved_team, standing_pos = _resolve_team_name_with_standings(team_name_raw, standings_index)
        raw_key = normalize_name(team_name_raw)
        resolved_key = normalize_name(resolved_team)
        if team_key and team_key not in {raw_key, resolved_key}:
            continue

        csv_pos = _parse_int(row.get("Pos"))
        forza_titolari = _parse_float(row.get("ForzaTitolari")) or 0.0
        portiere = _split_players_cell(row.get("Portiere"))

        items.append(
            {
                "pos": standing_pos if standing_pos is not None else (csv_pos if csv_pos is not None else idx + 1),
                "standing_pos": standing_pos,
                "team": resolved_team or team_name_raw,
                "modulo": _format_module(str(row.get("ModuloMigliore") or row.get("Modulo") or "").strip()),
                "forza_titolari": forza_titolari,
                "portiere": portiere[0] if portiere else "",
                "difensori": _split_players_cell(row.get("Difensori")),
                "centrocampisti": _split_players_cell(row.get("Centrocampisti")),
                "attaccanti": _split_players_cell(row.get("Attaccanti")),
                "panchina": [],
                "panchina_details": [],
                "capitano": "",
                "vice_capitano": "",
                "round": None,
                "source": "projection",
            }
        )

    return items


def _latest_formazioni_appkey_path() -> Optional[Path]:
    if not REAL_FORMATIONS_TMP_DIR.exists() or not REAL_FORMATIONS_TMP_DIR.is_dir():
        return None
    files = [p for p in REAL_FORMATIONS_TMP_DIR.glob(REAL_FORMATIONS_APPKEY_GLOB) if p.is_file()]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def _extract_js_object_literal(source: str, key: str) -> str:
    if not source:
        return ""
    match = re.search(rf"\b{re.escape(key)}\s*:\s*\{{", source)
    if match is None:
        return ""
    start = match.end() - 1
    depth = 0
    in_string = False
    escape = False
    quote = ""
    for idx in range(start, len(source)):
        ch = source[idx]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == quote:
                in_string = False
            continue
        if ch == '"' or ch == "'":
            in_string = True
            quote = ch
            continue
        if ch == "{":
            depth += 1
            continue
        if ch == "}":
            depth -= 1
            if depth == 0:
                return source[start : idx + 1]
    return ""


def _load_team_id_position_index_from_formazioni_html() -> Dict[int, int]:
    for candidate in REAL_FORMATIONS_CONTEXT_HTML_CANDIDATES:
        if not candidate.exists():
            continue
        try:
            raw = candidate.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if not raw:
            continue

        competition_literal = _extract_js_object_literal(raw, "currentCompetition")
        if not competition_literal:
            continue

        try:
            parsed = json.loads(competition_literal)
        except Exception:
            continue

        squads = parsed.get("squadre") if isinstance(parsed, dict) else []
        if not isinstance(squads, list):
            continue

        index: Dict[int, int] = {}
        for squad in squads:
            if not isinstance(squad, dict):
                continue
            team_id = _parse_int(squad.get("id"))
            pos = _parse_int(squad.get("pos"))
            if team_id is None or pos is None:
                continue
            index[team_id] = pos
        if index:
            return index
    return {}


def _build_standings_team_by_pos(
    standings_index: Optional[Dict[str, Dict[str, object]]],
) -> Dict[int, str]:
    if not isinstance(standings_index, dict):
        return {}
    team_by_pos: Dict[int, str] = {}
    for entry in standings_index.values():
        if not isinstance(entry, dict):
            continue
        pos = _parse_int(entry.get("pos"))
        team = str(entry.get("team") or "").strip()
        if pos is None or not team:
            continue
        team_by_pos[pos] = team
    return team_by_pos


def _extract_appkey_captains(cap_raw: object, players: List[Dict[str, object]]) -> tuple[str, str]:
    by_id: Dict[str, str] = {}
    for player in players:
        if not isinstance(player, dict):
            continue
        player_name = _canonicalize_name(str(player.get("n") or ""))
        if not player_name:
            continue
        for key in ("id", "i", "id_s"):
            value = str(player.get(key) or "").strip()
            if value:
                by_id[value] = player_name

    names: List[str] = []
    for token in re.split(r"[;,|]+", str(cap_raw or "")):
        current = token.strip()
        if not current:
            continue
        player_name = by_id.get(current)
        if not player_name:
            player_name = _canonicalize_name(current)
        if not player_name:
            continue
        if player_name not in names:
            names.append(player_name)

    captain = names[0] if names else ""
    vice = names[1] if len(names) > 1 else ""
    return captain, vice


def _parse_appkey_lineup(
    players: List[Dict[str, object]],
    module_raw: object,
    role_map: Dict[str, str],
) -> Dict[str, object]:
    normalized_players: List[Dict[str, str]] = []
    for idx, player in enumerate(players):
        if not isinstance(player, dict):
            continue
        player_name = _canonicalize_name(str(player.get("n") or ""))
        if not player_name:
            continue
        player_key = normalize_name(player_name)
        role = _role_from_text(player.get("r")) or _role_from_text(role_map.get(player_key, ""))
        player_id = str(player.get("id") or player.get("i") or f"p{idx}").strip()
        normalized_players.append(
            {
                "id": player_id or f"p{idx}",
                "name": player_name,
                "role": role,
                "order": str(idx),
            }
        )

    module_counts = _module_counts_from_str(module_raw)
    if module_counts is None:
        inferred = {"P": 0, "D": 0, "C": 0, "A": 0}
        for player in normalized_players[:11]:
            role = _role_from_text(player.get("role"))
            if role:
                inferred[role] = int(inferred.get(role, 0)) + 1
        if inferred["P"] == 1 and sum(int(inferred.get(role, 0)) for role in FORMATION_ROLE_ORDER) == 11:
            module_counts = inferred
        else:
            module_counts = {"P": 1, "D": 3, "C": 4, "A": 3}

    starters_by_role: Dict[str, List[str]] = {role: [] for role in FORMATION_ROLE_ORDER}
    remaining = {role: int(module_counts.get(role, 0)) for role in FORMATION_ROLE_ORDER}
    selected_indexes: Set[int] = set()

    for idx, player in enumerate(normalized_players):
        role = _role_from_text(player.get("role"))
        if not role:
            continue
        if remaining.get(role, 0) <= 0:
            continue
        starters_by_role[role].append(str(player.get("name") or ""))
        remaining[role] = int(remaining.get(role, 0)) - 1
        selected_indexes.add(idx)

    for role in FORMATION_ROLE_ORDER:
        while remaining.get(role, 0) > 0:
            candidate_idx = next(
                (
                    idx
                    for idx, player in enumerate(normalized_players)
                    if idx not in selected_indexes and _role_from_text(player.get("role")) == role
                ),
                None,
            )
            if candidate_idx is None:
                break
            candidate = normalized_players[candidate_idx]
            starters_by_role[role].append(str(candidate.get("name") or ""))
            remaining[role] = int(remaining.get(role, 0)) - 1
            selected_indexes.add(candidate_idx)

    for role in FORMATION_ROLE_ORDER:
        while remaining.get(role, 0) > 0:
            candidate_idx = next(
                (idx for idx in range(len(normalized_players)) if idx not in selected_indexes),
                None,
            )
            if candidate_idx is None:
                break
            candidate = normalized_players[candidate_idx]
            starters_by_role[role].append(str(candidate.get("name") or ""))
            remaining[role] = int(remaining.get(role, 0)) - 1
            selected_indexes.add(candidate_idx)

    reserves: List[Dict[str, str]] = []
    for idx, player in enumerate(normalized_players):
        if idx in selected_indexes:
            continue
        reserves.append(
            {
                "id": str(player.get("id") or f"r{idx}"),
                "name": str(player.get("name") or ""),
                "role": _role_from_text(player.get("role")),
                "order": str(len(reserves)),
            }
        )

    return {
        "portiere": starters_by_role["P"][0] if starters_by_role["P"] else "",
        "difensori": starters_by_role["D"],
        "centrocampisti": starters_by_role["C"],
        "attaccanti": starters_by_role["A"],
        "panchina_details": reserves,
    }


def _load_real_formazioni_rows_from_appkey_json(
    standings_index: Dict[str, Dict[str, object]],
) -> tuple[List[Dict[str, object]], List[int], Optional[Path]]:
    source_path = _latest_formazioni_appkey_path()
    if source_path is None:
        return [], [], None

    try:
        payload = json.loads(source_path.read_text(encoding="utf-8-sig"))
    except Exception:
        return [], [], None

    data_section = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data_section, dict):
        return [], [], None

    formations = data_section.get("formazioni")
    if not isinstance(formations, list):
        return [], [], None

    global_round = _parse_int(data_section.get("giornataLega") or payload.get("giornataLega"))
    role_map = _load_role_map()
    team_id_to_pos = _load_team_id_position_index_from_formazioni_html()
    standings_team_by_pos = _build_standings_team_by_pos(standings_index)

    items_by_key: Dict[tuple[Optional[int], str], Dict[str, object]] = {}
    rounds = set()

    for formation in formations:
        if not isinstance(formation, dict):
            continue
        squads = formation.get("sq")
        if not isinstance(squads, list):
            continue

        round_value = _parse_int(formation.get("giornata") or formation.get("round") or formation.get("turno"))
        if round_value is None:
            round_value = global_round
        if round_value is not None:
            rounds.add(round_value)

        for squad in squads:
            if not isinstance(squad, dict):
                continue
            team_id = _parse_int(squad.get("id"))
            if team_id is None:
                continue

            standing_pos = team_id_to_pos.get(team_id)
            team_name = standings_team_by_pos.get(standing_pos, "") if standing_pos is not None else ""
            if not team_name:
                continue

            resolved_team, resolved_pos = _resolve_team_name_with_standings(team_name, standings_index)
            players = squad.get("pl") if isinstance(squad.get("pl"), list) else []
            lineup = _parse_appkey_lineup(players, squad.get("m"), role_map)
            captain, vice_captain = _extract_appkey_captains(squad.get("cap"), players)

            item = {
                "pos": resolved_pos if resolved_pos is not None else (standing_pos if standing_pos is not None else 9999),
                "standing_pos": resolved_pos if resolved_pos is not None else standing_pos,
                "team": resolved_team or team_name,
                "modulo": _format_module(squad.get("m")),
                "forza_titolari": _parse_float(squad.get("t")),
                "portiere": lineup.get("portiere") or "",
                "difensori": lineup.get("difensori") or [],
                "centrocampisti": lineup.get("centrocampisti") or [],
                "attaccanti": lineup.get("attaccanti") or [],
                "panchina_details": lineup.get("panchina_details") or [],
                "capitano": captain,
                "vice_capitano": vice_captain,
                "round": round_value,
                "source": "real",
            }
            item["panchina"] = [str(reserve.get("name") or "").strip() for reserve in item["panchina_details"]]
            dedupe_key = (round_value, normalize_name(str(item.get("team") or "")))
            items_by_key[dedupe_key] = item

    if not items_by_key:
        return [], [], None

    return list(items_by_key.values()), sorted(rounds), source_path


def _formation_lineup_size(item: Dict[str, object]) -> int:
    total = 0
    if str(item.get("portiere") or "").strip():
        total += 1
    for field in ("difensori", "centrocampisti", "attaccanti"):
        values = item.get(field) if isinstance(item.get(field), list) else []
        total += len([value for value in values if str(value).strip()])
    return total


def _formation_starter_keyset(item: Dict[str, object]) -> Set[str]:
    keys: Set[str] = set()
    for raw_name in _lineup_player_names(item):
        canonical = _canonicalize_name(raw_name)
        key = normalize_name(canonical or raw_name)
        if key:
            keys.add(key)
    return keys


def _pick_appkey_candidate_by_similarity(
    item: Dict[str, object],
    appkey_items: List[Dict[str, object]],
    used_indexes: Set[int],
    preferred_round: Optional[int],
) -> tuple[Optional[int], int]:
    item_keys = _formation_starter_keyset(item)
    if len(item_keys) < 6:
        return None, 0

    best_index: Optional[int] = None
    best_overlap = 0
    for idx, candidate in enumerate(appkey_items):
        if idx in used_indexes:
            continue
        candidate_round = _parse_int(candidate.get("round"))
        if preferred_round is not None and candidate_round is not None and candidate_round != preferred_round:
            continue

        candidate_keys = _formation_starter_keyset(candidate)
        overlap = len(item_keys.intersection(candidate_keys))
        if overlap > best_overlap:
            best_overlap = overlap
            best_index = idx
    return best_index, best_overlap


def _merge_real_formations_with_appkey(
    items: List[Dict[str, object]],
    appkey_items: List[Dict[str, object]],
    appkey_rounds: List[int],
) -> List[Dict[str, object]]:
    if not appkey_items:
        return items

    appkey_by_key: Dict[tuple[Optional[int], str], int] = {}
    for idx, candidate in enumerate(appkey_items):
        team_key = normalize_name(str(candidate.get("team") or ""))
        if not team_key:
            continue
        key = (_parse_int(candidate.get("round")), team_key)
        appkey_by_key[key] = idx

    single_round = appkey_rounds[0] if len(appkey_rounds) == 1 else None
    merged_by_key: Dict[tuple[Optional[int], str], Dict[str, object]] = {}
    used_appkey_indexes: Set[int] = set()

    for item in items:
        team_key = normalize_name(str(item.get("team") or ""))
        if not team_key:
            continue

        current_round = _parse_int(item.get("round"))
        lookup_key = (current_round, team_key)
        candidate_idx = appkey_by_key.get(lookup_key)
        if candidate_idx is None and current_round is None and single_round is not None:
            candidate_idx = appkey_by_key.get((single_round, team_key))
            if candidate_idx is not None:
                item["round"] = single_round
                current_round = single_round

        candidate = appkey_items[candidate_idx] if candidate_idx is not None else None
        direct_overlap = 0
        if candidate is not None:
            direct_overlap = len(_formation_starter_keyset(item).intersection(_formation_starter_keyset(candidate)))

        preferred_round = current_round if current_round is not None else single_round
        similar_idx, similar_overlap = _pick_appkey_candidate_by_similarity(
            item,
            appkey_items,
            used_appkey_indexes,
            preferred_round,
        )
        if (
            similar_idx is not None
            and similar_overlap >= 7
            and (candidate is None or direct_overlap < 7 or similar_overlap > direct_overlap)
        ):
            candidate_idx = similar_idx
            candidate = appkey_items[similar_idx]
            selected_round = _parse_int(candidate.get("round"))
            if current_round is None and selected_round is not None:
                item["round"] = selected_round
                current_round = selected_round

        if candidate is not None:
            if candidate_idx is not None:
                used_appkey_indexes.add(candidate_idx)
            if _formation_lineup_size(item) < 11:
                for field in ("modulo", "portiere", "difensori", "centrocampisti", "attaccanti"):
                    item[field] = candidate.get(field)
            candidate_reserves = candidate.get("panchina_details")
            if isinstance(candidate_reserves, list) and candidate_reserves:
                item["panchina_details"] = candidate_reserves
                item["panchina"] = candidate.get("panchina") or [
                    str(reserve.get("name") or "").strip()
                    for reserve in candidate_reserves
                    if isinstance(reserve, dict)
                ]
            elif not isinstance(item.get("panchina_details"), list) or not item.get("panchina_details"):
                item["panchina_details"] = []
                item["panchina"] = []
            elif not isinstance(item.get("panchina"), list) or not item.get("panchina"):
                item["panchina"] = [
                    str(reserve.get("name") or "").strip()
                    for reserve in item.get("panchina_details") or []
                    if isinstance(reserve, dict)
                ]

            candidate_captain = str(candidate.get("capitano") or "").strip()
            candidate_vice = str(candidate.get("vice_capitano") or "").strip()
            if candidate_captain:
                item["capitano"] = candidate_captain
            if candidate_vice:
                item["vice_capitano"] = candidate_vice

            if _parse_float(item.get("forza_titolari")) is None and _parse_float(
                candidate.get("forza_titolari")
            ) is not None:
                item["forza_titolari"] = _parse_float(candidate.get("forza_titolari"))

        merged_by_key[(current_round, team_key)] = item

    for key, candidate_idx in appkey_by_key.items():
        candidate = appkey_items[candidate_idx]
        if key in merged_by_key:
            continue
        merged_by_key[key] = candidate

    return list(merged_by_key.values())


def _load_real_formazioni_rows(
    standings_index: Optional[Dict[str, Dict[str, object]]],
) -> tuple[List[Dict[str, object]], List[int], Optional[Path]]:
    standings_index = standings_index or {}
    candidate_paths: List[Path] = []
    for folder in REAL_FORMATIONS_DIR_CANDIDATES:
        latest = _latest_supported_file(folder)
        if latest is not None:
            candidate_paths.append(latest)
    for path in REAL_FORMATIONS_FILE_CANDIDATES:
        candidate_paths.append(path)

    seen_paths = set()
    ordered_paths: List[Path] = []
    for path in candidate_paths:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen_paths:
            continue
        seen_paths.add(key)
        ordered_paths.append(path)

    role_map = _load_role_map()
    appkey_items, appkey_rounds, appkey_source = _load_real_formazioni_rows_from_appkey_json(standings_index)

    for source_path in ordered_paths:
        rows = _read_tabular_rows(source_path)
        if not rows:
            continue

        items_by_key: Dict[tuple[Optional[int], str], Dict[str, object]] = {}
        rounds = set()
        for row in rows:
            normalized_row = _normalize_row(row)
            team_name_raw = _pick_row_value(
                normalized_row,
                ["team", "squadra", "fantateam", "fantasquadra", "teamname"],
            )
            if not team_name_raw:
                continue

            round_value = _parse_int(
                _pick_row_value(normalized_row, ["giornata", "round", "matchday", "turno"])
            )
            if round_value is not None:
                rounds.add(round_value)

            resolved_team, standing_pos = _resolve_team_name_with_standings(team_name_raw, standings_index)
            module_raw = _pick_row_value(normalized_row, ["modulo", "formation", "schema"])
            portiere_values = _split_players_cell(_pick_row_value(normalized_row, ["portiere", "p", "gk"]))
            difensori_values = _split_players_cell(
                _pick_row_value(normalized_row, ["difensori", "difesa", "d"])
            )
            centrocampisti_values = _split_players_cell(
                _pick_row_value(normalized_row, ["centrocampisti", "centrocampo", "c"])
            )
            attaccanti_values = _split_players_cell(
                _pick_row_value(normalized_row, ["attaccanti", "attacco", "a"])
            )

            if (
                not portiere_values
                and not difensori_values
                and not centrocampisti_values
                and not attaccanti_values
            ):
                (
                    fallback_portiere,
                    fallback_difensori,
                    fallback_centrocampisti,
                    fallback_attaccanti,
                ) = _extract_starters_from_titolari_columns(normalized_row, module_raw)
                if fallback_portiere:
                    portiere_values = [fallback_portiere]
                    difensori_values = fallback_difensori
                    centrocampisti_values = fallback_centrocampisti
                    attaccanti_values = fallback_attaccanti

            item = {
                "pos": standing_pos if standing_pos is not None else 9999,
                "standing_pos": standing_pos,
                "team": resolved_team or team_name_raw,
                "modulo": _format_module(module_raw),
                "forza_titolari": _parse_float(
                    _pick_row_value(
                        normalized_row,
                        ["forza_titolari", "forzatitolari", "forza"],
                    )
                ),
                "portiere": portiere_values[0] if portiere_values else "",
                "difensori": difensori_values,
                "centrocampisti": centrocampisti_values,
                "attaccanti": attaccanti_values,
                "panchina_details": _extract_reserve_players(normalized_row, role_map),
                "capitano": _pick_row_value(
                    normalized_row,
                    ["capitano", "captain", "cpt", "cap"],
                ),
                "vice_capitano": _pick_row_value(
                    normalized_row,
                    ["vice_capitano", "vicecapitano", "vicecaptain", "vice", "vc"],
                ),
                "round": round_value,
                "source": "real",
            }
            item["panchina"] = [str(reserve.get("name") or "").strip() for reserve in item["panchina_details"]]

            dedupe_key = (round_value, normalize_name(str(item["team"])))
            items_by_key[dedupe_key] = item

        items = list(items_by_key.values())
        merged_items = _merge_real_formations_with_appkey(items, appkey_items, appkey_rounds)
        rounds_in_items = {
            round_value
            for round_value in (_parse_int(item.get("round")) for item in merged_items)
            if round_value is not None
        }
        rounds_in_items.update(rounds)
        available_rounds = sorted(rounds_in_items)
        _recompute_forza_titolari(merged_items)
        return merged_items, available_rounds, source_path

    if appkey_items:
        _recompute_forza_titolari(appkey_items)
        return appkey_items, appkey_rounds, appkey_source

    return [], [], None


@router.get("/live/payload")
def live_payload(
    round: Optional[int] = Query(default=None, ge=1, le=99),
    db: Session = Depends(get_db),
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    _require_admin_key(x_admin_key, db, authorization)

    context = _load_live_round_context(db, round)
    matches: List[Dict[str, object]] = context.get("matches", [])
    catalog: Dict[str, List[Dict[str, str]]] = context.get("catalog", {})

    fixtures_payload: List[Dict[str, object]] = []
    teams_payload: List[Dict[str, object]] = []

    for match in matches:
        match_id = str(match.get("match_id") or "")
        home_team = str(match.get("home_team") or "").strip()
        away_team = str(match.get("away_team") or "").strip()
        six_politico = bool(match.get("six_politico"))

        fixtures_payload.append(
            {
                "match_id": match_id,
                "home_team": home_team,
                "away_team": away_team,
                "six_politico": six_politico,
            }
        )

        for team_name, opponent_name, home_away in (
            (home_team, away_team, "H"),
            (away_team, home_team, "A"),
        ):
            players_payload: List[Dict[str, object]] = []
            for player in catalog.get(team_name, []):
                player_name = str(player.get("name") or "").strip()
                if not player_name:
                    continue
                score = _resolve_live_player_score(player_name, context)
                players_payload.append(
                    {
                        "name": player_name,
                        "role": str(player.get("role") or "").strip().upper(),
                        **score,
                    }
                )

            teams_payload.append(
                {
                    "team": team_name,
                    "opponent": opponent_name,
                    "home_away": home_away,
                    "match_id": match_id,
                    "six_politico": six_politico,
                    "players": players_payload,
                }
            )

    return {
        "round": context.get("round"),
        "available_rounds": context.get("available_rounds", []),
        "fixtures": fixtures_payload,
        "teams": teams_payload,
        "event_fields": list(LIVE_EVENT_FIELDS),
        "bonus_malus": _reg_bonus_map(context.get("regulation", _default_regulation())),
    }


@router.post("/live/match-six")
def set_live_match_six(
    payload: LiveMatchToggleRequest,
    db: Session = Depends(get_db),
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    _require_admin_key(x_admin_key, db, authorization)

    club_index = _load_club_name_index()
    home_team = _display_team_name(payload.home_team, club_index)
    away_team = _display_team_name(payload.away_team, club_index)
    if not home_team or not away_team:
        raise HTTPException(status_code=400, detail="Squadre non valide")

    home_key = normalize_name(home_team)
    away_key = normalize_name(away_team)
    if home_key == away_key:
        raise HTTPException(status_code=400, detail="Match non valido")
    if home_key > away_key:
        home_team, away_team = away_team, home_team

    existing = None
    for row in db.query(LiveFixtureFlag).filter(LiveFixtureFlag.round == payload.round).all():
        pair = {normalize_name(str(row.home_team or "")), normalize_name(str(row.away_team or ""))}
        if pair == {home_key, away_key}:
            existing = row
            break

    if payload.six_politico:
        if existing is None:
            existing = LiveFixtureFlag(
                round=payload.round,
                home_team=home_team,
                away_team=away_team,
                six_politico=True,
                updated_at=datetime.utcnow(),
            )
            db.add(existing)
        else:
            existing.home_team = home_team
            existing.away_team = away_team
            existing.six_politico = True
            existing.updated_at = datetime.utcnow()
    else:
        if existing is not None:
            db.delete(existing)

    db.commit()

    return {
        "ok": True,
        "round": payload.round,
        "home_team": home_team,
        "away_team": away_team,
        "six_politico": bool(payload.six_politico),
    }


def _upsert_live_player_vote_internal(
    payload: LivePlayerVoteRequest,
    db: Session,
    *,
    commit: bool = True,
) -> Dict[str, object]:
    club_index = _load_club_name_index()
    team_name = _display_team_name(payload.team, club_index)
    player_name = _canonicalize_name(payload.player)
    if not team_name or not player_name:
        raise HTTPException(status_code=400, detail="Giocatore o squadra non validi")

    vote_value = _parse_live_value(payload.vote)
    fantavote_value = _parse_live_value(payload.fantavote)
    is_sv = bool(payload.is_sv)
    is_absent = bool(payload.is_absent)
    role_value = str(payload.role or "").strip().upper()[:8] or None
    event_counts = _live_event_counts(
        {
            field: getattr(payload, field, 0)
            for field in LIVE_EVENT_FIELDS
        }
    )
    regulation = _load_regulation()
    scoring_defaults = _reg_scoring_defaults(regulation)
    bonus_map = _reg_bonus_map(regulation)

    if is_sv:
        vote_value = None
        fantavote_value = None
        event_counts = {field: 0 for field in LIVE_EVENT_FIELDS}
        is_absent = False
    elif is_absent:
        vote_value = None
        fantavote_value = None
        event_counts = {field: 0 for field in LIVE_EVENT_FIELDS}

    team_key = normalize_name(team_name)
    player_key = normalize_name(player_name)
    existing = None
    for row in db.query(LivePlayerVote).filter(LivePlayerVote.round == payload.round).all():
        if normalize_name(str(row.team or "")) != team_key:
            continue
        if normalize_name(str(row.player_name or "")) != player_key:
            continue
        existing = row
        break

    old_event_counts: Dict[str, int]
    old_vote_value = float(existing.vote) if existing is not None and existing.vote is not None else None
    old_fantavote_value = (
        float(existing.fantavote) if existing is not None and existing.fantavote is not None else None
    )
    old_is_sv = bool(existing.is_sv) if existing is not None else False
    old_is_absent = bool(getattr(existing, "is_absent", False)) if existing is not None else False
    if existing is not None and not old_is_sv and not old_is_absent:
        old_event_counts = _live_event_counts(
            {field: getattr(existing, field, 0) for field in LIVE_EVENT_FIELDS}
        )
    else:
        old_event_counts = {field: 0 for field in LIVE_EVENT_FIELDS}
    old_has_appearance = _live_has_appearance(
        old_vote_value,
        old_fantavote_value,
        old_is_sv,
        old_is_absent,
        old_event_counts,
    )

    has_events = any(int(event_counts.get(field, 0)) > 0 for field in LIVE_EVENT_FIELDS)
    if not is_sv and not is_absent and vote_value is None and fantavote_value is None and not has_events:
        if existing is not None:
            delta = _stats_delta_from_live_events(
                old_event_counts,
                {field: 0 for field in LIVE_EVENT_FIELDS},
            )
            if old_has_appearance:
                delta["Partite"] = int(delta.get("Partite", 0)) - 1
            if _is_nonzero_stats_delta(delta):
                _sync_live_stats_for_player(player_name, team_name, role_value, delta)
            db.delete(existing)
            if commit:
                db.commit()
        return {
            "ok": True,
            "round": payload.round,
            "team": team_name,
            "player": player_name,
            "deleted": True,
        }

    if existing is None:
        existing = LivePlayerVote(
            round=payload.round,
            team=team_name,
            player_name=player_name,
            role=role_value,
            vote=vote_value,
            fantavote=fantavote_value,
            **event_counts,
            is_sv=is_sv,
            is_absent=is_absent,
            updated_at=datetime.utcnow(),
        )
        db.add(existing)
    else:
        existing.team = team_name
        existing.player_name = player_name
        if role_value:
            existing.role = role_value
        existing.vote = vote_value
        existing.fantavote = fantavote_value
        for field in LIVE_EVENT_FIELDS:
            setattr(existing, field, int(event_counts.get(field, 0)))
        existing.is_sv = is_sv
        existing.is_absent = is_absent
        existing.updated_at = datetime.utcnow()

    new_has_appearance = _live_has_appearance(
        vote_value,
        fantavote_value,
        is_sv,
        is_absent,
        event_counts,
    )
    delta = _stats_delta_from_live_events(old_event_counts, event_counts)
    appearance_delta = int(new_has_appearance) - int(old_has_appearance)
    if appearance_delta != 0:
        delta["Partite"] = int(delta.get("Partite", 0)) + appearance_delta
    if _is_nonzero_stats_delta(delta):
        _sync_live_stats_for_player(player_name, team_name, role_value, delta)

    if commit:
        db.commit()

    default_vote = _safe_float_value(scoring_defaults.get("default_vote"), 6.0)
    vote_number = default_vote if vote_value is None else float(vote_value)
    computed_fantavote: Optional[float] = None
    if not is_sv and not is_absent:
        computed_fantavote = _compute_live_fantavote(
            vote_number,
            event_counts,
            bonus_map,
            fantavote_override=fantavote_value,
        )
        if computed_fantavote is None:
            computed_fantavote = _safe_float_value(scoring_defaults.get("default_fantavote"), default_vote)

    return {
        "ok": True,
        "round": payload.round,
        "team": team_name,
        "player": player_name,
        "is_sv": is_sv,
        "is_absent": is_absent,
        "vote": vote_value,
        "fantavote": computed_fantavote if (not is_sv and not is_absent) else None,
        "events": event_counts,
        "bonus_total": None
        if (is_sv or is_absent or computed_fantavote is None)
        else round(float(computed_fantavote) - vote_number, 2),
        "vote_label": "X" if is_absent else ("SV" if is_sv else _format_live_number(vote_number)),
        "fantavote_label": "X"
        if is_absent
        else ("SV" if is_sv else _format_live_number(computed_fantavote)),
    }


@router.post("/live/player")
def upsert_live_player_vote(
    payload: LivePlayerVoteRequest,
    db: Session = Depends(get_db),
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    _require_admin_key(x_admin_key, db, authorization)
    return _upsert_live_player_vote_internal(payload, db, commit=True)


def _import_live_votes_internal(
    db: Session,
    *,
    round_value: Optional[int],
    season: Optional[str] = None,
    source_url: Optional[str] = None,
    source_html: Optional[str] = None,
) -> Dict[str, object]:
    resolved_round = _parse_int(round_value)
    if resolved_round is None or resolved_round <= 0:
        live_context = _load_live_round_context(db, None)
        resolved_round = _parse_int(live_context.get("round")) or 1

    season_slug = _normalize_season_slug(season)
    effective_source_url = str(source_url or "").strip() or _build_default_voti_url(resolved_round, season_slug)
    html_text = str(source_html or "").strip()
    source = "inline_html"

    if not html_text:
        source = "remote_url"
        try:
            html_text = _fetch_text_url(effective_source_url)
        except HTTPException as exc:
            if VOTI_PAGE_CACHE_PATH.exists():
                html_text = VOTI_PAGE_CACHE_PATH.read_text(encoding="utf-8", errors="replace")
                source = "cache_file"
            else:
                raise exc

    if not html_text:
        raise HTTPException(status_code=422, detail="HTML voti non disponibile")

    try:
        VOTI_PAGE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        VOTI_PAGE_CACHE_PATH.write_text(html_text, encoding="utf-8")
    except Exception:
        pass

    club_index = _load_club_name_index()
    parsed = _extract_fantacalcio_voti_rows(html_text, club_index)
    rows = parsed.get("rows") if isinstance(parsed, dict) else []
    rows = rows if isinstance(rows, list) else []
    if not rows:
        raise HTTPException(
            status_code=422,
            detail="Nessun voto giocatore rilevato dalla pagina voti",
        )

    imported = 0
    for item in rows:
        request_payload = LivePlayerVoteRequest(
            round=resolved_round,
            team=str(item.get("team") or ""),
            player=str(item.get("player") or ""),
            role=str(item.get("role") or ""),
            vote=None if item.get("vote") is None else str(item.get("vote")),
            fantavote=None if item.get("fantavote") is None else str(item.get("fantavote")),
            goal=int(item.get("goal") or 0),
            assist=int(item.get("assist") or 0),
            assist_da_fermo=int(item.get("assist_da_fermo") or 0),
            rigore_segnato=int(item.get("rigore_segnato") or 0),
            rigore_parato=int(item.get("rigore_parato") or 0),
            rigore_sbagliato=int(item.get("rigore_sbagliato") or 0),
            autogol=int(item.get("autogol") or 0),
            gol_subito_portiere=int(item.get("gol_subito_portiere") or 0),
            ammonizione=int(item.get("ammonizione") or 0),
            espulsione=int(item.get("espulsione") or 0),
            gol_vittoria=int(item.get("gol_vittoria") or 0),
            gol_pareggio=int(item.get("gol_pareggio") or 0),
            is_sv=bool(item.get("is_sv")),
            is_absent=bool(item.get("is_absent")),
        )
        _upsert_live_player_vote_internal(request_payload, db, commit=False)
        imported += 1

    db.commit()

    return {
        "ok": True,
        "round": resolved_round,
        "season": season_slug,
        "source": source,
        "source_url": effective_source_url,
        "imported_rows": imported,
        "parsed_rows": int(parsed.get("row_count", imported)) if isinstance(parsed, dict) else imported,
        "raw_rows": int(parsed.get("raw_row_count", imported)) if isinstance(parsed, dict) else imported,
        "teams_detected": int(parsed.get("team_count", 0)) if isinstance(parsed, dict) else 0,
        "skipped_rows": int(parsed.get("skipped_rows", 0)) if isinstance(parsed, dict) else 0,
    }


def _claim_scheduled_job_run(
    db: Session,
    *,
    job_name: str,
    min_interval_seconds: int,
) -> bool:
    now_ts = int(datetime.utcnow().timestamp())
    interval = max(1, int(min_interval_seconds))

    state = db.query(ScheduledJobState).filter(ScheduledJobState.job_name == job_name).first()
    if state is None:
        db.add(
            ScheduledJobState(
                job_name=job_name,
                last_run_ts=0,
                updated_at=datetime.utcnow(),
            )
        )
        try:
            db.commit()
        except Exception:
            db.rollback()
        state = db.query(ScheduledJobState).filter(ScheduledJobState.job_name == job_name).first()
        if state is None:
            return False

    previous_ts = int(state.last_run_ts or 0)
    if now_ts - previous_ts < interval:
        return False

    try:
        updated = (
            db.query(ScheduledJobState)
            .filter(
                ScheduledJobState.job_name == job_name,
                ScheduledJobState.last_run_ts == previous_ts,
            )
            .update(
                {
                    ScheduledJobState.last_run_ts: now_ts,
                    ScheduledJobState.updated_at: datetime.utcnow(),
                },
                synchronize_session=False,
            )
        )
        db.commit()
        return bool(updated == 1)
    except Exception:
        db.rollback()
        return False


def run_auto_live_import(
    db: Session,
    *,
    configured_round: Optional[int] = None,
    season: Optional[str] = None,
    min_interval_seconds: Optional[int] = None,
) -> Dict[str, object]:
    if min_interval_seconds is not None:
        claimed = _claim_scheduled_job_run(
            db,
            job_name="auto_live_import",
            min_interval_seconds=int(min_interval_seconds),
        )
        if not claimed:
            return {
                "ok": True,
                "skipped": True,
                "reason": "not_due_or_claimed_by_other_instance",
            }

    return _import_live_votes_internal(
        db,
        round_value=configured_round,
        season=season,
    )


@router.post("/live/import-voti")
def import_live_votes(
    payload: LiveImportVotesRequest,
    db: Session = Depends(get_db),
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    _require_admin_key(x_admin_key, db, authorization)
    return _import_live_votes_internal(
        db,
        round_value=payload.round,
        season=payload.season,
        source_url=payload.source_url,
        source_html=payload.source_html,
    )


@router.get("/formazioni")
def formazioni(
    team: Optional[str] = Query(default=None),
    round: Optional[int] = Query(default=None, ge=1, le=99),
    order_by: Optional[str] = Query(default=None, pattern="^(classifica|live_total)$"),
    limit: int = Query(default=200, ge=1, le=500),
    db: Session = Depends(get_db),
):
    team_key = normalize_name(team or "")
    regulation = _load_regulation()
    default_order, allowed_orders = _reg_ordering(regulation)
    selected_order = str(order_by or default_order).strip().lower()
    if selected_order not in allowed_orders:
        selected_order = default_order

    standings_index = _build_standings_index()
    status_matchday = _load_status_matchday()
    inferred_matchday_fixtures = _infer_matchday_from_fixtures() if status_matchday is None else None
    inferred_matchday_stats = (
        _infer_matchday_from_stats()
        if status_matchday is None and inferred_matchday_fixtures is None
        else None
    )
    default_matchday = (
        status_matchday
        if status_matchday is not None
        else (inferred_matchday_fixtures if inferred_matchday_fixtures is not None else inferred_matchday_stats)
    )
    real_rows, available_rounds, source_path = _load_real_formazioni_rows(standings_index)

    target_round = round if round is not None else default_matchday
    if target_round is None and available_rounds:
        target_round = max(available_rounds)
    if round is None and target_round is not None and available_rounds and target_round not in available_rounds:
        target_round = max(available_rounds)

    real_items = []
    if real_rows:
        for item in real_rows:
            item_team_key = normalize_name(str(item.get("team") or ""))
            item_round = _parse_int(item.get("round"))
            if team_key and item_team_key != team_key:
                continue
            if target_round is not None and item_round != target_round:
                continue
            real_items.append(item)

    if real_items:
        live_context = _load_live_round_context(db, target_round)
        _attach_live_scores_to_formations(real_items, live_context)
        if selected_order == "live_total":
            real_items.sort(key=_formations_sort_live_key)
        else:
            real_items.sort(key=_formations_sort_key)
        return {
            "items": real_items[:limit],
            "round": target_round,
            "source": "real",
            "available_rounds": available_rounds,
            "order_by": selected_order,
            "order_allowed": allowed_orders,
            "status_matchday": status_matchday,
            "inferred_matchday_fixtures": inferred_matchday_fixtures,
            "inferred_matchday_stats": inferred_matchday_stats,
            "source_path": str(source_path) if source_path else "",
        }

    projected_items = _load_projected_formazioni_rows(team_key, standings_index)
    for item in projected_items:
        item["round"] = target_round
    live_context = _load_live_round_context(db, target_round)
    _attach_live_scores_to_formations(projected_items, live_context)
    if selected_order == "live_total":
        projected_items.sort(key=_formations_sort_live_key)
    else:
        projected_items.sort(key=_formations_sort_key)

    payload_rounds = available_rounds[:]
    if target_round is not None and target_round not in payload_rounds:
        payload_rounds.append(target_round)
        payload_rounds.sort()

    if source_path is None:
        note = "File formazioni reali non trovato: mostrato XI migliore ordinato per classifica."
    elif target_round is not None:
        note = (
            f"Formazioni reali non disponibili per la giornata {target_round}: "
            "mostrato XI migliore ordinato per classifica."
        )
    else:
        note = "Formazioni reali non disponibili: mostrato XI migliore ordinato per classifica."

    return {
        "items": projected_items[:limit],
        "round": target_round,
        "source": "projection",
        "available_rounds": payload_rounds,
        "order_by": selected_order,
        "order_allowed": allowed_orders,
        "status_matchday": status_matchday,
        "inferred_matchday_fixtures": inferred_matchday_fixtures,
        "inferred_matchday_stats": inferred_matchday_stats,
        "source_path": str(source_path) if source_path else "",
        "note": note,
    }


@router.get("/team/{team_name}")
def team_roster(team_name: str):
    rose = _apply_qa_from_quot(_read_csv(ROSE_PATH))
    team_key = normalize_name(team_name)
    items = [row for row in rose if normalize_name(row.get("Team", "")) == team_key]
    return {"items": items}


@router.get("/stats/plusvalenze")
def stats_plusvalenze(
    limit: int = Query(default=20, ge=1, le=200),
    include_negatives: bool = Query(default=True),
    period: str = Query(default="december"),
):
    rose = _read_csv(ROSE_PATH)
    team_totals = defaultdict(lambda: {"acquisto": 0.0, "attuale": 0.0})
    for row in rose:
        team = row.get("Team", "")
        try:
            acquisto = float(row.get("PrezzoAcquisto", 0) or 0)
            attuale = float(row.get("PrezzoAttuale", 0) or 0)
        except ValueError:
            acquisto = 0.0
            attuale = 0.0
        team_totals[team]["acquisto"] += acquisto
        team_totals[team]["attuale"] += attuale

    period = period.strip().lower()
    baseline = 250.0 if period == "start" else None

    items = []
    for team, vals in team_totals.items():
        acquisto = baseline if baseline is not None else vals["acquisto"]
        plus = vals["attuale"] - acquisto
        perc = (plus / acquisto * 100) if acquisto else 0.0
        items.append(
            {
                "team": team,
                "acquisto": int(round(acquisto)),
                "attuale": int(round(vals["attuale"])),
                "plusvalenza": int(round(plus)),
                "percentuale": round(perc, 1),
            }
        )
    if not include_negatives:
        items = [item for item in items if item["plusvalenza"] >= 0]
    items.sort(key=lambda x: x["plusvalenza"], reverse=True)
    return {"items": items[:limit]}


@router.get("/stats/players")
def stats_players(limit: int = Query(default=20, ge=1, le=200)):
    stats = _read_csv(STATS_PATH)
    items = []
    for row in stats:
        row_name = _canonicalize_name(row.get("Giocatore", ""))
        try:
            gol = float(row.get("Gol", 0) or 0)
            autogol = float(row.get("Autogol", 0) or 0)
            rig_parati = float(row.get("RigoriParati", 0) or 0)
            rig_sbagliati = float(row.get("RigoriSbagliati", 0) or 0)
            assist = float(row.get("Assist", 0) or 0)
            amm = float(row.get("Ammonizioni", 0) or 0)
            esp = float(row.get("Espulsioni", 0) or 0)
            clean = float(row.get("Cleansheet", 0) or 0)
        except ValueError:
            gol = autogol = rig_parati = rig_sbagliati = assist = amm = esp = clean = 0.0

        score = (
            gol * 3
            + autogol * -2
            + rig_parati * 3
            + rig_sbagliati * -3
            + assist * 1
            + amm * -0.5
            + esp * -1
            + clean * 1
        )
        items.append(
            {
                "Giocatore": row_name,
                "Squadra": row.get("Squadra", ""),
                "Punteggio": round(score, 1),
            }
        )
    items.sort(key=lambda x: x["Punteggio"], reverse=True)
    return {"items": items[:limit]}


@router.get("/stats/player")
def stats_player(name: str = Query(..., min_length=1)):
    stats = _read_csv(STATS_PATH)
    target = normalize_name(_canonicalize_name(name))
    for row in stats:
        row_name = _canonicalize_name(row.get("Giocatore", ""))
        if normalize_name(row_name) == target:
            updated = dict(row)
            updated["Giocatore"] = row_name
            return {"item": updated}
    return {"item": None}


@router.get("/stats/{stat_name}")
def stats_by_stat(
    stat_name: str,
    limit: int = Query(default=200, ge=1, le=1000),
):
    safe = stat_name.strip().lower()
    file_map = {
        "gol": "gol.csv",
        "assist": "assist.csv",
        "ammonizioni": "ammonizioni.csv",
        "espulsioni": "espulsioni.csv",
        "cleansheet": "cleansheet.csv",
        "autogol": "autogol.csv",
    }
    filename = file_map.get(safe)
    if not filename:
        return {"items": []}
    path = STATS_DIR / filename
    items = _read_csv(path)
    role_map = _load_role_map()
    for item in items:
        name = _canonicalize_name(item.get("Giocatore", ""))
        item["Giocatore"] = name
        role = role_map.get(normalize_name(name))
        if role:
            item["Ruolo"] = role
    return {"items": items[:limit]}


@router.get("/market")
def market():
    if not MARKET_PATH.exists():
        data = _build_market_placeholder()
        data["items"] = _enrich_market_items(data.get("items", []))
        return data
    try:
        import json

        data = json.loads(MARKET_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            items = data.get("items", [])
            teams = data.get("teams", [])
            if not items:
                data = _build_market_placeholder()
                data["items"] = _enrich_market_items(data.get("items", []))
                return data
            return {"items": _enrich_market_items(items), "teams": teams}
        if isinstance(data, list):
            if not data:
                data = _build_market_placeholder()
                data["items"] = _enrich_market_items(data.get("items", []))
                return data
            return {"items": _enrich_market_items(data), "teams": []}
        data = _build_market_placeholder()
        data["items"] = _enrich_market_items(data.get("items", []))
        return data
    except json.JSONDecodeError:
        data = _build_market_placeholder()
        data["items"] = _enrich_market_items(data.get("items", []))
        return data


@router.post("/admin/market/refresh")
def refresh_market(
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db, authorization)
    _backup_or_500("market")
    data = _build_market_placeholder()
    data["items"] = _enrich_market_items(data.get("items", []))
    try:
        MARKET_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        raise HTTPException(status_code=500, detail="Impossibile aggiornare il mercato")
    return {
        "items": len(data.get("items", [])),
        "teams": len(data.get("teams", [])),
        "latest_date": sorted(
            [
                *(item.get("date") for item in data.get("items", []) if item.get("date")),
                *(team.get("last_date") for team in data.get("teams", []) if team.get("last_date")),
            ]
        )[-1]
        if data.get("items") or data.get("teams")
        else None,
    }


@router.post("/market/suggest")
def market_suggest(payload: dict = Body(default=None)):
    raise HTTPException(status_code=410, detail="Algoritmo mercato temporaneamente disattivato")


@router.get("/market/payload")
def market_payload(
    x_access_key: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    raise HTTPException(status_code=410, detail="Algoritmo mercato temporaneamente disattivato")
