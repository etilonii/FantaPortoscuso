import base64
import csv
import json
import logging
import math
import os
import re
import subprocess
import sys
import threading
import unicodedata
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from html import unescape as html_unescape
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query, Body, Depends, Header, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from apps.api.app.backup import run_backup_fail_fast
from apps.api.app.auth_utils import access_key_from_bearer, ensure_key_not_blocked
from apps.api.app.config import (
    BACKUP_DIR,
    BACKUP_KEEP_LAST,
    DATABASE_URL,
    AUTO_LEGHE_SYNC_SLOT_HOURS,
    LEGHE_ALIAS,
    LEGHE_USERNAME,
    LEGHE_PASSWORD,
    LEGHE_COMPETITION_ID,
    LEGHE_COMPETITION_NAME,
    LEGHE_FORMATIONS_MATCHDAY,
)
from apps.api.app.db import SessionLocal
from apps.api.app.deps import get_db
from apps.api.app.leghe_sync import (
    LegheSyncError,
    run_leghe_sync_and_pipeline,
    refresh_formazioni_context_from_leghe,
    fetch_leghe_formazioni_service_payloads,
)
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


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/data", tags=["data"])

DATA_DIR = Path(__file__).resolve().parents[4] / "data"
RUNTIME_DATA_DIR = DATA_DIR / "runtime"
RUNTIME_DB_DIR = RUNTIME_DATA_DIR / "db"
RUNTIME_STATS_DIR = RUNTIME_DATA_DIR / "stats"
ROSE_PATH = DATA_DIR / "rose_fantaportoscuso.csv"
QUOT_PATH = DATA_DIR / "quotazioni.csv"
RESIDUAL_CREDITS_PATH = DATA_DIR / "rose_nuovo_credits.csv"
STATS_PATH = RUNTIME_DATA_DIR / "statistiche_giocatori.csv"
MARKET_PATH = DATA_DIR / "market_latest.json"
STARTING_XI_REPORT_PATH = DATA_DIR / "reports" / "team_starting_xi.csv"
PLAYER_STRENGTH_REPORT_PATH = DATA_DIR / "reports" / "team_strength_players.csv"
PLAYER_TIERS_PATH = DATA_DIR / "player_tiers.csv"
TEAM_STRENGTH_RANKING_PATH = DATA_DIR / "reports" / "team_strength_ranking.csv"
TEAM_STARTING_STRENGTH_RANKING_PATH = DATA_DIR / "reports" / "team_starting_strength_ranking.csv"
SERIEA_FINAL_TABLE_REPORT_PATH = DATA_DIR / "reports" / "seriea_final_table_projection_round25_38.csv"
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
REAL_FORMATIONS_CONTEXT_HTML_GLOB = "formazioni*.html"
VOTI_PAGE_CACHE_PATH = DATA_DIR / "tmp" / "voti_page.html"
VOTI_BASE_URL = "https://www.fantacalcio.it/voti-fantacalcio-serie-a"
CALENDAR_BASE_URL = "https://www.fantacalcio.it/serie-a/calendario"
LEGHE_BASE_URL = "https://leghe.fantacalcio.it"
LEGHE_SYNC_TZ = ZoneInfo("Europe/Rome")
LEGHE_SYNC_SLOT_HOURS = max(1, int(AUTO_LEGHE_SYNC_SLOT_HOURS))
LEGHE_BOOTSTRAP_MAX_AGE_HOURS = 20
LEGHE_DAILY_ROSE_JOB_NAME = "auto_leghe_sync_rose_daily"
LEGHE_DAILY_LIVE_JOB_NAME = "auto_leghe_sync_live_daily"
SERIEA_LIVE_CONTEXT_JOB_NAME = "auto_seriea_live_context_sync"
LEGHE_DAILY_LIVE_HOUR_LOCAL = 12
AVAILABILITY_SYNC_JOB_NAME = "auto_player_availability_sync"
AVAILABILITY_SYNC_HOURS_LOCAL: Tuple[int, ...] = (3, 15)
INJURIES_SOURCE_URL = "https://www.fantacalcio.it/infortunati-serie-a"
SUSPENSIONS_SOURCE_URL = "https://www.fantacalcio.it/squalificati-e-diffidati-campionato-serie-a"
PROBABLE_FORMATIONS_SOURCE_URL = "https://www.fantacalcio.it/probabili-formazioni-serie-a"
AVAILABILITY_STATUS_PATH = DATA_DIR / "availability_status.json"
PROBABLE_FORMATIONS_STATUS_PATH = RUNTIME_DATA_DIR / "probabili_formazioni_status.json"
PROBABLE_FORMATIONS_SEED_PATH = DATA_DIR / "probabili_formazioni_status.json"
PROBABLE_FORMATIONS_MAX_AGE_HOURS = 4.0
INJURED_CLEAN_PATH = DATA_DIR / "infortunati_clean.txt"
SUSPENDED_CLEAN_PATH = DATA_DIR / "squalificati_clean.txt"
LEGHE_SYNC_WINDOWS: Tuple[Tuple[int, date, date], ...] = (
    (26, date(2026, 2, 20), date(2026, 2, 23)),
    (27, date(2026, 2, 27), date(2026, 3, 2)),
    (28, date(2026, 3, 6), date(2026, 3, 9)),
    (29, date(2026, 3, 13), date(2026, 3, 16)),
    (30, date(2026, 3, 20), date(2026, 3, 23)),
    (31, date(2026, 4, 3), date(2026, 4, 6)),
    (32, date(2026, 4, 10), date(2026, 4, 13)),
    (33, date(2026, 4, 17), date(2026, 4, 20)),
    (34, date(2026, 4, 24), date(2026, 4, 27)),
    (35, date(2026, 5, 1), date(2026, 5, 4)),
    (36, date(2026, 5, 8), date(2026, 5, 11)),
    (37, date(2026, 5, 15), date(2026, 5, 18)),
    (38, date(2026, 5, 22), date(2026, 5, 24)),
)
STATUS_PATH = DATA_DIR / "status.json"
SERIEA_CONTEXT_CANDIDATES = [
    DATA_DIR / "incoming" / "manual" / "seriea_context.csv",
    DATA_DIR / "config" / "seriea_context.csv",
]
MARKET_REPORT_GLOB = "rose_changes_*.csv"
ROSE_DIFF_GLOB = "diff_rose_*.txt"
STATS_DIR = RUNTIME_STATS_DIR
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
FIXTURES_PATH = RUNTIME_DB_DIR / "fixtures.csv"
REGULATION_PATH = DATA_DIR / "config" / "regolamento.json"
SEED_DB_DIR = DATA_DIR / "db"
LEGACY_SEED_DB_DIR = Path("/app/seed/db")
ROSE_XLSX_DIR = DATA_DIR / "archive" / "incoming" / "rose"
TABULAR_EXTENSIONS = {".csv", ".xlsx", ".xls"}
_RESIDUAL_CREDITS_CACHE: Dict[str, object] = {}
_NAME_LIST_CACHE: Dict[str, object] = {}
_LISTONE_NAME_CACHE: Dict[str, object] = {}
_PLAYER_FORCE_CACHE: Dict[str, object] = {}
_REGULATION_CACHE: Dict[str, object] = {}
_SERIEA_CONTEXT_CACHE: Dict[str, object] = {}
_AVAILABILITY_CACHE: Dict[str, object] = {}
_PROBABLE_FORMATIONS_CACHE: Dict[str, object] = {}
_FORMAZIONI_REMOTE_REFRESH_CACHE: Dict[str, float] = {}
_CLASSIFICA_MATCHDAY_TOTALS_CACHE: Dict[str, object] = {}
_CLASSIFICA_POSITIONS_CACHE: Dict[str, object] = {}
_ROUND_FIRST_KICKOFF_CACHE: Dict[str, object] = {}
_AUTO_VOTI_IMPORT_ATTEMPTED_ROUNDS: Set[int] = set()
_SYNC_COMPLETE_BACKGROUND_LOCK = threading.Lock()
_SYNC_COMPLETE_BACKGROUND_RUNNING = False

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
APPKEY_BONUS_GV_DEFAULT_INDEX = 8
APPKEY_BONUS_GP_DEFAULT_INDEX = 9

LEGHE_MATCHDAY_SYNC_START_HOUR_MON_SAT = 15
LEGHE_MATCHDAY_SYNC_START_HOUR_SUN = 12

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


def _runtime_seed_fallback_paths(path: Path) -> List[Path]:
    try:
        rel = path.relative_to(RUNTIME_DATA_DIR)
    except ValueError:
        return []

    candidates: List[Path] = [DATA_DIR / rel]
    if len(rel.parts) >= 2 and rel.parts[0] == "db":
        legacy_candidate = LEGACY_SEED_DB_DIR.joinpath(*rel.parts[1:])
        candidates.append(legacy_candidate)

    out: List[Path] = []
    seen: Set[str] = set()
    for candidate in candidates:
        marker = str(candidate)
        if marker in seen:
            continue
        seen.add(marker)
        out.append(candidate)
    return out


def _read_csv(path: Path) -> List[Dict[str, str]]:
    candidate_paths: List[Path] = [path, *_runtime_seed_fallback_paths(path)]
    seen_paths: Set[str] = set()

    for candidate in candidate_paths:
        marker = str(candidate)
        if marker in seen_paths:
            continue
        seen_paths.add(marker)
        if not candidate.exists():
            continue

        try:
            with candidate.open("r", encoding="utf-8") as f:
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
                if rows:
                    return rows
        except Exception:
            continue
    return []


def _clean_row_keys(row: Dict[object, object]) -> Dict[str, str]:
    cleaned: Dict[str, str] = {}
    for key, value in row.items():
        if key is None:
            continue
        clean_key = str(key).strip().lstrip("\ufeff")
        cleaned[clean_key] = "" if value is None else str(value)
    return cleaned


def _frame_looks_like_lineups(frame) -> bool:
    try:
        columns = {
            normalize_name(str(column or ""))
            for column in list(frame.columns)
            if str(column or "").strip()
        }
    except Exception:
        return False

    team_columns = {"team", "squadra", "fantateam", "fantasquadra", "teamname"}
    lineup_columns = {
        "portiere",
        "difensori",
        "centrocampisti",
        "attaccanti",
        "titolare1",
        "titolare_1",
        "starter1",
        "starter_1",
    }
    return bool(columns.intersection(team_columns)) and bool(columns.intersection(lineup_columns))


def _looks_like_team_name_cell(value: object) -> bool:
    raw = str(value or "").strip()
    if not raw:
        return False
    normalized = normalize_name(raw)
    if normalized in {"panchina", "modificatorecapitano"}:
        return False
    if normalized.startswith("totale"):
        return False
    if normalized.startswith("inserita"):
        return False
    if normalized.startswith("inverde"):
        return False
    if _strict_role_from_layout_cell(raw):
        return False
    if _normalize_module(raw):
        return False
    if re.fullmatch(r"[0-9.\-]+", raw):
        return False
    return bool(re.search(r"[A-Za-zÀ-ÿ]", raw))


def _strict_role_from_layout_cell(value: object) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    if raw in {"P", "D", "C", "A"}:
        return raw
    if raw in {"POR", "PORTIERE", "PORTIERI", "GK", "GOALKEEPER"}:
        return "P"
    if raw in {"DIF", "DIFENSORE", "DIFENSORI", "DEF", "DEFENDER"}:
        return "D"
    if raw in {"CEN", "CENTROCAMPO", "CENTROCAMPISTA", "CENTROCAMPISTI", "MID", "MIDFIELDER"}:
        return "C"
    if raw in {"ATT", "ATTACCANTE", "ATTACCANTI", "FWD", "FORWARD", "ST"}:
        return "A"
    return ""


def _sheet_round_from_name(sheet_name: str) -> Optional[int]:
    raw = str(sheet_name or "").strip().lower()
    if not raw:
        return None
    match = re.search(r"(\d+)\s*giornata", raw)
    if match:
        return _parse_int(match.group(1))
    return None


def _extract_dual_layout_formazioni_rows(path: Path) -> List[Dict[str, str]]:
    try:
        import pandas as pd
    except Exception:
        return []

    try:
        sheets = pd.read_excel(path, sheet_name=None)
    except Exception:
        return []

    if not isinstance(sheets, dict):
        return []

    extracted: List[Dict[str, str]] = []

    def _parse_layout_metric(cells: List[str], start: int, end: int) -> Optional[float]:
        # Metrics in Leghe dual layout can shift by one/two columns; scan the
        # side slice instead of relying on a fixed index.
        for idx in range(start, end):
            if idx < 0 or idx >= len(cells):
                continue
            raw_cell = str(cells[idx] or "").strip()
            parsed = _parse_float(raw_cell)
            if parsed is not None:
                return parsed
            embedded = re.search(r"-?\d+(?:[.,]\d+)?", raw_cell)
            if embedded:
                parsed_embedded = _parse_float(embedded.group(0))
                if parsed_embedded is not None:
                    return parsed_embedded
        return None

    for sheet_name, frame in sheets.items():
        if frame is None or frame.empty:
            continue

        rows: List[List[str]] = []
        columns = list(frame.columns)
        if not columns:
            continue
        max_cols = min(12, len(columns))
        for _, source_row in frame.fillna("").iterrows():
            values = [str(source_row.get(column, "") or "").strip() for column in columns[:max_cols]]
            if len(values) < 12:
                values.extend([""] * (12 - len(values)))
            rows.append(values)

        if not any(
            len(row) > 7 and _strict_role_from_layout_cell(row[0]) and _strict_role_from_layout_cell(row[6])
            for row in rows
        ):
            continue

        round_value = _sheet_round_from_name(str(sheet_name))
        index = 0
        while index < len(rows):
            current = rows[index]
            left_team = _canonicalize_name(current[0]) if len(current) > 0 else ""
            right_team = _canonicalize_name(current[6]) if len(current) > 6 else ""
            if not (_looks_like_team_name_cell(left_team) and _looks_like_team_name_cell(right_team)):
                index += 1
                continue

            left_module = ""
            right_module = ""
            if index + 1 < len(rows):
                module_row = rows[index + 1]
                left_module = _format_module(module_row[0] if len(module_row) > 0 else "")
                right_module = _format_module(module_row[6] if len(module_row) > 6 else "")

            left_players: List[Tuple[str, str]] = []
            right_players: List[Tuple[str, str]] = []
            left_mod_capitano: Optional[float] = None
            right_mod_capitano: Optional[float] = None
            left_totale_precalc: Optional[float] = None
            right_totale_precalc: Optional[float] = None
            cursor = index + 2
            while cursor < len(rows):
                row = rows[cursor]

                next_left_team = _canonicalize_name(row[0]) if len(row) > 0 else ""
                next_right_team = _canonicalize_name(row[6]) if len(row) > 6 else ""
                if _looks_like_team_name_cell(next_left_team) and _looks_like_team_name_cell(next_right_team):
                    break

                left_labels = [normalize_name(value) for value in row[0:6]]
                right_labels = [normalize_name(value) for value in row[6:12]]
                if "modificatorecapitano" in left_labels:
                    left_mod_capitano = _parse_layout_metric(row, 0, 6)
                if "modificatorecapitano" in right_labels:
                    right_mod_capitano = _parse_layout_metric(row, 6, 12)
                if any(label.startswith("totale") for label in left_labels):
                    left_totale_precalc = _parse_layout_metric(row, 0, 6)
                if any(label.startswith("totale") for label in right_labels):
                    right_totale_precalc = _parse_layout_metric(row, 6, 12)

                left_role = _strict_role_from_layout_cell(row[0] if len(row) > 0 else "")
                left_name = _canonicalize_name(row[1] if len(row) > 1 else "")
                right_role = _strict_role_from_layout_cell(row[6] if len(row) > 6 else "")
                right_name = _canonicalize_name(row[7] if len(row) > 7 else "")

                if left_role and left_name:
                    left_players.append((left_role, left_name))
                if right_role and right_name:
                    right_players.append((right_role, right_name))
                cursor += 1

            for team_name, module, players in (
                (left_team, left_module, left_players),
                (right_team, right_module, right_players),
            ):
                if not team_name or len(players) < 11:
                    continue

                starters = players[:11]
                reserves = players[11:]
                portiere = next((name for role, name in starters if role == "P"), "")
                difensori = [name for role, name in starters if role == "D"]
                centrocampisti = [name for role, name in starters if role == "C"]
                attaccanti = [name for role, name in starters if role == "A"]

                if not portiere and starters:
                    portiere = starters[0][1]

                extracted.append(
                    {
                        "giornata": str(round_value or ""),
                        "team": team_name,
                        "modulo": module,
                        "portiere": portiere,
                        "difensori": ";".join(difensori),
                        "centrocampisti": ";".join(centrocampisti),
                        "attaccanti": ";".join(attaccanti),
                        "panchina": ";".join(name for _, name in reserves),
                        "mod_capitano_precalc": (
                            str(left_mod_capitano)
                            if team_name == left_team and left_mod_capitano is not None
                            else (
                                str(right_mod_capitano)
                                if team_name == right_team and right_mod_capitano is not None
                                else ""
                            )
                        ),
                        "totale_precalc": (
                            str(left_totale_precalc)
                            if team_name == left_team and left_totale_precalc is not None
                            else (
                                str(right_totale_precalc)
                                if team_name == right_team and right_totale_precalc is not None
                                else ""
                            )
                        ),
                    }
                )

            index = cursor

    return extracted


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
        frames = [frame for frame in sheets.values() if frame is not None and not frame.empty]
        if not frames:
            return []

        extracted = _extract_dual_layout_formazioni_rows(path)
        if extracted:
            return extracted

        lineup_frames = [frame for frame in frames if _frame_looks_like_lineups(frame)]
        frames_to_scan = lineup_frames or frames

        for frame in frames_to_scan:
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


def _first_existing_data_path(path: Path) -> Optional[Path]:
    for candidate in [path, *_runtime_seed_fallback_paths(path)]:
        try:
            if candidate.exists() and candidate.is_file() and candidate.stat().st_size > 0:
                return candidate
        except Exception:
            continue
    return None


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
    ensure_key_not_blocked(db, record)


def _resolve_access_key_for_request(
    db: Session,
    *,
    authorization: str | None = None,
    x_access_key: str | None = None,
) -> AccessKey | None:
    record = access_key_from_bearer(authorization, db)
    if record is None:
        key_value = str(x_access_key or "").strip().lower()
        if not key_value:
            return None
        record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
        if not record or not record.used:
            raise HTTPException(status_code=401, detail="Key non valida")
        ensure_key_not_blocked(db, record)

    return record


def _require_login_key(
    db: Session,
    *,
    authorization: str | None = None,
    x_access_key: str | None = None,
) -> AccessKey:
    record = _resolve_access_key_for_request(
        db,
        authorization=authorization,
        x_access_key=x_access_key,
    )
    if record is None:
        raise HTTPException(status_code=401, detail="Login richiesto")
    return record


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
    roles: Dict[str, str] = {}

    # Primary source: full quotazioni list (covers players not present in fantasy rosters).
    for row in _read_csv(QUOT_PATH):
        name = (row.get("Giocatore") or "").strip()
        role = (row.get("Ruolo") or "").strip().upper()
        if not name or not role:
            continue
        roles[normalize_name(name)] = role

    # Fallback/override: league rosters (can contain the most up-to-date local corrections).
    for row in _read_csv(ROSE_PATH):
        name = (row.get("Giocatore") or "").strip()
        role = (row.get("Ruolo") or "").strip().upper()
        if not name or not role:
            continue
        roles[normalize_name(name)] = role

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


def _load_quotazione_enrichment_map() -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for row in _read_csv(QUOT_PATH):
        name = (row.get("Giocatore") or "").strip()
        if not name:
            continue
        key = normalize_name(name)
        if not key:
            continue
        out[key] = {
            "Squadra": str(row.get("Squadra") or "").strip(),
            "Ruolo": str(row.get("Ruolo") or "").strip().upper(),
        }
    return out


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
    enrichment_map = _load_quotazione_enrichment_map()
    if not qa_map and not enrichment_map:
        return rows
    out = []
    for row in rows:
        name_key = normalize_name(row.get("Giocatore", ""))
        qa = qa_map.get(name_key) if qa_map else None
        enrich = enrichment_map.get(name_key) if enrichment_map else None
        if qa is not None or enrich is not None:
            row = dict(row)
            if qa is not None:
                row["PrezzoAttuale"] = qa
            if isinstance(enrich, dict):
                squadra = str(enrich.get("Squadra") or "").strip()
                ruolo = str(enrich.get("Ruolo") or "").strip().upper()
                if squadra:
                    row["Squadra"] = squadra
                if ruolo:
                    row["Ruolo"] = ruolo
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
    scheduled_reference_round = _leghe_sync_reference_round_now()
    if status_matchday is not None and scheduled_reference_round is not None:
        return max(int(status_matchday), int(scheduled_reference_round))
    if status_matchday is not None:
        return status_matchday
    if scheduled_reference_round is not None:
        return int(scheduled_reference_round)

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
            logger.debug("Failed to parse residual credits from %s", path, exc_info=True)

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
                logger.debug("Failed to parse market JSON payload from %s", MARKET_PATH, exc_info=True)
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
        DATA_DIR / "incoming" / "classifica" / "classifica.xlsx",
        DATA_DIR / "classifica.xlsx",
        base_dir / "Classifica_FantaPortoscuso-25.xlsx",
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
                played_raw = str(
                    row.get("Partite Giocate")
                    or row.get("PG")
                    or row.get("G")
                    or row.get("Giocate")
                    or ""
                ).strip()
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
                out.append(
                    {
                        "pos": pos,
                        "team": team,
                        "played": played,
                        "points": points,
                        "played_backfilled": False,
                    }
                )
            out.sort(key=lambda x: x["pos"])
            return _backfill_standings_played_if_missing(out)

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
            normalized_key = str(k).replace(".", "").strip().lower()
            if pos_col is None and (k == "pos" or "posizione" in k):
                pos_col = v
            if team_col is None and ("squadra" in k or k == "team"):
                team_col = v
            if points_col is None and ("pt" in k and "tot" in k):
                points_col = v
            if played_col is None and (
                "partite" in normalized_key
                or "giocate" in normalized_key
                or normalized_key in {"pg", "g", "giornate", "giornata", "23"}
            ):
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
            out.append(
                {
                    "pos": pos,
                    "team": team,
                    "played": played,
                    "points": points,
                    "played_backfilled": False,
                }
            )

        out.sort(key=lambda x: x["pos"])
        return _backfill_standings_played_if_missing(out)
    except Exception:
        return []


def _backfill_standings_played_if_missing(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    if not rows:
        return rows

    has_played = any((_parse_int(row.get("played")) or 0) > 0 for row in rows)
    if has_played:
        for row in rows:
            row["played_backfilled"] = bool(row.get("played_backfilled", False))
        return rows

    fallback_played = _load_status_matchday()
    if fallback_played is None or fallback_played <= 0:
        fallback_played = _max_completed_round_from_fixtures()

    if fallback_played is None or fallback_played <= 0:
        return rows

    for row in rows:
        row["played"] = int(fallback_played)
        row["played_backfilled"] = True
    return rows


def _parse_classifica_matchday_totals_from_html(html_text: str) -> Tuple[Optional[int], Dict[str, float]]:
    round_value: Optional[int] = None
    if html_text:
        round_match = re.search(
            r"ultima\s+giornata\s+calcolata\s*(\d+)",
            html_text,
            flags=re.IGNORECASE,
        )
        if round_match is not None:
            round_value = _parse_int(round_match.group(1))

    totals: Dict[str, float] = {}
    if not html_text:
        return round_value, totals

    pattern = re.compile(
        r"<h5[^>]*class=['\"][^'\"]*team-name[^'\"]*['\"][^>]*>\s*(.*?)\s*</h5>"
        r".*?<div[^>]*class=['\"][^'\"]*team-fpt[^'\"]*['\"][^>]*>\s*([^<]+)\s*</div>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for team_raw, total_raw in pattern.findall(html_text):
        team_name = _canonicalize_name(_strip_html_tags(team_raw))
        if not team_name:
            continue
        total_value = _parse_float(str(total_raw or "").replace(" ", ""))
        if total_value is None:
            continue
        totals[normalize_name(team_name)] = round(float(total_value), 2)

    return round_value, totals


def _parse_classifica_positions_from_html(html_text: str) -> Dict[str, Dict[str, object]]:
    positions: Dict[str, Dict[str, object]] = {}
    if not html_text:
        return positions

    pattern = re.compile(
        r"<h5[^>]*class=['\"][^'\"]*team-name[^'\"]*['\"][^>]*>\s*(.*?)\s*</h5>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    seen: Set[str] = set()
    pos_counter = 1
    for team_raw in pattern.findall(html_text):
        team_name = _canonicalize_name(_strip_html_tags(team_raw))
        team_key = normalize_name(team_name)
        if not team_key or team_key in seen:
            continue
        seen.add(team_key)
        positions[team_key] = {
            "team": team_name,
            "pos": int(pos_counter),
        }
        pos_counter += 1
    return positions


def _load_classifica_positions() -> Dict[str, Dict[str, object]]:
    if not LEGHE_ALIAS:
        return {}

    now_ts = float(datetime.utcnow().timestamp())
    cached_ts = float(_CLASSIFICA_POSITIONS_CACHE.get("ts", 0.0) or 0.0)
    cached_positions_raw = _CLASSIFICA_POSITIONS_CACHE.get("positions")
    cached_positions = cached_positions_raw if isinstance(cached_positions_raw, dict) else {}

    if now_ts - cached_ts < 300 and cached_positions:
        out: Dict[str, Dict[str, object]] = {}
        for key, value in cached_positions.items():
            if not isinstance(value, dict):
                continue
            out[str(key)] = {
                "team": str(value.get("team") or ""),
                "pos": int(_parse_int(value.get("pos")) or 0),
            }
        return out

    url = f"{LEGHE_BASE_URL}/{LEGHE_ALIAS}/classifica"
    try:
        html_text = _fetch_text_url(url, timeout_seconds=20.0)
        parsed_positions = _parse_classifica_positions_from_html(html_text)
        if parsed_positions:
            _CLASSIFICA_POSITIONS_CACHE["ts"] = now_ts
            _CLASSIFICA_POSITIONS_CACHE["positions"] = {
                str(key): {"team": str(value.get("team") or ""), "pos": int(value.get("pos") or 0)}
                for key, value in parsed_positions.items()
                if isinstance(value, dict)
            }
            return parsed_positions
    except Exception:
        logger.debug("Unable to load classifica positions", exc_info=True)

    out: Dict[str, Dict[str, object]] = {}
    for key, value in cached_positions.items():
        if not isinstance(value, dict):
            continue
        out[str(key)] = {
            "team": str(value.get("team") or ""),
            "pos": int(_parse_int(value.get("pos")) or 0),
        }
    return out


def _apply_classifica_positions_override(
    items: List[Dict[str, object]],
    positions_index: Dict[str, Dict[str, object]],
) -> None:
    if not items or not positions_index:
        return
    for item in items:
        team_name = str(item.get("team") or "").strip()
        team_key = normalize_name(team_name)
        if not team_key:
            continue
        payload = positions_index.get(team_key)
        if not isinstance(payload, dict):
            continue
        pos_value = _parse_int(payload.get("pos"))
        if pos_value is None or pos_value <= 0:
            continue
        item["standing_pos"] = int(pos_value)
        item["pos"] = int(pos_value)


def _load_classifica_matchday_totals() -> Tuple[Optional[int], Dict[str, float]]:
    if not LEGHE_ALIAS:
        return None, {}

    now_ts = float(datetime.utcnow().timestamp())
    cached_ts = float(_CLASSIFICA_MATCHDAY_TOTALS_CACHE.get("ts", 0.0) or 0.0)
    cached_round = _parse_int(_CLASSIFICA_MATCHDAY_TOTALS_CACHE.get("round"))
    cached_totals_raw = _CLASSIFICA_MATCHDAY_TOTALS_CACHE.get("totals")
    cached_totals = cached_totals_raw if isinstance(cached_totals_raw, dict) else {}

    if now_ts - cached_ts < 300 and cached_totals:
        return cached_round, {str(k): float(v) for k, v in cached_totals.items()}

    url = f"{LEGHE_BASE_URL}/{LEGHE_ALIAS}/classifica"
    try:
        html_text = _fetch_text_url(url, timeout_seconds=20.0)
        parsed_round, parsed_totals = _parse_classifica_matchday_totals_from_html(html_text)
        if parsed_totals:
            _CLASSIFICA_MATCHDAY_TOTALS_CACHE["ts"] = now_ts
            _CLASSIFICA_MATCHDAY_TOTALS_CACHE["round"] = int(parsed_round) if parsed_round is not None else None
            _CLASSIFICA_MATCHDAY_TOTALS_CACHE["totals"] = {
                str(key): float(value) for key, value in parsed_totals.items()
            }
            return parsed_round, parsed_totals
    except Exception:
        logger.debug("Unable to load classifica matchday totals", exc_info=True)

    return cached_round, {str(k): float(v) for k, v in cached_totals.items()}


def _build_live_standings_rows(
    db: Session,
    *,
    requested_round: Optional[int] = None,
) -> Dict[str, object]:
    base_rows = _load_standings_rows()
    if not base_rows:
        return {"items": [], "round": requested_round, "source": "empty"}

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
    target_round = requested_round if requested_round is not None else default_matchday
    initial_target_round = _parse_int(target_round)
    base_played_hint = max((_parse_int(row.get("played")) or 0) for row in base_rows) if base_rows else 0

    real_rows, available_rounds, _source_path = _load_real_formazioni_rows(
        standings_index,
        preferred_round=target_round,
    )
    if target_round is None and available_rounds:
        target_round = max(available_rounds)
    if (
        requested_round is None
        and target_round is not None
        and available_rounds
        and target_round not in available_rounds
    ):
        latest_available = max(available_rounds)
        if target_round < latest_available:
            target_round = latest_available

    latest_live_votes_round: Optional[int] = None
    promoted_round_from_completed_votes: Optional[int] = None
    if requested_round is None:
        latest_live_votes_round = _latest_round_with_live_votes(db)
        if latest_live_votes_round is not None:
            should_promote_from_live = bool(
                target_round is None
                or int(latest_live_votes_round) > int(target_round)
                or int(latest_live_votes_round) > int(base_played_hint)
            )
            if should_promote_from_live:
                target_round = int(latest_live_votes_round)
                if _is_round_completed_from_fixtures(latest_live_votes_round):
                    promoted_round_from_completed_votes = int(latest_live_votes_round)
    fallback_base_played = 0
    if (
        requested_round is None
        and latest_live_votes_round is not None
        and target_round is not None
        and int(latest_live_votes_round) == int(target_round)
        and int(target_round) > 1
    ):
        # When standings source lacks PG, derive the baseline as previous round.
        fallback_base_played = int(target_round) - 1

    if target_round is not None and _parse_int(target_round) != initial_target_round:
        real_rows, available_rounds, _source_path = _load_real_formazioni_rows(
            standings_index,
            preferred_round=target_round,
        )

    formazioni_items: List[Dict[str, object]] = []
    for item in real_rows:
        item_round = _parse_int(item.get("round"))
        if target_round is not None and item_round != target_round:
            continue
        formazioni_items.append(item)

    if not formazioni_items and target_round is not None:
        # Try an explicit round refresh before falling back to projection.
        try:
            forced_source = (
                _refresh_formazioni_appkey_from_context_html(target_round)
                or _refresh_formazioni_appkey_from_service(target_round)
                or _latest_formazioni_appkey_path()
            )
            if forced_source is not None and forced_source.exists():
                forced_payload = json.loads(forced_source.read_text(encoding="utf-8-sig"))
                forced_items, _forced_rounds = _parse_formazioni_payload_to_items(forced_payload, standings_index)
                filtered_items = [
                    item
                    for item in forced_items
                    if _parse_int(item.get("round")) == int(target_round)
                ]
                if filtered_items:
                    _recompute_forza_titolari(filtered_items)
                    formazioni_items = filtered_items
        except Exception:
            logger.debug("Unable to refresh real formations for target round %s", target_round, exc_info=True)

    source = "real"
    if not formazioni_items:
        source = "projection"
        formazioni_items = _load_projected_formazioni_rows("", standings_index)
        for item in formazioni_items:
            item["round"] = target_round

    target_round_int = _parse_int(target_round)

    live_context = _load_live_round_context(db, target_round)
    _attach_live_scores_to_formations(formazioni_items, live_context)

    votes_payload = live_context.get("votes_by_team_player")
    has_live_votes_for_round = isinstance(votes_payload, dict) and len(votes_payload) > 0
    has_six_flags = bool(live_context.get("six_team_keys"))
    can_apply_live_totals = bool(has_live_votes_for_round or has_six_flags)

    if (
        not can_apply_live_totals
        and hasattr(db, "query")
        and target_round_int is not None
        and int(target_round_int) > 0
        and _is_round_completed_from_fixtures(target_round_int)
        and int(target_round_int) not in _AUTO_VOTI_IMPORT_ATTEMPTED_ROUNDS
    ):
        _AUTO_VOTI_IMPORT_ATTEMPTED_ROUNDS.add(int(target_round_int))
        try:
            _import_live_votes_internal(
                db,
                round_value=int(target_round_int),
                season=_normalize_season_slug(None),
            )
            live_context = _load_live_round_context(db, target_round_int)
            _attach_live_scores_to_formations(formazioni_items, live_context)
            votes_payload = live_context.get("votes_by_team_player")
            has_live_votes_for_round = isinstance(votes_payload, dict) and len(votes_payload) > 0
            has_six_flags = bool(live_context.get("six_team_keys"))
            can_apply_live_totals = bool(has_live_votes_for_round or has_six_flags)
        except Exception:
            logger.debug(
                "Automatic live votes import failed for round %s",
                target_round_int,
                exc_info=True,
            )

    live_total_by_team: Dict[str, float] = {}
    for item in formazioni_items:
        team_name = str(item.get("team") or "").strip()
        if not team_name:
            continue
        if can_apply_live_totals:
            live_total = item.get("totale_live")
            numeric_live = (
                float(live_total) if isinstance(live_total, (int, float)) else _parse_float(live_total)
            )
            if numeric_live is None:
                continue
        else:
            numeric_live = _parse_float(item.get("totale_precalc"))
            if numeric_live is None:
                total_source = str(item.get("totale_source") or "").strip().lower()
                if total_source not in {"precalc", "appkey_precalc", "xlsx_precalc"}:
                    continue
                live_total = item.get("totale_live")
                numeric_live = (
                    float(live_total) if isinstance(live_total, (int, float)) else _parse_float(live_total)
                )
                if numeric_live is None:
                    continue
        live_total_by_team[normalize_name(team_name)] = float(numeric_live)

    # When classifica provides official team totals for the same round,
    # prefer them to keep live standings aligned with Leghe final values.
    if target_round_int is not None and live_total_by_team:
        classifica_round, classifica_totals = _load_classifica_matchday_totals()
        if classifica_round is not None and int(classifica_round) == int(target_round_int):
            for team_key in list(live_total_by_team.keys()):
                official_total = classifica_totals.get(team_key)
                if official_total is None:
                    continue
                live_total_by_team[team_key] = float(official_total)

    enriched_rows: List[Dict[str, object]] = []
    covered_keys: Set[str] = set()
    for idx, row in enumerate(base_rows):
        team_name = str(row.get("team") or "").strip()
        if not team_name:
            continue
        team_key = normalize_name(team_name)
        covered_keys.add(team_key)

        pos_value = _parse_int(row.get("pos"))
        base_pos = pos_value if pos_value is not None else (idx + 1)
        base_points = float(row.get("points") or 0.0)
        base_played = _parse_int(row.get("played")) or 0
        base_played_backfilled = bool(row.get("played_backfilled"))
        if base_played <= 0 and fallback_base_played > 0:
            base_played = int(fallback_base_played)
            base_played_backfilled = True
        live_total = live_total_by_team.get(team_key)
        if (
            live_total is not None
            and target_round_int is not None
            and base_played_backfilled
            and int(base_played) >= int(target_round_int)
            and int(target_round_int) > 0
        ):
            base_played = int(target_round_int) - 1
        # Avoid double counting when official standings already include the
        # same round currently available in live totals.
        if (
            live_total is not None
            and target_round_int is not None
            and int(base_played) >= int(target_round_int)
            and not base_played_backfilled
        ):
            live_total = None

        points_live = base_points + (live_total if live_total is not None else 0.0)
        played_live = base_played + (1 if live_total is not None else 0)
        avg_live = (points_live / played_live) if played_live > 0 else 0.0

        enriched_rows.append(
            {
                "base_pos": int(base_pos),
                "pos": int(base_pos),
                "team": team_name,
                "played_base": int(base_played),
                "played_backfilled": bool(base_played_backfilled),
                "played_live": int(played_live),
                "played": int(played_live),
                "points_base": round(base_points, 2),
                "live_total": round(float(live_total), 2) if live_total is not None else None,
                "points_live": round(points_live, 2),
                "points": round(points_live, 2),
                "pts_avg": round(avg_live, 2),
            }
        )

    for team_key, live_total in live_total_by_team.items():
        if team_key in covered_keys:
            continue
        team_name = str(team_key or "").strip() or team_key
        enriched_rows.append(
            {
                "base_pos": 9999,
                "pos": 9999,
                "team": team_name,
                "played_base": 0,
                "played_live": 1,
                "played": 1,
                "points_base": 0.0,
                "live_total": round(float(live_total), 2),
                "points_live": round(float(live_total), 2),
                "points": round(float(live_total), 2),
                "pts_avg": round(float(live_total), 2),
            }
        )

    enriched_rows.sort(
        key=lambda row: (
            -float(row.get("points_live") or 0.0),
            int(row.get("base_pos") or 9999),
            normalize_name(str(row.get("team") or "")),
        )
    )
    for idx, row in enumerate(enriched_rows, start=1):
        row["live_pos"] = int(idx)
        row["pos"] = int(idx)

    return {
        "items": enriched_rows,
        "round": target_round,
        "source": source,
        "status_matchday": status_matchday,
        "inferred_matchday_fixtures": inferred_matchday_fixtures,
        "inferred_matchday_stats": inferred_matchday_stats,
        "latest_live_votes_round": latest_live_votes_round,
        "promoted_round_from_completed_votes": promoted_round_from_completed_votes,
    }


@router.get("/standings")
def standings(
    live: bool = Query(default=False),
    round: Optional[int] = Query(default=None, ge=1, le=99),
    db: Session = Depends(get_db),
):
    if not bool(live):
        return {"items": _load_standings_rows()}
    return _build_live_standings_rows(db, requested_round=round)


def _sort_rows_numeric(
    rows: List[Dict[str, str]],
    key_name: str,
    *,
    reverse: bool = False,
    default_value: float = 0.0,
) -> List[Dict[str, str]]:
    def _score(row: Dict[str, str]) -> float:
        parsed = _parse_float(row.get(key_name))
        if parsed is None:
            return float(default_value)
        return float(parsed)

    return sorted(rows, key=_score, reverse=reverse)


def _parse_seriea_current_table_rows(rows: List[Dict[str, str]]) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    for idx, row in enumerate(rows, start=1):
        team_name = str(row.get("Squad") or row.get("Team") or row.get("Squadra") or "").strip()
        if not team_name:
            continue

        pos = _parse_int(row.get("Pos") or row.get("pos"))
        points = _parse_int(row.get("Pts") or row.get("Punti") or row.get("Pt")) or 0
        played = _parse_int(row.get("MP") or row.get("Partite") or row.get("G")) or 0
        gf = _parse_int(row.get("GF") or row.get("Gf")) or 0
        ga = _parse_int(row.get("GA") or row.get("Gs")) or 0
        gd = _parse_int(row.get("GD") or row.get("Dr"))
        if gd is None:
            gd = int(gf) - int(ga)

        ppm = _parse_float(row.get("Pts/MP") or row.get("PPM"))
        if ppm is None:
            ppm = round(float(points) / float(played), 2) if played > 0 else 0.0

        last5_raw = str(row.get("Last5") or row.get("Forma") or "").strip()
        last5 = re.sub(r"\s+", " ", last5_raw)

        out.append(
            {
                "Pos": int(pos) if pos is not None else int(idx),
                "Squad": team_name,
                "MP": int(played),
                "GF": int(gf),
                "GA": int(ga),
                "GD": int(gd),
                "Pts": int(points),
                "Pts/MP": float(ppm),
                "Last5": last5,
            }
        )

    out.sort(
        key=lambda item: (
            int(item.get("Pos") or 999),
            normalize_name(str(item.get("Squad") or "")),
        )
    )
    return out


def _load_seriea_fixtures_for_insights(club_index: Dict[str, str]) -> List[Dict[str, object]]:
    rows = _read_csv_fallback(FIXTURES_PATH, SEED_DB_DIR / "fixtures.csv")
    fixtures: List[Dict[str, object]] = []

    for row in rows:
        home_away = str(row.get("home_away") or "").strip().upper()
        if home_away != "H":
            continue
        round_value = _parse_int(row.get("round"))
        if round_value is None or round_value <= 0:
            continue

        home_team = _display_team_name(str(row.get("team") or ""), club_index)
        away_team = _display_team_name(str(row.get("opponent") or ""), club_index)
        if not home_team or not away_team:
            continue

        home_score = _parse_int(row.get("home_score"))
        away_score = _parse_int(row.get("away_score"))
        if home_score is None:
            home_score = _parse_int(row.get("team_score"))
        if away_score is None:
            away_score = _parse_int(row.get("opponent_score"))

        match_status = _parse_int(row.get("match_status"))
        kickoff_iso = str(row.get("kickoff_iso") or "").strip()
        match_url = str(row.get("match_url") or "").strip()
        match_id = _parse_int(row.get("match_id"))
        if match_id is None and match_url:
            tail_match = re.search(r"/(\d+)(?:/[^/]*)?$", match_url)
            if tail_match is not None:
                match_id = _parse_int(tail_match.group(1))

        fixtures.append(
            {
                "round": int(round_value),
                "home_team": home_team,
                "away_team": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "match_status": int(match_status) if match_status is not None else 0,
                "kickoff_iso": kickoff_iso,
                "match_url": match_url,
                "match_id": int(match_id) if match_id is not None else None,
            }
        )

    fixtures.sort(
        key=lambda item: (
            int(item.get("round") or 0),
            str(item.get("kickoff_iso") or ""),
            normalize_name(str(item.get("home_team") or "")),
            normalize_name(str(item.get("away_team") or "")),
        )
    )
    return fixtures


def _parse_kickoff_local_datetime(value: object) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None

    parsed: Optional[datetime] = None
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception:
        parsed = None

    if parsed is None:
        for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except Exception:
                continue

    if parsed is None:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=LEGHE_SYNC_TZ)
    return parsed.astimezone(LEGHE_SYNC_TZ)


def _first_round_kickoff_local(
    round_value: Optional[int],
    fixture_rows: List[Dict[str, object]],
) -> Optional[datetime]:
    round_num = _parse_int(round_value)
    if round_num is None or round_num <= 0:
        return None

    kickoff_candidates: List[datetime] = []
    for fixture in fixture_rows:
        if _parse_int(fixture.get("round")) != int(round_num):
            continue
        kickoff_local = _parse_kickoff_local_datetime(fixture.get("kickoff_iso"))
        if kickoff_local is None:
            continue
        kickoff_candidates.append(kickoff_local)
    if kickoff_candidates:
        return min(kickoff_candidates)

    from_calendar = _fetch_round_first_kickoff_from_calendar(int(round_num), _normalize_season_slug(None))
    if from_calendar is not None:
        return from_calendar

    for matchday, start_day, _end_day in LEGHE_SYNC_WINDOWS:
        if int(matchday) != int(round_num):
            continue
        return datetime(
            start_day.year,
            start_day.month,
            start_day.day,
            0,
            0,
            0,
            tzinfo=LEGHE_SYNC_TZ,
        )
    return None


def _is_formazioni_real_unlocked_for_round(
    round_value: Optional[int],
    fixture_rows: List[Dict[str, object]],
    *,
    now_local: Optional[datetime] = None,
) -> Tuple[bool, Optional[datetime], str]:
    round_num = _parse_int(round_value)
    if round_num is None or round_num <= 0:
        return True, None, "no_round"

    kickoff_local = _first_round_kickoff_local(round_num, fixture_rows)
    if kickoff_local is None:
        return True, None, "kickoff_unknown"

    now_ref = now_local if now_local is not None else _leghe_sync_local_now()
    unlocked = bool(now_ref >= kickoff_local)
    return unlocked, kickoff_local, "kickoff_guard"


def _seriea_fixture_state(match_status: Optional[int]) -> str:
    status = int(match_status or 0)
    if status <= 0:
        return "scheduled"
    if status == 1:
        return "live"
    return "finished"


def _build_seriea_live_snapshot(
    table_rows: List[Dict[str, object]],
    fixture_rows: List[Dict[str, object]],
    *,
    preferred_round: Optional[int] = None,
) -> Dict[str, object]:
    available_rounds = sorted(
        {
            int(round_value)
            for round_value in (_parse_int(item.get("round")) for item in fixture_rows)
            if round_value is not None and round_value > 0
        }
    )
    enriched_rounds = sorted(
        {
            int(_parse_int(item.get("round")) or 0)
            for item in fixture_rows
            if (_parse_int(item.get("round")) or 0) > 0
            and (
                str(item.get("kickoff_iso") or "").strip()
                or str(item.get("match_url") or "").strip()
                or _parse_int(item.get("match_id")) is not None
                or _parse_int(item.get("match_status")) is not None
            )
        }
    )

    target_round = _parse_int(preferred_round)
    if target_round is not None and target_round in enriched_rounds:
        pass
    elif enriched_rounds:
        if target_round is not None:
            future = [round_value for round_value in enriched_rounds if round_value >= int(target_round)]
            target_round = future[0] if future else enriched_rounds[-1]
        else:
            target_round = enriched_rounds[-1]
    elif target_round is None or target_round not in available_rounds:
        target_round = available_rounds[-1] if available_rounds else None

    normalized_fixtures_by_round: Dict[int, List[Dict[str, object]]] = {}
    for item in fixture_rows:
        round_value = _parse_int(item.get("round"))
        if round_value is None or round_value <= 0:
            continue
        match_status = _parse_int(item.get("match_status")) or 0
        fixture_state = _seriea_fixture_state(match_status)
        home_score = _parse_int(item.get("home_score"))
        away_score = _parse_int(item.get("away_score"))
        normalized_fixtures_by_round.setdefault(int(round_value), []).append(
            {
                "round": int(round_value),
                "home_team": str(item.get("home_team") or ""),
                "away_team": str(item.get("away_team") or ""),
                "home_score": home_score,
                "away_score": away_score,
                "match_status": int(match_status),
                "state": fixture_state,
                "kickoff_iso": str(item.get("kickoff_iso") or ""),
                "match_url": str(item.get("match_url") or ""),
                "match_id": _parse_int(item.get("match_id")),
            }
        )

    fixtures_for_round: List[Dict[str, object]] = []
    if target_round is not None:
        fixtures_for_round = list(normalized_fixtures_by_round.get(int(target_round), []))

    fixtures_for_round.sort(
        key=lambda item: (
            str(item.get("kickoff_iso") or ""),
            normalize_name(str(item.get("home_team") or "")),
            normalize_name(str(item.get("away_team") or "")),
        )
    )

    all_fixtures: List[Dict[str, object]] = []
    for round_value in sorted(normalized_fixtures_by_round.keys()):
        round_rows = list(normalized_fixtures_by_round.get(int(round_value), []))
        round_rows.sort(
            key=lambda item: (
                str(item.get("kickoff_iso") or ""),
                normalize_name(str(item.get("home_team") or "")),
                normalize_name(str(item.get("away_team") or "")),
            )
        )
        all_fixtures.extend(round_rows)

    base_rows: List[Dict[str, object]] = []
    for idx, row in enumerate(table_rows, start=1):
        team_name = str(row.get("Squad") or row.get("Team") or row.get("Squadra") or "").strip()
        if not team_name:
            continue
        base_pos = _parse_int(row.get("Pos")) or idx
        points = _parse_int(row.get("Pts") or row.get("Punti")) or 0
        played = _parse_int(row.get("MP") or row.get("Partite") or row.get("G")) or 0
        gf = _parse_int(row.get("GF") or row.get("Gf")) or 0
        ga = _parse_int(row.get("GA") or row.get("Gs")) or 0
        last5 = re.sub(r"\s+", " ", str(row.get("Last5") or row.get("last5") or "").strip())
        base_rows.append(
            {
                "team": team_name,
                "base_pos": int(base_pos),
                "points_base": int(points),
                "played_base": int(played),
                "gf_base": int(gf),
                "ga_base": int(ga),
                "last5": last5,
                "points_live": int(points),
                "played_live": int(played),
                "gf_live": int(gf),
                "ga_live": int(ga),
                "live_matches": 0,
            }
        )

    by_team_key: Dict[str, Dict[str, object]] = {}
    for row in base_rows:
        by_team_key[normalize_name(row.get("team"))] = row

    rounds_to_apply: List[int] = [
        int(round_value)
        for round_value in available_rounds
        if target_round is None or int(round_value) <= int(target_round)
    ]
    fixtures_to_apply: List[Dict[str, object]] = []
    for round_value in rounds_to_apply:
        fixtures_to_apply.extend(normalized_fixtures_by_round.get(int(round_value), []))
    fixtures_to_apply.sort(
        key=lambda item: (
            int(_parse_int(item.get("round")) or 0),
            str(item.get("kickoff_iso") or ""),
            normalize_name(str(item.get("home_team") or "")),
            normalize_name(str(item.get("away_team") or "")),
        )
    )

    for fixture in fixtures_to_apply:
        state = str(fixture.get("state") or "scheduled")
        if state == "scheduled":
            continue
        home_score = _parse_int(fixture.get("home_score"))
        away_score = _parse_int(fixture.get("away_score"))
        if home_score is None or away_score is None:
            continue

        home_key = normalize_name(str(fixture.get("home_team") or ""))
        away_key = normalize_name(str(fixture.get("away_team") or ""))
        if not home_key or not away_key:
            continue
        home_row = by_team_key.get(home_key)
        away_row = by_team_key.get(away_key)
        if home_row is None or away_row is None:
            continue

        fixture_round = _parse_int(fixture.get("round")) or 0
        if (
            fixture_round > 0
            and int(home_row.get("played_live") or 0) >= int(fixture_round)
            and int(away_row.get("played_live") or 0) >= int(fixture_round)
        ):
            continue

        home_row["played_live"] = int(home_row.get("played_live") or 0) + 1
        away_row["played_live"] = int(away_row.get("played_live") or 0) + 1
        home_row["gf_live"] = int(home_row.get("gf_live") or 0) + int(home_score)
        home_row["ga_live"] = int(home_row.get("ga_live") or 0) + int(away_score)
        away_row["gf_live"] = int(away_row.get("gf_live") or 0) + int(away_score)
        away_row["ga_live"] = int(away_row.get("ga_live") or 0) + int(home_score)
        home_row["live_matches"] = int(home_row.get("live_matches") or 0) + 1
        away_row["live_matches"] = int(away_row.get("live_matches") or 0) + 1

        if home_score > away_score:
            home_row["points_live"] = int(home_row.get("points_live") or 0) + 3
        elif away_score > home_score:
            away_row["points_live"] = int(away_row.get("points_live") or 0) + 3
        else:
            home_row["points_live"] = int(home_row.get("points_live") or 0) + 1
            away_row["points_live"] = int(away_row.get("points_live") or 0) + 1

    live_rows = list(by_team_key.values())
    live_rows.sort(
        key=lambda item: (
            -int(item.get("points_live") or 0),
            -(int(item.get("gf_live") or 0) - int(item.get("ga_live") or 0)),
            -int(item.get("gf_live") or 0),
            normalize_name(str(item.get("team") or "")),
        )
    )

    payload_rows: List[Dict[str, object]] = []
    for idx, row in enumerate(live_rows, start=1):
        points_base = int(row.get("points_base") or 0)
        points_live = int(row.get("points_live") or 0)
        played_live = int(row.get("played_live") or 0)
        avg_live = round(float(points_live) / float(played_live), 2) if played_live > 0 else 0.0
        base_pos = int(row.get("base_pos") or idx)
        live_pos = int(idx)
        payload_rows.append(
            {
                "team": str(row.get("team") or ""),
                "base_pos": base_pos,
                "live_pos": live_pos,
                "pos": live_pos,
                "position_delta": int(base_pos - live_pos),
                "points_base": points_base,
                "points_live": points_live,
                "points": points_live,
                "live_delta": int(points_live - points_base),
                "played_base": int(row.get("played_base") or 0),
                "played_live": played_live,
                "played": played_live,
                "gf_base": int(row.get("gf_base") or 0),
                "ga_base": int(row.get("ga_base") or 0),
                "gf_live": int(row.get("gf_live") or 0),
                "ga_live": int(row.get("ga_live") or 0),
                "gd_live": int(row.get("gf_live") or 0) - int(row.get("ga_live") or 0),
                "pts_avg": avg_live,
                "live_matches": int(row.get("live_matches") or 0),
                "last5": str(row.get("last5") or ""),
                "Last5": str(row.get("last5") or ""),
            }
        )

    return {
        "round": int(target_round) if target_round is not None else None,
        "rounds": available_rounds,
        # Return all rounds so frontend can switch day without stale fallback.
        "fixtures": all_fixtures,
        "fixtures_current_round": fixtures_for_round,
        "table": payload_rows,
    }


@router.get("/insights/premium")
def premium_insights(
    db: Session = Depends(get_db),
    authorization: str | None = Header(default=None, alias="Authorization"),
    x_access_key: str | None = Header(default=None, alias="X-Access-Key"),
):
    _require_login_key(db, authorization=authorization, x_access_key=x_access_key)

    player_tiers_rows = _read_csv(PLAYER_TIERS_PATH)
    player_tiers = _sort_rows_numeric(
        player_tiers_rows,
        "score_auto",
        reverse=True,
        default_value=-1.0,
    )

    team_strength_total_rows = _read_csv(TEAM_STRENGTH_RANKING_PATH)
    team_strength_total = _sort_rows_numeric(
        team_strength_total_rows,
        "Pos",
        reverse=False,
        default_value=9999.0,
    )

    team_strength_starting_rows = _read_csv(TEAM_STARTING_STRENGTH_RANKING_PATH)
    team_strength_starting = _sort_rows_numeric(
        team_strength_starting_rows,
        "Pos",
        reverse=False,
        default_value=9999.0,
    )

    seriea_current_table: List[Dict[str, object]] = []
    seriea_context_path = _resolve_seriea_context_path()
    if seriea_context_path is not None:
        seriea_current_table = _parse_seriea_current_table_rows(_read_csv(seriea_context_path))

    club_index = _load_club_name_index()
    seriea_fixtures_all = _load_seriea_fixtures_for_insights(club_index)
    preferred_seriea_round = _leghe_sync_reference_round_with_lookahead(lookahead_days=1)
    if preferred_seriea_round is None:
        preferred_seriea_round = _leghe_sync_reference_round_now()
    seriea_snapshot = _build_seriea_live_snapshot(
        seriea_current_table,
        seriea_fixtures_all,
        preferred_round=preferred_seriea_round,
    )

    seriea_final_table_rows = _read_csv(SERIEA_FINAL_TABLE_REPORT_PATH)
    seriea_final_table = _sort_rows_numeric(
        seriea_final_table_rows,
        "rank",
        reverse=False,
        default_value=9999.0,
    )

    return {
        "player_tiers": player_tiers,
        "team_strength_total": team_strength_total,
        "team_strength_starting": team_strength_starting,
        "seriea_current_table": seriea_current_table,
        "seriea_round": seriea_snapshot.get("round"),
        "seriea_rounds": seriea_snapshot.get("rounds", []),
        "seriea_fixtures": seriea_snapshot.get("fixtures", []),
        "seriea_live_table": seriea_snapshot.get("table", []),
        "seriea_final_table": seriea_final_table,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


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
        # status.json may be written with UTF-8 BOM by external tools.
        raw = json.loads(STATUS_PATH.read_text(encoding="utf-8-sig"))
        if not isinstance(raw, dict):
            return None
        return _parse_int(raw.get("matchday"))
    except Exception:
        return None


def _round_play_state_from_fixtures() -> Dict[int, Dict[str, bool]]:
    rows = _read_csv_fallback(FIXTURES_PATH, SEED_DB_DIR / "fixtures.csv")
    by_round: Dict[int, Dict[str, bool]] = {}
    if not rows:
        return by_round

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

        current = by_round.setdefault(round_value, {"matches": False, "all_played": True, "any_played": False})
        current["matches"] = True
        current["all_played"] = bool(current["all_played"] and is_played)
        current["any_played"] = bool(current["any_played"] or is_played)

    return by_round


def _max_completed_round_from_fixtures() -> Optional[int]:
    by_round = _round_play_state_from_fixtures()
    if not by_round:
        return None
    completed = [
        int(round_value)
        for round_value, state in by_round.items()
        if bool(state.get("matches")) and bool(state.get("all_played"))
    ]
    if not completed:
        return None
    return max(completed)


def _is_round_completed_from_fixtures(round_value: Optional[int]) -> bool:
    parsed_round = _parse_int(round_value)
    if parsed_round is None:
        return False
    state = _round_play_state_from_fixtures().get(parsed_round)
    if not state or not bool(state.get("matches")):
        return False
    return bool(state.get("all_played"))


def _infer_matchday_from_fixtures() -> Optional[int]:
    by_round = _round_play_state_from_fixtures()
    if not by_round:
        return None

    any_score_found = any(bool(state.get("any_played")) for state in by_round.values())
    if not any_score_found:
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


def _latest_round_with_live_votes(db: Session | None) -> Optional[int]:
    if db is None:
        return None
    try:
        rows = db.query(LivePlayerVote.round).distinct().all()
    except OperationalError:
        return None
    except Exception:
        return None

    rounds: List[int] = []
    for row in rows or []:
        if isinstance(row, tuple):
            value = row[0] if row else None
        else:
            value = getattr(row, "round", row)
        parsed = _parse_int(value)
        if parsed is not None and parsed > 0:
            rounds.append(int(parsed))
    if not rounds:
        return None
    return max(rounds)


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


def _resolve_seriea_context_path() -> Optional[Path]:
    for candidate in SERIEA_CONTEXT_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def _load_seriea_context_index() -> Dict[str, object]:
    source = _resolve_seriea_context_path()
    if source is None:
        return {
            "path": None,
            "teams": {},
            "average_ppm": None,
        }

    cache_key = str(source.resolve())
    mtime = source.stat().st_mtime
    cached = _SERIEA_CONTEXT_CACHE.get(cache_key)
    if cached and cached.get("mtime") == mtime:
        return cached.get("data", {})

    rows = _read_csv(source)
    teams: Dict[str, Dict[str, object]] = {}
    ppm_values: List[float] = []
    for row in rows:
        team_raw = str(row.get("Squad") or row.get("Team") or row.get("Squadra") or "").strip()
        if not team_raw:
            continue
        team_key = normalize_name(team_raw)
        if not team_key:
            continue
        ppm = _parse_float(row.get("Pts/MP") or row.get("PPM") or row.get("PtsPerMatch"))
        if ppm is None:
            points = _parse_float(row.get("Pts") or row.get("Punti"))
            played = _parse_float(row.get("MP") or row.get("Partite"))
            if points is not None and played is not None and played > 0:
                ppm = round(points / played, 4)
        if ppm is None:
            continue
        ppm_values.append(float(ppm))
        teams[team_key] = {
            "team": team_raw,
            "ppm": float(ppm),
        }

    average_ppm = round(sum(ppm_values) / len(ppm_values), 4) if ppm_values else None
    data = {
        "path": source,
        "teams": teams,
        "average_ppm": average_ppm,
    }
    _SERIEA_CONTEXT_CACHE[cache_key] = {"mtime": mtime, "data": data}
    return data


def _availability_default_payload() -> Dict[str, object]:
    return {
        "fetched_at": "",
        "sources": {
            "probable": PROBABLE_FORMATIONS_SOURCE_URL,
            "injuries": INJURIES_SOURCE_URL,
            "suspensions": SUSPENSIONS_SOURCE_URL,
        },
        "injured": [],
        "suspended": [],
        "diffidati": [],
    }


def _probable_formations_default_payload() -> Dict[str, object]:
    return {
        "fetched_at": "",
        "source_url": PROBABLE_FORMATIONS_SOURCE_URL,
        "round": None,
        "entries": [],
        "last_update_label": "",
    }


def _probable_match_item_blocks(source_html: str) -> List[str]:
    source = str(source_html or "")
    starts: List[int] = []
    for match in re.finditer(r"<li[^>]*\bid=\"match-\d+\"[^>]*>", source, flags=re.IGNORECASE):
        tag = str(match.group(0) or "").lower()
        if "match-item" not in tag:
            continue
        starts.append(match.start())

    if not starts:
        return []

    blocks: List[str] = []
    for idx, start in enumerate(starts):
        end = starts[idx + 1] if idx + 1 < len(starts) else len(source)
        blocks.append(source[start:end])
    return blocks


def _extract_probable_round_from_match_block(block_html: str) -> Optional[int]:
    match = re.search(
        r"<div\s+class=\"matchweek\">\s*([1-9]\d?)\s*</div>",
        str(block_html or ""),
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    return _parse_int(match.group(1))


def _extract_probable_percentage(player_item_html: str) -> float:
    source = str(player_item_html or "")
    patterns = [
        r"aria-valuenow=\"([0-9]{1,3}(?:[.,][0-9]+)?)\"",
        r"--value:\s*([0-9]{1,3}(?:[.,][0-9]+)?)",
        r"([0-9]{1,3}(?:[.,][0-9]+)?)\s*%",
    ]
    for pattern in patterns:
        match = re.search(pattern, source, flags=re.IGNORECASE)
        if not match:
            continue
        parsed = _parse_float(match.group(1))
        if parsed is None:
            continue
        return max(0.0, min(100.0, float(parsed)))
    return 0.0


def _probable_bucket_from_player(list_kind: str, status_value: str) -> str:
    list_token = normalize_name(list_kind)
    if list_token == "reserves":
        return "panchina"
    status_token = normalize_name(status_value)
    if status_token == "warn":
        return "ballottaggio"
    return "titolare"


def _probable_weight_from_percent(percent_value: float, bucket: str) -> float:
    pct = max(0.0, min(100.0, float(percent_value)))
    pct_weight = pct / 100.0
    bucket_key = normalize_name(bucket)

    if bucket_key == "panchina":
        if pct <= 25.0:
            return 0.0
        return round(max(0.0, min(1.0, (pct - 25.0) / 75.0)), 4)

    if bucket_key == "ballottaggio":
        return round(max(0.0, min(1.0, max(0.45, pct_weight * 0.95))), 4)

    return round(max(0.0, min(1.0, max(0.60, pct_weight))), 4)


def _probable_multiplier_from_weight(weight_value: float, bucket: str) -> float:
    weight = max(0.0, min(1.0, float(weight_value)))
    bucket_key = normalize_name(bucket)

    if bucket_key == "panchina":
        if weight <= 0:
            return 0.05
        return round(0.45 + (0.55 * weight), 3)
    if bucket_key == "ballottaggio":
        return round(0.84 + (0.24 * weight), 3)
    return round(0.92 + (0.18 * weight), 3)


def _extract_probable_players_from_list(
    *,
    list_html: str,
    list_kind: str,
    team_name: str,
    team_key: str,
    round_value: int,
) -> List[Dict[str, object]]:
    entries: List[Dict[str, object]] = []
    for item in re.finditer(
        r"<li\s+class=\"player-item\s+pill\"([^>]*)>(.*?)</li>",
        str(list_html or ""),
        flags=re.IGNORECASE | re.DOTALL,
    ):
        attrs_raw = str(item.group(1) or "")
        body_raw = str(item.group(2) or "")

        status_match = re.search(r"data-status=\"([^\"]+)\"", attrs_raw, flags=re.IGNORECASE)
        status_value = str(status_match.group(1) or "").strip().lower() if status_match else ""

        role_match = re.search(
            r"<span\s+class=\"role\"[^>]*data-value=\"([a-z])\"",
            body_raw,
            flags=re.IGNORECASE,
        )
        role_value = _role_from_text(role_match.group(1) if role_match else "")

        name_match = re.search(
            r"<a[^>]*class=\"[^\"]*player-name[^\"]*\"[^>]*>(.*?)</a>",
            body_raw,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not name_match:
            continue
        player_name = _canonicalize_name(_strip_html_tags(name_match.group(1)))
        player_key = normalize_name(player_name)
        if not player_key:
            continue

        percentage = _extract_probable_percentage(body_raw)
        bucket = _probable_bucket_from_player(list_kind, status_value)
        weight = _probable_weight_from_percent(percentage, bucket)
        multiplier = _probable_multiplier_from_weight(weight, bucket)
        recommended = bool(
            bucket in {"titolare", "ballottaggio"}
            or (bucket == "panchina" and percentage > 25.0)
        )

        entries.append(
            {
                "round": int(round_value),
                "team": team_name,
                "team_key": team_key,
                "name": player_name,
                "name_key": player_key,
                "role": role_value,
                "list": list_kind,
                "status": status_value,
                "bucket": bucket,
                "percentage": round(float(percentage), 2),
                "weight": float(weight),
                "multiplier": float(multiplier),
                "recommended": recommended,
            }
        )
    return entries


def _extract_probable_formations_entries_from_html(
    source_html: str,
    club_index: Dict[str, str],
) -> Dict[str, object]:
    entries: List[Dict[str, object]] = []
    seen: Set[Tuple[int, str, str, str]] = set()
    rounds_seen: Set[int] = set()
    match_count = 0

    for match_block in _probable_match_item_blocks(source_html):
        round_value = _extract_probable_round_from_match_block(match_block)
        if round_value is None or round_value <= 0:
            continue
        rounds_seen.add(int(round_value))
        match_count += 1

        team_card_starts = [
            m.start()
            for m in re.finditer(
                r"<div\s+class=\"[^\"]*\bteam-card\b[^\"]*\"\s*>",
                match_block,
                flags=re.IGNORECASE,
            )
        ]
        for idx, start in enumerate(team_card_starts):
            end = team_card_starts[idx + 1] if idx + 1 < len(team_card_starts) else len(match_block)
            team_block = match_block[start:end]

            team_match = re.search(
                r"<h3\s+class=\"h6\s+team-name\">\s*(.*?)\s*</h3>",
                team_block,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if not team_match:
                continue
            team_name = _display_team_name(_strip_html_tags(team_match.group(1)), club_index)
            team_key = normalize_name(team_name)
            if not team_key:
                continue

            starters_match = re.search(
                r"<ul\s+class=\"player-list\s+starters\"[^>]*>(.*?)</ul>",
                team_block,
                flags=re.IGNORECASE | re.DOTALL,
            )
            reserves_match = re.search(
                r"<ul\s+class=\"player-list\s+reserves\"[^>]*>(.*?)</ul>",
                team_block,
                flags=re.IGNORECASE | re.DOTALL,
            )

            sections = [
                ("starters", starters_match.group(1) if starters_match else ""),
                ("reserves", reserves_match.group(1) if reserves_match else ""),
            ]
            for list_kind, list_html in sections:
                if not list_html:
                    continue
                for item in _extract_probable_players_from_list(
                    list_html=list_html,
                    list_kind=list_kind,
                    team_name=team_name,
                    team_key=team_key,
                    round_value=int(round_value),
                ):
                    marker = (
                        int(item.get("round") or 0),
                        str(item.get("team_key") or ""),
                        str(item.get("name_key") or ""),
                        str(item.get("list") or ""),
                    )
                    if marker in seen:
                        continue
                    seen.add(marker)
                    entries.append(item)

    entries.sort(
        key=lambda row: (
            int(row.get("round") or 0),
            str(row.get("team_key") or ""),
            str(row.get("name_key") or ""),
        )
    )

    label_match = re.search(
        (
            r"<div\s+class=\"label\s+label-dark\s+last-update[^\"]*\">"
            r".*?<span\s+class=\"date\">\s*(.*?)\s*</span>"
        ),
        str(source_html or ""),
        flags=re.IGNORECASE | re.DOTALL,
    )
    last_update_label = _strip_html_tags(label_match.group(1)) if label_match else ""

    return {
        "entries": entries,
        "round": max(rounds_seen) if rounds_seen else None,
        "match_count": match_count,
        "last_update_label": last_update_label,
    }


def _sync_probable_formations_source() -> Dict[str, object]:
    club_index = _load_club_name_index()
    try:
        html_text = _fetch_text_url(PROBABLE_FORMATIONS_SOURCE_URL, timeout_seconds=25.0)
    except Exception as exc:
        detail = exc.detail if isinstance(exc, HTTPException) and hasattr(exc, "detail") else str(exc)
        return {
            "ok": False,
            "error": f"probable_formations_fetch_failed: {detail}",
            "source": PROBABLE_FORMATIONS_SOURCE_URL,
        }

    extracted = _extract_probable_formations_entries_from_html(html_text, club_index)
    entries = extracted.get("entries") if isinstance(extracted, dict) else []
    entries = entries if isinstance(entries, list) else []
    if not entries:
        return {
            "ok": False,
            "error": "probable_formations_parse_failed: empty_entries",
            "source": PROBABLE_FORMATIONS_SOURCE_URL,
        }

    snapshot = _probable_formations_default_payload()
    snapshot["fetched_at"] = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    snapshot["round"] = _parse_int(extracted.get("round")) if isinstance(extracted, dict) else None
    snapshot["entries"] = entries
    snapshot["last_update_label"] = (
        str(extracted.get("last_update_label") or "") if isinstance(extracted, dict) else ""
    )

    serialized = json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n"
    _write_text_if_changed(PROBABLE_FORMATIONS_STATUS_PATH, serialized)
    _PROBABLE_FORMATIONS_CACHE.clear()

    return {
        "ok": True,
        "round": snapshot.get("round"),
        "entries_count": len(entries),
        "match_count": int(extracted.get("match_count") or 0) if isinstance(extracted, dict) else 0,
        "path": str(PROBABLE_FORMATIONS_STATUS_PATH),
        "fetched_at": str(snapshot.get("fetched_at") or ""),
        "source": PROBABLE_FORMATIONS_SOURCE_URL,
    }


def _read_probable_formations_status_file(path: Path) -> Dict[str, object]:
    defaults = _probable_formations_default_payload()
    if not path.exists():
        return defaults

    try:
        mtime = path.stat().st_mtime
    except Exception:
        return defaults

    cache_key = str(path)
    cached = _PROBABLE_FORMATIONS_CACHE.get(cache_key)
    if cached and cached.get("mtime") == mtime:
        return dict(cached.get("data") or defaults)

    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        parsed = {}

    if not isinstance(parsed, dict):
        parsed = {}

    data = _probable_formations_default_payload()
    data["fetched_at"] = str(parsed.get("fetched_at") or "")
    data["source_url"] = str(parsed.get("source_url") or PROBABLE_FORMATIONS_SOURCE_URL)
    data["round"] = _parse_int(parsed.get("round"))
    data["last_update_label"] = str(parsed.get("last_update_label") or "")
    raw_entries = parsed.get("entries")
    if isinstance(raw_entries, list):
        data["entries"] = [dict(item) for item in raw_entries if isinstance(item, dict)]

    _PROBABLE_FORMATIONS_CACHE[cache_key] = {"mtime": mtime, "data": data}
    return dict(data)


def _load_probable_formations_status(
    *,
    refresh_if_stale: bool = True,
    max_age_hours: float = PROBABLE_FORMATIONS_MAX_AGE_HOURS,
) -> Dict[str, object]:
    defaults = _probable_formations_default_payload()
    runtime_path = PROBABLE_FORMATIONS_STATUS_PATH
    seed_path = PROBABLE_FORMATIONS_SEED_PATH
    now_utc = datetime.now(tz=timezone.utc)

    def _active_path() -> Optional[Path]:
        if runtime_path.exists():
            return runtime_path
        if seed_path.exists():
            return seed_path
        return None

    should_refresh = False
    current_path = _active_path()
    if current_path is None:
        should_refresh = True
    elif refresh_if_stale and max_age_hours > 0:
        try:
            age_seconds = max(0.0, now_utc.timestamp() - float(current_path.stat().st_mtime))
            if age_seconds >= float(max_age_hours) * 3600.0:
                should_refresh = True
        except Exception:
            should_refresh = True

    if should_refresh and refresh_if_stale:
        sync_result = _sync_probable_formations_source()
        current_path = _active_path()
        if sync_result.get("ok") is False and current_path is None:
            return defaults

    current_path = _active_path()
    if current_path is None:
        return defaults
    return _read_probable_formations_status_file(current_path)


def _build_optimizer_probable_lookup(
    round_value: Optional[int],
) -> Dict[str, object]:
    target_round = _parse_int(round_value)
    status = _load_probable_formations_status(refresh_if_stale=True)
    raw_entries = status.get("entries") if isinstance(status, dict) else []
    entries = [dict(item) for item in raw_entries if isinstance(item, dict)] if isinstance(raw_entries, list) else []
    status_round = _parse_int(status.get("round")) if isinstance(status, dict) else None

    exact_entries: List[Dict[str, object]] = []
    if target_round is not None:
        exact_entries = [
            item
            for item in entries
            if _parse_int(item.get("round")) == int(target_round)
        ]
    selected = exact_entries
    if not selected and status_round is not None:
        selected = [item for item in entries if _parse_int(item.get("round")) == int(status_round)]
    if not selected:
        selected = entries

    used_round = target_round if exact_entries else (status_round if status_round is not None else None)
    if used_round is None and selected:
        used_round = _parse_int(selected[0].get("round"))

    by_name_team: Dict[Tuple[str, str], Dict[str, object]] = {}
    by_name: Dict[str, Dict[str, object]] = {}
    grouped_by_name: Dict[str, List[Dict[str, object]]] = defaultdict(list)

    for item in selected:
        name_key = normalize_name(str(item.get("name_key") or item.get("name") or ""))
        team_key = normalize_name(str(item.get("team_key") or item.get("team") or ""))
        if not name_key:
            continue
        percentage = max(0.0, min(100.0, float(_parse_float(item.get("percentage")) or 0.0)))
        bucket = str(item.get("bucket") or _probable_bucket_from_player(
            str(item.get("list") or ""),
            str(item.get("status") or ""),
        )).strip().lower()
        weight = _parse_float(item.get("weight"))
        if weight is None:
            weight = _probable_weight_from_percent(percentage, bucket)
        multiplier = _parse_float(item.get("multiplier"))
        if multiplier is None:
            multiplier = _probable_multiplier_from_weight(weight, bucket)

        entry = {
            "round": _parse_int(item.get("round")),
            "name": _canonicalize_name(str(item.get("name") or "")),
            "name_key": name_key,
            "team": str(item.get("team") or ""),
            "team_key": team_key,
            "role": _role_from_text(item.get("role")),
            "list": str(item.get("list") or ""),
            "status": str(item.get("status") or "").strip().lower(),
            "bucket": bucket,
            "percentage": round(float(percentage), 2),
            "weight": round(float(max(0.0, min(1.0, weight))), 4),
            "multiplier": round(float(max(0.05, min(1.20, multiplier))), 3),
            "recommended": bool(item.get("recommended", False)),
        }
        by_name_team[(name_key, team_key)] = entry
        grouped_by_name[name_key].append(entry)

    for name_key, rows in grouped_by_name.items():
        teams = {
            str(row.get("team_key") or "").strip()
            for row in rows
            if str(row.get("team_key") or "").strip()
        }
        if len(rows) == 1 or len(teams) <= 1:
            by_name[name_key] = rows[0]

    return {
        "by_name_team": by_name_team,
        "by_name": by_name,
        "entry_count": len(by_name_team),
        "round": used_round,
        "fetched_at": str(status.get("fetched_at") or "") if isinstance(status, dict) else "",
        "source_url": str(status.get("source_url") or PROBABLE_FORMATIONS_SOURCE_URL)
        if isinstance(status, dict)
        else PROBABLE_FORMATIONS_SOURCE_URL,
        "last_update_label": str(status.get("last_update_label") or "") if isinstance(status, dict) else "",
    }


def _player_probable_for_round(
    *,
    player_name: str,
    club_name: str,
    lookup: Dict[str, object],
) -> Optional[Dict[str, object]]:
    name_key = normalize_name(_canonicalize_name(player_name))
    team_key = normalize_name(club_name)
    if not name_key:
        return None

    by_name_team = lookup.get("by_name_team") if isinstance(lookup, dict) else {}
    if isinstance(by_name_team, dict):
        exact = by_name_team.get((name_key, team_key))
        if isinstance(exact, dict):
            return exact

    by_name = lookup.get("by_name") if isinstance(lookup, dict) else {}
    if isinstance(by_name, dict):
        fallback = by_name.get(name_key)
        if isinstance(fallback, dict):
            return fallback
    return None


def _write_text_if_changed(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = path.read_text(encoding="utf-8")
        if existing == content:
            path.touch()
            return
    except Exception:
        pass
    path.write_text(content, encoding="utf-8")


def _team_card_blocks(source_html: str) -> List[str]:
    source = str(source_html or "")
    starts = [
        match.start()
        for match in re.finditer(
            r'<div\s+id="team-\d+"\s+class="[^"]*team-card[^"]*"\s*>',
            source,
            flags=re.IGNORECASE,
        )
    ]
    if not starts:
        return []
    blocks: List[str] = []
    for idx, start in enumerate(starts):
        end = starts[idx + 1] if idx + 1 < len(starts) else len(source)
        blocks.append(source[start:end])
    return blocks


def _extract_probable_team_names_from_match_block(
    block_html: str,
    club_index: Dict[str, str],
) -> List[Tuple[str, str]]:
    teams: List[Tuple[str, str]] = []
    for match in re.finditer(
        r"<h3\s+class=\"h6\s+team-name\">\s*(.*?)\s*</h3>",
        str(block_html or ""),
        flags=re.IGNORECASE | re.DOTALL,
    ):
        team_name = _display_team_name(_strip_html_tags(match.group(1)), club_index)
        team_key = normalize_name(team_name)
        if not team_key:
            continue
        teams.append((team_name, team_key))
    return teams[:2]


def _extract_probable_section_players(
    section_html: str,
    *,
    with_note: bool,
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for item in re.finditer(
        r"<li>\s*(.*?)\s*</li>",
        str(section_html or ""),
        flags=re.IGNORECASE | re.DOTALL,
    ):
        item_html = str(item.group(1) or "")
        player_match = re.search(
            r"<a[^>]*class=\"[^\"]*player-name[^\"]*\"[^>]*>(.*?)</a>",
            item_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if player_match is None:
            continue
        player_name = _canonicalize_name(_strip_html_tags(player_match.group(1)))
        player_key = normalize_name(player_name)
        if not player_key:
            continue
        note = ""
        if with_note:
            note_match = re.search(
                r"<p[^>]*class=\"[^\"]*description[^\"]*\"[^>]*>(.*?)</p>",
                item_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            note = _strip_html_tags(note_match.group(1)) if note_match else ""
        out.append(
            {
                "name": player_name,
                "name_key": player_key,
                "note": note,
            }
        )
    return out


def _extract_probable_availability_entries_from_html(
    source_html: str,
    club_index: Dict[str, str],
) -> Dict[str, object]:
    injured: List[Dict[str, object]] = []
    suspended: List[Dict[str, object]] = []
    diffidati: List[Dict[str, object]] = []
    seen_injured: Set[Tuple[str, str]] = set()
    seen_suspended: Set[Tuple[str, str, Tuple[int, ...]]] = set()
    seen_diffidati: Set[Tuple[str, str]] = set()
    rounds_seen: Set[int] = set()

    for match_block in _probable_match_item_blocks(source_html):
        teams = _extract_probable_team_names_from_match_block(match_block, club_index)
        if not teams:
            continue
        round_value = _extract_probable_round_from_match_block(match_block)
        if round_value is not None and round_value > 0:
            rounds_seen.add(int(round_value))

        section_specs = (
            ("injured", "injureds", True),
            ("suspended", "suspendeds", False),
            ("diffidati", "cautioneds", False),
        )
        for section_kind, section_class, with_note in section_specs:
            section_match = re.search(
                rf"<section\s+class=\"{section_class}\">(.*?)</section>",
                match_block,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if section_match is None:
                continue
            section_html = str(section_match.group(1) or "")
            content_blocks = re.findall(
                r"<div\s+class=\"content\">(.*?)</div>",
                section_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if not content_blocks:
                continue

            for idx, content_html in enumerate(content_blocks):
                if idx >= len(teams):
                    break
                team_name, team_key = teams[idx]
                players = _extract_probable_section_players(content_html, with_note=with_note)
                for item in players:
                    name = str(item.get("name") or "")
                    name_key = str(item.get("name_key") or "")
                    if not name_key:
                        continue
                    note = str(item.get("note") or "")
                    if section_kind == "injured":
                        marker = (name_key, team_key)
                        if marker in seen_injured:
                            continue
                        seen_injured.add(marker)
                        injured.append(
                            {
                                "name": name,
                                "name_key": name_key,
                                "team": team_name,
                                "team_key": team_key,
                                "note": note,
                            }
                        )
                    elif section_kind == "suspended":
                        rounds = [int(round_value)] if round_value is not None and round_value > 0 else []
                        marker = (name_key, team_key, tuple(rounds))
                        if marker in seen_suspended:
                            continue
                        seen_suspended.add(marker)
                        suspended.append(
                            {
                                "name": name,
                                "name_key": name_key,
                                "team": team_name,
                                "team_key": team_key,
                                "rounds": rounds,
                                "indefinite": bool(not rounds),
                                "note": note,
                            }
                        )
                    else:
                        marker = (name_key, team_key)
                        if marker in seen_diffidati:
                            continue
                        seen_diffidati.add(marker)
                        diffidati.append(
                            {
                                "name": name,
                                "name_key": name_key,
                                "team": team_name,
                                "team_key": team_key,
                            }
                        )

    injured.sort(key=lambda row: (str(row.get("team_key") or ""), str(row.get("name_key") or "")))
    suspended.sort(key=lambda row: (str(row.get("team_key") or ""), str(row.get("name_key") or "")))
    diffidati.sort(key=lambda row: (str(row.get("team_key") or ""), str(row.get("name_key") or "")))
    return {
        "injured": injured,
        "suspended": suspended,
        "diffidati": diffidati,
        "round": max(rounds_seen) if rounds_seen else None,
    }


def _enrich_injured_from_secondary(
    primary_rows: List[Dict[str, object]],
    secondary_rows: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    if not primary_rows or not secondary_rows:
        return primary_rows
    secondary_map = {
        (
            normalize_name(str(item.get("name_key") or item.get("name") or "")),
            normalize_name(str(item.get("team_key") or item.get("team") or "")),
        ): item
        for item in secondary_rows
        if normalize_name(str(item.get("name_key") or item.get("name") or ""))
    }
    for row in primary_rows:
        key = (
            normalize_name(str(row.get("name_key") or row.get("name") or "")),
            normalize_name(str(row.get("team_key") or row.get("team") or "")),
        )
        extra = secondary_map.get(key)
        if not isinstance(extra, dict):
            continue
        note = str(row.get("note") or "").strip()
        if not note:
            row["note"] = str(extra.get("note") or "").strip()
    return primary_rows


def _enrich_suspended_from_secondary(
    primary_rows: List[Dict[str, object]],
    secondary_rows: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    if not primary_rows or not secondary_rows:
        return primary_rows
    secondary_map = {
        (
            normalize_name(str(item.get("name_key") or item.get("name") or "")),
            normalize_name(str(item.get("team_key") or item.get("team") or "")),
        ): item
        for item in secondary_rows
        if normalize_name(str(item.get("name_key") or item.get("name") or ""))
    }
    for row in primary_rows:
        key = (
            normalize_name(str(row.get("name_key") or row.get("name") or "")),
            normalize_name(str(row.get("team_key") or row.get("team") or "")),
        )
        extra = secondary_map.get(key)
        if not isinstance(extra, dict):
            continue
        rounds = [
            int(value)
            for value in (row.get("rounds") or [])
            if _parse_int(value) is not None and int(value) > 0
        ]
        if not rounds:
            extra_rounds = [
                int(value)
                for value in (extra.get("rounds") or [])
                if _parse_int(value) is not None and int(value) > 0
            ]
            if extra_rounds:
                row["rounds"] = sorted(set(extra_rounds))
        row["indefinite"] = bool(not row.get("rounds"))
        note = str(row.get("note") or "").strip()
        if not note:
            row["note"] = str(extra.get("note") or "").strip()
    return primary_rows


def _extract_rounds_from_suspension_note(note: str) -> List[int]:
    cleaned = _strip_html_tags(note).lower()
    if not cleaned:
        return []
    if "giornat" not in cleaned:
        return []
    rounds = {
        int(value)
        for value in re.findall(r"([1-9]\d?)\s*(?:a|ª|°|º)?", cleaned, flags=re.IGNORECASE)
        if 1 <= int(value) <= 99
    }
    return sorted(rounds)


def _extract_injured_entries_from_html(
    source_html: str,
    club_index: Dict[str, str],
) -> List[Dict[str, object]]:
    entries: List[Dict[str, object]] = []
    seen: Set[Tuple[str, str]] = set()

    for block in _team_card_blocks(source_html):
        team_match = re.search(
            r'<span\s+class="team-name">\s*(.*?)\s*</span>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if team_match is None:
            continue
        team_name = _display_team_name(_strip_html_tags(team_match.group(1)), club_index)
        team_key = normalize_name(team_name)
        if not team_key:
            continue

        for item in re.finditer(
            (
                r'<li>\s*<strong\s+class="item-name">\s*(.*?)\s*</strong>'
                r"\s*(?:<div\s+class=\"item-description\">(.*?)</div>)?"
                r"\s*</li>"
            ),
            block,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            player_name = _canonicalize_name(_strip_html_tags(item.group(1)))
            player_key = normalize_name(player_name)
            if not player_key:
                continue
            marker = (player_key, team_key)
            if marker in seen:
                continue
            seen.add(marker)
            note = _strip_html_tags(item.group(2) or "")
            entries.append(
                {
                    "name": player_name,
                    "name_key": player_key,
                    "team": team_name,
                    "team_key": team_key,
                    "note": note,
                }
            )
    entries.sort(key=lambda row: (str(row.get("team_key") or ""), str(row.get("name_key") or "")))
    return entries


def _extract_suspension_entries_from_html(
    source_html: str,
    club_index: Dict[str, str],
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    suspended: List[Dict[str, object]] = []
    diffidati: List[Dict[str, object]] = []
    seen_suspended: Set[Tuple[str, str, Tuple[int, ...]]] = set()
    seen_diffidati: Set[Tuple[str, str]] = set()

    for block in _team_card_blocks(source_html):
        team_match = re.search(
            r'<span\s+class="team-name">\s*(.*?)\s*</span>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if team_match is None:
            continue
        team_name = _display_team_name(_strip_html_tags(team_match.group(1)), club_index)
        team_key = normalize_name(team_name)
        if not team_key:
            continue

        suspended_section_match = re.search(
            (
                r"<strong\s+class=\"label\s+label-danger\">Squalificati</strong>"
                r"(.*?)"
                r"(?:<strong\s+class=\"label\s+label-warn\">Diffidati</strong>|$)"
            ),
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        suspended_section = suspended_section_match.group(1) if suspended_section_match else ""
        for item in re.finditer(
            (
                r'<li>\s*<strong\s+class="item-name">\s*(.*?)\s*</strong>'
                r"\s*(?:<p\s+class=\"item-description\">\s*(.*?)\s*</p>)?"
                r"\s*</li>"
            ),
            suspended_section,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            player_name = _canonicalize_name(_strip_html_tags(item.group(1)))
            player_key = normalize_name(player_name)
            if not player_key:
                continue
            note = _strip_html_tags(item.group(2) or "")
            rounds = _extract_rounds_from_suspension_note(note)
            rounds_tuple = tuple(rounds)
            marker = (player_key, team_key, rounds_tuple)
            if marker in seen_suspended:
                continue
            seen_suspended.add(marker)
            suspended.append(
                {
                    "name": player_name,
                    "name_key": player_key,
                    "team": team_name,
                    "team_key": team_key,
                    "rounds": list(rounds),
                    "indefinite": bool(not rounds),
                    "note": note,
                }
            )

        diffidati_section_match = re.search(
            r"<strong\s+class=\"label\s+label-warn\">Diffidati</strong>(.*?)$",
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        diffidati_section = diffidati_section_match.group(1) if diffidati_section_match else ""
        for item in re.finditer(
            r'<li>\s*<strong\s+class="item-name">\s*(.*?)\s*</strong>\s*</li>',
            diffidati_section,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            player_name = _canonicalize_name(_strip_html_tags(item.group(1)))
            player_key = normalize_name(player_name)
            if not player_key:
                continue
            marker = (player_key, team_key)
            if marker in seen_diffidati:
                continue
            seen_diffidati.add(marker)
            diffidati.append(
                {
                    "name": player_name,
                    "name_key": player_key,
                    "team": team_name,
                    "team_key": team_key,
                }
            )

    suspended.sort(key=lambda row: (str(row.get("team_key") or ""), str(row.get("name_key") or "")))
    diffidati.sort(key=lambda row: (str(row.get("team_key") or ""), str(row.get("name_key") or "")))
    return suspended, diffidati


def _write_name_list_file(path: Path, names: List[str]) -> None:
    cleaned: List[str] = []
    seen: Set[str] = set()
    for value in names:
        name = _canonicalize_name(str(value or "").strip())
        key = normalize_name(name)
        if not key or key in seen:
            continue
        seen.add(key)
        cleaned.append(name)
    cleaned.sort(key=lambda item: normalize_name(item))
    content = ("\n".join(cleaned) + "\n") if cleaned else ""
    _write_text_if_changed(path, content)


def _sync_player_availability_sources() -> Dict[str, object]:
    club_index = _load_club_name_index()
    primary_mode = "probable_primary"
    warnings: List[str] = []

    probable_entries = {"injured": [], "suspended": [], "diffidati": [], "round": None}
    probable_error = ""
    try:
        probable_html = _fetch_text_url(PROBABLE_FORMATIONS_SOURCE_URL, timeout_seconds=25.0)
        probable_entries = _extract_probable_availability_entries_from_html(probable_html, club_index)
    except Exception as exc:
        detail = exc.detail if isinstance(exc, HTTPException) and hasattr(exc, "detail") else str(exc)
        probable_error = f"probable_fetch_failed: {detail}"

    fallback_injuries: List[Dict[str, object]] = []
    fallback_suspended: List[Dict[str, object]] = []
    fallback_diffidati: List[Dict[str, object]] = []

    try:
        injuries_html = _fetch_text_url(INJURIES_SOURCE_URL, timeout_seconds=25.0)
        fallback_injuries = _extract_injured_entries_from_html(injuries_html, club_index)
    except Exception as exc:
        detail = exc.detail if isinstance(exc, HTTPException) and hasattr(exc, "detail") else str(exc)
        warnings.append(f"injuries_fetch_failed: {detail}")

    try:
        suspensions_html = _fetch_text_url(SUSPENSIONS_SOURCE_URL, timeout_seconds=25.0)
        fallback_suspended, fallback_diffidati = _extract_suspension_entries_from_html(
            suspensions_html,
            club_index,
        )
    except Exception as exc:
        detail = exc.detail if isinstance(exc, HTTPException) and hasattr(exc, "detail") else str(exc)
        warnings.append(f"suspensions_fetch_failed: {detail}")

    probable_has_data = bool(
        probable_entries.get("injured")
        or probable_entries.get("suspended")
        or probable_entries.get("diffidati")
    )
    if probable_has_data:
        injuries = [dict(item) for item in (probable_entries.get("injured") or []) if isinstance(item, dict)]
        suspended = [dict(item) for item in (probable_entries.get("suspended") or []) if isinstance(item, dict)]
        diffidati = [dict(item) for item in (probable_entries.get("diffidati") or []) if isinstance(item, dict)]
        injuries = _enrich_injured_from_secondary(injuries, fallback_injuries)
        suspended = _enrich_suspended_from_secondary(suspended, fallback_suspended)
    else:
        primary_mode = "legacy_fallback"
        if probable_error:
            warnings.append(probable_error)
        injuries = fallback_injuries
        suspended = fallback_suspended
        diffidati = fallback_diffidati

    if not (injuries or suspended or diffidati):
        detail = "; ".join(warnings) if warnings else (probable_error or "no availability entries parsed")
        return {
            "ok": False,
            "error": detail,
            "source": PROBABLE_FORMATIONS_SOURCE_URL,
        }

    snapshot = _availability_default_payload()
    snapshot["fetched_at"] = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    snapshot["sources"] = {
        "mode": primary_mode,
        "probable": PROBABLE_FORMATIONS_SOURCE_URL,
        "injuries": INJURIES_SOURCE_URL,
        "suspensions": SUSPENSIONS_SOURCE_URL,
    }
    if probable_entries.get("round") is not None:
        snapshot["probable_round"] = _parse_int(probable_entries.get("round"))
    snapshot["injured"] = injuries
    snapshot["suspended"] = suspended
    snapshot["diffidati"] = diffidati

    serialized = json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n"
    _write_text_if_changed(AVAILABILITY_STATUS_PATH, serialized)
    _write_name_list_file(INJURED_CLEAN_PATH, [str(item.get("name") or "") for item in injuries])
    _write_name_list_file(SUSPENDED_CLEAN_PATH, [str(item.get("name") or "") for item in suspended])
    _AVAILABILITY_CACHE.clear()

    return {
        "ok": True,
        "mode": primary_mode,
        "injured_count": len(injuries),
        "suspended_count": len(suspended),
        "diffidati_count": len(diffidati),
        "path": str(AVAILABILITY_STATUS_PATH),
        "fetched_at": str(snapshot.get("fetched_at") or ""),
        "warnings": warnings,
    }


def _load_availability_status() -> Dict[str, object]:
    defaults = _availability_default_payload()
    if not AVAILABILITY_STATUS_PATH.exists():
        return defaults

    try:
        mtime = AVAILABILITY_STATUS_PATH.stat().st_mtime
    except Exception:
        return defaults

    cache_key = str(AVAILABILITY_STATUS_PATH)
    cached = _AVAILABILITY_CACHE.get(cache_key)
    if cached and cached.get("mtime") == mtime:
        return dict(cached.get("data") or defaults)

    try:
        parsed = json.loads(AVAILABILITY_STATUS_PATH.read_text(encoding="utf-8"))
    except Exception:
        parsed = {}

    if not isinstance(parsed, dict):
        parsed = {}
    data = _availability_default_payload()
    data["fetched_at"] = str(parsed.get("fetched_at") or "")
    data["sources"] = dict(parsed.get("sources") or data["sources"])
    for key in ("injured", "suspended", "diffidati"):
        values = parsed.get(key)
        if isinstance(values, list):
            data[key] = [dict(item) for item in values if isinstance(item, dict)]
    _AVAILABILITY_CACHE[cache_key] = {"mtime": mtime, "data": data}
    return dict(data)


def _build_optimizer_unavailability_lookup(
    round_value: Optional[int],
) -> Dict[str, object]:
    target_round = _parse_int(round_value)
    status = _load_availability_status()
    by_name_team: Dict[Tuple[str, str], Dict[str, object]] = {}
    by_name: Dict[str, Dict[str, object]] = {}
    grouped_by_name: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    excluded_count = 0

    for item in status.get("injured") or []:
        if not isinstance(item, dict):
            continue
        name_key = normalize_name(str(item.get("name_key") or item.get("name") or ""))
        team_key = normalize_name(str(item.get("team_key") or item.get("team") or ""))
        if not name_key:
            continue
        entry = {
            "status": "injured",
            "name": _canonicalize_name(str(item.get("name") or "")),
            "team": str(item.get("team") or ""),
            "team_key": team_key,
            "note": str(item.get("note") or ""),
            "rounds": [],
            "indefinite": True,
        }
        by_name_team[(name_key, team_key)] = entry
        grouped_by_name[name_key].append(entry)
        excluded_count += 1

    for item in status.get("suspended") or []:
        if not isinstance(item, dict):
            continue
        rounds = [
            int(value)
            for value in (item.get("rounds") or [])
            if _parse_int(value) is not None and int(value) > 0
        ]
        indefinite = bool(item.get("indefinite")) or not rounds
        if target_round is not None and not indefinite and int(target_round) not in rounds:
            continue
        name_key = normalize_name(str(item.get("name_key") or item.get("name") or ""))
        team_key = normalize_name(str(item.get("team_key") or item.get("team") or ""))
        if not name_key:
            continue
        entry = {
            "status": "suspended",
            "name": _canonicalize_name(str(item.get("name") or "")),
            "team": str(item.get("team") or ""),
            "team_key": team_key,
            "note": str(item.get("note") or ""),
            "rounds": rounds,
            "indefinite": indefinite,
        }
        by_name_team[(name_key, team_key)] = entry
        grouped_by_name[name_key].append(entry)
        excluded_count += 1

    for name_key, entries in grouped_by_name.items():
        teams = {str(entry.get("team_key") or "").strip() for entry in entries if str(entry.get("team_key") or "").strip()}
        if len(entries) == 1 or len(teams) <= 1:
            by_name[name_key] = entries[0]

    return {
        "by_name_team": by_name_team,
        "by_name": by_name,
        "excluded_count": excluded_count,
        "fetched_at": str(status.get("fetched_at") or ""),
    }


def _player_unavailability_for_round(
    *,
    player_name: str,
    club_name: str,
    lookup: Dict[str, object],
) -> Optional[Dict[str, object]]:
    name_key = normalize_name(_canonicalize_name(player_name))
    team_key = normalize_name(club_name)
    if not name_key:
        return None
    by_name_team = lookup.get("by_name_team") if isinstance(lookup, dict) else {}
    if isinstance(by_name_team, dict):
        exact = by_name_team.get((name_key, team_key))
        if isinstance(exact, dict):
            return exact
    by_name = lookup.get("by_name") if isinstance(lookup, dict) else {}
    if isinstance(by_name, dict):
        fallback = by_name.get(name_key)
        if isinstance(fallback, dict):
            return fallback
    return None


def _availability_due_slots_for_local_dt(local_now: datetime) -> List[datetime]:
    day_start_local = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    due: List[datetime] = []
    for hour in sorted({max(0, min(23, int(value))) for value in AVAILABILITY_SYNC_HOURS_LOCAL}):
        slot_local = day_start_local.replace(hour=hour)
        if local_now >= slot_local:
            due.append(slot_local)
    return due


def _run_availability_sync_if_due(
    db: Session,
    *,
    local_now: datetime,
) -> Dict[str, object]:
    due_slots = _availability_due_slots_for_local_dt(local_now)
    if not due_slots:
        return {
            "ok": True,
            "skipped": True,
            "reason": "before_first_availability_slot",
            "timezone": str(LEGHE_SYNC_TZ),
        }

    attempts: List[Dict[str, object]] = []
    executed_any = False
    for slot_local in due_slots:
        slot_utc_ts = int(slot_local.astimezone(timezone.utc).timestamp())
        claimed = _claim_scheduled_job_slot(
            db,
            job_name=AVAILABILITY_SYNC_JOB_NAME,
            slot_ts=slot_utc_ts,
        )
        if not claimed:
            attempts.append(
                {
                    "ok": True,
                    "skipped": True,
                    "slot_local": slot_local.isoformat(),
                    "reason": "slot_already_processed",
                }
            )
            continue

        executed_any = True
        result = _sync_player_availability_sources()
        result["slot_local"] = slot_local.isoformat()
        if result.get("ok") is False:
            _release_scheduled_job_slot(
                db,
                job_name=AVAILABILITY_SYNC_JOB_NAME,
                slot_ts=slot_utc_ts,
            )
        attempts.append(result)

    if not executed_any:
        return {
            "ok": True,
            "skipped": True,
            "reason": "availability_already_synced_for_due_slots",
            "timezone": str(LEGHE_SYNC_TZ),
            "slots": [slot.isoformat() for slot in due_slots],
        }

    latest = attempts[-1] if attempts else {"ok": True}
    latest = dict(latest)
    latest["attempts"] = attempts
    latest["timezone"] = str(LEGHE_SYNC_TZ)
    return latest


def _optimizer_context_defaults() -> Dict[str, object]:
    return {
        "home_bonus": {"P": 0.02, "D": 0.02, "C": 0.03, "A": 0.04},
        "away_penalty": {"P": -0.02, "D": -0.02, "C": -0.03, "A": -0.04},
        "own_weight": {"P": 0.05, "D": 0.05, "C": 0.04, "A": 0.04},
        "opp_weight": {"P": 0.08, "D": 0.08, "C": 0.07, "A": 0.09},
        "min_multiplier": 0.82,
        "max_multiplier": 1.20,
    }


def _parse_optimizer_role_weights(
    raw: object,
    defaults: Dict[str, float],
) -> Dict[str, float]:
    parsed = {role: float(value) for role, value in defaults.items()}
    if not isinstance(raw, dict):
        return parsed

    for key, value in raw.items():
        role = _role_from_text(key)
        if role not in {"P", "D", "C", "A"}:
            continue
        parsed_value = _parse_float(value)
        if parsed_value is None:
            continue
        parsed[role] = float(parsed_value)
    return parsed


def _load_optimizer_context_config(regulation: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    defaults = _optimizer_context_defaults()
    source = regulation if isinstance(regulation, dict) else _load_regulation()
    raw = source.get("optimizer_context") if isinstance(source, dict) else {}
    if not isinstance(raw, dict):
        return defaults

    fixture_raw = raw.get("fixture_multiplier")
    scope = fixture_raw if isinstance(fixture_raw, dict) else raw

    home_bonus = _parse_optimizer_role_weights(scope.get("home_bonus"), defaults["home_bonus"])
    away_penalty = _parse_optimizer_role_weights(scope.get("away_penalty"), defaults["away_penalty"])
    own_weight = _parse_optimizer_role_weights(scope.get("own_weight"), defaults["own_weight"])
    opp_weight = _parse_optimizer_role_weights(scope.get("opp_weight"), defaults["opp_weight"])

    min_multiplier = _parse_float(scope.get("min_multiplier"))
    max_multiplier = _parse_float(scope.get("max_multiplier"))
    min_value = float(min_multiplier) if min_multiplier is not None else float(defaults["min_multiplier"])
    max_value = float(max_multiplier) if max_multiplier is not None else float(defaults["max_multiplier"])
    if min_value > max_value:
        min_value, max_value = max_value, min_value

    return {
        "home_bonus": home_bonus,
        "away_penalty": away_penalty,
        "own_weight": own_weight,
        "opp_weight": opp_weight,
        "min_multiplier": min_value,
        "max_multiplier": max_value,
    }


def _optimizer_fixture_multiplier(
    role: str,
    home_away: str,
    own_ppm: Optional[float],
    opponent_ppm: Optional[float],
    league_ppm: Optional[float],
    context_cfg: Optional[Dict[str, object]] = None,
) -> float:
    base_role = _role_from_text(role)
    if base_role not in {"P", "D", "C", "A"}:
        base_role = "C"

    defaults = _optimizer_context_defaults()
    resolved_cfg = context_cfg if isinstance(context_cfg, dict) else defaults
    home_bonus = _parse_optimizer_role_weights(resolved_cfg.get("home_bonus"), defaults["home_bonus"])
    away_penalty = _parse_optimizer_role_weights(resolved_cfg.get("away_penalty"), defaults["away_penalty"])
    own_weight = _parse_optimizer_role_weights(resolved_cfg.get("own_weight"), defaults["own_weight"])
    opp_weight = _parse_optimizer_role_weights(resolved_cfg.get("opp_weight"), defaults["opp_weight"])
    min_multiplier = _parse_float(resolved_cfg.get("min_multiplier"))
    max_multiplier = _parse_float(resolved_cfg.get("max_multiplier"))
    min_value = float(min_multiplier) if min_multiplier is not None else 0.82
    max_value = float(max_multiplier) if max_multiplier is not None else 1.20
    if min_value > max_value:
        min_value, max_value = max_value, min_value

    modifier = 0.0
    where = str(home_away or "").strip().upper()
    if where == "H":
        modifier += home_bonus[base_role]
    elif where == "A":
        modifier += away_penalty[base_role]

    if (
        league_ppm is not None
        and league_ppm > 0
        and own_ppm is not None
    ):
        own_delta = (float(own_ppm) - float(league_ppm)) / float(league_ppm)
        modifier += own_delta * own_weight[base_role]

    if (
        league_ppm is not None
        and league_ppm > 0
        and opponent_ppm is not None
    ):
        # Positive when opponent is weaker than average.
        opponent_delta = (float(league_ppm) - float(opponent_ppm)) / float(league_ppm)
        modifier += opponent_delta * opp_weight[base_role]

    raw_multiplier = 1.0 + modifier
    # Keep fixture impact material but bounded.
    return max(min_value, min(max_value, raw_multiplier))


def _player_force_value(
    player_name: str,
    force_map: Dict[str, float],
    qa_map: Dict[str, float],
) -> float:
    canonical = _canonicalize_name(player_name)
    lookup_keys = [
        normalize_name(strip_star(canonical)),
        normalize_name(strip_star(player_name)),
        normalize_name(canonical),
        normalize_name(player_name),
    ]
    for key in lookup_keys:
        if not key:
            continue
        value = force_map.get(key)
        if value is not None:
            return float(value)
    for key in lookup_keys:
        if not key:
            continue
        qa = qa_map.get(key)
        if qa is not None and qa > 0:
            # Conservative fallback when force report misses the player.
            return float(qa) * 3.0
    return 0.0


def _captain_mode(value: object) -> str:
    mode = normalize_name(str(value or "balanced"))
    if mode in {"safe", "balanced", "upside"}:
        return mode
    return "balanced"


def _captain_selection_score(
    player: Dict[str, object],
    captain_mode: str,
    league_avg_ppm: Optional[float],
) -> float:
    score = float(player.get("adjusted_force") or 0.0)
    role = _role_from_text(player.get("role"))
    home_away = str(player.get("fixture_home_away") or "").strip().upper()
    own_ppm = _parse_float(player.get("club_ppm"))
    opponent_ppm = _parse_float(player.get("opponent_ppm"))
    mode = _captain_mode(captain_mode)

    if mode == "safe":
        if role == "A":
            score *= 0.97
        elif role == "C":
            score *= 1.01
        elif role == "D":
            score *= 1.005

        if home_away == "A":
            score *= 0.97

        if own_ppm is not None and opponent_ppm is not None:
            delta = opponent_ppm - own_ppm
            if delta >= 0.25:
                score *= 0.94
            elif delta > 0:
                score *= 0.97

        if (
            league_avg_ppm is not None
            and opponent_ppm is not None
            and opponent_ppm >= (league_avg_ppm + 0.35)
        ):
            score *= 0.95

    elif mode == "upside":
        if role == "A":
            score *= 1.05
        elif role == "C":
            score *= 1.02

        if home_away == "H":
            score *= 1.01

        if own_ppm is not None and opponent_ppm is not None and (own_ppm - opponent_ppm) >= 0.25:
            score *= 1.03

    return round(score, 2)


def _optimizer_player_recommendation_reason(player: Dict[str, object]) -> str:
    name = str(player.get("name") or "").strip()
    role = _role_from_text(player.get("role")) or "-"
    base = float(player.get("base_force") or 0.0)
    adjusted = float(player.get("adjusted_force") or 0.0)
    factor = float(player.get("fixture_factor") or 1.0)
    probable_factor = float(player.get("probable_factor") or 1.0)
    probable_bucket = str(player.get("probable_bucket") or "unknown").strip().lower()
    probable_percentage = _parse_float(player.get("probable_percentage"))
    home_away = str(player.get("fixture_home_away") or "").strip().upper() or "-"
    opponent = str(player.get("fixture_opponent") or "").strip() or "?"
    own_ppm = _parse_float(player.get("club_ppm"))
    opp_ppm = _parse_float(player.get("opponent_ppm"))

    fixture_note = "matchup neutro"
    if factor >= 1.06:
        fixture_note = "matchup favorevole"
    elif factor <= 0.95:
        fixture_note = "matchup sfavorevole"

    ppm_note = ""
    if own_ppm is not None and opp_ppm is not None:
        ppm_note = f" | ppm {own_ppm:.2f} vs {opp_ppm:.2f}"

    probable_note = ""
    if probable_bucket in {"titolare", "ballottaggio", "panchina"}:
        pct_label = (
            f"{int(round(float(probable_percentage)))}%"
            if probable_percentage is not None
            else "n/d"
        )
        probable_note = (
            f" | probabili {probable_bucket} {pct_label} "
            f"(x{probable_factor:.3f})"
        )
    elif abs(probable_factor - 1.0) > 0.0001:
        probable_note = f" | probabili x{probable_factor:.3f}"

    return (
        f"{name} ({role}): base {base:.2f}, adjusted {adjusted:.2f}, "
        f"fattore {factor:.3f} ({fixture_note}), {home_away} vs {opponent}{ppm_note}{probable_note}"
    )


def _captain_explain_payload(
    player: Optional[Dict[str, object]],
    captain_mode: str,
    league_avg_ppm: Optional[float],
) -> Dict[str, object]:
    mode = _captain_mode(captain_mode)
    if not player:
        return {"name": "", "mode": mode}

    adjusted = float(player.get("adjusted_force") or 0.0)
    captain_score = _captain_selection_score(player, mode, league_avg_ppm)
    home_away = str(player.get("fixture_home_away") or "").strip().upper()
    opponent = str(player.get("fixture_opponent") or "").strip()

    if mode == "safe":
        mode_note = "modalita safe: penalizza trasferte e avversari forti"
    elif mode == "upside":
        mode_note = "modalita upside: spinge profili offensivi ad alto picco"
    else:
        mode_note = "modalita balanced: ranking su forza contestuale"

    return {
        "name": str(player.get("name") or ""),
        "role": _role_from_text(player.get("role")),
        "mode": mode,
        "base_force": round(float(player.get("base_force") or 0.0), 2),
        "adjusted_force": round(adjusted, 2),
        "captain_score": captain_score,
        "fixture_factor": round(float(player.get("fixture_factor") or 1.0), 3),
        "fixture_home_away": home_away,
        "fixture_opponent": opponent,
        "reason": (
            f"Scelto per captain_score {captain_score:.2f} "
            f"(adjusted {adjusted:.2f}); {mode_note}."
        ),
    }


def _optimizer_player_detail_payload(player: Dict[str, object]) -> Dict[str, object]:
    probable_percentage = _parse_float(player.get("probable_percentage"))
    return {
        "name": str(player.get("name") or ""),
        "role": _role_from_text(player.get("role")),
        "club": str(player.get("club") or ""),
        "base_force": round(float(player.get("base_force") or 0.0), 2),
        "adjusted_force": round(float(player.get("adjusted_force") or 0.0), 2),
        "fixture_factor": round(float(player.get("fixture_factor") or 1.0), 3),
        "fixture_home_away": str(player.get("fixture_home_away") or ""),
        "fixture_opponent": str(player.get("fixture_opponent") or ""),
        "probable_bucket": str(player.get("probable_bucket") or ""),
        "probable_percentage": round(float(probable_percentage), 2)
        if probable_percentage is not None
        else None,
        "probable_recommended": bool(player.get("probable_recommended", False)),
    }


def _build_optimizer_lineup(
    players: List[Dict[str, object]],
    allowed_modules: List[str],
    captain_mode: str = "balanced",
    league_avg_ppm: Optional[float] = None,
) -> Dict[str, object]:
    best_payload: Optional[Dict[str, object]] = None
    captain_mode = _captain_mode(captain_mode)
    role_buckets: Dict[str, List[Dict[str, object]]] = {"P": [], "D": [], "C": [], "A": []}
    for player in players:
        role = _role_from_text(player.get("role"))
        if role in role_buckets:
            role_buckets[role].append(player)

    for role in role_buckets:
        role_buckets[role].sort(
            key=lambda item: (
                -float(item.get("adjusted_force") or 0.0),
                -float(item.get("base_force") or 0.0),
                normalize_name(str(item.get("name") or "")),
            )
        )

    fallback_modules = ["343", "352", "433", "442", "451", "541", "532"]
    modules = allowed_modules[:] if allowed_modules else fallback_modules
    if not modules:
        modules = fallback_modules

    for module in modules:
        counts = _module_counts_from_str(module)
        if counts is None:
            continue
        d_need = int(counts.get("D", 0))
        c_need = int(counts.get("C", 0))
        a_need = int(counts.get("A", 0))
        if (
            len(role_buckets["P"]) < 1
            or len(role_buckets["D"]) < d_need
            or len(role_buckets["C"]) < c_need
            or len(role_buckets["A"]) < a_need
        ):
            continue

        chosen: List[Dict[str, object]] = []
        chosen.extend(role_buckets["P"][:1])
        chosen.extend(role_buckets["D"][:d_need])
        chosen.extend(role_buckets["C"][:c_need])
        chosen.extend(role_buckets["A"][:a_need])

        chosen_keys = {
            normalize_name(str(player.get("name") or ""))
            for player in chosen
            if str(player.get("name") or "").strip()
        }
        bench = [
            player
            for player in players
            if normalize_name(str(player.get("name") or "")) not in chosen_keys
        ]
        bench.sort(
            key=lambda item: (
                0 if _role_from_text(item.get("role")) == "P" else 1,
                -float(item.get("adjusted_force") or 0.0),
                normalize_name(str(item.get("name") or "")),
            )
        )

        adjusted_total = round(sum(float(player.get("adjusted_force") or 0.0) for player in chosen), 2)
        base_total = round(sum(float(player.get("base_force") or 0.0) for player in chosen), 2)

        ranked = [player for player in chosen if _role_from_text(player.get("role")) != "P"] or chosen
        ranked.sort(
            key=lambda item: (
                -_captain_selection_score(item, captain_mode, league_avg_ppm),
                -float(item.get("adjusted_force") or 0.0),
                -float(item.get("base_force") or 0.0),
                normalize_name(str(item.get("name") or "")),
            )
        )
        captain_player = ranked[0] if ranked else None
        vice_player = ranked[1] if len(ranked) > 1 else None
        captain = str(captain_player.get("name") or "") if captain_player else ""
        vice = str(vice_player.get("name") or "") if vice_player else ""
        captain_explain = _captain_explain_payload(captain_player, captain_mode, league_avg_ppm)
        vice_explain = _captain_explain_payload(vice_player, captain_mode, league_avg_ppm)

        payload = {
            "module": module,
            "lineup": {
                "portiere": str(chosen[0].get("name") or "") if chosen else "",
                "difensori": [str(player.get("name") or "") for player in chosen[1 : 1 + d_need]],
                "centrocampisti": [
                    str(player.get("name") or "")
                    for player in chosen[1 + d_need : 1 + d_need + c_need]
                ],
                "attaccanti": [
                    str(player.get("name") or "")
                    for player in chosen[1 + d_need + c_need : 1 + d_need + c_need + a_need]
                ],
                "portiere_details": [
                    _optimizer_player_detail_payload(player)
                    for player in chosen[:1]
                ],
                "difensori_details": [
                    _optimizer_player_detail_payload(player)
                    for player in chosen[1 : 1 + d_need]
                ],
                "centrocampisti_details": [
                    _optimizer_player_detail_payload(player)
                    for player in chosen[1 + d_need : 1 + d_need + c_need]
                ],
                "attaccanti_details": [
                    _optimizer_player_detail_payload(player)
                    for player in chosen[1 + d_need + c_need : 1 + d_need + c_need + a_need]
                ],
                "panchina_details": [
                    _optimizer_player_detail_payload(player)
                    for player in bench
                ],
            },
            "captain": captain,
            "vice_captain": vice,
            "captain_mode": captain_mode,
            "captain_explain": captain_explain,
            "vice_captain_explain": vice_explain,
            "totals": {
                "base_force": base_total,
                "adjusted_force": adjusted_total,
            },
        }
        if best_payload is None or float(payload["totals"]["adjusted_force"]) > float(
            best_payload["totals"]["adjusted_force"]
        ):
            best_payload = payload

    if best_payload is not None:
        return best_payload

    # Last-resort fallback: keep top 11 by adjusted force with coarse role split.
    sorted_players = sorted(
        players,
        key=lambda item: (
            -float(item.get("adjusted_force") or 0.0),
            -float(item.get("base_force") or 0.0),
            normalize_name(str(item.get("name") or "")),
        ),
    )
    by_role: Dict[str, List[Dict[str, object]]] = {"P": [], "D": [], "C": [], "A": []}
    for player in sorted_players:
        role = _role_from_text(player.get("role"))
        if role in by_role:
            by_role[role].append(player)
    goalkeeper = by_role["P"][:1]
    defenders = by_role["D"][:3]
    midfielders = by_role["C"][:4]
    attackers = by_role["A"][:3]
    starters = goalkeeper + defenders + midfielders + attackers
    bench = [
        player
        for player in sorted_players
        if normalize_name(str(player.get("name") or "")) not in {
            normalize_name(str(starter.get("name") or "")) for starter in starters
        }
    ]
    ranked_captains = [player for player in starters if _role_from_text(player.get("role")) != "P"] or starters
    ranked_captains.sort(
        key=lambda item: (
            -_captain_selection_score(item, captain_mode, league_avg_ppm),
            -float(item.get("adjusted_force") or 0.0),
            -float(item.get("base_force") or 0.0),
            normalize_name(str(item.get("name") or "")),
        )
    )
    captain_player = ranked_captains[0] if ranked_captains else None
    vice_player = ranked_captains[1] if len(ranked_captains) > 1 else None
    captain = str(captain_player.get("name") or "") if captain_player else ""
    vice = str(vice_player.get("name") or "") if vice_player else ""
    captain_explain = _captain_explain_payload(captain_player, captain_mode, league_avg_ppm)
    vice_explain = _captain_explain_payload(vice_player, captain_mode, league_avg_ppm)
    module = _module_from_role_counts(
        {
            "P": 1 if goalkeeper else 0,
            "D": len(defenders),
            "C": len(midfielders),
            "A": len(attackers),
        }
    ) or (allowed_modules[0] if allowed_modules else "343")
    return {
        "module": module,
        "lineup": {
            "portiere": str(goalkeeper[0].get("name") or "") if goalkeeper else "",
            "difensori": [str(player.get("name") or "") for player in defenders],
            "centrocampisti": [str(player.get("name") or "") for player in midfielders],
            "attaccanti": [str(player.get("name") or "") for player in attackers],
            "portiere_details": [
                _optimizer_player_detail_payload(player)
                for player in goalkeeper
            ],
            "difensori_details": [
                _optimizer_player_detail_payload(player)
                for player in defenders
            ],
            "centrocampisti_details": [
                _optimizer_player_detail_payload(player)
                for player in midfielders
            ],
            "attaccanti_details": [
                _optimizer_player_detail_payload(player)
                for player in attackers
            ],
            "panchina_details": [
                _optimizer_player_detail_payload(player)
                for player in bench
            ],
        },
        "captain": captain,
        "vice_captain": vice,
        "captain_mode": captain_mode,
        "captain_explain": captain_explain,
        "vice_captain_explain": vice_explain,
        "totals": {
            "base_force": round(sum(float(player.get("base_force") or 0.0) for player in starters), 2),
            "adjusted_force": round(sum(float(player.get("adjusted_force") or 0.0) for player in starters), 2),
        },
    }


def _build_contextual_optimizer_payload(
    team_key: str,
    db: Session,
    round_value: Optional[int],
    captain_mode: str = "balanced",
) -> Optional[Dict[str, object]]:
    rose_rows = _apply_qa_from_quot(_read_csv(ROSE_PATH))
    team_rows = [row for row in rose_rows if normalize_name(row.get("Team", "")) == team_key]
    if not team_rows:
        return None

    team_name = str(team_rows[0].get("Team") or "").strip()
    regulation = _load_regulation()
    optimizer_context_cfg = _load_optimizer_context_config(regulation)
    allowed_modules = _allowed_modules_from_regulation(regulation)
    club_index = _load_club_name_index()
    fixture_rows = _load_fixture_rows_for_live(db, club_index)
    rounds = _rounds_from_fixture_rows(fixture_rows)

    target_round = _parse_int(round_value)
    if target_round is None:
        target_round = _resolve_current_round(rounds)
    if target_round is None or target_round <= 0:
        target_round = rounds[-1] if rounds else 1

    fixture_index: Dict[str, Dict[str, str]] = {}
    for fixture in fixture_rows:
        if _parse_int(fixture.get("round")) != target_round:
            continue
        team_name_real = _display_team_name(str(fixture.get("team") or ""), club_index)
        opponent_name = _display_team_name(str(fixture.get("opponent") or ""), club_index)
        team_real_key = normalize_name(team_name_real)
        if not team_real_key:
            continue
        fixture_index[team_real_key] = {
            "opponent": opponent_name,
            "home_away": str(fixture.get("home_away") or "").strip().upper(),
        }

    context_data = _load_seriea_context_index()
    context_index = context_data.get("teams") if isinstance(context_data, dict) else {}
    context_index = context_index if isinstance(context_index, dict) else {}
    league_avg_ppm = _parse_float(context_data.get("average_ppm")) if isinstance(context_data, dict) else None
    if league_avg_ppm is None:
        league_avg_ppm = 1.25

    resolved_captain_mode = _captain_mode(captain_mode)
    force_map = _load_player_force_map()
    qa_map = _load_qa_map()
    unavailability_lookup = _build_optimizer_unavailability_lookup(target_round)
    probable_lookup = _build_optimizer_probable_lookup(target_round)

    players_payload: List[Dict[str, object]] = []
    unavailable_players: List[Dict[str, object]] = []
    for row in team_rows:
        player_name = _canonicalize_name(str(row.get("Giocatore") or ""))
        if not player_name:
            continue
        role = _role_from_text(row.get("Ruolo"))
        if not role:
            continue
        club_name = _display_team_name(str(row.get("Squadra") or ""), club_index)
        club_key = normalize_name(club_name)
        fixture_ctx = fixture_index.get(club_key, {})
        opponent_name = str(fixture_ctx.get("opponent") or "").strip()
        opponent_key = normalize_name(opponent_name)
        home_away = str(fixture_ctx.get("home_away") or "").strip().upper()

        unavailable_entry = _player_unavailability_for_round(
            player_name=player_name,
            club_name=club_name,
            lookup=unavailability_lookup,
        )
        if unavailable_entry:
            status_label = str(unavailable_entry.get("status") or "").strip().lower()
            rounds = [
                int(value)
                for value in (unavailable_entry.get("rounds") or [])
                if _parse_int(value) is not None and int(value) > 0
            ]
            unavailable_players.append(
                {
                    "name": player_name,
                    "role": role,
                    "club": club_name,
                    "status": status_label or "unavailable",
                    "reason": (
                        "Infortunato"
                        if status_label == "injured"
                        else (
                            f"Squalificato (giornate {', '.join(str(value) for value in rounds)})"
                            if rounds
                            else "Squalificato"
                        )
                    ),
                    "note": str(unavailable_entry.get("note") or ""),
                    "rounds": rounds,
                    "indefinite": bool(unavailable_entry.get("indefinite", False)),
                }
            )
            continue

        probable_entry = _player_probable_for_round(
            player_name=player_name,
            club_name=club_name,
            lookup=probable_lookup,
        )
        probable_bucket = (
            str(probable_entry.get("bucket") or "").strip().lower()
            if isinstance(probable_entry, dict)
            else ""
        )
        probable_percentage = (
            _parse_float(probable_entry.get("percentage"))
            if isinstance(probable_entry, dict)
            else None
        )
        probable_weight = (
            _parse_float(probable_entry.get("weight"))
            if isinstance(probable_entry, dict)
            else None
        )
        probable_factor_raw = (
            _parse_float(probable_entry.get("multiplier"))
            if isinstance(probable_entry, dict)
            else None
        )
        probable_factor = (
            max(0.05, min(1.20, float(probable_factor_raw)))
            if probable_factor_raw is not None
            else 1.0
        )
        if probable_weight is None and probable_percentage is not None:
            probable_weight = _probable_weight_from_percent(probable_percentage, probable_bucket)
        probable_recommended = bool(
            probable_bucket in {"titolare", "ballottaggio"}
            or (probable_bucket == "panchina" and (probable_percentage or 0.0) > 25.0)
        )

        own_ppm = _parse_float((context_index.get(club_key) or {}).get("ppm"))
        opp_ppm = _parse_float((context_index.get(opponent_key) or {}).get("ppm"))
        fixture_factor = _optimizer_fixture_multiplier(
            role=role,
            home_away=home_away,
            own_ppm=own_ppm,
            opponent_ppm=opp_ppm,
            league_ppm=league_avg_ppm,
            context_cfg=optimizer_context_cfg,
        )
        base_force = _player_force_value(player_name, force_map, qa_map)
        adjusted_force = round(base_force * fixture_factor * probable_factor, 2)

        payload = {
            "name": player_name,
            "role": role,
            "club": club_name,
            "base_force": round(base_force, 2),
            "fixture_factor": round(fixture_factor, 3),
            "probable_factor": round(probable_factor, 3),
            "probable_bucket": probable_bucket,
            "probable_percentage": round(float(probable_percentage), 2)
            if probable_percentage is not None
            else None,
            "probable_weight": round(float(probable_weight), 4)
            if probable_weight is not None
            else None,
            "probable_recommended": probable_recommended,
            "probable_round": _parse_int(probable_lookup.get("round")),
            "adjusted_force": adjusted_force,
            "fixture_home_away": home_away,
            "fixture_opponent": opponent_name,
            "club_ppm": own_ppm,
            "opponent_ppm": opp_ppm,
        }
        payload["recommendation_reason"] = _optimizer_player_recommendation_reason(payload)
        players_payload.append(payload)

    if not players_payload:
        return {
            "team": team_name,
            "round": int(target_round),
            "available_rounds": rounds,
            "source": "context_optimizer",
            "context_source_path": (
                str(context_data.get("path"))
                if isinstance(context_data, dict) and context_data.get("path")
                else ""
            ),
            "optimizer_context": optimizer_context_cfg,
            "captain_mode": resolved_captain_mode,
            "module": "",
            "lineup": {
                "portiere": "",
                "difensori": [],
                "centrocampisti": [],
                "attaccanti": [],
                "portiere_details": [],
                "difensori_details": [],
                "centrocampisti_details": [],
                "attaccanti_details": [],
                "panchina_details": [],
            },
            "captain": "",
            "vice_captain": "",
            "captain_explain": {},
            "vice_captain_explain": {},
            "totals": {"base_force": 0.0, "adjusted_force": 0.0},
            "players_ranked": [],
            "availability": {
                "fetched_at": str(unavailability_lookup.get("fetched_at") or ""),
                "excluded_count": int(len(unavailable_players)),
                "unavailable_players": unavailable_players,
                "note": "Nessun giocatore disponibile per il round selezionato.",
            },
            "probable_formations": {
                "fetched_at": str(probable_lookup.get("fetched_at") or ""),
                "round": _parse_int(probable_lookup.get("round")),
                "entry_count": int(probable_lookup.get("entry_count") or 0),
                "last_update_label": str(probable_lookup.get("last_update_label") or ""),
                "source_url": str(probable_lookup.get("source_url") or PROBABLE_FORMATIONS_SOURCE_URL),
            },
        }

    lineup_payload = _build_optimizer_lineup(
        players_payload,
        allowed_modules,
        captain_mode=resolved_captain_mode,
        league_avg_ppm=league_avg_ppm,
    )
    selected_keys = {
        normalize_name(name)
        for section in ("portiere", "difensori", "centrocampisti", "attaccanti")
        for name in (
            [lineup_payload.get("lineup", {}).get("portiere", "")]
            if section == "portiere"
            else lineup_payload.get("lineup", {}).get(section, [])
        )
        if str(name or "").strip()
    }
    selected_players = [
        player
        for player in players_payload
        if normalize_name(player.get("name")) in selected_keys
    ]
    selected_players.sort(
        key=lambda item: (
            -float(item.get("adjusted_force") or 0.0),
            -float(item.get("base_force") or 0.0),
            normalize_name(str(item.get("name") or "")),
        )
    )

    return {
        "team": team_name,
        "round": int(target_round),
        "available_rounds": rounds,
        "source": "context_optimizer",
        "context_source_path": str(context_data.get("path")) if isinstance(context_data, dict) and context_data.get("path") else "",
        "optimizer_context": optimizer_context_cfg,
        "captain_mode": lineup_payload.get("captain_mode", resolved_captain_mode),
        "module": _format_module(lineup_payload.get("module")),
        "lineup": lineup_payload.get("lineup", {}),
        "captain": lineup_payload.get("captain", ""),
        "vice_captain": lineup_payload.get("vice_captain", ""),
        "captain_explain": lineup_payload.get("captain_explain", {}),
        "vice_captain_explain": lineup_payload.get("vice_captain_explain", {}),
        "totals": lineup_payload.get("totals", {"base_force": 0.0, "adjusted_force": 0.0}),
        "players_ranked": selected_players,
        "availability": {
            "fetched_at": str(unavailability_lookup.get("fetched_at") or ""),
            "excluded_count": int(len(unavailable_players)),
            "unavailable_players": unavailable_players,
        },
        "probable_formations": {
            "fetched_at": str(probable_lookup.get("fetched_at") or ""),
            "round": _parse_int(probable_lookup.get("round")),
            "entry_count": int(probable_lookup.get("entry_count") or 0),
            "last_update_label": str(probable_lookup.get("last_update_label") or ""),
            "source_url": str(probable_lookup.get("source_url") or PROBABLE_FORMATIONS_SOURCE_URL),
        },
    }


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


def _build_calendar_round_url(round_value: int, season_slug: str) -> str:
    return f"{CALENDAR_BASE_URL}/{int(round_value)}/{season_slug}"


def _fetch_round_first_kickoff_from_calendar(
    round_value: int,
    season_slug: Optional[str] = None,
) -> Optional[datetime]:
    round_num = _parse_int(round_value)
    if round_num is None or round_num <= 0:
        return None

    resolved_season = _normalize_season_slug(season_slug)
    cache_key = f"{resolved_season}:{int(round_num)}"
    now_ts = float(datetime.utcnow().timestamp())
    cached = _ROUND_FIRST_KICKOFF_CACHE.get(cache_key)
    if isinstance(cached, dict) and (now_ts - float(cached.get("ts", 0.0) or 0.0) < 1800):
        cached_iso = str(cached.get("kickoff_iso") or "").strip()
        if cached_iso:
            parsed_cached = _parse_kickoff_local_datetime(cached_iso)
            if parsed_cached is not None:
                return parsed_cached

    url = _build_calendar_round_url(int(round_num), resolved_season)
    try:
        html_text = _fetch_text_url(url, timeout_seconds=20.0)
    except Exception:
        return None

    kickoff_candidates: List[datetime] = []
    pattern = re.compile(
        (
            r"<meta\s+itemprop=['\"]startDate['\"]\s+content=['\"]([^'\"]+)['\"][^>]*>"
            r".*?<span[^>]*class=['\"][^'\"]*hour[^'\"]*['\"][^>]*>\s*([^<]+)\s*</span>"
        ),
        flags=re.IGNORECASE | re.DOTALL,
    )
    for date_raw, hour_raw in pattern.findall(html_text):
        date_match = re.search(r"(\d{4}-\d{2}-\d{2})", str(date_raw or ""))
        if date_match is None:
            continue
        hour_match = re.search(r"(\d{1,2}):(\d{2})", str(hour_raw or ""))
        hour = int(hour_match.group(1)) if hour_match else 0
        minute = int(hour_match.group(2)) if hour_match else 0
        try:
            kickoff_local = datetime(
                int(date_match.group(1)[0:4]),
                int(date_match.group(1)[5:7]),
                int(date_match.group(1)[8:10]),
                hour,
                minute,
                0,
                tzinfo=LEGHE_SYNC_TZ,
            )
        except Exception:
            continue
        kickoff_candidates.append(kickoff_local)

    if not kickoff_candidates:
        return None

    first_kickoff = min(kickoff_candidates)
    _ROUND_FIRST_KICKOFF_CACHE[cache_key] = {
        "ts": now_ts,
        "kickoff_iso": first_kickoff.isoformat(),
    }
    return first_kickoff


def _calendar_slugify(value: object) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", raw)
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    slug = re.sub(r"[^A-Za-z0-9]+", "-", ascii_text).strip("-").lower()
    return slug


def _build_calendar_match_summary_url(
    *,
    round_value: int,
    season_slug: str,
    home_team: str,
    away_team: str,
    match_id: int,
) -> str:
    home_slug = _calendar_slugify(home_team)
    away_slug = _calendar_slugify(away_team)
    slug = f"{home_slug}-{away_slug}".strip("-")
    return f"{CALENDAR_BASE_URL}/{int(round_value)}/{season_slug}/{slug}/{int(match_id)}/riepilogo"


def _extract_round_match_refs_from_calendar_html(
    html_text: str,
    club_index: Dict[str, str],
) -> List[Dict[str, object]]:
    if not html_text:
        return []

    select_match = re.search(
        r"<select[^>]+id=['\"]matchControl['\"][^>]*>(.*?)</select>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if select_match is None:
        return []

    refs: List[Dict[str, object]] = []
    seen_ids: Set[int] = set()
    options_html = select_match.group(1)
    for value_raw, _ in re.findall(
        r"<option[^>]*value=['\"]([^'\"]+)['\"][^>]*>(.*?)</option>",
        options_html,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        parts = [html_unescape(str(p or "")).strip() for p in str(value_raw).split("/") if str(p or "").strip()]
        if len(parts) < 3:
            continue
        home_raw, away_raw, match_id_raw = parts[-3], parts[-2], parts[-1]
        match_id = _parse_int(match_id_raw)
        if match_id is None or match_id <= 0:
            continue
        if match_id in seen_ids:
            continue

        home_team = _display_team_name(home_raw, club_index)
        away_team = _display_team_name(away_raw, club_index)
        if not home_team or not away_team:
            continue

        seen_ids.add(match_id)
        refs.append(
            {
                "match_id": int(match_id),
                "home_team": home_team,
                "away_team": away_team,
            }
        )
    return refs


def _extract_scorer_events_from_match_summary_html(html_text: str) -> List[Dict[str, object]]:
    if not html_text:
        return []

    block_match = re.search(
        r"<div[^>]+id=['\"]scorersTemplateTarget['\"][^>]*>(.*?)</div>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    source = block_match.group(1) if block_match else html_text
    events: List[Dict[str, object]] = []
    li_pattern = re.compile(
        r"<li[^>]*class=['\"]([^'\"]+)['\"][^>]*>(.*?)</li>",
        flags=re.IGNORECASE | re.DOTALL,
    )

    for idx, (class_raw, item_html) in enumerate(li_pattern.findall(source)):
        class_tokens = {str(token or "").strip().lower() for token in str(class_raw or "").split() if token}
        if not class_tokens:
            continue

        side = "home" if "home" in class_tokens else ("away" if "away" in class_tokens else "")
        if side not in {"home", "away"}:
            continue
        # `type-aut` is an own goal: count it for the opposite side, never for
        # the scorer bonus attribution.
        is_own_goal = any(token == "type-aut" or token.startswith("type-aut") for token in class_tokens)
        effective_side = "away" if (is_own_goal and side == "home") else ("home" if is_own_goal else side)

        name_match = re.search(
            r"<a[^>]*class=['\"][^'\"]*player-name[^'\"]*['\"][^>]*>.*?<span>([^<]+)</span>",
            item_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if name_match is None:
            name_match = re.search(
                r"<span class=['\"][^'\"]*player-name[^'\"]*['\"]>([^<]+)</span>",
                item_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
        if name_match is None:
            continue

        player_name = _canonicalize_name(_strip_html_tags(name_match.group(1)))
        if not player_name:
            continue

        minute_match = re.search(
            r"<span class=['\"]minute[^'\"]*['\"]>([^<]+)</span>",
            item_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        minute_raw = _strip_html_tags(minute_match.group(1)) if minute_match else ""
        minute_raw = minute_raw.strip()
        minute_clean = minute_raw.replace("’", "'").replace("`", "'")
        minute_base = _parse_int(re.sub(r"[^0-9+]", "", minute_clean).split("+")[0])
        minute_extra = None
        if "+" in minute_clean:
            minute_extra = _parse_int(minute_clean.split("+", 1)[1])

        events.append(
            {
                "side": effective_side,
                "raw_side": side,
                "player": player_name,
                "minute_raw": minute_raw,
                "minute_base": minute_base,
                "minute_extra": minute_extra,
                "own_goal": bool(is_own_goal),
                "order": idx,
            }
        )
    return events


def _decisive_badge_from_scorer_events(
    events: List[Dict[str, object]],
) -> Optional[Dict[str, object]]:
    if not events:
        return None

    home_goals = sum(1 for item in events if str(item.get("side") or "") == "home")
    away_goals = sum(1 for item in events if str(item.get("side") or "") == "away")
    if home_goals <= 0 and away_goals <= 0:
        return None

    if home_goals == away_goals:
        last_event = events[-1]
        if bool(last_event.get("own_goal")):
            return None
        return {
            "event": "gol_pareggio",
            "side": str(last_event.get("side") or ""),
            "player": str(last_event.get("player") or ""),
        }

    if home_goals > away_goals:
        threshold = away_goals + 1
        count = 0
        for item in events:
            if str(item.get("side") or "") != "home":
                continue
            count += 1
            if count == threshold:
                if bool(item.get("own_goal")):
                    return None
                return {
                    "event": "gol_vittoria",
                    "side": "home",
                    "player": str(item.get("player") or ""),
                }
        return None

    threshold = home_goals + 1
    count = 0
    for item in events:
        if str(item.get("side") or "") != "away":
            continue
        count += 1
        if count == threshold:
            if bool(item.get("own_goal")):
                return None
            return {
                "event": "gol_vittoria",
                "side": "away",
                "player": str(item.get("player") or ""),
            }
    return None


def _load_round_decisive_badges_from_calendar_pages(
    *,
    round_value: int,
    season_slug: str,
    club_index: Dict[str, str],
) -> Dict[Tuple[str, str], Dict[str, int]]:
    badges: Dict[Tuple[str, str], Dict[str, int]] = {}
    try:
        round_html = _fetch_text_url(_build_calendar_round_url(round_value, season_slug))
    except Exception:
        return badges

    refs = _extract_round_match_refs_from_calendar_html(round_html, club_index)
    if not refs:
        return badges

    for ref in refs:
        match_id = _parse_int(ref.get("match_id"))
        home_team = str(ref.get("home_team") or "").strip()
        away_team = str(ref.get("away_team") or "").strip()
        if match_id is None or match_id <= 0 or not home_team or not away_team:
            continue

        summary_url = _build_calendar_match_summary_url(
            round_value=round_value,
            season_slug=season_slug,
            home_team=home_team,
            away_team=away_team,
            match_id=match_id,
        )

        try:
            summary_html = _fetch_text_url(summary_url)
        except Exception:
            continue

        events = _extract_scorer_events_from_match_summary_html(summary_html)
        decisive = _decisive_badge_from_scorer_events(events)
        if not decisive:
            continue

        event_key = str(decisive.get("event") or "").strip()
        side = str(decisive.get("side") or "").strip()
        player_name = _canonicalize_name(str(decisive.get("player") or ""))
        if event_key not in {"gol_vittoria", "gol_pareggio"} or side not in {"home", "away"} or not player_name:
            continue

        team_name = home_team if side == "home" else away_team
        team_key = normalize_name(team_name)
        player_key = normalize_name(player_name)
        if not team_key or not player_key:
            continue

        key = (team_key, player_key)
        current = badges.get(key)
        if current is None:
            current = {"gol_vittoria": 0, "gol_pareggio": 0}
            badges[key] = current
        current[event_key] = max(1, int(current.get(event_key, 0)))

    return badges


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


def _parse_fc_grade_value(raw_value: str, max_value: float = 10.0) -> Tuple[Optional[float], bool]:
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

    if parsed < 0 or parsed > float(max_value):
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
            vote_value, vote_is_sv = _parse_fc_grade_value(raw_vote, max_value=10.0)
            fantavote_value, fantavote_is_sv = _parse_fc_grade_value(raw_fantavote, max_value=30.0)
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

            # Fantacalcio sometimes emits masked "55" grades for subentrati
            # with no valid rating; treat this specific pattern as SV.
            raw_vote_compact = str(raw_vote or "").strip()
            raw_fantavote_compact = str(raw_fantavote or "").strip()
            row_html_lower = row_html.lower()
            if (
                not is_sv
                and not has_events
                and raw_vote_compact == "55"
                and raw_fantavote_compact == "55"
                and ("in.webp" in row_html_lower or "subentrato" in row_html_lower)
            ):
                vote_value = None
                fantavote_value = None
                is_sv = True

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


def _reg_appkey_bonus_indexes(regulation: Dict[str, object]) -> Dict[str, int]:
    defaults = {
        "gol_vittoria": APPKEY_BONUS_GV_DEFAULT_INDEX,
        "gol_pareggio": APPKEY_BONUS_GP_DEFAULT_INDEX,
    }
    live_import = regulation.get("live_import") if isinstance(regulation, dict) else {}
    live_import = live_import if isinstance(live_import, dict) else {}
    raw_map = (
        live_import.get("appkey_bonus_indexes")
        if isinstance(live_import.get("appkey_bonus_indexes"), dict)
        else {}
    )

    resolved = dict(defaults)
    for key in ("gol_vittoria", "gol_pareggio"):
        parsed = _parse_int(raw_map.get(key))
        if parsed is None or parsed < 0:
            continue
        resolved[key] = int(parsed)
    return resolved


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


def _infer_decisive_events_from_fantavote(
    vote_value: Optional[float],
    fantavote_value: Optional[float],
    event_counts: Dict[str, int],
    bonus_map: Dict[str, float],
) -> Dict[str, int]:
    if vote_value is None or fantavote_value is None:
        return event_counts

    current_gv = max(0, int(event_counts.get("gol_vittoria", 0) or 0))
    current_gp = max(0, int(event_counts.get("gol_pareggio", 0) or 0))
    # Do not overwrite explicit manual decisive-goal input.
    if current_gv > 0 or current_gp > 0:
        return event_counts

    scored_total = max(
        0,
        int(event_counts.get("goal", 0) or 0) + int(event_counts.get("rigore_segnato", 0) or 0),
    )
    if scored_total <= 0:
        return event_counts

    gv_bonus = float(bonus_map.get("gol_vittoria", 0.0))
    gp_bonus = float(bonus_map.get("gol_pareggio", 0.0))
    if gv_bonus <= 0.0 and gp_bonus <= 0.0:
        return event_counts

    base_total = float(vote_value)
    for field in LIVE_EVENT_FIELDS:
        if field in {"gol_vittoria", "gol_pareggio"}:
            continue
        count = max(0, int(event_counts.get(field, 0) or 0))
        if count <= 0:
            continue
        base_total += float(bonus_map.get(field, 0.0)) * count

    target_extra = float(fantavote_value) - base_total
    if target_extra <= 0.10:
        return event_counts

    best_gv = 0
    best_gp = 0
    best_error = float("inf")
    for gv_count in range(0, scored_total + 1):
        for gp_count in range(0, scored_total - gv_count + 1):
            if gv_count == 0 and gp_count == 0:
                continue
            extra = (float(gv_count) * gv_bonus) + (float(gp_count) * gp_bonus)
            error = abs(extra - target_extra)
            if error + 1e-9 < best_error:
                best_error = error
                best_gv = gv_count
                best_gp = gp_count
                continue
            if abs(error - best_error) <= 1e-9:
                # Prefer fewer inferred events, then prefer GV over GP.
                current_total = best_gv + best_gp
                candidate_total = gv_count + gp_count
                if candidate_total < current_total:
                    best_gv = gv_count
                    best_gp = gp_count
                elif candidate_total == current_total and gv_count > best_gv:
                    best_gv = gv_count
                    best_gp = gp_count

    if best_error > 0.15:
        return event_counts

    inferred = dict(event_counts)
    inferred["gol_vittoria"] = int(best_gv)
    inferred["gol_pareggio"] = int(best_gp)
    return inferred


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

    vice_enabled = bool(captain_cfg.get("vice_captain_enabled", True))
    captain_name = _canonicalize_name(str(item.get("capitano") or item.get("captain") or "").strip())
    vice_name = _canonicalize_name(
        str(
            item.get("vice_capitano")
            or item.get("vicecaptain")
            or item.get("vice_captain")
            or ""
        ).strip()
    )
    selected_player = ""
    selected_vote = None

    candidates: List[str] = [captain_name]
    if vice_enabled:
        candidates.append(vice_name)

    for candidate in candidates:
        if not candidate:
            continue
        payload = _player_score_lookup(player_scores, candidate)
        vote_value = _safe_number(payload.get("vote")) if isinstance(payload, dict) else None
        if vote_value is not None:
            selected_player = candidate
            selected_vote = vote_value
            break

    if selected_vote is None:
        fallback_value = _safe_number(item.get("mod_capitano_precalc"))
        if fallback_value is not None:
            return {
                "value": round(float(fallback_value), 2),
                "captain_player": "",
                "captain_vote": None,
            }
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

        appkey_scores_raw = item.get("appkey_scores")
        appkey_scores = appkey_scores_raw if isinstance(appkey_scores_raw, dict) else {}
        player_scores: Dict[str, Dict[str, object]] = {}
        for player_name in sorted(players_set, key=lambda value: normalize_name(value)):
            resolved = _resolve_live_player_score(player_name, context)
            fallback = appkey_scores.get(normalize_name(player_name))
            if isinstance(fallback, dict):
                resolved_source = str(resolved.get("source") or "").strip().lower() if isinstance(resolved, dict) else ""
                fallback_vote = _safe_number(fallback.get("vote"))
                fallback_fantavote = _safe_number(fallback.get("fantavote"))
                should_use_fallback = (
                    resolved_source in {"default", "six_politico", ""}
                    or _safe_number(resolved.get("fantavote")) is None
                )
                if should_use_fallback and (fallback_vote is not None or fallback_fantavote is not None):
                    resolved = {
                        **resolved,
                        "vote": fallback_vote,
                        "fantavote": fallback_fantavote,
                        "vote_label": _format_live_number(fallback_vote),
                        "fantavote_label": _format_live_number(fallback_fantavote),
                        "source": "appkey",
                    }
            player_scores[player_name] = resolved

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
        captain_player = str(captain_modifier.get("captain_player") or "").strip()
        captain_vote = _safe_number(captain_modifier.get("captain_vote"))

        # Final fallback for sparse xlsx payloads: when captain data is missing
        # and no captain modifier can be resolved, infer it from the official
        # lineup total if available.
        if not captain_player and captain_vote is None and abs(mod_capitano) < 0.0001 and base_count:
            total_precalc = _safe_number(item.get("totale_precalc"))
            if total_precalc is not None:
                inferred_mod_cap = round(float(total_precalc) - (base_total + mod_difesa), 2)
                if abs(inferred_mod_cap) <= 2.0:
                    mod_capitano = inferred_mod_cap

        live_total = round(base_total + mod_difesa + mod_capitano, 2) if base_count else None
        base_total_value = round(base_total, 2) if base_count else None
        total_source = "computed" if base_count else ""
        if live_total is None:
            total_precalc = _safe_number(item.get("totale_precalc"))
            if total_precalc is not None:
                live_total = round(float(total_precalc), 2)
                total_source = "precalc"

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
        item["totale_source"] = total_source
        item["totale_live_label"] = _format_live_number(live_total)
        item["live_components"] = {
            "base": base_total_value,
            "mod_difesa": round(mod_difesa, 2),
            "mod_capitano": round(mod_capitano, 2),
            "totale_live": live_total,
            "difesa_average_vote": defense_modifier.get("average_vote"),
            "captain_player": captain_player,
            "captain_vote": captain_vote,
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


def _context_html_candidates() -> List[Path]:
    candidates: List[Path] = []
    seen: Set[str] = set()

    def _register(path: Path) -> None:
        if not path.exists() or not path.is_file():
            return
        key = str(path.resolve())
        if key in seen:
            return
        seen.add(key)
        candidates.append(path)

    for fixed_path in REAL_FORMATIONS_CONTEXT_HTML_CANDIDATES:
        _register(fixed_path)

    if REAL_FORMATIONS_TMP_DIR.exists() and REAL_FORMATIONS_TMP_DIR.is_dir():
        try:
            dynamic_paths = sorted(
                [p for p in REAL_FORMATIONS_TMP_DIR.glob(REAL_FORMATIONS_CONTEXT_HTML_GLOB) if p.is_file()],
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
        except Exception:
            logger.debug("Failed to list context HTML candidates", exc_info=True)
            dynamic_paths = []
        for dynamic_path in dynamic_paths:
            _register(dynamic_path)

    return candidates


def _refresh_formazioni_context_html_live() -> Optional[Path]:
    if not LEGHE_ALIAS:
        return None

    target_path = REAL_FORMATIONS_TMP_DIR / "formazioni_page.html"
    now_ts = datetime.utcnow().timestamp()
    last_ts = float(_FORMAZIONI_REMOTE_REFRESH_CACHE.get("last_ts", 0.0) or 0.0)
    # Avoid hammering Leghe when many clients poll /data/formazioni.
    if now_ts - last_ts < 45 and target_path.exists():
        return target_path

    try:
        username = LEGHE_USERNAME if LEGHE_USERNAME and LEGHE_PASSWORD else None
        password = LEGHE_PASSWORD if LEGHE_USERNAME and LEGHE_PASSWORD else None
        refresh_formazioni_context_from_leghe(
            alias=LEGHE_ALIAS,
            out_path=target_path,
            username=username,
            password=password,
            out_xlsx_path=DATA_DIR / "incoming" / "formazioni" / "formazioni.xlsx",
            competition_id=LEGHE_COMPETITION_ID,
            competition_name=LEGHE_COMPETITION_NAME,
            formations_matchday=LEGHE_FORMATIONS_MATCHDAY,
        )
    except Exception:
        logger.warning("Formazioni live refresh failed, using local cache", exc_info=True)
    finally:
        _FORMAZIONI_REMOTE_REFRESH_CACHE["last_ts"] = now_ts

    return target_path if target_path.exists() else None


def _refresh_formazioni_xlsx_for_round(round_value: Optional[int]) -> Optional[Path]:
    requested_round = _parse_int(round_value)
    if (
        requested_round is None
        or requested_round <= 0
        or not LEGHE_ALIAS
        or not LEGHE_USERNAME
        or not LEGHE_PASSWORD
    ):
        return None

    out_path = DATA_DIR / "incoming" / "formazioni" / "formazioni.xlsx"
    cache_key = f"formazioni_xlsx_round_{int(requested_round)}"
    now_ts = datetime.utcnow().timestamp()
    last_ts = float(_FORMAZIONI_REMOTE_REFRESH_CACHE.get(cache_key, 0.0) or 0.0)
    if now_ts - last_ts < 180 and out_path.exists():
        return out_path

    try:
        result = refresh_formazioni_context_from_leghe(
            alias=LEGHE_ALIAS,
            out_path=REAL_FORMATIONS_TMP_DIR / "formazioni_page.html",
            username=LEGHE_USERNAME,
            password=LEGHE_PASSWORD,
            out_xlsx_path=out_path,
            competition_id=LEGHE_COMPETITION_ID,
            competition_name=LEGHE_COMPETITION_NAME,
            formations_matchday=int(requested_round),
        )
        xlsx_meta = result.get("formazioni_xlsx") if isinstance(result, dict) else {}
        xlsx_meta = xlsx_meta if isinstance(xlsx_meta, dict) else {}
        selected_round = _parse_int(xlsx_meta.get("selected_matchday"))
        rows_count = _parse_int(xlsx_meta.get("rows")) or 0
        if out_path.exists() and rows_count > 0 and (
            selected_round is None or int(selected_round) == int(requested_round)
        ):
            return out_path
    except Exception:
        logger.debug("Unable to refresh formazioni xlsx for round %s", requested_round, exc_info=True)
    finally:
        _FORMAZIONI_REMOTE_REFRESH_CACHE[cache_key] = now_ts

    return out_path if out_path.exists() else None


def _extract_current_turn_from_formazioni_context_html() -> Optional[int]:
    pattern = re.compile(r'currentTurn\s*:\s*"?(?P<turn>\d+)"?', re.IGNORECASE)
    for candidate in _context_html_candidates():
        try:
            source = candidate.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if not source:
            continue
        match = pattern.search(source)
        if match is None:
            continue
        parsed = _parse_int(match.group("turn"))
        if parsed is not None and parsed > 0:
            return int(parsed)
    return None


def _normalize_service_player_entry(raw_player: object, order: int) -> Optional[Dict[str, object]]:
    if not isinstance(raw_player, dict):
        return None

    player_name = _canonicalize_name(
        str(
            raw_player.get("n")
            or raw_player.get("nome")
            or raw_player.get("name")
            or raw_player.get("player")
            or raw_player.get("giocatore")
            or ""
        )
    )
    if not player_name:
        return None

    role_raw = raw_player.get("r")
    if role_raw in (None, ""):
        role_raw = raw_player.get("ruolo")
    if role_raw in (None, ""):
        role_raw = raw_player.get("role")
    role_value = _role_from_text(role_raw)

    player_id_raw = (
        raw_player.get("id")
        or raw_player.get("i")
        or raw_player.get("id_calciatore")
        or raw_player.get("player_id")
        or f"p{order}"
    )
    player_id = str(player_id_raw).strip() or f"p{order}"

    normalized = {
        "id": player_id,
        "i": player_id,
        "n": player_name,
        "r": role_value or "",
    }
    bonus_raw = raw_player.get("b")
    if isinstance(bonus_raw, list):
        normalized["b"] = bonus_raw
    return normalized


def _normalize_service_squad(
    raw_squad: object,
    *,
    fallback_team_id: Optional[int] = None,
    valid_team_ids: Optional[Set[int]] = None,
) -> Optional[Dict[str, object]]:
    if not isinstance(raw_squad, dict):
        return None

    team_id = _parse_int(
        raw_squad.get("id")
        or raw_squad.get("id_squadra")
        or raw_squad.get("idTeam")
        or raw_squad.get("team_id")
    )
    if team_id is None:
        team_id = fallback_team_id
    if team_id is None:
        return None
    if valid_team_ids and int(team_id) not in valid_team_ids:
        return None

    players_raw: List[object] = []
    for key in ("pl", "players", "calciatori", "giocatori", "roster", "lineup", "titolari"):
        value = raw_squad.get(key)
        if isinstance(value, list):
            players_raw.extend(value)
            if key != "titolari":
                break
    for key in ("titolari", "panchina", "reserves", "bench"):
        value = raw_squad.get(key)
        if isinstance(value, list):
            players_raw.extend(value)

    normalized_players: List[Dict[str, object]] = []
    seen_players: Set[str] = set()
    for idx, player_raw in enumerate(players_raw):
        normalized_player = _normalize_service_player_entry(player_raw, idx)
        if not normalized_player:
            continue
        dedupe_key = normalize_name(str(normalized_player.get("n") or ""))
        if dedupe_key and dedupe_key in seen_players:
            continue
        if dedupe_key:
            seen_players.add(dedupe_key)
        normalized_players.append(normalized_player)

    if not normalized_players:
        return None

    module_raw = raw_squad.get("m")
    if module_raw in (None, ""):
        module_raw = raw_squad.get("modulo")
    if module_raw in (None, ""):
        module_raw = raw_squad.get("schema")

    captain_raw = raw_squad.get("cap")
    if captain_raw in (None, ""):
        captain_raw = raw_squad.get("captain")
    if captain_raw in (None, ""):
        captain_raw = raw_squad.get("capitano")

    strength_raw = raw_squad.get("t")
    if strength_raw in (None, ""):
        strength_raw = raw_squad.get("forza_titolari")

    normalized_squad: Dict[str, object] = {
        "id": int(team_id),
        "pl": normalized_players,
    }
    if module_raw not in (None, ""):
        normalized_squad["m"] = module_raw
    if captain_raw not in (None, ""):
        normalized_squad["cap"] = captain_raw
    parsed_strength = _parse_float(strength_raw)
    if parsed_strength is not None:
        normalized_squad["t"] = parsed_strength
    return normalized_squad


def _build_formazioni_payload_from_service_response(
    response_payload: Dict[str, object],
    *,
    round_hint: Optional[int],
) -> Optional[Dict[str, object]]:
    if not isinstance(response_payload, dict):
        return None

    data_section = response_payload.get("data")
    if isinstance(data_section, dict) and isinstance(data_section.get("formazioni"), list):
        return response_payload
    if isinstance(response_payload.get("formazioni"), list):
        return {
            "data": {
                "giornataLega": _parse_int(round_hint),
                "formazioni": response_payload.get("formazioni"),
            }
        }

    parsed_round = _parse_int(round_hint)
    for container in (response_payload, data_section if isinstance(data_section, dict) else {}):
        if not isinstance(container, dict):
            continue
        parsed_round = _parse_int(
            container.get("giornataLega")
            or container.get("giornata_lega")
            or container.get("giornata")
            or container.get("round")
            or container.get("turno")
            or parsed_round
        )
        if parsed_round is not None:
            break

    queue: List[Tuple[object, Optional[int]]] = [(response_payload, None)]
    visited: Set[Tuple[int, Optional[int]]] = set()
    normalized_squads: List[Dict[str, object]] = []
    seen_team_ids: Set[int] = set()
    valid_team_ids = set(_load_team_id_position_index_from_formazioni_html().keys())

    while queue:
        node, inherited_team_id = queue.pop()
        if isinstance(node, list):
            for value in node:
                queue.append((value, inherited_team_id))
            continue
        if not isinstance(node, dict):
            continue
        marker = (id(node), inherited_team_id)
        if marker in visited:
            continue
        visited.add(marker)

        fallback_team_id = _parse_int(
            node.get("id")
            or node.get("id_squadra")
            or node.get("idTeam")
            or node.get("team_id")
            or inherited_team_id
        )
        normalized_squad = _normalize_service_squad(
            node,
            fallback_team_id=fallback_team_id,
            valid_team_ids=valid_team_ids,
        )
        if normalized_squad:
            team_id = _parse_int(normalized_squad.get("id"))
            if team_id is not None and team_id not in seen_team_ids:
                seen_team_ids.add(team_id)
                normalized_squads.append(normalized_squad)

        for value in node.values():
            if isinstance(value, (dict, list)):
                queue.append((value, fallback_team_id))

    if not normalized_squads:
        return None

    selected_round = _parse_int(parsed_round)
    formation_entry: Dict[str, object] = {"sq": normalized_squads}
    if selected_round is not None:
        formation_entry["giornata"] = int(selected_round)

    payload: Dict[str, object] = {"data": {"formazioni": [formation_entry]}}
    if selected_round is not None:
        payload["data"]["giornataLega"] = int(selected_round)
    return payload


def _decode_appkey_payload_token(token: str) -> Optional[Dict[str, object]]:
    raw_token = str(token or "").strip()
    if not raw_token:
        return None

    token_candidates = [raw_token, raw_token.replace("-", "+").replace("_", "/")]
    tried: Set[str] = set()
    for candidate in token_candidates:
        if candidate in tried:
            continue
        tried.add(candidate)

        padded = candidate + ("=" * ((4 - (len(candidate) % 4)) % 4))
        try:
            decoded = base64.b64decode(padded)
        except Exception:
            logger.debug("Base64 decode failed for appkey token candidate")
            continue

        payload: object
        try:
            payload = json.loads(decoded.decode("utf-8"))
        except Exception:
            try:
                payload = json.loads(decoded.decode("utf-8-sig"))
            except Exception:
                logger.debug("JSON decode failed for appkey token candidate")
                continue

        if not isinstance(payload, dict):
            continue
        data_section = payload.get("data")
        if isinstance(data_section, dict) and isinstance(data_section.get("formazioni"), list):
            return payload

    return None


def _extract_lt_appkey_payloads_from_html(source: str) -> List[Dict[str, object]]:
    if not source:
        return []

    pattern = re.compile(
        r"__\.s\(\s*['\"]lt['\"]\s*,\s*__\.dp\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\)",
        flags=re.IGNORECASE,
    )
    payloads: List[Dict[str, object]] = []
    seen_tokens: Set[str] = set()

    for match in pattern.finditer(source):
        token = str(match.group(1) or "").strip()
        if not token or token in seen_tokens:
            continue
        seen_tokens.add(token)

        decoded = _decode_appkey_payload_token(token)
        if decoded is None:
            continue
        payloads.append(decoded)

    return payloads


def _extract_formazioni_context_alias(source: str) -> Optional[str]:
    if not source:
        return None

    patterns = (
        r"\balias\s*:\s*['\"]([^'\"]+)['\"]",
        r"\"alias\"\s*:\s*\"([^\"]+)\"",
    )
    for pattern in patterns:
        match = re.search(pattern, source, flags=re.IGNORECASE)
        if match is None:
            continue
        alias = str(match.group(1) or "").strip().strip("/")
        if alias:
            return alias
    return None


def _extract_formazioni_context_app_key(source: str) -> Optional[str]:
    if not source:
        return None
    match = re.search(r"authAppKey\s*:\s*['\"]([^'\"]+)['\"]", source, flags=re.IGNORECASE)
    if match is None:
        return None
    app_key = str(match.group(1) or "").strip()
    return app_key or None


def _extract_formazioni_tmp_entries_from_html(source: str) -> List[Dict[str, object]]:
    if not source:
        return []

    pattern = re.compile(
        r"__\.s\(\s*['\"]tmp['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)",
        flags=re.IGNORECASE,
    )
    entries: List[Dict[str, object]] = []
    seen_raw: Set[str] = set()

    for match in pattern.finditer(source):
        raw = str(match.group(1) or "").strip()
        if not raw or raw in seen_raw:
            continue
        seen_raw.add(raw)

        parts = [segment.strip() for segment in raw.split("|")]
        if len(parts) < 3:
            continue

        round_value = _parse_int(parts[0])
        timestamp = _parse_int(parts[1])
        competition_id = _parse_int(parts[2])
        last_comp_round = _parse_int(parts[3]) if len(parts) > 3 else None
        competition_type = _parse_int(parts[4]) if len(parts) > 4 else None
        team_ids_raw = parts[5] if len(parts) > 5 else ""
        team_ids = []
        for token in team_ids_raw.split(","):
            parsed_team = _parse_int(token)
            if parsed_team is not None and parsed_team > 0:
                team_ids.append(int(parsed_team))

        entries.append(
            {
                "round": round_value,
                "timestamp": timestamp,
                "competition_id": competition_id,
                "last_comp_round": last_comp_round,
                "competition_type": competition_type,
                "team_ids": team_ids,
            }
        )

    entries.sort(
        key=lambda entry: int(_parse_int(entry.get("timestamp")) or 0),
        reverse=True,
    )
    return entries


def _download_formazioni_pagina_payload(
    *,
    alias: str,
    app_key: str,
    competition_id: int,
    round_value: int,
    timestamp: int,
) -> Optional[Dict[str, object]]:
    if not alias or not app_key:
        return None

    round_num = _parse_int(round_value)
    timestamp_num = _parse_int(timestamp)
    competition_num = _parse_int(competition_id)
    if round_num is None or timestamp_num is None or competition_num is None:
        return None

    url = (
        f"{LEGHE_BASE_URL}/servizi/V1_LegheFormazioni/Pagina"
        f"?id_comp={int(competition_num)}"
        f"&r={int(round_num)}"
        f"&f={int(round_num)}_{int(timestamp_num)}.json"
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.7,en;q=0.6",
        "app_key": str(app_key),
        "Referer": f"{LEGHE_BASE_URL}/{alias}/formazioni/{int(round_num)}",
        "X-Requested-With": "XMLHttpRequest",
    }
    req = Request(url, headers=headers, method="GET")
    try:
        with urlopen(req, timeout=30) as response:
            payload_bytes = response.read()
    except (HTTPError, URLError, TimeoutError, ValueError):
        return None

    try:
        parsed = json.loads(payload_bytes.decode("utf-8", errors="replace"))
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None

    return _build_formazioni_payload_from_service_response(parsed, round_hint=round_num)


def _refresh_formazioni_appkey_from_context_tmp(
    source: str,
    round_value: Optional[int],
) -> Optional[Path]:
    requested_round = _parse_int(round_value)
    if not source:
        return None

    app_key = _extract_formazioni_context_app_key(source)
    alias = str(LEGHE_ALIAS or _extract_formazioni_context_alias(source) or "").strip()
    if not app_key or not alias:
        return None

    entries = _extract_formazioni_tmp_entries_from_html(source)
    if not entries and requested_round is None:
        return None

    now_ts = datetime.utcnow().timestamp()

    for entry in entries:
        round_num = _parse_int(entry.get("round"))
        timestamp_num = _parse_int(entry.get("timestamp"))
        competition_id = _parse_int(entry.get("competition_id"))
        if round_num is None or timestamp_num is None or competition_id is None:
            continue
        if requested_round is not None and int(round_num) != int(requested_round):
            continue

        cache_key = f"context_pagina_{int(competition_id)}_{int(round_num)}_{int(timestamp_num)}"
        cached_round_path = REAL_FORMATIONS_TMP_DIR / f"formazioni_{int(round_num)}_appkey.json"
        last_ts = float(_FORMAZIONI_REMOTE_REFRESH_CACHE.get(cache_key, 0.0) or 0.0)
        if now_ts - last_ts < 90:
            if cached_round_path.exists():
                return cached_round_path
            continue

        payload: Optional[Dict[str, object]] = None
        try:
            payload = _download_formazioni_pagina_payload(
                alias=alias,
                app_key=app_key,
                competition_id=int(competition_id),
                round_value=int(round_num),
                timestamp=int(timestamp_num),
            )
        finally:
            _FORMAZIONI_REMOTE_REFRESH_CACHE[cache_key] = now_ts

        if not payload:
            continue

        try:
            written_path = _write_formazioni_appkey_payload(payload, int(round_num))
        except Exception:
            continue
        return written_path

    # Historical rounds might not be listed in "tmp" entries when current matchday
    # has already advanced. Try the requested round explicitly using recent entries.
    if requested_round is not None:
        explicit_candidates: List[Tuple[int, int]] = []
        for entry in entries:
            timestamp_num = _parse_int(entry.get("timestamp"))
            competition_id = _parse_int(entry.get("competition_id"))
            if timestamp_num is None or competition_id is None:
                continue
            explicit_candidates.append((int(timestamp_num), int(competition_id)))
        if not explicit_candidates:
            competition_literal = _extract_js_object_literal(source, "currentCompetition")
            if competition_literal:
                try:
                    parsed_competition = json.loads(competition_literal)
                except Exception:
                    parsed_competition = {}
                if isinstance(parsed_competition, dict):
                    competition_id = _parse_int(parsed_competition.get("id"))
                    timestamp_num = _parse_int(parsed_competition.get("state"))
                    if competition_id is not None and timestamp_num is not None:
                        explicit_candidates.append((int(timestamp_num), int(competition_id)))
        if not explicit_candidates and LEGHE_COMPETITION_ID is not None:
            fallback_timestamp = int(datetime.utcnow().timestamp() * 1000)
            explicit_candidates.append((fallback_timestamp, int(LEGHE_COMPETITION_ID)))
        explicit_candidates.sort(reverse=True)

        cached_round_path = REAL_FORMATIONS_TMP_DIR / f"formazioni_{int(requested_round)}_appkey.json"
        for timestamp_num, competition_id in explicit_candidates[:5]:
            cache_key = (
                f"context_pagina_forced_{int(competition_id)}_"
                f"{int(requested_round)}_{int(timestamp_num)}"
            )
            last_ts = float(_FORMAZIONI_REMOTE_REFRESH_CACHE.get(cache_key, 0.0) or 0.0)
            if now_ts - last_ts < 90:
                if cached_round_path.exists():
                    return cached_round_path
                continue

            payload: Optional[Dict[str, object]] = None
            try:
                payload = _download_formazioni_pagina_payload(
                    alias=alias,
                    app_key=app_key,
                    competition_id=int(competition_id),
                    round_value=int(requested_round),
                    timestamp=int(timestamp_num),
                )
            finally:
                _FORMAZIONI_REMOTE_REFRESH_CACHE[cache_key] = now_ts

            if not payload:
                continue
            payload_rounds = _extract_appkey_payload_rounds(payload)
            if payload_rounds and int(requested_round) not in payload_rounds:
                continue
            try:
                written_path = _write_formazioni_appkey_payload(payload, int(requested_round))
            except Exception:
                continue
            return written_path

    return None


def _extract_appkey_payload_rounds(payload: Dict[str, object]) -> List[int]:
    rounds: Set[int] = set()
    data_section = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data_section, dict):
        return []

    global_round = _parse_int(data_section.get("giornataLega") or payload.get("giornataLega"))
    if global_round is not None:
        rounds.add(global_round)

    formations = data_section.get("formazioni")
    if not isinstance(formations, list):
        return sorted(rounds)

    for formation in formations:
        if not isinstance(formation, dict):
            continue
        round_value = _parse_int(formation.get("giornata") or formation.get("round") or formation.get("turno"))
        if round_value is not None:
            rounds.add(round_value)
    return sorted(rounds)


def _write_formazioni_appkey_payload(payload: Dict[str, object], round_value: Optional[int]) -> Path:
    round_num = _parse_int(round_value)
    filename = f"formazioni_{round_num}_appkey.json" if round_num is not None else "formazioni_appkey.json"
    path = REAL_FORMATIONS_TMP_DIR / filename
    serialized = json.dumps(payload, ensure_ascii=False, indent=4) + "\n"

    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = path.read_text(encoding="utf-8-sig")
        if existing == serialized:
            path.touch()
            return path
    except Exception:
        pass

    path.write_text(serialized, encoding="utf-8")
    return path


def _refresh_formazioni_appkey_from_context_html(round_value: Optional[int]) -> Optional[Path]:
    requested_round = _parse_int(round_value)
    refreshed_path: Optional[Path] = None
    refreshed_mtime = -1.0

    for candidate in _context_html_candidates():
        try:
            source = candidate.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        if not source:
            continue

        payloads = _extract_lt_appkey_payloads_from_html(source)
        matched_requested_round = False

        for payload in payloads:
            payload_rounds = _extract_appkey_payload_rounds(payload)
            if requested_round is not None and payload_rounds and requested_round not in payload_rounds:
                continue
            if requested_round is not None and payload_rounds and requested_round in payload_rounds:
                matched_requested_round = True

            if requested_round is not None:
                selected_round = requested_round
            elif payload_rounds:
                selected_round = payload_rounds[-1]
            else:
                selected_round = None

            try:
                written_path = _write_formazioni_appkey_payload(payload, selected_round)
            except Exception:
                continue

            try:
                current_mtime = written_path.stat().st_mtime
            except Exception:
                current_mtime = -1.0

            if current_mtime >= refreshed_mtime:
                refreshed_path = written_path
                refreshed_mtime = current_mtime

        should_try_tmp = not payloads or (
            requested_round is not None and not matched_requested_round
        )
        if not should_try_tmp:
            continue

        tmp_path = _refresh_formazioni_appkey_from_context_tmp(source, requested_round)
        if tmp_path is None:
            continue
        try:
            tmp_mtime = tmp_path.stat().st_mtime
        except Exception:
            tmp_mtime = -1.0
        if tmp_mtime >= refreshed_mtime:
            refreshed_path = tmp_path
            refreshed_mtime = tmp_mtime

    return refreshed_path


def _refresh_formazioni_appkey_from_service(round_value: Optional[int]) -> Optional[Path]:
    requested_round = _parse_int(round_value)
    if not LEGHE_ALIAS or not LEGHE_USERNAME or not LEGHE_PASSWORD:
        return None

    cache_key = f"service_round_{int(requested_round) if requested_round is not None else 0}"
    now_ts = datetime.utcnow().timestamp()
    last_ts = float(_FORMAZIONI_REMOTE_REFRESH_CACHE.get(cache_key, 0.0) or 0.0)
    if now_ts - last_ts < 90:
        if requested_round is not None:
            cached_round_path = REAL_FORMATIONS_TMP_DIR / f"formazioni_{int(requested_round)}_appkey.json"
            if cached_round_path.exists():
                return cached_round_path
        return None

    team_ids = sorted(_load_team_id_position_index_from_formazioni_html().keys())
    service_payloads: List[Dict[str, object]] = []

    try:
        response = fetch_leghe_formazioni_service_payloads(
            alias=LEGHE_ALIAS,
            username=LEGHE_USERNAME,
            password=LEGHE_PASSWORD,
            competition_id=LEGHE_COMPETITION_ID,
            matchday=requested_round,
            team_ids=team_ids,
        )
    except Exception:
        logger.warning("Formazioni service fetch failed for round %s", requested_round, exc_info=True)
        _FORMAZIONI_REMOTE_REFRESH_CACHE[cache_key] = now_ts
        return None

    raw_payloads = response.get("payloads")
    if isinstance(raw_payloads, list):
        for item in raw_payloads:
            if not isinstance(item, dict):
                continue
            payload = item.get("payload")
            if isinstance(payload, dict):
                service_payloads.append(payload)

    if not service_payloads:
        _FORMAZIONI_REMOTE_REFRESH_CACHE[cache_key] = now_ts
        return None

    fallback_round = _parse_int(response.get("matchday"))
    refreshed_path: Optional[Path] = None
    refreshed_score = -1

    for payload in service_payloads:
        normalized = _build_formazioni_payload_from_service_response(
            payload,
            round_hint=requested_round if requested_round is not None else fallback_round,
        )
        if not normalized:
            continue

        payload_rounds = _extract_appkey_payload_rounds(normalized)
        if requested_round is not None and payload_rounds and requested_round not in payload_rounds:
            continue

        if requested_round is not None:
            selected_round = requested_round
        elif payload_rounds:
            selected_round = payload_rounds[-1]
        else:
            selected_round = fallback_round

        try:
            written_path = _write_formazioni_appkey_payload(normalized, selected_round)
        except Exception:
            continue

        formations = normalized.get("data", {}).get("formazioni") if isinstance(normalized.get("data"), dict) else []
        squad_count = 0
        if isinstance(formations, list):
            for formation in formations:
                if not isinstance(formation, dict):
                    continue
                squads = formation.get("sq")
                if isinstance(squads, list):
                    squad_count += len([s for s in squads if isinstance(s, dict)])

        score = int(squad_count)
        if requested_round is not None and selected_round == requested_round:
            score += 1000
        if score >= refreshed_score:
            refreshed_path = written_path
            refreshed_score = score

    _FORMAZIONI_REMOTE_REFRESH_CACHE[cache_key] = now_ts
    return refreshed_path


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

        # Fallback when currentCompetition is not available: infer the ordering
        # from tmp entries (team_ids array is typically aligned to standings pos).
        tmp_entries = _extract_formazioni_tmp_entries_from_html(raw)
        for entry in tmp_entries:
            raw_team_ids = entry.get("team_ids")
            if not isinstance(raw_team_ids, list):
                continue
            team_ids: List[int] = []
            seen: Set[int] = set()
            for raw_id in raw_team_ids:
                parsed_id = _parse_int(raw_id)
                if parsed_id is None or parsed_id <= 0:
                    continue
                if parsed_id in seen:
                    continue
                seen.add(parsed_id)
                team_ids.append(int(parsed_id))
            if len(team_ids) < 10:
                continue
            inferred_index = {team_id: pos for pos, team_id in enumerate(team_ids, start=1)}
            if inferred_index:
                return inferred_index
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
    player_names: Set[str] = set()
    for player in players:
        if not isinstance(player, dict):
            continue
        player_name = _canonicalize_name(str(player.get("n") or ""))
        if not player_name:
            continue
        player_names.add(player_name)
        for key in ("id", "i", "id_s", "tk"):
            value = str(player.get(key) or "").strip()
            if value:
                by_id[value] = player_name

    tokens: List[str] = []
    if isinstance(cap_raw, (list, tuple, set)):
        tokens = [str(value or "").strip() for value in cap_raw]
    elif isinstance(cap_raw, dict):
        ordered_keys = (
            "captain",
            "capitano",
            "cap",
            "c",
            "vice_captain",
            "vicecapitano",
            "vice",
            "vc",
            "ids",
            "id",
        )
        for key in ordered_keys:
            value = cap_raw.get(key)
            if isinstance(value, (list, tuple, set)):
                tokens.extend(str(item or "").strip() for item in value)
            elif value not in (None, ""):
                tokens.append(str(value).strip())
    else:
        raw_text = str(cap_raw or "").strip()
        if raw_text:
            if raw_text.startswith("[") or raw_text.startswith("{"):
                try:
                    decoded = json.loads(raw_text)
                except Exception:
                    decoded = None
                if isinstance(decoded, (list, tuple, set)):
                    tokens.extend(str(item or "").strip() for item in decoded)
                elif isinstance(decoded, dict):
                    for key in (
                        "captain",
                        "capitano",
                        "cap",
                        "c",
                        "vice_captain",
                        "vicecapitano",
                        "vice",
                        "vc",
                        "ids",
                        "id",
                    ):
                        value = decoded.get(key)
                        if isinstance(value, (list, tuple, set)):
                            tokens.extend(str(item or "").strip() for item in value)
                        elif value not in (None, ""):
                            tokens.append(str(value).strip())
            if not tokens:
                tokens.extend(part.strip() for part in re.split(r"[;,|/]+", raw_text) if part.strip())
            if not tokens:
                tokens.append(raw_text)

    cleaned_tokens: List[str] = []
    numeric_tokens: List[str] = []
    for token in tokens:
        cleaned = re.sub(r"^[\s\[\]\(\)\{\}\"']+|[\s\[\]\(\)\{\}\"']+$", "", str(token or ""))
        if not cleaned:
            continue
        cleaned_tokens.append(cleaned)
        for numeric in re.findall(r"\d+", cleaned):
            numeric_tokens.append(str(int(numeric)))

    names: List[str] = []
    seen_tokens: Set[str] = set()
    for current in [*cleaned_tokens, *numeric_tokens]:
        cleaned_current = str(current or "").strip()
        if not cleaned_current:
            continue
        dedupe_key = cleaned_current.lower()
        if dedupe_key in seen_tokens:
            continue
        seen_tokens.add(dedupe_key)

        player_name = by_id.get(cleaned_current)
        if player_name is None and cleaned_current.isdigit():
            player_name = by_id.get(str(int(cleaned_current)))
        if not player_name:
            canonical_token = _canonicalize_name(cleaned_current)
            if canonical_token in player_names:
                player_name = canonical_token
        if not player_name:
            continue
        if player_name not in names:
            names.append(player_name)
        if len(names) >= 2:
            break

    captain = names[0] if names else ""
    vice = names[1] if len(names) > 1 else ""
    return captain, vice


def _appkey_bonus_event_counts(
    bonus_raw: object,
    bonus_indexes: Dict[str, int],
) -> Dict[str, int]:
    counts = {"gol_vittoria": 0, "gol_pareggio": 0}
    if not isinstance(bonus_raw, list):
        return counts

    for field in ("gol_vittoria", "gol_pareggio"):
        index = _parse_int(bonus_indexes.get(field))
        if index is None or index < 0 or index >= len(bonus_raw):
            continue
        parsed = _parse_int(bonus_raw[index])
        counts[field] = max(0, int(parsed or 0))
    return counts


def _load_live_decisive_badges_from_appkey(
    round_value: Optional[int],
    standings_index: Optional[Dict[str, Dict[str, object]]],
    regulation: Dict[str, object],
) -> tuple[Dict[str, Dict[str, int]], Optional[Path]]:
    source_path = _refresh_formazioni_appkey_from_context_html(round_value) or _latest_formazioni_appkey_path()
    if source_path is None:
        return {}, None

    try:
        payload = json.loads(source_path.read_text(encoding="utf-8-sig"))
    except Exception:
        return {}, source_path

    data_section = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data_section, dict):
        return {}, source_path

    formations = data_section.get("formazioni")
    if not isinstance(formations, list):
        return {}, source_path

    requested_round = _parse_int(round_value)
    global_round = _parse_int(data_section.get("giornataLega") or payload.get("giornataLega"))
    bonus_indexes = _reg_appkey_bonus_indexes(regulation)
    _ = standings_index

    decisive_map: Dict[str, Dict[str, int]] = {}
    for formation in formations:
        if not isinstance(formation, dict):
            continue
        formation_round = _parse_int(
            formation.get("giornata") or formation.get("round") or formation.get("turno")
        )
        if formation_round is None:
            formation_round = global_round
        if requested_round is not None and formation_round is not None and formation_round != requested_round:
            continue

        squads = formation.get("sq")
        if not isinstance(squads, list):
            continue

        for squad in squads:
            if not isinstance(squad, dict):
                continue

            players = squad.get("pl")
            if not isinstance(players, list):
                continue
            for player in players:
                if not isinstance(player, dict):
                    continue
                player_name = _canonicalize_name(str(player.get("n") or ""))
                player_key = normalize_name(player_name)
                if not player_key:
                    continue
                event_counts = _appkey_bonus_event_counts(player.get("b"), bonus_indexes)
                gv_count = max(0, int(event_counts.get("gol_vittoria", 0) or 0))
                gp_count = max(0, int(event_counts.get("gol_pareggio", 0) or 0))
                if gv_count <= 0 and gp_count <= 0:
                    continue
                key = player_key
                current = decisive_map.get(key)
                if current is None:
                    decisive_map[key] = {
                        "gol_vittoria": gv_count,
                        "gol_pareggio": gp_count,
                    }
                else:
                    current["gol_vittoria"] = max(int(current.get("gol_vittoria", 0)), gv_count)
                    current["gol_pareggio"] = max(int(current.get("gol_pareggio", 0)), gp_count)

    return decisive_map, source_path


def _overlay_decisive_badges_from_appkey(
    rows: List[Dict[str, object]],
    decisive_map: Dict[str, Dict[str, int]],
) -> int:
    if not rows or not decisive_map:
        return 0

    player_occurrences: Dict[str, int] = defaultdict(int)
    for row in rows:
        player_key = normalize_name(str(row.get("player") or ""))
        if player_key:
            player_occurrences[player_key] += 1

    applied = 0
    for row in rows:
        player_key = normalize_name(str(row.get("player") or ""))
        if not player_key:
            continue
        # Avoid bad matches for omonimi across different teams.
        if int(player_occurrences.get(player_key, 0)) != 1:
            continue

        appkey_counts = decisive_map.get(player_key)
        if not appkey_counts:
            continue

        current_gv = max(0, int(_parse_int(row.get("gol_vittoria")) or 0))
        current_gp = max(0, int(_parse_int(row.get("gol_pareggio")) or 0))
        merged_gv = max(current_gv, max(0, int(appkey_counts.get("gol_vittoria", 0) or 0)))
        merged_gp = max(current_gp, max(0, int(appkey_counts.get("gol_pareggio", 0) or 0)))

        if merged_gv != current_gv or merged_gp != current_gp:
            row["gol_vittoria"] = merged_gv
            row["gol_pareggio"] = merged_gp
            applied += 1

    return applied


def _overlay_decisive_badges_from_round_results(
    rows: List[Dict[str, object]],
    *,
    round_value: Optional[int],
    season_slug: Optional[str],
    club_index: Dict[str, str],
) -> int:
    target_round = _parse_int(round_value)
    if target_round is None or target_round <= 0:
        return 0
    if not rows:
        return 0

    applied = 0
    timeline_badges = _load_round_decisive_badges_from_calendar_pages(
        round_value=target_round,
        season_slug=_normalize_season_slug(season_slug),
        club_index=club_index,
    )
    if timeline_badges:
        for row in rows:
            team_name = _display_team_name(str(row.get("team") or ""), club_index)
            team_key = normalize_name(team_name)
            player_key = normalize_name(str(row.get("player") or ""))
            if not team_key or not player_key:
                continue

            source_counts = timeline_badges.get((team_key, player_key))
            if not source_counts:
                continue

            current_gv = max(0, int(_parse_int(row.get("gol_vittoria")) or 0))
            current_gp = max(0, int(_parse_int(row.get("gol_pareggio")) or 0))
            merged_gv = max(current_gv, int(source_counts.get("gol_vittoria", 0) or 0))
            merged_gp = max(current_gp, int(source_counts.get("gol_pareggio", 0) or 0))
            if merged_gv != current_gv or merged_gp != current_gp:
                row["gol_vittoria"] = merged_gv
                row["gol_pareggio"] = merged_gp
                applied += 1

    return applied


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


def _parse_appkey_player_scores(players: List[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    out: Dict[str, Dict[str, object]] = {}
    for player in players:
        if not isinstance(player, dict):
            continue
        player_name = _canonicalize_name(str(player.get("n") or player.get("name") or "").strip())
        if not player_name:
            continue
        player_key = normalize_name(player_name)
        vote = _parse_float(player.get("vt") or player.get("vote"))
        fantavote = _parse_float(player.get("fv") or player.get("fantavote"))
        if vote is not None and abs(float(vote)) >= 50:
            vote = None
        if fantavote is not None and abs(float(fantavote)) >= 50:
            fantavote = None
        out[player_key] = {
            "vote": round(float(vote), 2) if vote is not None else None,
            "fantavote": round(float(fantavote), 2) if fantavote is not None else None,
            "source": "appkey",
        }
    return out


def _parse_formazioni_payload_to_items(
    payload: Dict[str, object],
    standings_index: Dict[str, Dict[str, object]],
) -> tuple[List[Dict[str, object]], List[int]]:
    data_section = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data_section, dict):
        return [], []

    formations = data_section.get("formazioni")
    if not isinstance(formations, list):
        return [], []

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
            appkey_scores = _parse_appkey_player_scores(players)
            captain, vice_captain = _extract_appkey_captains(squad.get("cap"), players)
            total_precalc = _parse_float(squad.get("t"))

            item = {
                "pos": resolved_pos if resolved_pos is not None else (standing_pos if standing_pos is not None else 9999),
                "standing_pos": resolved_pos if resolved_pos is not None else standing_pos,
                "team": resolved_team or team_name,
                "modulo": _format_module(squad.get("m")),
                "forza_titolari": _parse_float(squad.get("t")),
                "totale_precalc": total_precalc,
                "portiere": lineup.get("portiere") or "",
                "difensori": lineup.get("difensori") or [],
                "centrocampisti": lineup.get("centrocampisti") or [],
                "attaccanti": lineup.get("attaccanti") or [],
                "panchina_details": lineup.get("panchina_details") or [],
                "appkey_scores": appkey_scores,
                "capitano": captain,
                "vice_capitano": vice_captain,
                "round": round_value,
                "source": "real",
            }
            item["panchina"] = [str(reserve.get("name") or "").strip() for reserve in item["panchina_details"]]
            dedupe_key = (round_value, normalize_name(str(item.get("team") or "")))
            items_by_key[dedupe_key] = item

    if not items_by_key:
        return [], sorted(rounds)
    return list(items_by_key.values()), sorted(rounds)


def _load_real_formazioni_rows_from_appkey_json(
    standings_index: Dict[str, Dict[str, object]],
    *,
    preferred_round: Optional[int] = None,
) -> tuple[List[Dict[str, object]], List[int], Optional[Path]]:
    current_turn = _extract_current_turn_from_formazioni_context_html()
    requested_round = _parse_int(preferred_round)
    source_path: Optional[Path] = None

    if requested_round is not None:
        cached_round_path = REAL_FORMATIONS_TMP_DIR / f"formazioni_{int(requested_round)}_appkey.json"
        if cached_round_path.exists():
            source_path = cached_round_path
    if source_path is None and requested_round is not None:
        source_path = _refresh_formazioni_appkey_from_context_html(requested_round)
    if current_turn is not None:
        source_path = source_path or _refresh_formazioni_appkey_from_context_html(current_turn)
    if source_path is None:
        source_path = _refresh_formazioni_appkey_from_context_html(None)

    if source_path is None and requested_round is not None:
        source_path = _refresh_formazioni_appkey_from_service(requested_round)
    if source_path is None and current_turn is not None:
        source_path = _refresh_formazioni_appkey_from_service(current_turn)
    if source_path is None:
        source_path = _latest_formazioni_appkey_path()
    if source_path is None:
        return [], [], None

    try:
        payload = json.loads(source_path.read_text(encoding="utf-8-sig"))
    except Exception:
        return [], [], None

    items, rounds = _parse_formazioni_payload_to_items(payload, standings_index)
    if requested_round is not None and requested_round not in rounds and LEGHE_ALIAS and LEGHE_USERNAME and LEGHE_PASSWORD:
        service_path = _refresh_formazioni_appkey_from_service(requested_round)
        if service_path is not None:
            try:
                service_payload = json.loads(service_path.read_text(encoding="utf-8-sig"))
            except Exception:
                service_payload = {}
            service_items, service_rounds = _parse_formazioni_payload_to_items(service_payload, standings_index)
            if service_items and requested_round in service_rounds:
                return service_items, service_rounds, service_path

    if (
        current_turn is not None
        and current_turn not in rounds
        and LEGHE_ALIAS
        and LEGHE_USERNAME
        and LEGHE_PASSWORD
    ):
        service_path = _refresh_formazioni_appkey_from_service(current_turn)
        if service_path is not None:
            try:
                service_payload = json.loads(service_path.read_text(encoding="utf-8-sig"))
            except Exception:
                service_payload = {}
            service_items, service_rounds = _parse_formazioni_payload_to_items(service_payload, standings_index)
            if service_items and current_turn in service_rounds:
                return service_items, service_rounds, service_path

    return items, rounds, source_path


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
    *,
    preferred_round: Optional[int] = None,
) -> tuple[List[Dict[str, object]], List[int], Optional[Path]]:
    standings_index = standings_index or {}
    requested_round = _parse_int(preferred_round)
    _refresh_formazioni_context_html_live()

    appkey_items, appkey_rounds, appkey_source = _load_real_formazioni_rows_from_appkey_json(
        standings_index,
        preferred_round=requested_round,
    )
    fallback_appkey_items: List[Dict[str, object]] = []
    if appkey_items:
        _recompute_forza_titolari(appkey_items)
        if requested_round is None or requested_round in appkey_rounds:
            return appkey_items, appkey_rounds, appkey_source
        fallback_appkey_items = appkey_items

    if requested_round is not None:
        _refresh_formazioni_xlsx_for_round(requested_round)

    candidate_paths: List[Path] = []
    for folder in REAL_FORMATIONS_DIR_CANDIDATES:
        latest = _latest_supported_file(folder)
        if latest is not None:
            candidate_paths.append(latest)
    if not LEGHE_ALIAS:
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
                "mod_capitano_precalc": _parse_float(
                    _pick_row_value(
                        normalized_row,
                        [
                            "mod_capitano_precalc",
                            "mod_capitano",
                            "modificatore_capitano",
                            "modificatorecapitano",
                        ],
                    )
                ),
                "totale_precalc": _parse_float(
                    _pick_row_value(
                        normalized_row,
                        [
                            "totale_precalc",
                            "totale",
                            "totale_live",
                            "total_live",
                            "total",
                        ],
                    )
                ),
                "round": round_value,
                "source": "real",
            }
            item["panchina"] = [str(reserve.get("name") or "").strip() for reserve in item["panchina_details"]]
            lineup_size = (
                (1 if str(item.get("portiere") or "").strip() else 0)
                + len([name for name in item.get("difensori") or [] if str(name).strip()])
                + len([name for name in item.get("centrocampisti") or [] if str(name).strip()])
                + len([name for name in item.get("attaccanti") or [] if str(name).strip()])
            )
            reserve_size = len([entry for entry in item.get("panchina_details") or [] if isinstance(entry, dict)])
            quality_score = lineup_size * 10 + reserve_size
            if str(item.get("modulo") or "").strip():
                quality_score += 5
            if round_value is not None:
                quality_score += 100
            item["_quality_score"] = quality_score

            dedupe_key = (round_value, normalize_name(str(item["team"])))
            existing_item = items_by_key.get(dedupe_key)
            existing_score = (
                int(existing_item.get("_quality_score", -1))
                if isinstance(existing_item, dict)
                else -1
            )
            if quality_score >= existing_score:
                items_by_key[dedupe_key] = item

        items = list(items_by_key.values())
        for item in items:
            item.pop("_quality_score", None)
        merged_items = _merge_real_formations_with_appkey(items, appkey_items, appkey_rounds)
        rounds_in_items = {
            round_value
            for round_value in (_parse_int(item.get("round")) for item in merged_items)
            if round_value is not None
        }
        rounds_in_items.update(rounds)
        available_rounds = sorted(rounds_in_items)
        _recompute_forza_titolari(merged_items)
        if requested_round is not None:
            has_requested_round = any(_parse_int(item.get("round")) == requested_round for item in merged_items)
            if not has_requested_round:
                continue
        return merged_items, available_rounds, source_path

    if fallback_appkey_items:
        return fallback_appkey_items, appkey_rounds, appkey_source

    return [], [], None


@router.get("/live/payload")
def live_payload(
    round: Optional[int] = Query(default=None, ge=1, le=99),
    db: Session = Depends(get_db),
    x_access_key: str | None = Header(default=None, alias="X-Access-Key"),
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    _require_login_key(db, authorization=authorization, x_access_key=x_access_key or x_admin_key)

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
    elif vote_value is not None and fantavote_value is not None:
        # Do not infer decisive-goal badges from vote/fantavote deltas:
        # this can create false positives (e.g. assigning a "gol pareggio"
        # to the wrong scorer). We rely on explicit source events plus
        # appkey/calendar overlays for decisive badges.
        event_counts = dict(event_counts)

    # Persist the computed fantasy vote (including bonuses/maluses) so downstream
    # sections don't rely on stale raw source values.
    default_vote = _safe_float_value(scoring_defaults.get("default_vote"), 6.0)
    default_fantavote = _safe_float_value(scoring_defaults.get("default_fantavote"), default_vote)
    vote_number_for_calc = default_vote if vote_value is None else float(vote_value)
    computed_fantavote: Optional[float] = None
    if not is_sv and not is_absent:
        computed_fantavote = _compute_live_fantavote(
            vote_number_for_calc,
            event_counts,
            bonus_map,
            fantavote_override=fantavote_value,
        )
        if computed_fantavote is None:
            computed_fantavote = default_fantavote

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
            fantavote=computed_fantavote,
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
        existing.fantavote = computed_fantavote
        for field in LIVE_EVENT_FIELDS:
            setattr(existing, field, int(event_counts.get(field, 0)))
        existing.is_sv = is_sv
        existing.is_absent = is_absent
        existing.updated_at = datetime.utcnow()

    new_has_appearance = _live_has_appearance(
        vote_value,
        computed_fantavote,
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
        else round(float(computed_fantavote) - vote_number_for_calc, 2),
        "vote_label": "X"
        if is_absent
        else ("SV" if is_sv else _format_live_number(vote_number_for_calc)),
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

    regulation = _load_regulation()
    standings_index = _build_standings_index()
    decisive_badges, decisive_source_path = _load_live_decisive_badges_from_appkey(
        resolved_round,
        standings_index,
        regulation,
    )
    appkey_badges_applied = _overlay_decisive_badges_from_appkey(rows, decisive_badges)
    club_index = _load_club_name_index()
    deterministic_badges_applied = _overlay_decisive_badges_from_round_results(
        rows,
        round_value=resolved_round,
        season_slug=season_slug,
        club_index=club_index,
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
        "appkey_decisive_source": str(decisive_source_path) if decisive_source_path else None,
        "appkey_decisive_players": len(decisive_badges),
        "appkey_decisive_applied": appkey_badges_applied,
        "deterministic_decisive_applied": deterministic_badges_applied,
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
            logger.debug("Scheduled job state commit failed for %s", job_name, exc_info=True)
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


def _leghe_sync_local_now(now_utc: Optional[datetime] = None) -> datetime:
    current = now_utc if now_utc is not None else datetime.now(tz=timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    return current.astimezone(LEGHE_SYNC_TZ)


def _leghe_sync_round_for_local_dt(local_dt: datetime) -> Optional[int]:
    local_day = local_dt.date()
    for matchday, start_day, end_day in LEGHE_SYNC_WINDOWS:
        if start_day <= local_day <= end_day:
            return int(matchday)
    return None


def _leghe_sync_reference_round_for_local_dt(local_dt: datetime) -> Optional[int]:
    if not LEGHE_SYNC_WINDOWS:
        return None

    local_day = local_dt.date()
    ordered_windows = sorted(LEGHE_SYNC_WINDOWS, key=lambda item: item[1])
    previous_round: Optional[int] = None

    for matchday, start_day, end_day in ordered_windows:
        current_round = int(matchday)
        if start_day <= local_day <= end_day:
            return current_round
        if local_day < start_day:
            if previous_round is not None:
                return int(previous_round)
            return max(1, current_round - 1)
        previous_round = current_round

    return int(previous_round) if previous_round is not None else None


def _leghe_sync_reference_round_now() -> Optional[int]:
    return _leghe_sync_reference_round_for_local_dt(_leghe_sync_local_now())


def _leghe_sync_reference_round_with_lookahead(
    *,
    lookahead_days: int = 0,
    now_utc: Optional[datetime] = None,
) -> Optional[int]:
    local_now = _leghe_sync_local_now(now_utc)
    days = int(lookahead_days or 0)
    if days != 0:
        local_now = local_now + timedelta(days=days)
    return _leghe_sync_reference_round_for_local_dt(local_now)


def _leghe_sync_slot_start_local(local_dt: datetime) -> datetime:
    slot_hours = max(1, int(LEGHE_SYNC_SLOT_HOURS))
    slot_hour = (int(local_dt.hour) // slot_hours) * slot_hours
    return local_dt.replace(hour=slot_hour, minute=0, second=0, microsecond=0)


def _leghe_matchday_sync_window_label() -> str:
    return (
        f"mon-sat {LEGHE_MATCHDAY_SYNC_START_HOUR_MON_SAT:02d}:00-23:59, "
        f"sun {LEGHE_MATCHDAY_SYNC_START_HOUR_SUN:02d}:00-23:59"
    )


def _is_leghe_matchday_sync_allowed_now(local_dt: datetime) -> bool:
    weekday = int(local_dt.weekday())  # Monday=0, Sunday=6
    hour = int(local_dt.hour)
    if weekday == 6:
        return hour >= int(LEGHE_MATCHDAY_SYNC_START_HOUR_SUN)
    return hour >= int(LEGHE_MATCHDAY_SYNC_START_HOUR_MON_SAT)


def leghe_sync_seconds_until_next_slot(now_utc: Optional[datetime] = None) -> int:
    local_now = _leghe_sync_local_now(now_utc)
    slot_start_local = _leghe_sync_slot_start_local(local_now)
    next_slot_local = slot_start_local + timedelta(hours=max(1, int(LEGHE_SYNC_SLOT_HOURS)))
    delta_seconds = int(math.ceil((next_slot_local - local_now).total_seconds()))
    return max(1, delta_seconds)


def _claim_scheduled_job_slot(
    db: Session,
    *,
    job_name: str,
    slot_ts: int,
) -> bool:
    target_ts = max(0, int(slot_ts))

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
            logger.debug("Scheduled job state commit failed for %s", job_name, exc_info=True)
            db.rollback()
        state = db.query(ScheduledJobState).filter(ScheduledJobState.job_name == job_name).first()
        if state is None:
            return False

    previous_ts = int(state.last_run_ts or 0)
    if previous_ts >= target_ts:
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
                    ScheduledJobState.last_run_ts: target_ts,
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


def _release_scheduled_job_slot(
    db: Session,
    *,
    job_name: str,
    slot_ts: int,
) -> bool:
    target_ts = max(0, int(slot_ts))
    fallback_ts = max(0, target_ts - 1)
    try:
        updated = (
            db.query(ScheduledJobState)
            .filter(
                ScheduledJobState.job_name == job_name,
                ScheduledJobState.last_run_ts == target_ts,
            )
            .update(
                {
                    ScheduledJobState.last_run_ts: fallback_ts,
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


def run_auto_seriea_live_context_sync(
    db: Session,
    *,
    configured_round: Optional[int] = None,
    season: Optional[str] = None,
    min_interval_seconds: Optional[int] = None,
) -> Dict[str, object]:
    if min_interval_seconds is not None:
        claimed = _claim_scheduled_job_run(
            db,
            job_name=SERIEA_LIVE_CONTEXT_JOB_NAME,
            min_interval_seconds=int(min_interval_seconds),
        )
        if not claimed:
            return {
                "ok": True,
                "skipped": True,
                "reason": "not_due_or_claimed_by_other_instance",
            }

    resolved_round = _parse_int(configured_round)
    if resolved_round is None or resolved_round <= 0:
        candidates = [
            _leghe_sync_reference_round_now(),
            _load_status_matchday(),
            _infer_matchday_from_fixtures(),
            _infer_matchday_from_stats(),
        ]
        positive = [int(value) for value in candidates if value is not None and int(value) > 0]
        resolved_round = max(positive) if positive else None

    season_slug = _normalize_season_slug(season)
    root_dir = Path(__file__).resolve().parents[4]
    script_path = root_dir / "scripts" / "sync_seriea_live_context.py"
    if not script_path.exists():
        return {
            "ok": False,
            "error": f"missing script: {script_path}",
            "round": int(resolved_round) if resolved_round is not None else None,
            "season": season_slug,
        }

    argv = [sys.executable, str(script_path), "--season", season_slug]
    if resolved_round is not None and int(resolved_round) > 0:
        argv.extend(["--round", str(int(resolved_round))])

    try:
        proc = subprocess.run(
            argv,
            cwd=str(root_dir),
            capture_output=True,
            text=True,
            check=False,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "round": int(resolved_round) if resolved_round is not None else None,
            "season": season_slug,
            "argv": argv,
        }

    stdout_text = (proc.stdout or "").strip()
    stderr_text = (proc.stderr or "").strip()
    if int(proc.returncode or 0) != 0:
        return {
            "ok": False,
            "error": stderr_text or stdout_text or f"sync_seriea_live_context rc={proc.returncode}",
            "round": int(resolved_round) if resolved_round is not None else None,
            "season": season_slug,
            "argv": argv,
            "returncode": int(proc.returncode or 0),
            "stdout": stdout_text[-1200:],
            "stderr": stderr_text[-1200:],
        }

    _SERIEA_CONTEXT_CACHE.clear()
    return {
        "ok": True,
        "round": int(resolved_round) if resolved_round is not None else None,
        "season": season_slug,
        "argv": argv,
        "returncode": int(proc.returncode or 0),
        "stdout": stdout_text[-1200:],
    }


def _run_live_import_for_round_safe(
    db: Session,
    *,
    round_value: Optional[int],
    season: Optional[str] = None,
) -> Dict[str, object]:
    resolved_round = _parse_int(round_value)
    try:
        result = _import_live_votes_internal(
            db,
            round_value=resolved_round,
            season=season,
        )
        if isinstance(result, dict):
            result.setdefault("ok", True)
            return result
        return {
            "ok": True,
            "round": int(resolved_round) if resolved_round is not None else None,
        }
    except HTTPException as exc:
        detail = exc.detail if hasattr(exc, "detail") else str(exc)
        return {
            "ok": False,
            "round": int(resolved_round) if resolved_round is not None else None,
            "error": str(detail),
        }
    except Exception as exc:
        return {
            "ok": False,
            "round": int(resolved_round) if resolved_round is not None else None,
            "error": str(exc),
        }


def _run_daily_live_noon_import_if_due(
    db: Session,
    *,
    local_now: datetime,
) -> Dict[str, object]:
    day_start_local = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    noon_local = day_start_local.replace(hour=int(LEGHE_DAILY_LIVE_HOUR_LOCAL))
    if local_now < noon_local:
        return {
            "ok": True,
            "skipped": True,
            "reason": "before_daily_live_noon_slot",
            "daily_slot_local": noon_local.isoformat(),
            "timezone": str(LEGHE_SYNC_TZ),
        }

    noon_utc_ts = int(noon_local.astimezone(timezone.utc).timestamp())
    claimed_noon_live = _claim_scheduled_job_slot(
        db,
        job_name=LEGHE_DAILY_LIVE_JOB_NAME,
        slot_ts=noon_utc_ts,
    )
    if not claimed_noon_live:
        return {
            "ok": True,
            "skipped": True,
            "reason": "daily_live_noon_already_synced",
            "daily_slot_local": noon_local.isoformat(),
            "timezone": str(LEGHE_SYNC_TZ),
        }

    reference_round = _leghe_sync_reference_round_for_local_dt(local_now)
    if reference_round is None:
        reference_round = _latest_round_with_live_votes(db)

    try:
        result = run_auto_live_import(
            db,
            configured_round=reference_round,
        )
    except HTTPException as exc:
        _release_scheduled_job_slot(
            db,
            job_name=LEGHE_DAILY_LIVE_JOB_NAME,
            slot_ts=noon_utc_ts,
        )
        detail = exc.detail if hasattr(exc, "detail") else str(exc)
        return {
            "ok": False,
            "error": str(detail),
            "mode": "daily_live_noon_import",
            "daily_slot_local": noon_local.isoformat(),
            "timezone": str(LEGHE_SYNC_TZ),
            "scheduled_round": int(reference_round) if reference_round is not None else None,
        }
    except Exception as exc:
        _release_scheduled_job_slot(
            db,
            job_name=LEGHE_DAILY_LIVE_JOB_NAME,
            slot_ts=noon_utc_ts,
        )
        return {
            "ok": False,
            "error": str(exc),
            "mode": "daily_live_noon_import",
            "daily_slot_local": noon_local.isoformat(),
            "timezone": str(LEGHE_SYNC_TZ),
            "scheduled_round": int(reference_round) if reference_round is not None else None,
        }

    if isinstance(result, dict) and result.get("ok") is False:
        _release_scheduled_job_slot(
            db,
            job_name=LEGHE_DAILY_LIVE_JOB_NAME,
            slot_ts=noon_utc_ts,
        )

    if isinstance(result, dict):
        result["mode"] = "daily_live_noon_import"
        result["daily_slot_local"] = noon_local.isoformat()
        result["timezone"] = str(LEGHE_SYNC_TZ)
        result["scheduled_round"] = int(reference_round) if reference_round is not None else None
    return result


def leghe_bootstrap_sync_required(
    *,
    now_utc: Optional[datetime] = None,
    max_age_hours: int = LEGHE_BOOTSTRAP_MAX_AGE_HOURS,
) -> bool:
    check_paths = [ROSE_PATH, QUOT_PATH, STATS_PATH]
    now = now_utc if now_utc is not None else datetime.now(tz=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    threshold = now - timedelta(hours=max(1, int(max_age_hours)))

    for path in check_paths:
        try:
            effective_path = _first_existing_data_path(path)
            if effective_path is None:
                return True
            mtime_utc = datetime.fromtimestamp(effective_path.stat().st_mtime, tz=timezone.utc)
            if mtime_utc < threshold:
                return True
        except Exception:
            return True
    return False


def run_bootstrap_leghe_sync(
    db: Session,
    *,
    run_pipeline: bool = True,
    now_utc: Optional[datetime] = None,
) -> Dict[str, object]:
    local_now = _leghe_sync_local_now(now_utc)

    if not LEGHE_ALIAS or not LEGHE_USERNAME or not LEGHE_PASSWORD:
        return {
            "ok": True,
            "skipped": True,
            "reason": "missing_leghe_env",
            "required": ["LEGHE_ALIAS", "LEGHE_USERNAME", "LEGHE_PASSWORD"],
            "mode": "bootstrap_force_sync",
        }

    requested_round = None
    env_round = _parse_int(LEGHE_FORMATIONS_MATCHDAY)
    status_round = _load_status_matchday()
    inferred_round = _infer_matchday_from_fixtures()
    window_round = _leghe_sync_round_for_local_dt(local_now)
    reference_round = _leghe_sync_reference_round_for_local_dt(local_now)
    live_votes_round = _latest_round_with_live_votes(db)

    candidates = [
        requested_round,
        env_round,
        status_round,
        inferred_round,
        window_round,
        reference_round,
        live_votes_round,
    ]
    valid_rounds = [int(value) for value in candidates if _parse_int(value) is not None and int(value or 0) > 0]
    resolved_round = max(valid_rounds) if valid_rounds else None

    availability_sync_result = _sync_player_availability_sources()
    live_import_result = (
        _run_live_import_for_round_safe(db, round_value=resolved_round)
        if resolved_round is not None
        else {"ok": True, "skipped": True, "reason": "round_unresolved"}
    )

    try:
        result = run_leghe_sync_and_pipeline(
            alias=LEGHE_ALIAS,
            username=LEGHE_USERNAME,
            password=LEGHE_PASSWORD,
            date_stamp=local_now.date().isoformat(),
            competition_id=LEGHE_COMPETITION_ID,
            competition_name=LEGHE_COMPETITION_NAME,
            formations_matchday=resolved_round,
            fetch_quotazioni=True,
            fetch_global_stats=True,
            run_pipeline=bool(run_pipeline),
        )
    except LegheSyncError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "mode": "bootstrap_force_sync",
            "round": int(resolved_round) if resolved_round is not None else None,
            "live_import": live_import_result,
            "availability_sync": availability_sync_result,
        }

    if isinstance(result, dict):
        result["mode"] = "bootstrap_force_sync"
        result["round"] = int(resolved_round) if resolved_round is not None else None
        result["live_import"] = live_import_result
        result["availability_sync"] = availability_sync_result
        warnings = list(result.get("warnings") or [])
        if isinstance(live_import_result, dict) and live_import_result.get("ok") is False:
            error_msg = str(live_import_result.get("error") or "unknown")
            warnings.append(f"live_import failed: {error_msg}")
        if isinstance(availability_sync_result, dict) and availability_sync_result.get("ok") is False:
            warnings.append(
                f"availability_sync failed: {availability_sync_result.get('error') or 'unknown'}"
            )
        result["warnings"] = warnings
    return result


def run_auto_leghe_sync(
    db: Session,
    *,
    min_interval_seconds: Optional[int] = None,
    run_pipeline: bool = True,
    now_utc: Optional[datetime] = None,
) -> Dict[str, object]:
    _ = min_interval_seconds

    local_now = _leghe_sync_local_now(now_utc)
    availability_sync_result = _run_availability_sync_if_due(
        db,
        local_now=local_now,
    )
    scheduled_matchday = _leghe_sync_round_for_local_dt(local_now)
    if scheduled_matchday is None:
        daily_live_noon_result = _run_daily_live_noon_import_if_due(
            db,
            local_now=local_now,
        )
        daily_live_noon_skipped = bool(daily_live_noon_result.get("skipped"))

        if not LEGHE_ALIAS or not LEGHE_USERNAME or not LEGHE_PASSWORD:
            if not daily_live_noon_skipped:
                if isinstance(daily_live_noon_result, dict):
                    daily_live_noon_result["availability_sync"] = availability_sync_result
                return daily_live_noon_result
            return {
                "ok": True,
                "skipped": True,
                "reason": "outside_scheduled_match_windows",
                "local_time": local_now.isoformat(),
                "timezone": str(LEGHE_SYNC_TZ),
                "missing_leghe_env": True,
                "required": ["LEGHE_ALIAS", "LEGHE_USERNAME", "LEGHE_PASSWORD"],
                "daily_live_noon": daily_live_noon_result,
                "availability_sync": availability_sync_result,
            }

        day_start_local = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_utc_ts = int(day_start_local.astimezone(timezone.utc).timestamp())
        claimed_daily_rose = _claim_scheduled_job_slot(
            db,
            job_name=LEGHE_DAILY_ROSE_JOB_NAME,
            slot_ts=day_start_utc_ts,
        )
        if not claimed_daily_rose:
            if not daily_live_noon_skipped:
                if isinstance(daily_live_noon_result, dict):
                    daily_live_noon_result["availability_sync"] = availability_sync_result
                return daily_live_noon_result
            return {
                "ok": True,
                "skipped": True,
                "reason": "outside_scheduled_match_windows_and_daily_rose_already_synced",
                "local_time": local_now.isoformat(),
                "daily_slot_local": day_start_local.isoformat(),
                "timezone": str(LEGHE_SYNC_TZ),
                "daily_live_noon": daily_live_noon_result,
                "availability_sync": availability_sync_result,
            }

        try:
            result = run_leghe_sync_and_pipeline(
                alias=LEGHE_ALIAS,
                username=LEGHE_USERNAME,
                password=LEGHE_PASSWORD,
                date_stamp=local_now.date().isoformat(),
                competition_id=LEGHE_COMPETITION_ID,
                competition_name=LEGHE_COMPETITION_NAME,
                formations_matchday=LEGHE_FORMATIONS_MATCHDAY,
                download_rose=True,
                download_classifica=False,
                download_formazioni=False,
                download_formazioni_xlsx=False,
                fetch_quotazioni=True,
                fetch_global_stats=True,
                run_pipeline=bool(run_pipeline),
            )
            if isinstance(result, dict) and result.get("ok") is False:
                _release_scheduled_job_slot(
                    db,
                    job_name=LEGHE_DAILY_ROSE_JOB_NAME,
                    slot_ts=day_start_utc_ts,
                )
            if isinstance(result, dict):
                result["mode"] = "daily_rose_sync"
                result["daily_slot_local"] = day_start_local.isoformat()
                result["timezone"] = str(LEGHE_SYNC_TZ)
                result["daily_live_noon"] = daily_live_noon_result
                result["availability_sync"] = availability_sync_result
                warnings = list(result.get("warnings") or [])
                if isinstance(availability_sync_result, dict) and availability_sync_result.get("ok") is False:
                    warnings.append(
                        f"availability_sync failed: {availability_sync_result.get('error') or 'unknown'}"
                    )
                result["warnings"] = warnings
            return result
        except LegheSyncError as exc:
            _release_scheduled_job_slot(
                db,
                job_name=LEGHE_DAILY_ROSE_JOB_NAME,
                slot_ts=day_start_utc_ts,
            )
            return {
                "ok": False,
                "error": str(exc),
                "mode": "daily_rose_sync",
                "daily_slot_local": day_start_local.isoformat(),
                "timezone": str(LEGHE_SYNC_TZ),
                "daily_live_noon": daily_live_noon_result,
                "availability_sync": availability_sync_result,
            }
        except Exception as exc:
            _release_scheduled_job_slot(
                db,
                job_name=LEGHE_DAILY_ROSE_JOB_NAME,
                slot_ts=day_start_utc_ts,
            )
            return {
                "ok": False,
                "error": str(exc),
                "mode": "daily_rose_sync",
                "daily_slot_local": day_start_local.isoformat(),
                "timezone": str(LEGHE_SYNC_TZ),
                "daily_live_noon": daily_live_noon_result,
                "availability_sync": availability_sync_result,
            }

    if not _is_leghe_matchday_sync_allowed_now(local_now):
        return {
            "ok": True,
            "skipped": True,
            "reason": "outside_matchday_sync_hours",
            "matchday": int(scheduled_matchday),
            "local_time": local_now.isoformat(),
            "allowed_window_local": _leghe_matchday_sync_window_label(),
            "timezone": str(LEGHE_SYNC_TZ),
            "availability_sync": availability_sync_result,
        }

    slot_start_local = _leghe_sync_slot_start_local(local_now)
    slot_start_utc_ts = int(slot_start_local.astimezone(timezone.utc).timestamp())
    claimed = _claim_scheduled_job_slot(
        db,
        job_name="auto_leghe_sync",
        slot_ts=slot_start_utc_ts,
    )
    if not claimed:
        return {
            "ok": True,
            "skipped": True,
            "reason": "slot_already_processed_or_claimed_by_other_instance",
            "matchday": int(scheduled_matchday),
            "slot_start_local": slot_start_local.isoformat(),
            "timezone": str(LEGHE_SYNC_TZ),
            "availability_sync": availability_sync_result,
        }

    if not LEGHE_ALIAS or not LEGHE_USERNAME or not LEGHE_PASSWORD:
        return {
            "ok": True,
            "skipped": True,
            "reason": "missing_leghe_env",
            "required": ["LEGHE_ALIAS", "LEGHE_USERNAME", "LEGHE_PASSWORD"],
            "matchday": int(scheduled_matchday),
            "slot_start_local": slot_start_local.isoformat(),
            "timezone": str(LEGHE_SYNC_TZ),
            "availability_sync": availability_sync_result,
        }

    try:
        live_import_result = _run_live_import_for_round_safe(
            db,
            round_value=int(scheduled_matchday),
        )
        result = run_leghe_sync_and_pipeline(
            alias=LEGHE_ALIAS,
            username=LEGHE_USERNAME,
            password=LEGHE_PASSWORD,
            date_stamp=local_now.date().isoformat(),
            competition_id=LEGHE_COMPETITION_ID,
            competition_name=LEGHE_COMPETITION_NAME,
            formations_matchday=int(scheduled_matchday),
            fetch_quotazioni=False,
            fetch_global_stats=False,
            run_pipeline=bool(run_pipeline),
        )
        if isinstance(result, dict):
            result["scheduled_matchday"] = int(scheduled_matchday)
            result["slot_start_local"] = slot_start_local.isoformat()
            result["timezone"] = str(LEGHE_SYNC_TZ)
            result["live_import"] = live_import_result
            result["availability_sync"] = availability_sync_result
            warnings = list(result.get("warnings") or [])
            if isinstance(live_import_result, dict) and live_import_result.get("ok") is False:
                error_msg = str(live_import_result.get("error") or "unknown")
                warnings.append(f"live_import failed: {error_msg}")
            if isinstance(availability_sync_result, dict) and availability_sync_result.get("ok") is False:
                warnings.append(
                    f"availability_sync failed: {availability_sync_result.get('error') or 'unknown'}"
                )
            result["warnings"] = warnings
        return result
    except LegheSyncError as exc:
        return {
            "ok": False,
            "error": str(exc),
            "scheduled_matchday": int(scheduled_matchday),
            "slot_start_local": slot_start_local.isoformat(),
            "timezone": str(LEGHE_SYNC_TZ),
            "availability_sync": availability_sync_result,
        }


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


@router.post("/admin/leghe/sync")
def admin_leghe_sync(
    force: bool = Query(default=False),
    run_pipeline: bool = Query(default=True),
    fetch_quotazioni: bool = Query(default=False),
    fetch_global_stats: bool = Query(default=False),
    formations_matchday: Optional[int] = Query(default=None, ge=1, le=99),
    db: Session = Depends(get_db),
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    _require_admin_key(x_admin_key, db, authorization)

    if force:
        if not LEGHE_ALIAS or not LEGHE_USERNAME or not LEGHE_PASSWORD:
            raise HTTPException(
                status_code=400,
                detail="Missing env vars: LEGHE_ALIAS, LEGHE_USERNAME, LEGHE_PASSWORD",
            )
        try:
            return run_leghe_sync_and_pipeline(
                alias=LEGHE_ALIAS,
                username=LEGHE_USERNAME,
                password=LEGHE_PASSWORD,
                competition_id=LEGHE_COMPETITION_ID,
                competition_name=LEGHE_COMPETITION_NAME,
                formations_matchday=formations_matchday or LEGHE_FORMATIONS_MATCHDAY,
                fetch_quotazioni=bool(fetch_quotazioni),
                fetch_global_stats=bool(fetch_global_stats),
                run_pipeline=bool(run_pipeline),
            )
        except LegheSyncError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    result = run_auto_leghe_sync(
        db,
        run_pipeline=bool(run_pipeline),
    )
    if result.get("ok") is False:
        raise HTTPException(status_code=502, detail=str(result.get("error") or "Leghe sync failed"))
    return result


@router.post("/admin/availability/sync")
def admin_sync_player_availability(
    db: Session = Depends(get_db),
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    _require_admin_key(x_admin_key, db, authorization)
    result = _sync_player_availability_sources()
    if result.get("ok") is False:
        raise HTTPException(status_code=502, detail=str(result.get("error") or "Availability sync failed"))
    return result


def _run_sync_complete_total_internal(
    db: Session,
    *,
    run_pipeline: bool,
    fetch_quotazioni: bool,
    fetch_global_stats: bool,
    formations_matchday: Optional[int],
) -> Dict[str, object]:
    local_now = _leghe_sync_local_now()
    requested_round = _parse_int(formations_matchday)
    env_round = _parse_int(LEGHE_FORMATIONS_MATCHDAY)
    status_round = _load_status_matchday()
    inferred_round = _infer_matchday_from_fixtures()
    window_round = _leghe_sync_round_for_local_dt(local_now)
    reference_round = _leghe_sync_reference_round_for_local_dt(local_now)
    live_votes_round = _latest_round_with_live_votes(db)

    candidates = [
        requested_round,
        env_round,
        status_round,
        inferred_round,
        window_round,
        reference_round,
        live_votes_round,
    ]
    valid_rounds = [int(value) for value in candidates if _parse_int(value) is not None and int(value or 0) > 0]
    resolved_round = max(valid_rounds) if valid_rounds else None

    availability_sync_result = _sync_player_availability_sources()
    live_import_result = _run_live_import_for_round_safe(
        db,
        round_value=resolved_round,
    )

    try:
        result = run_leghe_sync_and_pipeline(
            alias=LEGHE_ALIAS,
            username=LEGHE_USERNAME,
            password=LEGHE_PASSWORD,
            date_stamp=local_now.date().isoformat(),
            competition_id=LEGHE_COMPETITION_ID,
            competition_name=LEGHE_COMPETITION_NAME,
            formations_matchday=resolved_round,
            fetch_quotazioni=bool(fetch_quotazioni),
            fetch_global_stats=bool(fetch_global_stats),
            run_pipeline=bool(run_pipeline),
        )
    except LegheSyncError as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "ok": False,
                "mode": "sync_complete_total",
                "error": str(exc),
                "round": int(resolved_round) if resolved_round is not None else None,
                "live_import": live_import_result,
                "availability_sync": availability_sync_result,
            },
        ) from exc

    if isinstance(result, dict):
        result["mode"] = "sync_complete_total"
        result["round"] = int(resolved_round) if resolved_round is not None else None
        result["live_import"] = live_import_result
        result["availability_sync"] = availability_sync_result
        warnings = list(result.get("warnings") or [])
        if isinstance(live_import_result, dict) and live_import_result.get("ok") is False:
            error_msg = str(live_import_result.get("error") or "unknown")
            warnings.append(f"live_import failed: {error_msg}")
        if isinstance(availability_sync_result, dict) and availability_sync_result.get("ok") is False:
            warnings.append(
                f"availability_sync failed: {availability_sync_result.get('error') or 'unknown'}"
            )
        result["warnings"] = warnings
    return result if isinstance(result, dict) else {"ok": True, "mode": "sync_complete_total"}


def _sync_complete_background_worker(
    *,
    run_pipeline: bool,
    fetch_quotazioni: bool,
    fetch_global_stats: bool,
    formations_matchday: Optional[int],
) -> None:
    global _SYNC_COMPLETE_BACKGROUND_RUNNING
    db = SessionLocal()
    try:
        result = _run_sync_complete_total_internal(
            db,
            run_pipeline=bool(run_pipeline),
            fetch_quotazioni=bool(fetch_quotazioni),
            fetch_global_stats=bool(fetch_global_stats),
            formations_matchday=formations_matchday,
        )
        logger.info(
            "sync_complete_total background finished: ok=%s round=%s warnings=%s",
            result.get("ok", True),
            result.get("round"),
            len(list(result.get("warnings") or [])),
        )
    except Exception:
        logger.exception("sync_complete_total background failed")
    finally:
        try:
            db.close()
        except Exception:
            pass
        with _SYNC_COMPLETE_BACKGROUND_LOCK:
            _SYNC_COMPLETE_BACKGROUND_RUNNING = False


def _enqueue_sync_complete_background(
    *,
    run_pipeline: bool,
    fetch_quotazioni: bool,
    fetch_global_stats: bool,
    formations_matchday: Optional[int],
) -> Dict[str, object]:
    global _SYNC_COMPLETE_BACKGROUND_RUNNING
    with _SYNC_COMPLETE_BACKGROUND_LOCK:
        if _SYNC_COMPLETE_BACKGROUND_RUNNING:
            return {
                "ok": True,
                "queued": False,
                "running": True,
                "mode": "sync_complete_total",
                "message": "Sync completa totale gia in corso.",
            }
        _SYNC_COMPLETE_BACKGROUND_RUNNING = True
        worker = threading.Thread(
            target=_sync_complete_background_worker,
            kwargs={
                "run_pipeline": bool(run_pipeline),
                "fetch_quotazioni": bool(fetch_quotazioni),
                "fetch_global_stats": bool(fetch_global_stats),
                "formations_matchday": formations_matchday,
            },
            name="sync-complete-total-worker",
            daemon=True,
        )
        worker.start()
    return {
        "ok": True,
        "queued": True,
        "running": True,
        "mode": "sync_complete_total",
        "message": "Sync completa totale avviata in background.",
    }


@router.post("/admin/leghe/sync-complete")
def admin_leghe_sync_complete(
    run_pipeline: bool = Query(default=True),
    fetch_quotazioni: bool = Query(default=True),
    fetch_global_stats: bool = Query(default=True),
    formations_matchday: Optional[int] = Query(default=None, ge=1, le=99),
    background: bool = Query(default=True),
    db: Session = Depends(get_db),
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    _require_admin_key(x_admin_key, db, authorization)

    if not LEGHE_ALIAS or not LEGHE_USERNAME or not LEGHE_PASSWORD:
        raise HTTPException(
            status_code=400,
            detail="Missing env vars: LEGHE_ALIAS, LEGHE_USERNAME, LEGHE_PASSWORD",
        )

    if bool(background):
        return _enqueue_sync_complete_background(
            run_pipeline=bool(run_pipeline),
            fetch_quotazioni=bool(fetch_quotazioni),
            fetch_global_stats=bool(fetch_global_stats),
            formations_matchday=formations_matchday,
        )

    return _run_sync_complete_total_internal(
        db,
        run_pipeline=bool(run_pipeline),
        fetch_quotazioni=bool(fetch_quotazioni),
        fetch_global_stats=bool(fetch_global_stats),
        formations_matchday=formations_matchday,
    )


@router.get("/formazioni")
def formazioni(
    team: Optional[str] = Query(default=None),
    round: Optional[int] = Query(default=None, ge=1, le=99),
    order_by: Optional[str] = Query(default=None, pattern="^(classifica|live_total)$"),
    limit: int = Query(default=200, ge=1, le=500),
    db: Session = Depends(get_db),
    x_access_key: str | None = Header(default=None, alias="X-Access-Key"),
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    team_key = normalize_name(team or "")
    regulation = _load_regulation()
    default_order, allowed_orders = _reg_ordering(regulation)
    selected_order = str(order_by or default_order).strip().lower()
    if selected_order not in allowed_orders:
        selected_order = default_order

    standings_index = _build_standings_index()
    status_matchday = _load_status_matchday()
    scheduled_reference_round = _leghe_sync_reference_round_now()
    latest_live_votes_round = _latest_round_with_live_votes(db)
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
    target_round = (
        round
        if round is not None
        else (
            int(scheduled_reference_round)
            if scheduled_reference_round is not None
            else default_matchday
        )
    )
    if round is None and latest_live_votes_round is not None:
        if target_round is None or int(latest_live_votes_round) > int(target_round):
            target_round = int(latest_live_votes_round)

    club_index = _load_club_name_index()
    seriea_fixtures_for_kickoff = _load_seriea_fixtures_for_insights(club_index)
    real_unlocked, first_kickoff_local, real_unlock_reason = _is_formazioni_real_unlocked_for_round(
        target_round,
        seriea_fixtures_for_kickoff,
    )
    real_rows: List[Dict[str, object]] = []
    available_rounds: List[int] = []
    source_path: Optional[Path] = None
    if real_unlocked:
        real_rows, available_rounds, source_path = _load_real_formazioni_rows(
            standings_index,
            preferred_round=target_round,
        )
        if target_round is None and available_rounds:
            target_round = max(available_rounds)

    fixture_rows_for_rounds = _load_fixture_rows_for_live(db, club_index)
    fixture_rounds = _rounds_from_fixture_rows(fixture_rows_for_rounds)
    schedule_rounds = [int(matchday) for matchday, _start, _end in LEGHE_SYNC_WINDOWS]
    payload_rounds_set: Set[int] = {
        int(value)
        for value in available_rounds
        if isinstance(value, int) and int(value) > 0
    }
    payload_rounds_set.update(fixture_rounds)
    payload_rounds_set.update(schedule_rounds)
    if schedule_rounds:
        payload_rounds_set.add(max(1, min(schedule_rounds) - 1))
    for extra_round in (
        target_round,
        status_matchday,
        inferred_matchday_fixtures,
        inferred_matchday_stats,
        latest_live_votes_round,
        scheduled_reference_round,
    ):
        parsed_extra = _parse_int(extra_round)
        if parsed_extra is not None and parsed_extra > 0:
            payload_rounds_set.add(int(parsed_extra))
    payload_rounds = sorted(payload_rounds_set)

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

    classifica_positions = _load_classifica_positions()
    if classifica_positions:
        _apply_classifica_positions_override(real_items, classifica_positions)

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
            "available_rounds": payload_rounds,
            "order_by": selected_order,
            "order_allowed": allowed_orders,
            "status_matchday": status_matchday,
            "inferred_matchday_fixtures": inferred_matchday_fixtures,
            "inferred_matchday_stats": inferred_matchday_stats,
            "scheduled_reference_round": scheduled_reference_round,
            "latest_live_votes_round": latest_live_votes_round,
            "source_path": str(source_path) if source_path else "",
            "real_unlocked": bool(real_unlocked),
            "real_unlock_reason": real_unlock_reason,
            "first_kickoff_local": first_kickoff_local.isoformat() if isinstance(first_kickoff_local, datetime) else "",
            "note": "",
        }

    projected_items = _load_projected_formazioni_rows(team_key, standings_index)
    for item in projected_items:
        item["round"] = target_round
    if classifica_positions:
        _apply_classifica_positions_override(projected_items, classifica_positions)
    live_context = _load_live_round_context(db, target_round)
    _attach_live_scores_to_formations(projected_items, live_context)
    if selected_order == "live_total":
        projected_items.sort(key=_formations_sort_live_key)
    else:
        projected_items.sort(key=_formations_sort_key)

    if source_path is None:
        if not real_unlocked and target_round is not None:
            kickoff_label = (
                first_kickoff_local.strftime("%d/%m/%Y %H:%M")
                if isinstance(first_kickoff_local, datetime)
                else "orario non disponibile"
            )
            note = (
                f"Formazioni reali disponibili solo dal calcio d'inizio della giornata {target_round} "
                f"({kickoff_label}): mostrato XI migliore ordinato per classifica."
            )
        else:
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
        "scheduled_reference_round": scheduled_reference_round,
        "latest_live_votes_round": latest_live_votes_round,
        "source_path": str(source_path) if source_path else "",
        "real_unlocked": bool(real_unlocked),
        "real_unlock_reason": real_unlock_reason,
        "first_kickoff_local": first_kickoff_local.isoformat() if isinstance(first_kickoff_local, datetime) else "",
        "note": note,
    }


@router.get("/formazioni/optimizer")
def formazione_optimizer(
    team: str = Query(..., min_length=1),
    round: Optional[int] = Query(default=None, ge=1, le=99),
    captain_mode: str = Query(default="balanced"),
    db: Session = Depends(get_db),
    x_access_key: str | None = Header(default=None, alias="X-Access-Key"),
    x_admin_key: str | None = Header(default=None, alias="X-Admin-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
):
    _require_login_key(db, authorization=authorization, x_access_key=x_access_key or x_admin_key)
    team_key = normalize_name(team)
    if not team_key:
        raise HTTPException(status_code=400, detail="Team non valido")

    payload = _build_contextual_optimizer_payload(team_key, db, round, captain_mode=captain_mode)
    if payload is None:
        raise HTTPException(status_code=404, detail="Team non trovato o rosa non disponibile")
    return payload


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
    rose = _apply_qa_from_quot(_read_csv(ROSE_PATH))
    team_totals = defaultdict(lambda: {"acquisto": 0.0, "attuale": 0.0})
    for row in rose:
        team = str(row.get("Team") or "").strip()
        if not team:
            continue
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
