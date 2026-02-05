import csv
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, Query, Body, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from apps.api.app.engine.market_engine import suggest_transfers
from apps.api.app.deps import get_db
from apps.api.app.models import AccessKey, Fixture, Player, PlayerStats, Team, TeamKey
from apps.api.app.utils.names import normalize_name, strip_star, is_starred


router = APIRouter(prefix="/data", tags=["data"])

DATA_DIR = Path(__file__).resolve().parents[4] / "data"
ROSE_PATH = DATA_DIR / "rose_fantaportoscuso.csv"
QUOT_PATH = DATA_DIR / "quotazioni.csv"
RESIDUAL_CREDITS_PATH = DATA_DIR / "rose_nuovo_credits.csv"
STATS_PATH = DATA_DIR / "statistiche_giocatori.csv"
MARKET_PATH = DATA_DIR / "market_latest.json"
MARKET_REPORT_GLOB = "rose_changes_*.csv"
STATS_DIR = DATA_DIR / "stats"
PLAYER_CARDS_PATH = DATA_DIR / "db" / "quotazioni_master.csv"
PLAYER_STATS_PATH = DATA_DIR / "db" / "player_stats.csv"
TEAMS_PATH = DATA_DIR / "db" / "teams.csv"
FIXTURES_PATH = DATA_DIR / "db" / "fixtures.csv"
SEED_DB_DIR = Path("/app/seed/db")
ROSE_XLSX_DIR = DATA_DIR / "archive" / "incoming" / "rose"
_RESIDUAL_CREDITS_CACHE: Dict[str, object] = {}
_NAME_LIST_CACHE: Dict[str, object] = {}
_LISTONE_NAME_CACHE: Dict[str, object] = {}


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


def _count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as handle:
            return sum(1 for _ in handle)
    except Exception:
        return 0


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


def _require_admin_key(x_admin_key: str | None, db: Session) -> None:
    if not x_admin_key:
        raise HTTPException(status_code=401, detail="Admin key richiesta")
    key_value = x_admin_key.strip().lower()
    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record or not record.is_admin:
        raise HTTPException(status_code=403, detail="Admin key non valida")
    if not record.used:
        raise HTTPException(status_code=403, detail="Admin key non ancora attivata")


def _strip_leading_initial(value: str) -> str:
    return re.sub(r"^[A-Za-z]\.?\s+", "", value or "")


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
    raw = (value or "").strip()
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


def _latest_market_report() -> Optional[Path]:
    reports_dir = DATA_DIR / "reports"
    if not reports_dir.exists():
        return None
    candidates = sorted(reports_dir.glob(MARKET_REPORT_GLOB), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


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
            return (info or {}).get("Ruolo", "") or ""

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

    current_round = min(rounds) if rounds else 1

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
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db)
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
    payload = payload or {}
    user_squad = payload.get("user_squad", [])
    credits = float(payload.get("credits_residui", 0) or 0)
    players_pool = payload.get("players_pool", [])
    teams_data = payload.get("teams_data", {})
    fixtures = payload.get("fixtures", [])
    current_round = int(payload.get("currentRound") or payload.get("current_round") or 1)
    params = payload.get("params", {}) or {}

    if not teams_data:
        teams_data = _build_teams_data_from_user_squad(user_squad)

    k_pool = max(int(params.get("k_pool", 60)), 20)
    m_out = max(int(params.get("m_out", 8)), 6)
    beam_width = max(int(params.get("beam_width", 200)), 200)

    required_outs = params.get("required_outs") or []
    if not required_outs:
        starred = []
        for p in user_squad:
            name = (p.get("Giocatore") or p.get("nome") or "").strip()
            if name.endswith("*"):
                cleaned = name[:-1].strip()
                if cleaned:
                    starred.append(cleaned)
        if starred:
            required_outs = starred
    exclude_ins = params.get("exclude_ins") or []
    fixed_swaps = params.get("fixed_swaps") or []
    include_outs_any = params.get("include_outs_any") or []
    debug = bool(params.get("debug") or payload.get("debug"))

    solutions = suggest_transfers(
        user_squad=user_squad,
        credits_residui=credits,
        players_pool=players_pool,
        teams_data=teams_data,
        fixtures=fixtures,
        current_round=current_round,
        max_changes=max(int(params.get("max_changes", 5)), len(required_outs)),
        k_pool=k_pool,
        m_out=m_out,
        beam_width=beam_width,
        required_outs=required_outs,
        exclude_ins=exclude_ins,
        fixed_swaps=fixed_swaps,
        include_outs_any=include_outs_any,
        debug=debug,
    )

    out = []
    for sol in solutions:
        out.append(
            {
                "budget_initial": sol.budget_initial,
                "budget_final": sol.budget_final,
                "total_gain": sol.total_gain,
                "recommended_outs": sol.recommended_outs,
                "warnings": sol.warnings,
                "swaps": [
                    {
                        "out": s.out_player.get("nome") or s.out_player.get("Giocatore"),
                        "in": s.in_player.get("nome") or s.in_player.get("Giocatore"),
                        "qa_out": s.qa_out,
                        "qa_in": s.qa_in,
                        "gain": s.gain,
                        "cost_net": s.cost_net,
                    }
                    for s in sol.swaps
                ],
            }
        )
    return {"solutions": out}


@router.get("/market/payload")
def market_payload(
    x_access_key: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    if not x_access_key:
        raise HTTPException(status_code=401, detail="Access key richiesta")
    key_value = x_access_key.strip().lower()
    record = db.query(TeamKey).filter(TeamKey.key == key_value).first()
    if not record:
        raise HTTPException(status_code=404, detail="Team non associato alla key")
    payload = _build_market_suggest_payload(record.team, db)
    return {"team": record.team, "payload": payload}
