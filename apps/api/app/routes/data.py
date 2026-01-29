import csv
import re
import random
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

from fastapi import APIRouter, Query, Body, Depends, Header, HTTPException
from sqlalchemy.orm import Session

from apps.api.app.engine.market_engine import suggest_transfers, Swap, Solution, value_season
from apps.api.app.deps import get_db
from apps.api.app.models import Fixture, Player, PlayerStats, Team, TeamKey


router = APIRouter(prefix="/data", tags=["data"])

DATA_DIR = Path(__file__).resolve().parents[4] / "data"
ROSE_PATH = DATA_DIR / "rose_fantaportoscuso.csv"
QUOT_PATH = DATA_DIR / "quotazioni.csv"
STATS_PATH = DATA_DIR / "statistiche_giocatori.csv"
MARKET_PATH = DATA_DIR / "market_latest.json"
MARKET_REPORT_GLOB = "rose_changes_*.csv"
STATS_DIR = DATA_DIR / "stats"
PLAYER_CARDS_PATH = DATA_DIR / "db" / "quotazioni_master.csv"
ROSE_XLSX_DIR = DATA_DIR / "archive" / "incoming" / "rose"
_RESIDUAL_CREDITS_CACHE: Dict[str, object] = {}


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _matches(text: str, query: str) -> bool:
    return query.lower() in text.lower()


def _normalize_name(value: str) -> str:
    import unicodedata

    value = (value or "").replace("*", "").strip().lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", "", value)
    return value



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
    }
    df = df.rename(columns=col_map)
    out = {}
    for _, r in df.iterrows():
        name = str(r.get("Giocatore", "")).strip()
        if not name:
            continue
        out[_normalize_name(name)] = {
            "Squadra": r.get("Squadra", ""),
            "PrezzoAttuale": r.get("PrezzoAttuale", 0),
        }
    return out


def _load_player_cards_map() -> Dict[str, Dict[str, str]]:
    rows = _read_csv(PLAYER_CARDS_PATH)
    out = {}
    for row in rows:
        name = (row.get("nome") or "").strip()
        if not name:
            continue
        out[_normalize_name(name)] = {
            "Squadra": row.get("club", ""),
            "PrezzoAttuale": row.get("QA", 0),
        }
    return out


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
    try:
        import pandas as pd
    except Exception:
        return {}

    df = pd.read_excel(path, header=None)
    credits: Dict[str, float] = {}
    left_team = ""
    right_team = ""
    pending_left: Optional[float] = None
    pending_right: Optional[float] = None
    header_tokens = {"Ruolo", "Calciatore", "Squadra", "Costo"}
    for _, row in df.iterrows():
        left_cell = row.iloc[0]
        right_cell = row.iloc[5] if len(row) > 5 else None

        if isinstance(left_cell, str):
            value = left_cell.strip()
            if value and value not in header_tokens and "Crediti Residui" not in value:
                left_team = value
                if pending_left is not None:
                    credits[_normalize_name(left_team)] = pending_left
                    pending_left = None
            elif "Crediti Residui" in value and left_team:
                match = re.search(r"Crediti\\s+Residui:\\s*(\\d+(?:[\\.,]\\d+)?)", value)
                if match:
                    credits[_normalize_name(left_team)] = float(match.group(1).replace(",", "."))
            elif "Crediti Residui" in value and not left_team:
                match = re.search(r"Crediti\\s+Residui:\\s*(\\d+(?:[\\.,]\\d+)?)", value)
                if match:
                    pending_left = float(match.group(1).replace(",", "."))

        if isinstance(right_cell, str):
            value = right_cell.strip()
            if value and value not in header_tokens and "Crediti Residui" not in value:
                right_team = value
                if pending_right is not None:
                    credits[_normalize_name(right_team)] = pending_right
                    pending_right = None
            elif "Crediti Residui" in value and right_team:
                match = re.search(r"Crediti\\s+Residui:\\s*(\\d+(?:[\\.,]\\d+)?)", value)
                if match:
                    credits[_normalize_name(right_team)] = float(match.group(1).replace(",", "."))
            elif "Crediti Residui" in value and not right_team:
                match = re.search(r"Crediti\\s+Residui:\\s*(\\d+(?:[\\.,]\\d+)?)", value)
                if match:
                    pending_right = float(match.group(1).replace(",", "."))

    _RESIDUAL_CREDITS_CACHE["path"] = str(path)
    _RESIDUAL_CREDITS_CACHE["mtime"] = mtime
    _RESIDUAL_CREDITS_CACHE["data"] = credits
    return credits


def _build_market_placeholder() -> Dict[str, List[Dict[str, str]]]:
    report_path = _latest_market_report()
    if not report_path:
        return {"items": [], "teams": []}
    rose_rows = _read_csv(ROSE_PATH)
    quot_rows = _read_csv(QUOT_PATH)
    old_quot_map = _load_old_quotazioni_map()
    player_cards_map = _load_player_cards_map()
    quot_map = {}
    for row in quot_rows:
        name = (row.get("Giocatore") or "").strip()
        if not name:
            continue
        quot_map[_normalize_name(name)] = {
            "Squadra": row.get("Squadra", ""),
            "PrezzoAttuale": row.get("PrezzoAttuale", 0),
        }
    rose_team_map: Dict[str, Dict[str, Dict[str, str]]] = defaultdict(dict)
    for row in rose_rows:
        team = (row.get("Team") or "").strip()
        name = (row.get("Giocatore") or "").strip()
        if not team or not name:
            continue
        rose_team_map[team.lower()][_normalize_name(name)] = {
            "Squadra": row.get("Squadra", ""),
            "PrezzoAttuale": row.get("PrezzoAttuale", 0),
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
        pairs = max(len(added), len(removed))
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
        for i in range(pairs):
            out_name = removed[i] if i < len(removed) else ""
            in_name = added[i] if i < len(added) else ""
            out_key = _normalize_name(out_name)
            in_key = _normalize_name(in_name)
            out_info = (
                (player_cards_map.get(out_key) if out_name.strip().endswith("*") else None)
                or (old_quot_map.get(out_key) if out_name.strip().endswith("*") else None)
                or team_map.get(out_key)
                or quot_map.get(out_key)
            )
            in_info = (
                (player_cards_map.get(in_key) if in_name.strip().endswith("*") else None)
                or (old_quot_map.get(in_key) if in_name.strip().endswith("*") else None)
                or team_map.get(in_key)
                or quot_map.get(in_key)
            )
            out_value = float((out_info or {}).get("PrezzoAttuale", 0) or 0)
            in_value = float((in_info or {}).get("PrezzoAttuale", 0) or 0)
            items.append(
                {
                    "team": team,
                    "date": stamp,
                    "out": out_name,
                    "out_missing": out_name.strip().endswith("*"),
                    "out_squadra": (out_info or {}).get("Squadra", ""),
                    "out_value": out_value,
                    "in": in_name,
                    "in_missing": in_name.strip().endswith("*"),
                    "in_squadra": (in_info or {}).get("Squadra", ""),
                    "in_value": in_value,
                    "delta": in_value - out_value,
                }
            )
    return {"items": items, "teams": teams}


def _build_market_suggest_payload(team_name: str, db: Session) -> Dict[str, object]:
    rose_rows = _read_csv(ROSE_PATH)
    team_key = _normalize_name(team_name)
    residual_map = _load_residual_credits_map()
    credits_residui = float(residual_map.get(team_key, 0) or 0)
    user_squad = []
    for row in rose_rows:
        if _normalize_name(row.get("Team", "")) != team_key:
            continue
        user_squad.append(
            {
                "Giocatore": row.get("Giocatore", ""),
                "Ruolo": row.get("Ruolo", ""),
                "Squadra": row.get("Squadra", ""),
                "PrezzoAttuale": row.get("PrezzoAttuale", 0),
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

    current_round = min(rounds) if rounds else 1

    return {
        "user_squad": user_squad,
        "credits_residui": credits_residui,
        "players_pool": players_pool,
        "teams_data": teams_data,
        "fixtures": fixtures,
        "currentRound": current_round,
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
    rose = _read_csv(ROSE_PATH)
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
    rose = _read_csv(ROSE_PATH)
    ruolo = ruolo.upper()
    order = order.strip().lower()
    items_map: Dict[str, Dict[str, str]] = {}
    for row in rose:
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
        if not current or price > float(current.get("PrezzoAttuale", 0) or 0):
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
    rose = _read_csv(ROSE_PATH)
    team_key = _normalize_name(team_name)
    items = [row for row in rose if _normalize_name(row.get("Team", "")) == team_key]
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
                "Giocatore": row.get("Giocatore", ""),
                "Squadra": row.get("Squadra", ""),
                "Punteggio": round(score, 1),
            }
        )
    items.sort(key=lambda x: x["Punteggio"], reverse=True)
    return {"items": items[:limit]}


@router.get("/stats/player")
def stats_player(name: str = Query(..., min_length=1)):
    stats = _read_csv(STATS_PATH)
    target = name.strip().lower()
    for row in stats:
        if row.get("Giocatore", "").strip().lower() == target:
            return {"item": row}
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
    return {"items": items[:limit]}


@router.get("/market")
def market():
    if not MARKET_PATH.exists():
        return _build_market_placeholder()
    try:
        import json

        data = json.loads(MARKET_PATH.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            items = data.get("items", [])
            teams = data.get("teams", [])
            if not items:
                return _build_market_placeholder()
            return {"items": items, "teams": teams}
        if isinstance(data, list):
            if not data:
                return _build_market_placeholder()
            return {"items": data, "teams": []}
        return _build_market_placeholder()
    except json.JSONDecodeError:
        return _build_market_placeholder()


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

    def _norm_name(name: str) -> str:
        value = (name or "").lower()
        value = re.sub(r"[^a-z0-9]+", "", value)
        return value

    def _swap_key(swap) -> tuple[str, str]:
        return (
            _norm_name(swap.out_player.get("nome") or swap.out_player.get("Giocatore") or ""),
            _norm_name(swap.in_player.get("nome") or swap.in_player.get("Giocatore") or ""),
        )

    def _role_of(player: dict) -> str:
        return str(player.get("ruolo_base") or player.get("Ruolo") or "").upper()

    def _has_role_diversity(sol) -> bool:
        roles = {_role_of(s.in_player) for s in sol.swaps}
        return "C" in roles and "A" in roles

    def _diff_swaps(a, b) -> int:
        a_set = {_swap_key(s) for s in a.swaps}
        b_set = {_swap_key(s) for s in b.swaps}
        return len(a_set - b_set)

    def _run_suggest(local_squad, local_pool, seed: int):
        pool_copy = list(local_pool)
        random.Random(seed).shuffle(pool_copy)
        k_pool = max(int(params.get("k_pool", 60)), 120)
        m_out = max(int(params.get("m_out", 8)), 15)
        beam_width = max(int(params.get("beam_width", 200)), 500)
        return suggest_transfers(
            user_squad=local_squad,
            credits_residui=credits,
            players_pool=pool_copy,
            teams_data=teams_data,
            fixtures=fixtures,
            current_round=current_round,
            max_changes=int(params.get("max_changes", 5)),
            k_pool=k_pool,
            m_out=m_out,
            beam_width=beam_width,
            seed=seed,
            allow_overbudget=True,
            max_negative_gain=-5.0,
            max_negative_swaps=3,
            max_negative_sum=6.0,
            require_roles={"C", "A"},
        )

    selected = []
    exclude_outs = set()
    exclude_ins = set()
    for idx in range(3):
        filtered_squad = [
            p for p in user_squad
            if _norm_name(p.get("nome") or p.get("Giocatore") or "") not in exclude_outs
        ]
        filtered_pool = [
            p for p in players_pool
            if _norm_name(p.get("nome") or p.get("Giocatore") or "") not in exclude_ins
        ]
        pool = _run_suggest(filtered_squad, filtered_pool, seed=idx + len(exclude_outs))
        if not pool:
            break
        diverse_pool = [sol for sol in pool if _has_role_diversity(sol)]
        if diverse_pool:
            pool = diverse_pool
        pick = None
        for sol in pool:
            if all(_diff_swaps(sol, prev) >= 3 for prev in selected):
                pick = sol
                break
        if not pick:
            break
        selected.append(pick)
        swaps_sorted = sorted(pick.swaps, key=lambda s: s.gain, reverse=True)
        added = 0
        for s in swaps_sorted:
            out_name = s.out_player.get("nome") or s.out_player.get("Giocatore") or ""
            if out_name.strip().endswith(" *"):
                continue
            in_name = s.in_player.get("nome") or s.in_player.get("Giocatore") or ""
            exclude_outs.add(_norm_name(out_name))
            exclude_ins.add(_norm_name(in_name))
            added += 1
            if added >= 3:
                break

    solutions = selected
    if solutions:
        base_sol = solutions[0]
        base_outs = {
            _norm_name(s.out_player.get("nome") or s.out_player.get("Giocatore") or "")
            for s in base_sol.swaps
        }
        base_ins = {
            _norm_name(s.in_player.get("nome") or s.in_player.get("Giocatore") or "")
            for s in base_sol.swaps
        }
        squad_names = {
            _norm_name(p.get("nome") or p.get("Giocatore") or "")
            for p in user_squad
            if p.get("nome") or p.get("Giocatore")
        }

        in_pool_by_role = {"P": [], "D": [], "C": [], "A": []}
        for p in players_pool:
            name = p.get("nome") or p.get("Giocatore")
            if not name:
                continue
            if _norm_name(name) in squad_names:
                continue
            if str(name).strip().endswith(" *"):
                continue
            role = _role_of(p)
            if role in in_pool_by_role:
                in_pool_by_role[role].append(p)

        def _alt_candidates(out_player: dict) -> list[dict]:
            role = _role_of(out_player)
            out_qa = float(out_player.get("QA") or out_player.get("PrezzoAttuale") or 0)
            pool = []
            for p in in_pool_by_role.get(role, []):
                name = p.get("nome") or p.get("Giocatore")
                if not name:
                    continue
                n = _norm_name(name)
                if n in base_ins:
                    continue
                in_qa = float(p.get("QA") or p.get("PrezzoAttuale") or 0)
                if in_qa > out_qa + 1:
                    continue
                pool.append(p)
            pool.sort(
                key=lambda x: value_season(x, players_pool, teams_data, fixtures, current_round),
                reverse=True,
            )
            return pool

        def _build_variant(offset: int) -> Solution | None:
            swaps = list(base_sol.swaps)
            replaced = 0
            for idx, s in enumerate(base_sol.swaps):
                if replaced >= 3:
                    break
                alts = _alt_candidates(s.out_player)
                if len(alts) <= offset:
                    continue
                alt = alts[offset]
                swaps[idx] = Swap(
                    s.out_player,
                    alt,
                    s.gain,
                    s.qa_out,
                    float(alt.get("QA") or alt.get("PrezzoAttuale") or 0),
                )
                replaced += 1
            if replaced < 3:
                return None
            return Solution(
                swaps=swaps,
                budget_initial=base_sol.budget_initial,
                budget_final=base_sol.budget_final,
                total_gain=base_sol.total_gain,
                recommended_outs=base_sol.recommended_outs,
                warnings=base_sol.warnings,
            )

        def _diff_swaps_min(sol_a, sol_b) -> int:
            a_set = {_swap_key(s) for s in sol_a.swaps}
            b_set = {_swap_key(s) for s in sol_b.swaps}
            return len(a_set - b_set)

        needs_variants = False
        if len(solutions) >= 2 and _diff_swaps_min(solutions[0], solutions[1]) < 3:
            needs_variants = True
        if len(solutions) >= 3 and _diff_swaps_min(solutions[0], solutions[2]) < 3:
            needs_variants = True

        if len(solutions) < 3 or needs_variants:
            variant1 = _build_variant(0)
            variant2 = _build_variant(1)
            if variant1:
                if len(solutions) >= 2:
                    solutions[1] = variant1
                else:
                    solutions.append(variant1)
            if variant2:
                if len(solutions) >= 3:
                    solutions[2] = variant2
                else:
                    solutions.append(variant2)

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
