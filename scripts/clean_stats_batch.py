import argparse
import hashlib
import json
import sys
from pathlib import Path
import subprocess
import shutil
from datetime import date
import csv
import unicodedata
import re

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.app.backup import run_backup_fail_fast
from apps.api.app.config import BACKUP_DIR, BACKUP_KEEP_LAST, DATABASE_URL

DATA_DIR = ROOT / "data"
TEMPLATE_DIR = DATA_DIR / "templates" / "stats"
OUT_DIR = DATA_DIR / "stats"
STATE_PATH = DATA_DIR / "history" / "last_stats_update.json"
INCOMING_DIR = DATA_DIR / "incoming" / "stats"
ARCHIVE_INCOMING = DATA_DIR / "archive" / "incoming" / "stats"
HISTORY_DIR = DATA_DIR / "history" / "stats_raw"
CLEANER_PATH = ROOT / "scripts" / "clean_stats.py"

STATS = [
    ("gol_template.csv", "gol.csv", "Gol"),
    ("assist_template.csv", "assist.csv", "Assist"),
    ("ammonizioni_template.csv", "ammonizioni.csv", "Ammonizioni"),
    ("cleansheet_template.csv", "cleansheet.csv", "Cleansheet"),
    ("espulsioni_template.csv", "espulsioni.csv", "Espulsioni"),
    ("autogol_template.csv", "autogol.csv", "Autogol"),
    ("rigoriparati_template.csv", "rigoriparati.csv", "RigoriParati"),
    ("rigorisegnati_template.csv", "rigorisegnati.csv", "RigoriSegnati"),
    ("rigorisbagliati_template.csv", "rigorisbagliati.csv", "RigoriSbagliati"),
    ("gol_subiti_template.csv", "gol_subiti.csv", "GolSubiti"),
    ("partite_template.csv", "partite.csv", "Partite"),
    ("mediavoto_template.csv", "mediavoto.csv", "Mediavoto"),
    ("fantamedia_template.csv", "fantamedia.csv", "Fantamedia"),
    ("gwin_template.csv", "gwin.csv", "GolVittoria"),
    ("gpar_template.csv", "gpar.csv", "GolPareggio"),
]

STAT_FILES = {
    "Gol": "gol.csv",
    "Assist": "assist.csv",
    "Ammonizioni": "ammonizioni.csv",
    "Espulsioni": "espulsioni.csv",
    "Cleansheet": "cleansheet.csv",
    "Autogol": "autogol.csv",
    "RigoriParati": "rigoriparati.csv",
    "RigoriSegnati": "rigorisegnati.csv",
    "RigoriSbagliati": "rigorisbagliati.csv",
    "GolSubiti": "gol_subiti.csv",
    "Partite": "partite.csv",
    "Mediavoto": "mediavoto.csv",
    "Fantamedia": "fantamedia.csv",
    "GolVittoria": "gwin.csv",
    "GolPareggio": "gpar.csv",
}

STATS_PLAYERS_PATH = DATA_DIR / "statistiche_giocatori.csv"
ROSE_PATH = DATA_DIR / "rose_fantaportoscuso.csv"
QUOT_PATH = DATA_DIR / "quotazioni.csv"
QUOT_MASTER_PATH = DATA_DIR / "db" / "quotazioni_master.csv"
HIST_QUOT_DIR = DATA_DIR / "history" / "quotazioni"
RIGORISTI_TEMPLATE_PATH = DATA_DIR / "templates" / "rigoristi_template.csv"
PLAYER_STATS_PATH = DATA_DIR / "db" / "player_stats.csv"
RIGORISTI_REPORT_PATH = DATA_DIR / "reports" / "rigoristi_missing_report.txt"
R8_DISORDINATI_PATH = DATA_DIR / "templates" / "stats" / "R8_disordinati_template.csv"

NAME_FIXES = {
    "k ndri": "N'Dri",
    "ndri": "N'Dri",
    "b dia": "Dia",
    "aaron martin": "Martin",
    "jacobo ramon": "Ramon",
    "y mina": "Mina",
    "l pellegrini": "Pellegrini Lo.",
    "m thuram": "Thuram",
    "m lautaro": "Martinez L.",
    "lautaro m": "Martinez L.",
    "jesus rodriguez": "Rodriguez Je.",
    "g zappa": "Zappa",
    "a obert": "Obert",
    "m soule": "Soulè",
    "soule": "Soulè",
    "h calhanoglu": "Calhanoglu",
    "milinkovic savic v": "Milinkovic-Savic V.",
}

ABBR_MAP = {
    "ATA": "Atalanta",
    "BOL": "Bologna",
    "CAG": "Cagliari",
    "COM": "Como",
    "CRE": "Cremonese",
    "EMP": "Empoli",
    "FIO": "Fiorentina",
    "GEN": "Genoa",
    "INT": "Inter",
    "JUV": "Juventus",
    "LAZ": "Lazio",
    "LEC": "Lecce",
    "MIL": "Milan",
    "NAP": "Napoli",
    "PAR": "Parma",
    "PIS": "Pisa",
    "ROM": "Roma",
    "SAS": "Sassuolo",
    "TOR": "Torino",
    "UDI": "Udinese",
    "VER": "Verona",
}


def norm(text: str) -> str:
    text = str(text)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"['`´’]", "", text)
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    text = " ".join(text.split())
    return text


def norm_team(raw: str) -> str:
    team = str(raw or "").strip()
    if not team:
        return ""
    key = re.sub(r"[^A-Za-z]", "", team).upper()
    if key in ABBR_MAP:
        return ABBR_MAP[key]
    return team.title()


def _iter_quotazioni_files() -> list[Path]:
    files: list[Path] = []
    if HIST_QUOT_DIR.exists():
        files.extend(sorted(HIST_QUOT_DIR.glob("quotazioni_*.csv"), key=lambda p: p.stat().st_mtime))
    if QUOT_PATH.exists():
        files.append(QUOT_PATH)
    if not files and QUOT_MASTER_PATH.exists():
        files.append(QUOT_MASTER_PATH)
    return files


def load_canon() -> tuple[list[tuple[str, str]], dict[str, str], dict[str, str], dict[str, str]]:
    files = _iter_quotazioni_files()
    if not files:
        return [], {}, {}, {}
    latest_path = QUOT_PATH if QUOT_PATH.exists() else files[-1]
    latest_names: set[str] = set()
    try:
        latest_df = pd.read_csv(latest_path)
        for _, row in latest_df.iterrows():
            name = str(row.get("Giocatore") or row.get("nome") or "").strip()
            if name:
                latest_names.add(norm(name))
    except Exception:
        latest_names = set()

    name_map: dict[str, str] = {}
    team_map: dict[str, str] = {}
    role_map: dict[str, str] = {}
    for path in reversed(files):
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        for _, row in df.iterrows():
            base = str(row.get("Giocatore") or row.get("nome") or "").strip()
            if not base:
                continue
            key = norm(re.sub(r"\s*\*\s*$", "", base))
            if key not in name_map:
                name_map[key] = re.sub(r"\s*\*\s*$", "", base).strip()
            team = norm_team(str(row.get("Squadra") or row.get("club") or "").strip())
            role = str(row.get("Ruolo") or row.get("ruolo") or "").strip()
            if team and key not in team_map:
                team_map[key] = team
            if role and key not in role_map:
                role_map[key] = role

    canon_list: list[tuple[str, str]] = []
    display_map: dict[str, str] = {}
    for key, base in name_map.items():
        display = f"{base} *" if key not in latest_names else base
        canon_list.append((key, display))
        display_map[key] = display
    return canon_list, team_map, role_map, display_map


def canon_initial(name: str) -> str:
    last = name.split()[-1]
    return last[0].upper() if last else ""


def resolve_name(raw: str, team: str, canon_list, team_map) -> tuple[str, bool]:
    raw = str(raw).strip()
    if not raw:
        return raw, False
    fixed = NAME_FIXES.get(norm(raw))
    if fixed:
        fixed_key = norm(fixed)
        for k, display in canon_list:
            if k == fixed_key:
                return display, True
        return fixed, True
    key = norm(raw)
    exact = [name for k, name in canon_list if k == key]
    if exact:
        return exact[0], True

    team = norm_team(team)

    m = re.match(r"^(?P<init>[A-Z])\.?\s+(?P<rest>.*)$", raw)
    init = None
    base = None
    if m:
        init = m.group("init").upper()
        base = m.group("rest").strip()
        key2 = norm(base)
        candidates = [(k, name) for k, name in canon_list if k.startswith(key2) or key2 in k]
        if team:
            filtered = [
                pair for pair in candidates if norm_team(team_map.get(pair[0], "")) == team
            ]
            candidates = filtered or candidates
        if len(candidates) == 1:
            return candidates[0][1], True
        if len(candidates) > 1 and init:
            init_matches = [n for _, n in candidates if canon_initial(n) == init]
            if len(init_matches) == 1:
                return init_matches[0], True

    parts = key.split()
    last = parts[-1] if parts else key
    candidates = [(k, name) for k, name in canon_list if k.startswith(last) or k.endswith(last) or last in k]
    if team:
        filtered = [
            pair for pair in candidates if norm_team(team_map.get(pair[0], "")) == team
        ]
        candidates = filtered or candidates
    if len(candidates) == 1:
        return candidates[0][1], True

    return raw, False


def split_row(line: str) -> list[str]:
    if "\t" in line:
        return [col.strip() for col in line.split("\t")]
    return [col.strip() for col in next(csv.reader([line]))]


def update_rigoristi_from_template() -> None:
    if not RIGORISTI_TEMPLATE_PATH.exists():
        return
    raw = RIGORISTI_TEMPLATE_PATH.read_text(encoding="utf-8").strip()
    if not raw:
        return

    canon_list, team_map, _, _ = load_canon()
    lines = [line.strip().lstrip("\ufeff") for line in raw.splitlines()]
    lines = [line for line in lines if line]

    idx = None
    for i, line in enumerate(lines):
        if line.lower().startswith("classifica rigoristi teorici"):
            idx = i
            break

    penalty_lines = lines if idx is None else lines[:idx]
    top_lines = [] if idx is None else lines[idx:]

    penalty_map: dict[str, dict[str, int]] = {}
    missing_penalty: list[str] = []

    for line in penalty_lines:
        if line.lower().startswith("giocatore"):
            continue
        parts = split_row(line)
        if len(parts) < 4:
            continue
        name_raw = parts[0]
        team = parts[2] if len(parts) > 2 else ""
        rig_seg = parts[3] if len(parts) > 3 else "0"
        rig_sbagl = parts[4] if len(parts) > 4 else "0"
        name, ok = resolve_name(name_raw, team, canon_list, team_map)
        if not ok:
            missing_penalty.append(name_raw)
        try:
            seg_val = int(float(rig_seg or 0))
        except ValueError:
            seg_val = 0
        try:
            sb_val = int(float(rig_sbagl or 0))
        except ValueError:
            sb_val = 0
        penalty_map[name] = {"seg": seg_val, "sbagl": sb_val}

    pk_role_map: dict[str, float] = {}
    missing_pk: list[str] = []
    if top_lines:
        header = split_row(top_lines[0])
        teams = header[1:] if header else []
        for row in top_lines[1:4]:
            parts = split_row(row)
            if not parts:
                continue
            try:
                rank = int(parts[0])
            except ValueError:
                continue
            pk_value = {1: 1.0, 2: 0.8, 3: 0.5}.get(rank)
            if pk_value is None:
                continue
            for col_idx, team in enumerate(teams, start=1):
                if col_idx >= len(parts):
                    continue
                player_raw = parts[col_idx].strip()
                if not player_raw:
                    continue
                name, ok = resolve_name(player_raw, team, canon_list, team_map)
                if not ok:
                    missing_pk.append(player_raw)
                current = pk_role_map.get(name, 0)
                pk_role_map[name] = max(current, pk_value)

    if not PLAYER_STATS_PATH.exists():
        return

    df = pd.read_csv(PLAYER_STATS_PATH)
    if "RIGSBAGL_S" not in df.columns:
        df["RIGSBAGL_S"] = 0
    if "RIGSBAGL_R8" not in df.columns:
        df["RIGSBAGL_R8"] = 0
    if "PKRole" not in df.columns:
        df["PKRole"] = 0

    for name, stats in penalty_map.items():
        mask = df["Giocatore"] == name
        if not mask.any():
            continue
        df.loc[mask, "RIGSEG_S"] = stats["seg"]
        df.loc[mask, "RIGSBAGL_S"] = stats["sbagl"]

    for name, pk_val in pk_role_map.items():
        mask = df["Giocatore"] == name
        if not mask.any():
            continue
        df.loc[mask, "PKRole"] = pk_val

    numeric_cols = ["RIGSEG_S", "RIGSEG_R8", "RIGSBAGL_S", "RIGSBAGL_R8", "PKRole"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df.to_csv(PLAYER_STATS_PATH, index=False)

    if missing_penalty or missing_pk:
        lines_out = ["MISSING RIGORI:\n"]
        for name in sorted(set(missing_penalty)):
            lines_out.append(f"- {name}")
        lines_out.append("\nMISSING PKROLE:\n")
        for name in sorted(set(missing_pk)):
            lines_out.append(f"- {name}")
        RIGORISTI_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        RIGORISTI_REPORT_PATH.write_text("\n".join(lines_out), encoding="utf-8")
    elif RIGORISTI_REPORT_PATH.exists():
        RIGORISTI_REPORT_PATH.unlink()


def update_r8_disordinati_template() -> list[str]:
    if not R8_DISORDINATI_PATH.exists() or not PLAYER_STATS_PATH.exists():
        return []

    raw = R8_DISORDINATI_PATH.read_text(encoding="utf-8").strip()
    if not raw:
        return []

    canon_list, team_map, _, _ = load_canon()
    current_col = None
    rows: list[tuple[str, str, float, str]] = []

    for line in raw.splitlines():
        line = line.strip().lstrip("\ufeff")
        if not line:
            continue
        parts = split_row(line)
        if len(parts) < 3:
            continue

        header = parts[2].lower()
        if "rig parati" in header:
            current_col = "RIGPAR_R8"
            continue
        if "autogol" in header:
            current_col = "AUTOGOL_R8"
            continue
        if "rigori sbagliati" in header:
            current_col = "RIGSBAGL_R8"
            continue
        if "gol subiti" in header:
            current_col = "GOLS_R8"
            continue
        if parts[0].lower() == "nome":
            continue

        if not current_col:
            continue
        name_raw = parts[0]
        team = parts[1] if len(parts) > 1 else ""
        value_raw = parts[2] if len(parts) > 2 else "0"
        try:
            value = float(value_raw)
        except ValueError:
            value = 0.0
        rows.append((name_raw, team, value, current_col))

    if not rows:
        return []

    df = pd.read_csv(PLAYER_STATS_PATH)
    for col in ["RIGPAR_R8", "AUTOGOL_R8", "RIGSBAGL_R8", "GOLS_R8"]:
        if col not in df.columns:
            df[col] = 0

    missing: list[str] = []
    for name_raw, team, value, col in rows:
        name, ok = resolve_name(name_raw, team, canon_list, team_map)
        if not ok:
            missing.append(name_raw)
        mask = df["Giocatore"] == name
        if not mask.any():
            continue
        df.loc[mask, col] = value

    for col in ["RIGPAR_R8", "AUTOGOL_R8", "RIGSBAGL_R8", "GOLS_R8"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    df.to_csv(PLAYER_STATS_PATH, index=False)
    return missing


def clean_simple_stat(template_path: Path, out_path: Path, stat_col: str, report_path: Path) -> None:
    canon_list, team_map, _, _ = load_canon()
    df = pd.read_csv(template_path)
    col_map = {
        "GolVittoria": ["GolVittoria", "Gol Vittoria"],
        "GolPareggio": ["GolPareggio", "Goal Pareggio"],
    }
    candidates = col_map.get(stat_col, [stat_col])
    stat_source = None
    for c in candidates:
        if c in df.columns:
            stat_source = c
            break
    if "Giocatore" not in df.columns or not stat_source:
        return

    missing = []
    rows = []
    for _, row in df.iterrows():
        raw_name = str(row.get("Giocatore", "")).strip()
        if not raw_name:
            continue
        resolved, ok = resolve_name(raw_name, "", canon_list, team_map)
        if not ok:
            missing.append(raw_name)
        value = pd.to_numeric(row.get(stat_source, 0), errors="coerce")
        if pd.isna(value):
            value = 0
        rows.append({"Giocatore": resolved, stat_col: value})

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out = pd.DataFrame(rows)
    if stat_col in df_out.columns:
        df_out[stat_col] = pd.to_numeric(df_out[stat_col], errors="coerce").fillna(0)
        df_out = df_out.sort_values(by=[stat_col, "Giocatore"], ascending=[False, True])
    df_out.to_csv(out_path, index=False)

    if missing:
        lines = ["MISSING:\n"]
        for name in sorted(set(missing)):
            lines.append(f"- {name}")
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text("\n".join(lines), encoding="utf-8")
    elif report_path.exists():
        report_path.unlink()


def file_hash(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def signature_for(template_path: Path) -> str:
    base = file_hash(template_path)
    if CLEANER_PATH.exists():
        return f"{base}:{file_hash(CLEANER_PATH)}"
    return base


def load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")

def today_stamp() -> str:
    return date.today().strftime("%Y-%m-%d")

def rotate_history(folder: Path, prefix: str, keep: int) -> None:
    files = sorted(folder.glob(f"{prefix}_*.*"), key=lambda p: p.name)
    if len(files) <= keep:
        return
    for old in files[: len(files) - keep]:
        old.unlink(missing_ok=True)

def latest_incoming(prefix: str) -> Path | None:
    if not INCOMING_DIR.exists():
        return None
    candidates = list(INCOMING_DIR.glob(f"{prefix}_*.csv")) + list(INCOMING_DIR.glob(f"{prefix}_*.xlsx"))
    if not candidates:
        return None
    pattern = re.compile(rf"^{re.escape(prefix)}_\d{{4}}-\d{{2}}-\d{{2}}$")
    filtered = [p for p in candidates if pattern.match(p.stem)]
    if filtered:
        candidates = filtered
    return max(candidates, key=lambda p: p.stat().st_mtime)

def archive_incoming(path: Path, keep: int) -> None:
    ARCHIVE_INCOMING.mkdir(parents=True, exist_ok=True)
    dest = ARCHIVE_INCOMING / path.name
    path.replace(dest)
    rotate_history(ARCHIVE_INCOMING, path.stem.split("_")[0], keep)

def archive_template(path: Path, prefix: str, keep: int) -> None:
    if not path.exists():
        return
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    dest = HISTORY_DIR / f"{prefix}_{today_stamp()}.csv"
    path.replace(dest)
    rotate_history(HISTORY_DIR, prefix, keep)


def load_role_map() -> tuple[dict, dict]:
    role_map = {}
    team_map = {}
    if not ROSE_PATH.exists():
        return role_map, team_map
    with ROSE_PATH.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = str(row.get("Giocatore", "")).strip()
            if not name:
                continue
            if name not in role_map and row.get("Ruolo"):
                role_map[name] = str(row.get("Ruolo", "")).strip()
            if name not in team_map and row.get("Squadra"):
                team_map[name] = str(row.get("Squadra", "")).strip()
    return role_map, team_map


def update_statistiche_giocatori() -> None:
    role_map, team_map = load_role_map()

    canon_list, listone_team_map, listone_role_map, display_map = load_canon()
    base_players = [name for _, name in canon_list]
    if listone_role_map:
        for key, role in listone_role_map.items():
            if role and key not in role_map:
                role_map[display_map.get(key, key)] = role

    if STATS_PLAYERS_PATH.exists():
        base_df = pd.read_csv(STATS_PLAYERS_PATH)
    else:
        base_df = pd.DataFrame(columns=["Giocatore", "Squadra"])

    def _canon_key(name: str) -> str:
        base = str(name or "").strip()
        base = re.sub(r"\s*\*\s*$", "", base)
        return norm(base)

    def _to_canon(name: str) -> str:
        key = _canon_key(name)
        return display_map.get(key, str(name or "").strip())

    base_df["Giocatore"] = base_df["Giocatore"].astype(str).str.strip().apply(_to_canon)
    base_df["Squadra"] = base_df["Squadra"].astype(str).str.strip()
    if not base_df.empty:
        base_df = base_df.drop_duplicates(subset=["Giocatore"], keep="last")

    players = set(base_players)

    if not players:
        return

    rows = []
    for name in sorted(players):
        key = _canon_key(name)
        team = listone_team_map.get(key, "")
        if not team:
            if not base_df.empty:
                match = base_df.loc[base_df["Giocatore"] == name, "Squadra"].head(1).tolist()
                team = match[0] if match else ""
        rows.append({"Giocatore": name, "Squadra": team})

    merged = pd.DataFrame(rows)

    # Ensure columns exist
    for col in [
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
    ]:
        if col not in merged.columns:
            merged[col] = 0

    # Preserve rigori columns from existing file if present
    for col in [
        "RigoriParati",
        "RigoriSegnati",
        "RigoriSbagliati",
        "GolVittoria",
        "GolPareggio",
        "Partite",
        "Mediavoto",
        "Fantamedia",
    ]:
        if col in base_df.columns:
            base_col = base_df[["Giocatore", col]].drop_duplicates(subset=["Giocatore"], keep="last")
            merged = merged.merge(
                base_col,
                on="Giocatore",
                how="left",
                suffixes=("", "_old"),
            )
            merged[col] = merged[f"{col}_old"].fillna(merged[col])
            merged = merged.drop(columns=[f"{col}_old"])

    # Merge stats from cleaned files
    for stat_name, filename in STAT_FILES.items():
        path = OUT_DIR / filename
        if not path.exists():
            continue
        try:
            df_stat = pd.read_csv(path)
        except pd.errors.EmptyDataError:
            continue
        if stat_name not in df_stat.columns:
            continue
        df_stat = df_stat[["Giocatore", stat_name]].copy()
        df_stat["Giocatore"] = (
            df_stat["Giocatore"]
            .astype(str)
            .str.strip()
            .apply(_to_canon)
        )
        # Deduplicate any repeated players in stat files (use max to avoid inflation)
        df_stat[stat_name] = pd.to_numeric(df_stat[stat_name], errors="coerce").fillna(0)
        df_stat = df_stat.groupby("Giocatore", as_index=False)[stat_name].max()
        merged = merged.merge(df_stat, on="Giocatore", how="left", suffixes=("", "_new"))
        merged[stat_name] = merged[f"{stat_name}_new"].fillna(merged[stat_name])
        merged = merged.drop(columns=[f"{stat_name}_new"])

    # Normalize numeric stats
    int_cols = [
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
    ]
    for col in int_cols:
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0).astype(int)

    for col in ["Mediavoto", "Fantamedia"]:
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0).round(2)

    # Sorting by role P-D-C-A then name
    role_order = {"P": 0, "D": 1, "C": 2, "A": 3}
    merged["__role_order"] = merged["Giocatore"].map(lambda n: role_order.get(role_map.get(n, ""), 9))
    merged = merged.sort_values(by=["__role_order", "Giocatore"], ascending=[True, True]).drop(
        columns=["__role_order"]
    )

    STATS_PLAYERS_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(STATS_PLAYERS_PATH, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean stats templates and update outputs.")
    parser.add_argument("--force", action="store_true", help="Force re-clean even if unchanged")
    args = parser.parse_args()

    state = load_state()
    last = state.get("last_signature", {})
    updated = False
    keep = state.get("keep", 5)
    backup_done = False
    backup_failed = False
    clean_failures: list[str] = []

    def ensure_backup() -> None:
        nonlocal backup_done, backup_failed
        if backup_done or backup_failed:
            return
        try:
            run_backup_fail_fast(
                DATABASE_URL,
                BACKUP_DIR,
                BACKUP_KEEP_LAST,
                prefix="stats",
                base_dir=ROOT,
            )
            backup_done = True
        except Exception as exc:
            # Do not stop the whole stats pipeline if backup is not available
            # (for example unsupported DATABASE_URL in cloud env).
            backup_failed = True
            print(f"Warning: backup skipped ({exc})")

    for template_name, out_name, stat in STATS:
        prefix = template_name.replace("_template.csv", "")
        incoming = latest_incoming(prefix)
        if incoming:
            # Move current template to history and replace with incoming
            ensure_backup()
            template_path = TEMPLATE_DIR / template_name
            archive_template(template_path, prefix, keep)
            TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy2(incoming, template_path)
            archive_incoming(incoming, keep)
            # Record signature reset
            last.pop(template_name, None)

        in_path = TEMPLATE_DIR / template_name
        if not in_path.exists() and incoming:
            ensure_backup()
            TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy2(incoming, in_path)
        if not in_path.exists():
            print(f"Template mancante: {in_path}")
            continue
        sig = signature_for(in_path)
        if last.get(template_name) == sig and not args.force:
            print(f"Update già eseguito ({template_name}).")
            continue

        out_path = OUT_DIR / out_name
        report_path = DATA_DIR / "reports" / f"{out_path.stem}_missing_report.txt"
        ensure_backup()
        if stat in ("GolVittoria", "GolPareggio"):
            try:
                clean_simple_stat(in_path, out_path, stat, report_path)
            except Exception as exc:
                clean_failures.append(f"{template_name}: {exc}")
                print(f"Warning: clean failed ({template_name}): {exc}")
                continue
        else:
            cmd = [
                sys.executable,
                str(ROOT / "scripts" / "clean_stats.py"),
                "--in",
                str(in_path),
                "--out",
                str(out_path),
                "--stat",
                stat,
                "--expand-team",
                "--report",
                str(report_path),
            ]
            try:
                subprocess.run(cmd, check=True)
            except Exception as exc:
                clean_failures.append(f"{template_name}: {exc}")
                print(f"Warning: clean failed ({template_name}): {exc}")
                continue
        last[template_name] = sig
        updated = True

    r8_sig_key = R8_DISORDINATI_PATH.name
    if R8_DISORDINATI_PATH.exists():
        r8_sig = signature_for(R8_DISORDINATI_PATH)
        if last.get(r8_sig_key) != r8_sig or args.force:
            ensure_backup()
            try:
                missing_r8 = update_r8_disordinati_template()
                last[r8_sig_key] = r8_sig
                updated = True
                if missing_r8:
                    print(f"R8 disordinati: {len(set(missing_r8))} nomi non risolti.")
            except Exception as exc:
                clean_failures.append(f"{r8_sig_key}: {exc}")
                print(f"Warning: update R8 disordinati failed: {exc}")

    if updated:
        ensure_backup()
        state["last_signature"] = last
        try:
            save_state(state)
        except Exception as exc:
            clean_failures.append(f"save_state: {exc}")
            print(f"Warning: save state failed: {exc}")
        try:
            update_statistiche_giocatori()
        except Exception as exc:
            clean_failures.append(f"update_statistiche_giocatori: {exc}")
            print(f"Warning: update statistiche_giocatori failed: {exc}")
        try:
            update_rigoristi_from_template()
        except Exception as exc:
            clean_failures.append(f"update_rigoristi_from_template: {exc}")
            print(f"Warning: update rigoristi failed: {exc}")
        if clean_failures:
            print(f"Stats cleaned with warnings ({len(clean_failures)}).")
        else:
            print("Stats cleaned.")
    else:
        try:
            update_rigoristi_from_template()
        except Exception as exc:
            clean_failures.append(f"update_rigoristi_from_template: {exc}")
            print(f"Warning: update rigoristi failed: {exc}")
        if clean_failures:
            print(f"Nessun aggiornamento statistiche (warnings: {len(clean_failures)}).")
        else:
            print("Nessun aggiornamento statistiche.")


if __name__ == "__main__":
    main()
