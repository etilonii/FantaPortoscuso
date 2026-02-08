from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
INCOMING_DIR = DATA_DIR / "incoming"
STAGING_DIR = DATA_DIR / "staging"
MARTS_DIR = DATA_DIR / "marts"
DB_DIR = DATA_DIR / "db"
DB_PATH = DB_DIR / "pipeline_v2.db"
STATE_PATH = DB_DIR / "pipeline_v2_state.json"

SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}

LEGACY_OUTPUTS = {
    "classifica": DATA_DIR / "classifica.csv",
    "rose": DATA_DIR / "rose_fantaportoscuso.csv",
    "quotazioni": DATA_DIR / "quotazioni.csv",
    "stats": DATA_DIR / "statistiche_giocatori.csv",
}

MART_OUTPUT_NAMES = {
    "classifica": "classifica.csv",
    "rose": "rose_fantaportoscuso.csv",
    "quotazioni": "quotazioni.csv",
    "stats": "statistiche_giocatori.csv",
}


@dataclass(frozen=True)
class DomainSpec:
    name: str
    parser: Callable[[Path], pd.DataFrame]
    incoming_dir: Path
    staging_dir: Path
    marts_dir: Path
    legacy_output: Path
    mart_output_name: str


def _today_stamp() -> str:
    return date.today().strftime("%Y-%m-%d")


def _norm_header(value: object) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("'", "")
    text = text.replace(".", "")
    text = text.replace("+", "plus")
    text = text.replace("-", "minus")
    text = re.sub(r"[^a-z0-9]+", "", text)
    return text


def _norm_player_key(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _clean_text_series(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )


def _to_float_series(series: pd.Series) -> pd.Series:
    cleaned = (
        series.astype("string")
        .fillna("")
        .str.strip()
        .str.replace(",", ".", regex=False)
        .replace({"": "0", "nan": "0", "None": "0"})
    )
    return pd.to_numeric(cleaned, errors="coerce").fillna(0.0)


def _to_int_series(series: pd.Series) -> pd.Series:
    return _to_float_series(series).round().astype(int)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _ensure_dirs() -> None:
    for domain in ("classifica", "rose", "quotazioni", "stats"):
        (INCOMING_DIR / domain).mkdir(parents=True, exist_ok=True)
        (STAGING_DIR / domain).mkdir(parents=True, exist_ok=True)
        (MARTS_DIR / domain).mkdir(parents=True, exist_ok=True)
    DB_DIR.mkdir(parents=True, exist_ok=True)


def _latest_file(folder: Path) -> Path | None:
    if not folder.exists():
        return None
    files = [
        p
        for p in folder.iterdir()
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
    ]
    if not files:
        return None
    return max(files, key=lambda p: p.stat().st_mtime)


def _pick_source(
    *,
    domain: str,
    incoming_dir: Path,
    legacy_output: Path,
    prefer_current: bool,
    explicit_path: str | None,
) -> Path:
    if explicit_path:
        src = Path(explicit_path).expanduser()
        if not src.is_absolute():
            src = (ROOT / src).resolve()
        if not src.exists() or not src.is_file():
            raise FileNotFoundError(f"[{domain}] source path not found: {src}")
        return src

    if prefer_current and legacy_output.exists():
        return legacy_output

    incoming = _latest_file(incoming_dir)
    if incoming is not None:
        return incoming
    if legacy_output.exists():
        return legacy_output
    raise FileNotFoundError(
        f"[{domain}] no source file found in {incoming_dir} and no fallback {legacy_output}"
    )


def _rename_by_alias(df: pd.DataFrame, alias_map: dict[str, str]) -> pd.DataFrame:
    rename_map: dict[str, str] = {}
    for col in df.columns:
        mapped = alias_map.get(_norm_header(col))
        if mapped:
            rename_map[col] = mapped
    if rename_map:
        df = df.rename(columns=rename_map)
    return df


def _require_columns(df: pd.DataFrame, required: list[str], domain: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[{domain}] missing required columns: {missing}")


def _read_any(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    return pd.read_csv(path)


def _find_header_map(raw: pd.DataFrame) -> tuple[int, dict[str, int]]:
    header_row = None
    for i in range(min(50, len(raw))):
        row = raw.iloc[i].astype(str)
        if (row == "Calciatore").any():
            header_row = i
            break
    if header_row is None:
        raise ValueError("Header row with 'Calciatore' not found in Squadre_master.")
    header = raw.iloc[header_row].astype(str).tolist()
    label_map: dict[str, int] = {}
    for idx, val in enumerate(header):
        label_map[val] = idx
    return header_row, label_map


def _parse_squadre_master(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, header=None)
    header_row, label_map = _find_header_map(raw)

    player_col = label_map.get("Calciatore")
    squadra_col = label_map.get("Squadra")
    acquisto_col = label_map.get("Q.acq.")
    attuale_col = label_map.get("Q.att.")

    if player_col is None or squadra_col is None or acquisto_col is None or attuale_col is None:
        raise ValueError("Missing required columns in Squadre_master header.")

    role_cols = [0, 8, 9]
    block_starts: list[tuple[int, int, str]] = []
    for col in role_cols:
        series = raw.iloc[:, col].astype(str)
        idxs = raw.index[series.str.contains("Crediti residui", case=False, na=False)].tolist()
        for idx in idxs:
            team_name = raw.iloc[idx - 1, col] if idx > 0 else None
            if pd.isna(team_name) or str(team_name).strip() == "":
                continue
            block_starts.append((idx, col, str(team_name).strip()))

    if not block_starts:
        raise ValueError("No team blocks found in Squadre_master.")

    block_starts.sort(key=lambda x: x[0])
    rows: list[dict[str, object]] = []
    for i, (start_idx, role_col, team_name) in enumerate(block_starts):
        end_idx = block_starts[i + 1][0] if i + 1 < len(block_starts) else len(raw)
        data_start = header_row + 1
        if data_start < start_idx:
            data_start = start_idx + 1
        for r in range(data_start, end_idx):
            role = raw.iloc[r, role_col]
            player = raw.iloc[r, player_col]
            if pd.isna(role) or pd.isna(player):
                continue
            role = str(role).strip().upper()
            if role not in {"P", "D", "C", "A"}:
                continue
            player_name = str(player).strip()
            if not player_name:
                continue
            squadra = raw.iloc[r, squadra_col]
            acquisto = raw.iloc[r, acquisto_col]
            attuale = raw.iloc[r, attuale_col]
            rows.append(
                {
                    "Team": team_name,
                    "Giocatore": player_name,
                    "Ruolo": role,
                    "Squadra": "" if pd.isna(squadra) else str(squadra).strip(),
                    "PrezzoAcquisto": 0 if pd.isna(acquisto) else acquisto,
                    "PrezzoAttuale": 0 if pd.isna(attuale) else attuale,
                }
            )
    return pd.DataFrame(rows)


def _parse_rose_nuovo(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, header=None)
    rows: list[dict[str, object]] = []

    def _is_header(row_idx: int, col_idx: int) -> bool:
        if row_idx >= len(raw):
            return False
        val = raw.iloc[row_idx, col_idx]
        return isinstance(val, str) and val.strip().lower() == "ruolo"

    def _get_team(row_idx: int, col_idx: int) -> str | None:
        if row_idx < 0 or row_idx >= len(raw):
            return None
        val = raw.iloc[row_idx, col_idx]
        if pd.isna(val):
            return None
        name = str(val).strip()
        if not name or name.lower().startswith("crediti"):
            return None
        return name

    def _parse_block(team: str, start_row: int, col_idx: int) -> None:
        for r in range(start_row, len(raw)):
            role_val = raw.iloc[r, col_idx]
            if pd.isna(role_val):
                continue
            role = str(role_val).strip().upper()
            if role.lower().startswith("crediti"):
                break
            if role.lower() == "ruolo" and r > start_row:
                break
            if role not in {"P", "D", "C", "A"}:
                continue
            player = raw.iloc[r, col_idx + 1]
            if pd.isna(player):
                continue
            player_name = str(player).strip()
            if not player_name:
                continue
            squadra = raw.iloc[r, col_idx + 2]
            costo = raw.iloc[r, col_idx + 3]
            rows.append(
                {
                    "Team": team,
                    "Giocatore": player_name,
                    "Ruolo": role,
                    "Squadra": "" if pd.isna(squadra) else str(squadra).strip(),
                    "PrezzoAcquisto": 0 if pd.isna(costo) else costo,
                    "PrezzoAttuale": 0 if pd.isna(costo) else costo,
                }
            )

    for i in range(len(raw) - 1):
        if _is_header(i + 1, 0):
            team = _get_team(i, 0)
            if team:
                _parse_block(team, i + 2, 0)
        if _is_header(i + 1, 5):
            team = _get_team(i, 5)
            if team:
                _parse_block(team, i + 2, 5)

    if not rows:
        raise ValueError("No team blocks found in rose_nuovo format.")
    return pd.DataFrame(rows)


def _parse_quotazioni_listone(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, header=None)
    header_row = None
    for i in range(min(10, len(raw))):
        row = raw.iloc[i].astype(str).str.strip().tolist()
        if "Nome" in row and "Qt.A" in row:
            header_row = i
            break
    if header_row is None:
        raise ValueError("Header row with 'Nome'/'Qt.A' not found in listone quotazioni.")
    df = pd.read_excel(path, header=header_row)
    col_map = {
        "Nome": "Giocatore",
        "Squadra": "Squadra",
        "Qt.A": "PrezzoAttuale",
        "Qt.I": "PrezzoIniziale",
        "R": "Ruolo",
        "RM": "RuoloMantra",
        "FVM": "FVM",
    }
    df = df.rename(columns=col_map)
    df["Giocatore"] = _clean_text_series(df["Giocatore"])
    return df


def _parse_classifica(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        raw = pd.read_excel(path, header=None)
        header_row = None
        for i in range(min(25, len(raw))):
            labels = {_norm_header(v) for v in raw.iloc[i].tolist() if str(v).strip()}
            has_team = "squadra" in labels or "team" in labels
            has_pos = "pos" in labels or "posizione" in labels
            has_points = "pttotali" in labels or "punti" in labels or "puntitotali" in labels
            if has_team and has_pos and has_points:
                header_row = i
                break
        if header_row is not None:
            df = pd.read_excel(path, header=header_row)
        else:
            df = pd.read_excel(path)
    else:
        df = pd.read_csv(path)

    alias_map = {
        "pos": "Pos",
        "posizione": "Pos",
        "squadra": "Squadra",
        "team": "Squadra",
        "partitegiocate": "Partite Giocate",
        "pg": "Partite Giocate",
        "pttotali": "Pt. totali",
        "puntitotali": "Pt. totali",
        "punti": "Pt. totali",
    }
    df = _rename_by_alias(df, alias_map)
    _require_columns(df, ["Squadra"], "classifica")

    if "Pos" not in df.columns:
        df["Pos"] = range(1, len(df) + 1)
    if "Partite Giocate" not in df.columns:
        df["Partite Giocate"] = 0
    if "Pt. totali" not in df.columns:
        df["Pt. totali"] = 0

    df = df[["Pos", "Squadra", "Partite Giocate", "Pt. totali"]].copy()
    df["Squadra"] = _clean_text_series(df["Squadra"])
    df = df[df["Squadra"] != ""].copy()
    df["Pos"] = _to_int_series(df["Pos"])
    df["Partite Giocate"] = _to_int_series(df["Partite Giocate"])
    df["Pt. totali"] = _to_float_series(df["Pt. totali"]).round(1)
    df = df.sort_values(["Pos", "Squadra"]).reset_index(drop=True)
    return df


def _parse_rose(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        try:
            df = _parse_rose_nuovo(path)
        except Exception:
            df = _parse_squadre_master(path)
    else:
        df = pd.read_csv(path)

    alias_map = {
        "team": "Team",
        "fantateam": "Team",
        "giocatore": "Giocatore",
        "calciatore": "Giocatore",
        "ruolo": "Ruolo",
        "squadra": "Squadra",
        "prezzoacquisto": "PrezzoAcquisto",
        "qacq": "PrezzoAcquisto",
        "prezzoattuale": "PrezzoAttuale",
        "qatt": "PrezzoAttuale",
    }
    df = _rename_by_alias(df, alias_map)
    required = ["Team", "Giocatore", "Ruolo", "Squadra", "PrezzoAcquisto", "PrezzoAttuale"]
    _require_columns(df, required, "rose")
    df = df[required].copy()

    df["Team"] = _clean_text_series(df["Team"])
    df["Giocatore"] = _clean_text_series(df["Giocatore"])
    df["Ruolo"] = _clean_text_series(df["Ruolo"]).str[:1].str.upper()
    df["Squadra"] = _clean_text_series(df["Squadra"])
    df["PrezzoAcquisto"] = _to_float_series(df["PrezzoAcquisto"]).round(2)
    df["PrezzoAttuale"] = _to_float_series(df["PrezzoAttuale"]).round(2)

    df = df[(df["Team"] != "") & (df["Giocatore"] != "")].copy()
    df = df[df["Ruolo"].isin({"P", "D", "C", "A"})].copy()
    df = df.drop_duplicates(subset=["Team", "Giocatore"], keep="last")
    df = df.sort_values(["Team", "Ruolo", "Giocatore"]).reset_index(drop=True)
    return df


def _parse_quotazioni(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = _parse_quotazioni_listone(path)
    else:
        df = pd.read_csv(path)

    alias_map = {
        "id": "Id",
        "ruolo": "Ruolo",
        "ruolomantra": "RuoloMantra",
        "giocatore": "Giocatore",
        "nome": "Giocatore",
        "squadra": "Squadra",
        "prezzoattuale": "PrezzoAttuale",
        "qta": "PrezzoAttuale",
        "prezzoiniziale": "PrezzoIniziale",
        "qti": "PrezzoIniziale",
        "fvm": "FVM",
    }
    df = _rename_by_alias(df, alias_map)
    _require_columns(df, ["Giocatore", "PrezzoAttuale"], "quotazioni")

    for col in ["Id", "Ruolo", "RuoloMantra", "Squadra", "PrezzoIniziale", "FVM"]:
        if col not in df.columns:
            df[col] = ""

    out_cols = [
        "Id",
        "Ruolo",
        "RuoloMantra",
        "Giocatore",
        "Squadra",
        "PrezzoAttuale",
        "PrezzoIniziale",
        "FVM",
    ]
    df = df[out_cols].copy()

    for col in ["Ruolo", "RuoloMantra", "Giocatore", "Squadra"]:
        df[col] = _clean_text_series(df[col])
    df["PrezzoAttuale"] = _to_float_series(df["PrezzoAttuale"]).round(2)
    df["PrezzoIniziale"] = _to_float_series(df["PrezzoIniziale"]).round(2)
    df["FVM"] = _to_float_series(df["FVM"]).round(2)
    df = df[df["Giocatore"] != ""].copy()

    key = df["Giocatore"].map(_norm_player_key)
    df = df.loc[~key.duplicated(keep="last")].copy()
    df = df.sort_values(["PrezzoAttuale", "Giocatore"], ascending=[False, True]).reset_index(drop=True)
    return df


def _extract_stats_sheet(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    sheet = "Tutti" if "Tutti" in xls.sheet_names else xls.sheet_names[0]
    raw = pd.read_excel(path, sheet_name=sheet, header=None)

    header_row = None
    for i in range(min(20, len(raw))):
        labels = {_norm_header(v) for v in raw.iloc[i].tolist() if str(v).strip()}
        has_name = "nome" in labels or "giocatore" in labels
        has_mv = "mediavoto" in labels or "mv" in labels
        if has_name and has_mv:
            header_row = i
            break
    if header_row is None:
        raise ValueError("Unable to detect stats header row in XLSX.")

    header = [str(v).strip() for v in raw.iloc[header_row].tolist()]
    data = raw.iloc[header_row + 1 :].copy()
    data.columns = header
    data = data.dropna(how="all")
    return data


def _parse_stats(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = _extract_stats_sheet(path)
    else:
        df = pd.read_csv(path)

    alias_map = {
        "giocatore": "Giocatore",
        "nome": "Giocatore",
        "squadra": "Squadra",
        "golfatti": "Gol",
        "gf": "Gol",
        "gol": "Gol",
        "autogol": "Autogol",
        "au": "Autogol",
        "rigoriparati": "RigoriParati",
        "rp": "RigoriParati",
        "rigorisegnati": "RigoriSegnati",
        "rplus": "RigoriSegnati",
        "rigorisbagliati": "RigoriSbagliati",
        "rminus": "RigoriSbagliati",
        "assist": "Assist",
        "ass": "Assist",
        "ammonizioni": "Ammonizioni",
        "amm": "Ammonizioni",
        "espulsioni": "Espulsioni",
        "esp": "Espulsioni",
        "cleansheet": "Cleansheet",
        "cs": "Cleansheet",
        "partitegiocate": "Partite",
        "partite": "Partite",
        "pv": "Partite",
        "mediavoto": "Mediavoto",
        "mv": "Mediavoto",
        "fantamedia": "Fantamedia",
        "fm": "Fantamedia",
        "golsubiti": "GolSubiti",
        "gs": "GolSubiti",
        "golvittoria": "GolVittoria",
        "gwin": "GolVittoria",
        "golpareggio": "GolPareggio",
        "gpar": "GolPareggio",
    }
    df = _rename_by_alias(df, alias_map)

    required = [
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
        "Partite",
        "Mediavoto",
        "Fantamedia",
        "GolSubiti",
    ]
    _require_columns(df, required, "stats")
    if "Cleansheet" not in df.columns:
        df["Cleansheet"] = 0
    if "GolVittoria" not in df.columns:
        df["GolVittoria"] = 0
    if "GolPareggio" not in df.columns:
        df["GolPareggio"] = 0

    out_cols = [
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
    ]
    df = df[out_cols].copy()
    df["Giocatore"] = _clean_text_series(df["Giocatore"])
    df["Squadra"] = _clean_text_series(df["Squadra"])

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
        "GolVittoria",
        "GolPareggio",
        "GolSubiti",
    ]
    for col in int_cols:
        df[col] = _to_int_series(df[col])
    for col in ["Mediavoto", "Fantamedia"]:
        df[col] = _to_float_series(df[col]).round(2)

    df = df[df["Giocatore"] != ""].copy()
    key = df["Giocatore"].map(_norm_player_key)
    df = df.loc[~key.duplicated(keep="last")].copy()
    df = df.sort_values(["Giocatore"]).reset_index(drop=True)
    return df


def _save_staging(df: pd.DataFrame, domain: str, stamp: str) -> Path:
    out = STAGING_DIR / domain / f"{domain}_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    return out


def _write_db_tables(conn: sqlite3.Connection, domain: str, df: pd.DataFrame) -> None:
    stg_table = f"stg_{domain}"
    mart_table = f"mart_{domain}"
    df.to_sql(stg_table, conn, if_exists="replace", index=False)
    df.to_sql(mart_table, conn, if_exists="replace", index=False)


def _create_run_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at TEXT NOT NULL,
            domain TEXT NOT NULL,
            source_path TEXT NOT NULL,
            source_sha256 TEXT NOT NULL,
            rows_count INTEGER NOT NULL,
            staging_path TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _insert_run(
    conn: sqlite3.Connection,
    *,
    run_at: str,
    domain: str,
    source_path: Path,
    source_sha256: str,
    rows_count: int,
    staging_path: Path,
) -> None:
    conn.execute(
        """
        INSERT INTO pipeline_runs (
            run_at, domain, source_path, source_sha256, rows_count, staging_path
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_at,
            domain,
            str(source_path),
            source_sha256,
            rows_count,
            str(staging_path),
        ),
    )
    conn.commit()


def _export_outputs(
    conn: sqlite3.Connection,
    *,
    domain: str,
    marts_dir: Path,
    mart_output_name: str,
    legacy_output: Path,
    write_legacy: bool,
) -> None:
    table = f"mart_{domain}"
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    marts_dir.mkdir(parents=True, exist_ok=True)
    marts_out = marts_dir / mart_output_name
    df.to_csv(marts_out, index=False)
    if write_legacy:
        legacy_output.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(legacy_output, index=False)


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _build_specs() -> dict[str, DomainSpec]:
    return {
        "classifica": DomainSpec(
            name="classifica",
            parser=_parse_classifica,
            incoming_dir=INCOMING_DIR / "classifica",
            staging_dir=STAGING_DIR / "classifica",
            marts_dir=MARTS_DIR / "classifica",
            legacy_output=LEGACY_OUTPUTS["classifica"],
            mart_output_name=MART_OUTPUT_NAMES["classifica"],
        ),
        "rose": DomainSpec(
            name="rose",
            parser=_parse_rose,
            incoming_dir=INCOMING_DIR / "rose",
            staging_dir=STAGING_DIR / "rose",
            marts_dir=MARTS_DIR / "rose",
            legacy_output=LEGACY_OUTPUTS["rose"],
            mart_output_name=MART_OUTPUT_NAMES["rose"],
        ),
        "quotazioni": DomainSpec(
            name="quotazioni",
            parser=_parse_quotazioni,
            incoming_dir=INCOMING_DIR / "quotazioni",
            staging_dir=STAGING_DIR / "quotazioni",
            marts_dir=MARTS_DIR / "quotazioni",
            legacy_output=LEGACY_OUTPUTS["quotazioni"],
            mart_output_name=MART_OUTPUT_NAMES["quotazioni"],
        ),
        "stats": DomainSpec(
            name="stats",
            parser=_parse_stats,
            incoming_dir=INCOMING_DIR / "stats",
            staging_dir=STAGING_DIR / "stats",
            marts_dir=MARTS_DIR / "stats",
            legacy_output=LEGACY_OUTPUTS["stats"],
            mart_output_name=MART_OUTPUT_NAMES["stats"],
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Pipeline v2: import incoming XLSX/CSV, normalize, load SQLite, "
            "and export canonical CSV marts."
        )
    )
    parser.add_argument(
        "--domains",
        default="classifica,rose,quotazioni,stats",
        help="Comma-separated domains: classifica,rose,quotazioni,stats",
    )
    parser.add_argument(
        "--date",
        default=_today_stamp(),
        help="Run date stamp (YYYY-MM-DD), used for staging file names.",
    )
    parser.add_argument(
        "--prefer-current",
        action="store_true",
        help="Use current data/*.csv as source before checking incoming/",
    )
    parser.add_argument(
        "--no-legacy-write",
        action="store_true",
        help="Do not overwrite legacy root CSV files in data/.",
    )
    parser.add_argument("--source-classifica", default=None, help="Explicit source path for classifica.")
    parser.add_argument("--source-rose", default=None, help="Explicit source path for rose.")
    parser.add_argument("--source-quotazioni", default=None, help="Explicit source path for quotazioni.")
    parser.add_argument("--source-stats", default=None, help="Explicit source path for stats.")
    args = parser.parse_args()

    _ensure_dirs()
    specs = _build_specs()
    selected = [d.strip().lower() for d in args.domains.split(",") if d.strip()]
    invalid = [d for d in selected if d not in specs]
    if invalid:
        raise ValueError(f"Invalid domains: {invalid}")

    explicit_by_domain = {
        "classifica": args.source_classifica,
        "rose": args.source_rose,
        "quotazioni": args.source_quotazioni,
        "stats": args.source_stats,
    }

    run_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    state = _load_state()
    state.setdefault("runs", [])
    run_record: dict[str, object] = {
        "run_at": run_at,
        "date_stamp": args.date,
        "domains": {},
    }

    with sqlite3.connect(DB_PATH) as conn:
        _create_run_table(conn)
        for domain in selected:
            spec = specs[domain]
            source = _pick_source(
                domain=domain,
                incoming_dir=spec.incoming_dir,
                legacy_output=spec.legacy_output,
                prefer_current=args.prefer_current,
                explicit_path=explicit_by_domain[domain],
            )
            source_hash = _sha256(source)
            df = spec.parser(source)
            staging_path = _save_staging(df, domain, args.date)
            _write_db_tables(conn, domain, df)
            _insert_run(
                conn,
                run_at=run_at,
                domain=domain,
                source_path=source,
                source_sha256=source_hash,
                rows_count=len(df),
                staging_path=staging_path,
            )
            _export_outputs(
                conn,
                domain=domain,
                marts_dir=spec.marts_dir,
                mart_output_name=spec.mart_output_name,
                legacy_output=spec.legacy_output,
                write_legacy=not args.no_legacy_write,
            )
            run_record["domains"][domain] = {
                "source": str(source),
                "source_sha256": source_hash,
                "rows": len(df),
                "staging": str(staging_path),
                "legacy_output_written": (not args.no_legacy_write),
            }
            print(f"[ok] {domain}: rows={len(df)} source={source}")

    runs = state.get("runs", [])
    if not isinstance(runs, list):
        runs = []
    runs.append(run_record)
    state["runs"] = runs[-50:]
    state["last_run"] = run_record
    _save_state(state)
    print(f"[ok] pipeline_v2 completed. db={DB_PATH}")


if __name__ == "__main__":
    main()
