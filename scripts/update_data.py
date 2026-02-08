import argparse
import hashlib
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import List, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.api.app.backup import run_backup_fail_fast
from apps.api.app.config import BACKUP_DIR, BACKUP_KEEP_LAST, DATABASE_URL

DATA_DIR = ROOT / "data"
ROSE_PATH = DATA_DIR / "rose_fantaportoscuso.csv"
QUOT_PATH = DATA_DIR / "quotazioni.csv"
MASTER_QUOT_PATH = DATA_DIR / "db" / "quotazioni_master.csv"
TEAMS_PATH = DATA_DIR / "db" / "teams.csv"

HIST_ROSE = DATA_DIR / "history" / "rose"
HIST_QUOT = DATA_DIR / "history" / "quotazioni"
HIST_TEAMS = DATA_DIR / "history" / "teams"
HIST_DIFFS = DATA_DIR / "history" / "diffs"
HIST_MARKET = DATA_DIR / "history" / "market"
INCOMING_ROSE = DATA_DIR / "incoming" / "rose"
INCOMING_QUOT = DATA_DIR / "incoming" / "quotazioni"
INCOMING_TEAMS = DATA_DIR / "incoming" / "teams"
ARCHIVE_INCOMING = DATA_DIR / "archive" / "incoming"
STATE_PATH = DATA_DIR / "history" / "last_update.json"
MARKET_LATEST = DATA_DIR / "market_latest.json"


def _today_stamp() -> str:
    return date.today().strftime("%Y-%m-%d")


def _rotate_history(folder: Path, pattern_prefix: str, keep: int) -> None:
    files = sorted(folder.glob(f"{pattern_prefix}_*.csv"), key=lambda p: p.name)
    if len(files) <= keep:
        return
    for old in files[: len(files) - keep]:
        old.unlink(missing_ok=True)

def _file_signature(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


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


def _latest_incoming(folder: Path) -> Path | None:
    if not folder.exists():
        return None
    candidates = list(folder.glob("*.csv")) + list(folder.glob("*.xlsx")) + list(folder.glob("*.xls"))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _read_input(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported file type: {path.suffix}")


def _write_csv(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)


def _validate_columns(df: pd.DataFrame, required: List[str], label: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{label} missing columns: {missing}")


def _is_blank(value: object) -> bool:
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _norm_key(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _row_numbers(mask: pd.Series, limit: int = 10) -> str:
    idx = mask[mask].index.tolist()[:limit]
    if not idx:
        return ""
    # +2 because CSV row numbers are 1-based and row 1 is the header.
    rows = [str(i + 2) for i in idx]
    return ", ".join(rows)


def _raise_csv_validation_error(file_label: str, issues: List[str]) -> None:
    formatted = "\n".join([f"- {msg}" for msg in issues])
    raise ValueError(f"CSV VALIDATION ERROR\nFile: {file_label}\n{formatted}")


def _validate_csv_input(
    df: pd.DataFrame,
    *,
    file_label: str,
    required_cols: List[str],
    allowed_cols: List[str],
    key_cols: List[str],
    numeric_cols: dict[str, str],
    enum_cols: dict[str, set[str]] | None = None,
) -> None:
    issues: List[str] = []

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        issues.append(f"colonne richieste mancanti: {missing}")

    extra = [c for c in df.columns if c not in allowed_cols]
    if extra:
        issues.append(f"colonne extra non previste: {extra}")

    if df.empty:
        issues.append("file vuoto: nessuna riga dati presente")

    present_required = [c for c in required_cols if c in df.columns]
    if present_required and not df.empty:
        incomplete_mask = df[present_required].apply(lambda row: any(_is_blank(v) for v in row), axis=1)
        if incomplete_mask.any():
            issues.append(
                f"righe vuote/incomplete rilevate alle righe: {_row_numbers(incomplete_mask)}"
            )

    missing_key_cols = [c for c in key_cols if c not in df.columns]
    if missing_key_cols:
        issues.append(f"colonne chiave primaria mancanti: {missing_key_cols}")
    elif key_cols and not df.empty:
        key_df = pd.DataFrame({c: df[c].map(_norm_key) for c in key_cols})
        key_blank_mask = key_df.apply(lambda row: any(v == "" for v in row), axis=1)
        if key_blank_mask.any():
            issues.append(
                f"chiave primaria vuota alle righe: {_row_numbers(key_blank_mask)}"
            )
        dup_mask = key_df.duplicated(keep=False)
        if dup_mask.any():
            issues.append(
                f"duplicati sulla chiave primaria ({', '.join(key_cols)}) alle righe: {_row_numbers(dup_mask)}"
            )

    for col, kind in numeric_cols.items():
        if col not in df.columns or df.empty:
            continue
        stripped = df[col].map(lambda x: "" if pd.isna(x) else str(x).strip())
        numeric = pd.to_numeric(df[col], errors="coerce")
        invalid_mask = (stripped != "") & numeric.isna()
        if invalid_mask.any():
            issues.append(
                f"valori non numerici in '{col}' alle righe: {_row_numbers(invalid_mask)}"
            )
            continue
        if kind == "int":
            int_mask = (stripped != "") & ((numeric % 1) != 0)
            if int_mask.any():
                issues.append(
                    f"valori non interi in '{col}' alle righe: {_row_numbers(int_mask)}"
                )

    if enum_cols:
        for col, allowed_values in enum_cols.items():
            if col not in df.columns or df.empty:
                continue
            normalized = df[col].map(lambda x: "" if _is_blank(x) else str(x).strip().upper())
            invalid_mask = (normalized != "") & (~normalized.isin(allowed_values))
            if invalid_mask.any():
                issues.append(
                    f"valori non validi in '{col}' alle righe: {_row_numbers(invalid_mask)}"
                )

    if issues:
        _raise_csv_validation_error(file_label, issues)


def _normalize_teams_columns(df: pd.DataFrame) -> pd.DataFrame:
    col_map = {
        "team": "name",
        "nome": "name",
        "squadra": "name",
        "ppg_s": "PPG_S",
        "ppg r8": "PPG_R8",
        "ppg_r8": "PPG_R8",
        "gfpg_s": "GFpg_S",
        "gfpg r8": "GFpg_R8",
        "gfpg_r8": "GFpg_R8",
        "gapg_s": "GApg_S",
        "gapg r8": "GApg_R8",
        "gapg_r8": "GApg_R8",
        "mood": "MoodTeam",
        "moodteam": "MoodTeam",
        "coachstyle_p": "CoachStyle_P",
        "coachstyle_d": "CoachStyle_D",
        "coachstyle_c": "CoachStyle_C",
        "coachstyle_a": "CoachStyle_A",
        "coachstability": "CoachStability",
        "coachboost": "CoachBoost",
        "gamesremaining": "GamesRemaining",
        "giornaterimanenti": "GamesRemaining",
    }
    renamed = {}
    for col in df.columns:
        key = str(col).strip().lower()
        key = key.replace("-", "_").replace(" ", "")
        mapped = col_map.get(key)
        if mapped:
            renamed[col] = mapped
    if renamed:
        df = df.rename(columns=renamed)
    return df


def _archive_current(current_path: Path, history_dir: Path, stamp: str, prefix: str, keep: int) -> None:
    if current_path.exists():
        history_dir.mkdir(parents=True, exist_ok=True)
        dest = history_dir / f"{prefix}_{stamp}.csv"
        current_path.replace(dest)
        _rotate_history(history_dir, prefix, keep)


def _archive_incoming(src: Path, stamp: str, keep: int) -> None:
    if not src.exists():
        return
    dest_dir = ARCHIVE_INCOMING / src.parent.name
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{src.stem}_{stamp}{src.suffix}"
    src.replace(dest)
    _rotate_history(dest_dir, src.stem, keep)


def _diff_quotazioni(prev: pd.DataFrame, curr: pd.DataFrame) -> list[str]:
    changes = []
    prev_map = {str(r.get("Giocatore", "")).strip().lower(): r for _, r in prev.iterrows()}
    curr_map = {str(r.get("Giocatore", "")).strip().lower(): r for _, r in curr.iterrows()}

    for name, row in curr_map.items():
        if not name:
            continue
        if name not in prev_map:
            changes.append(f"+ {row.get('Giocatore','')}: {row.get('PrezzoAttuale','')} ({row.get('Squadra','')})")
        else:
            prev_row = prev_map[name]
            prev_price = str(prev_row.get("PrezzoAttuale", ""))
            curr_price = str(row.get("PrezzoAttuale", ""))
            prev_team = str(prev_row.get("Squadra", ""))
            curr_team = str(row.get("Squadra", ""))
            if prev_price != curr_price or (prev_team != curr_team and curr_team):
                team_part = f"({curr_team})" if prev_team == curr_team else f"({prev_team}, {curr_team})"
                changes.append(f"{row.get('Giocatore','')}: {prev_price} -> {curr_price} {team_part}".strip())

    for name, row in prev_map.items():
        if not name:
            continue
        if name not in curr_map:
            changes.append(f"- {row.get('Giocatore','')}: {row.get('PrezzoAttuale','')} ({row.get('Squadra','')})")
    return changes


def _diff_rose(prev: pd.DataFrame, curr: pd.DataFrame) -> list[str]:
    def _norm_player(name: str) -> str:
        base = str(name or "").strip()
        if not base:
            return ""
        base = re.sub(r"\s*\*\s*$", "", base)
        return base.strip().lower()

    def _row_map(df: pd.DataFrame) -> dict:
        out = {}
        for _, row in df.iterrows():
            team = str(row.get("Team", "")).strip()
            name = str(row.get("Giocatore", "")).strip()
            if not team or not name:
                continue
            key = _norm_player(name)
            if not key:
                continue
            out.setdefault(team, {})[key] = row
        return out

    prev_map = _row_map(prev)
    curr_map = _row_map(curr)
    teams = sorted(set(prev_map.keys()) | set(curr_map.keys()))
    changes = []

    for team in teams:
        prev_players = prev_map.get(team, {})
        curr_players = curr_map.get(team, {})
        removed = [k for k in prev_players.keys() if k not in curr_players]
        added = [k for k in curr_players.keys() if k not in prev_players]

        pairs = []
        for role in ["P", "D", "C", "A"]:
            removed_role = [k for k in removed if str(prev_players[k].get("Ruolo", "")).strip() == role]
            added_role = [k for k in added if str(curr_players[k].get("Ruolo", "")).strip() == role]
            for i in range(min(len(removed_role), len(added_role))):
                pairs.append((removed_role[i], added_role[i]))

        if not pairs:
            continue

        parts = []
        for out_key, in_key in pairs:
            out_row = prev_players[out_key]
            in_row = curr_players[in_key]
            parts.append(
                f"{out_row.get('Giocatore','')}, {out_row.get('PrezzoAttuale','')}, {out_row.get('Ruolo','')}, {out_row.get('Squadra','')} -> "
                f"{in_row.get('Giocatore','')}, {in_row.get('PrezzoAttuale','')}, {in_row.get('Ruolo','')}, {in_row.get('Squadra','')}"
            )
        changes.append(f"{team}: " + "; ".join(parts))
    return changes


def _write_diff(lines: list[str], stamp: str, label: str) -> None:
    if not lines:
        return
    HIST_DIFFS.mkdir(parents=True, exist_ok=True)
    out = HIST_DIFFS / f"diff_{label}_{stamp}.txt"
    out.write_text("\n".join(lines), encoding="utf-8")


def _build_market_diff(prev: pd.DataFrame, curr: pd.DataFrame, stamp: str) -> list[dict]:
    def _norm_player(name: str) -> str:
        base = str(name or "").strip()
        if not base:
            return ""
        base = re.sub(r"\s*\*\s*$", "", base)
        return base.strip().lower()

    def _row_map(df: pd.DataFrame) -> dict:
        out = {}
        for _, row in df.iterrows():
            team = str(row.get("Team", "")).strip()
            name = str(row.get("Giocatore", "")).strip()
            if not team or not name:
                continue
            key = _norm_player(name)
            if not key:
                continue
            out.setdefault(team, {})[key] = row
        return out

    prev_map = _row_map(prev)
    curr_map = _row_map(curr)
    teams = sorted(set(prev_map.keys()) | set(curr_map.keys()))
    changes = []
    for team in teams:
        prev_players = prev_map.get(team, {})
        curr_players = curr_map.get(team, {})
        removed = sorted([k for k in prev_players.keys() if k not in curr_players])
        added = sorted([k for k in curr_players.keys() if k not in prev_players])

        pairs = max(len(removed), len(added))
        for i in range(pairs):
            out_key = removed[i] if i < len(removed) else None
            in_key = added[i] if i < len(added) else None
            out_row = prev_players.get(out_key) if out_key else None
            in_row = curr_players.get(in_key) if in_key else None

            out_val = float(out_row.get("PrezzoAttuale", 0) or 0) if out_row is not None else 0
            in_val = float(in_row.get("PrezzoAttuale", 0) or 0) if in_row is not None else 0

            out_name = out_row.get("Giocatore", "") if out_row is not None else ""
            in_name = in_row.get("Giocatore", "") if in_row is not None else ""
            # Ignore changes that are only asterisk changes (same player)
            if _norm_player(out_name) and _norm_player(out_name) == _norm_player(in_name):
                continue

            changes.append(
                {
                    "team": team,
                    "date": stamp,
                    "out": out_name,
                    "out_squadra": out_row.get("Squadra", "") if out_row is not None else "",
                    "out_value": out_val,
                    "in": in_name,
                    "in_squadra": in_row.get("Squadra", "") if in_row is not None else "",
                    "in_value": in_val,
                    "delta": out_val - in_val,
                }
            )
    return changes


def _market_from_diff_rose(lines: list[str], stamp: str) -> list[dict]:
    def _parse_side(text: str) -> dict:
        parts = [p.strip() for p in text.split(",")]
        if len(parts) < 4:
            return {"name": "", "value": 0.0, "role": "", "team": ""}
        name = parts[0]
        try:
            value = float(str(parts[1]).replace(",", "."))
        except ValueError:
            value = 0.0
        role = parts[2]
        team = ", ".join(parts[3:]).strip()
        return {"name": name, "value": value, "role": role, "team": team}

    items: list[dict] = []
    for line in lines:
        if ": " not in line:
            continue
        team, rest = line.split(": ", 1)
        team = team.strip()
        for part in [p.strip() for p in rest.split(";") if p.strip()]:
            if " -> " not in part:
                continue
            out_txt, in_txt = part.split(" -> ", 1)
            out_side = _parse_side(out_txt)
            in_side = _parse_side(in_txt)
            if not out_side["name"] or not in_side["name"]:
                continue
            items.append(
                {
                    "team": team,
                    "date": stamp,
                    "out": out_side["name"],
                    "out_squadra": out_side["team"],
                    "out_value": out_side["value"],
                    "in": in_side["name"],
                    "in_squadra": in_side["team"],
                    "in_value": in_side["value"],
                    "delta": out_side["value"] - in_side["value"],
                    "out_ruolo": out_side["role"],
                    "in_ruolo": in_side["role"],
                }
            )
    return items


def _load_market_history() -> list[dict]:
    if not HIST_MARKET.exists():
        return []
    items = []
    seen = set()
    for path in sorted(HIST_MARKET.glob("market_*.json"), reverse=True):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                for item in data:
                    out_name = str(item.get("out", "")).strip()
                    in_name = str(item.get("in", "")).strip()
                    if out_name and in_name:
                        out_key = re.sub(r"\s*\*\s*$", "", out_name).strip().lower()
                        in_key = re.sub(r"\s*\*\s*$", "", in_name).strip().lower()
                        if out_key and out_key == in_key:
                            continue
                    team_key = str(item.get("team", "")).strip().lower()
                    date_key = str(item.get("date", "")).strip()
                    dedupe_key = (team_key, date_key, out_name.lower(), in_name.lower())
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    items.append(item)
        except json.JSONDecodeError:
            continue
    return items


def _latest_history_file(folder: Path, prefix: str) -> Path | None:
    if not folder.exists():
        return None
    files = sorted(folder.glob(f"{prefix}_*.csv"), key=lambda p: p.name)
    return files[-1] if files else None


def _rebuild_market_from_history(stamp: str) -> None:
    if not ROSE_PATH.exists():
        print("Rose correnti mancanti, impossibile ricostruire il mercato.")
        return
    prev_path = _latest_history_file(HIST_ROSE, "rose_fantaportoscuso")
    if prev_path is None:
        print("Nessun storico rose trovato per ricostruire il mercato.")
        return
    diff_path = HIST_DIFFS / f"diff_rose_{stamp}.txt"
    if diff_path.exists():
        diff_lines = diff_path.read_text(encoding="utf-8").splitlines()
        market_changes = _market_from_diff_rose(diff_lines, stamp)
    else:
        prev_rose = pd.read_csv(prev_path)
        curr_rose = pd.read_csv(ROSE_PATH)
        market_changes = _build_market_diff(prev_rose, curr_rose, stamp)
    HIST_MARKET.mkdir(parents=True, exist_ok=True)
    market_path = HIST_MARKET / f"market_{stamp}.json"
    market_path.write_text(json.dumps(market_changes, indent=2), encoding="utf-8")
    all_items = _load_market_history()
    MARKET_LATEST.write_text(
        json.dumps(
            {
                "items": all_items,
                "teams": _market_team_summary(all_items),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print("Mercato ricostruito da storico rose.")


def _market_team_summary(items: list[dict]) -> list[dict]:
    summary = {}
    for item in items:
        team = str(item.get("team", "")).strip()
        if not team:
            continue
        summary.setdefault(team, {"team": team, "delta": 0.0, "players": set(), "last_date": ""})
        entry = summary[team]
        entry["delta"] += float(item.get("delta", 0) or 0)
        for key in ("out", "in"):
            name = str(item.get(key, "")).strip()
            if name:
                entry["players"].add(name.lower())
        date_val = str(item.get("date", "")).strip()
        if date_val and (entry["last_date"] == "" or date_val > entry["last_date"]):
            entry["last_date"] = date_val

    result = []
    for entry in summary.values():
        result.append(
            {
                "team": entry["team"],
                "delta": entry["delta"],
                "changed_count": len(entry["players"]),
                "last_date": entry["last_date"],
            }
        )
    result.sort(key=lambda x: (x["delta"], x["team"]), reverse=True)
    return result


def _sync_rose_with_quotazioni(rose_df: pd.DataFrame, quot_df: pd.DataFrame) -> pd.DataFrame:
    # Map by player name; keep rose rows even if not in quotazioni (Castellanos rule).
    def _norm(value: str) -> str:
        base = re.sub(r"\s*\*\s*$", "", str(value or "").strip()).strip().lower()
        base = re.sub(r"[^a-z0-9]+", "", base)
        return base

    quot_map = {}
    for _, row in quot_df.iterrows():
        name = (
            row.get("Giocatore")
            or row.get("nome")
            or row.get("Nome")
            or row.get("Calciatore")
            or ""
        )
        name = str(name).strip()
        if not name:
            continue
        qa_val = (
            row.get("PrezzoAttuale")
            or row.get("QuotazioneAttuale")
            or row.get("QA")
            or ""
        )
        squadra_val = row.get("Squadra") or row.get("club") or ""
        quot_map[_norm(name)] = {
            "PrezzoAttuale": qa_val,
            "Squadra": squadra_val,
        }

    updated = rose_df.copy()
    for idx, row in updated.iterrows():
        name = _norm(row.get("Giocatore", ""))
        if not name or name not in quot_map:
            continue
        info = quot_map[name]
        if info.get("PrezzoAttuale", "") != "":
            updated.at[idx, "PrezzoAttuale"] = info["PrezzoAttuale"]
        if info.get("Squadra", ""):
            updated.at[idx, "Squadra"] = info["Squadra"]
    return updated


def _find_header_map(raw: pd.DataFrame) -> Tuple[int, dict]:
    header_row = None
    for i in range(min(50, len(raw))):
        row = raw.iloc[i].astype(str)
        if (row == "Calciatore").any():
            header_row = i
            break
    if header_row is None:
        raise ValueError("Header row with 'Calciatore' not found in Squadre_master.")
    header = raw.iloc[header_row].astype(str).tolist()
    label_map = {}
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
    block_starts = []
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
    rows = []
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
    df["Giocatore"] = df["Giocatore"].astype(str).str.strip()
    return df


def _parse_rose_nuovo(path: Path) -> pd.DataFrame:
    raw = pd.read_excel(path, header=None)
    rows = []

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


def main() -> None:
    parser = argparse.ArgumentParser(description="Update rose/quotazioni data with history rotation.")
    parser.add_argument("--rose", type=str, help="Path to new rose file (CSV or XLSX)")
    parser.add_argument("--quotazioni", type=str, help="Path to new quotazioni file (CSV or XLSX)")
    parser.add_argument("--teams", type=str, help="Path to new teams file (CSV or XLSX)")
    parser.add_argument("--date", type=str, default=_today_stamp(), help="Date stamp YYYY-MM-DD")
    parser.add_argument("--keep", type=int, default=5, help="How many history files to keep")
    parser.add_argument("--sync-rose", action="store_true", help="Update rose PrezzoAttuale/Squadra from quotazioni")
    parser.add_argument("--auto", action="store_true", help="Auto-pick newest incoming files")
    parser.add_argument("--rebuild-market", action="store_true", help="Rebuild market using latest rose history")
    args = parser.parse_args()

    backup_done = False

    def ensure_backup() -> None:
        nonlocal backup_done
        if backup_done:
            return
        run_backup_fail_fast(
            DATABASE_URL,
            BACKUP_DIR,
            BACKUP_KEEP_LAST,
            prefix="update",
            base_dir=ROOT,
        )
        backup_done = True

    if args.rebuild_market:
        _rebuild_market_from_history(args.date)
        return

    if args.auto:
        args.rose = args.rose or (str(_latest_incoming(INCOMING_ROSE)) if _latest_incoming(INCOMING_ROSE) else None)
        args.quotazioni = args.quotazioni or (
            str(_latest_incoming(INCOMING_QUOT)) if _latest_incoming(INCOMING_QUOT) else None
        )
        args.teams = args.teams or (
            str(_latest_incoming(INCOMING_TEAMS)) if _latest_incoming(INCOMING_TEAMS) else None
        )

    if not args.rose and not args.quotazioni and not args.teams:
        # Still sync PrezzoAttuale from master/quotazioni if rose exists.
        if ROSE_PATH.exists():
            rose_df = pd.read_csv(ROSE_PATH)
            quot_df = None
            if MASTER_QUOT_PATH.exists():
                quot_df = pd.read_csv(MASTER_QUOT_PATH)
            elif QUOT_PATH.exists():
                quot_df = pd.read_csv(QUOT_PATH)
            if quot_df is not None:
                ensure_backup()
                updated = _sync_rose_with_quotazioni(rose_df, quot_df)
                _write_csv(updated, ROSE_PATH)
                print("Rose sincronizzata con quotazioni (PrezzoAttuale).")
        print("Nessun nuovo file da aggiornare.")
        return

    stamp = args.date
    state = _load_state()
    last_sig = state.get("last_signature", {})
    current_sig = {}

    updated_quot = False
    updated_rose = False
    updated_teams = False
    archive_quot = False
    archive_rose = False
    archive_teams = False
    prepared_quot: pd.DataFrame | None = None
    prepared_rose: pd.DataFrame | None = None
    prepared_teams: pd.DataFrame | None = None
    prev_quot = pd.DataFrame()
    prev_rose = pd.DataFrame()
    prev_teams = pd.DataFrame()
    if args.quotazioni:
        quot_path = Path(args.quotazioni)
        current_sig["quotazioni"] = _file_signature(quot_path)
        if last_sig.get("quotazioni") == current_sig["quotazioni"]:
            print("Update già eseguito (quotazioni).")
            archive_quot = True
        else:
            prev_quot = pd.read_csv(QUOT_PATH) if QUOT_PATH.exists() else pd.DataFrame()
            prepared_quot = _read_input(quot_path)
            if not all(col in prepared_quot.columns for col in ["Giocatore", "PrezzoAttuale"]):
                prepared_quot = _parse_quotazioni_listone(quot_path)
            _validate_csv_input(
                prepared_quot,
                file_label=quot_path.name,
                required_cols=["Giocatore", "PrezzoAttuale"],
                allowed_cols=[
                    "Giocatore",
                    "Squadra",
                    "PrezzoAttuale",
                    "PrezzoIniziale",
                    "Ruolo",
                    "RuoloMantra",
                    "FVM",
                ],
                key_cols=["Giocatore"],
                numeric_cols={
                    "PrezzoAttuale": "float",
                    "PrezzoIniziale": "float",
                    "FVM": "float",
                },
            )

    if args.rose:
        rose_path = Path(args.rose)
        current_sig["rose"] = _file_signature(rose_path)
        if last_sig.get("rose") == current_sig["rose"]:
            print("Update già eseguito (rose).")
            archive_rose = True
        else:
            prev_rose = pd.read_csv(ROSE_PATH) if ROSE_PATH.exists() else pd.DataFrame()
            prepared_rose = _read_input(rose_path)
            required_cols = ["Team", "Giocatore", "Ruolo", "Squadra", "PrezzoAcquisto", "PrezzoAttuale"]
            if not all(col in prepared_rose.columns for col in required_cols) and rose_path.suffix.lower() in {
                ".xlsx",
                ".xls",
            }:
                try:
                    prepared_rose = _parse_rose_nuovo(rose_path)
                except ValueError:
                    prepared_rose = _parse_squadre_master(rose_path)
            _validate_csv_input(
                prepared_rose,
                file_label=rose_path.name,
                required_cols=required_cols,
                allowed_cols=required_cols,
                key_cols=["Team", "Giocatore"],
                numeric_cols={
                    "PrezzoAcquisto": "float",
                    "PrezzoAttuale": "float",
                },
                enum_cols={"Ruolo": {"P", "D", "C", "A"}},
            )

    if args.teams:
        teams_path = Path(args.teams)
        current_sig["teams"] = _file_signature(teams_path)
        if last_sig.get("teams") == current_sig["teams"]:
            print("Update già eseguito (teams).")
            archive_teams = True
        else:
            prev_teams = pd.read_csv(TEAMS_PATH) if TEAMS_PATH.exists() else pd.DataFrame()
            prepared_teams = _read_input(teams_path)
            prepared_teams = _normalize_teams_columns(prepared_teams)
            required_cols = [
                "name",
                "PPG_S",
                "PPG_R8",
                "GFpg_S",
                "GFpg_R8",
                "GApg_S",
                "GApg_R8",
                "MoodTeam",
                "CoachStyle_P",
                "CoachStyle_D",
                "CoachStyle_C",
                "CoachStyle_A",
                "CoachStability",
                "CoachBoost",
                "GamesRemaining",
            ]
            _validate_csv_input(
                prepared_teams,
                file_label=teams_path.name,
                required_cols=required_cols,
                allowed_cols=required_cols,
                key_cols=["name"],
                numeric_cols={
                    "PPG_S": "float",
                    "PPG_R8": "float",
                    "GFpg_S": "float",
                    "GFpg_R8": "float",
                    "GApg_S": "float",
                    "GApg_R8": "float",
                    "MoodTeam": "float",
                    "CoachStyle_P": "float",
                    "CoachStyle_D": "float",
                    "CoachStyle_C": "float",
                    "CoachStyle_A": "float",
                    "CoachStability": "float",
                    "CoachBoost": "float",
                    "GamesRemaining": "int",
                },
            )

    if prepared_quot is not None:
        ensure_backup()
        _archive_current(QUOT_PATH, HIST_QUOT, stamp, "quotazioni", args.keep)
        _write_csv(prepared_quot, QUOT_PATH)
        if not prev_quot.empty:
            diff_lines = _diff_quotazioni(prev_quot, prepared_quot)
            _write_diff(diff_lines, stamp, "quotazioni")
        updated_quot = True
        state.setdefault("last_signature", {})["quotazioni"] = current_sig["quotazioni"]
        archive_quot = True

    if prepared_rose is not None:
        ensure_backup()
        _archive_current(ROSE_PATH, HIST_ROSE, stamp, "rose_fantaportoscuso", args.keep)
        _write_csv(prepared_rose, ROSE_PATH)
        if not prev_rose.empty:
            diff_lines = _diff_rose(prev_rose, prepared_rose)
            _write_diff(diff_lines, stamp, "rose")
            market_changes = _market_from_diff_rose(diff_lines, stamp)
            HIST_MARKET.mkdir(parents=True, exist_ok=True)
            market_path = HIST_MARKET / f"market_{stamp}.json"
            market_path.write_text(json.dumps(market_changes, indent=2), encoding="utf-8")
            # Keep full market history (no rotation) for season-long tracking
            all_items = _load_market_history()
            MARKET_LATEST.write_text(
                json.dumps(
                    {
                        "items": all_items,
                        "teams": _market_team_summary(all_items),
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        updated_rose = True
        state.setdefault("last_signature", {})["rose"] = current_sig["rose"]
        archive_rose = True

    if prepared_teams is not None:
        ensure_backup()
        _archive_current(TEAMS_PATH, HIST_TEAMS, stamp, "teams", args.keep)
        _write_csv(prepared_teams, TEAMS_PATH)
        updated_teams = True
        state.setdefault("last_signature", {})["teams"] = current_sig["teams"]
        archive_teams = True

    if ROSE_PATH.exists():
        rose_df = pd.read_csv(ROSE_PATH)
        if MASTER_QUOT_PATH.exists():
            quot_df = pd.read_csv(MASTER_QUOT_PATH)
        elif QUOT_PATH.exists():
            quot_df = pd.read_csv(QUOT_PATH)
        else:
            quot_df = None
        if quot_df is not None:
            ensure_backup()
            updated = _sync_rose_with_quotazioni(rose_df, quot_df)
            _write_csv(updated, ROSE_PATH)

    if archive_quot and args.quotazioni:
        _archive_incoming(Path(args.quotazioni), stamp, args.keep)
    if archive_rose and args.rose:
        _archive_incoming(Path(args.rose), stamp, args.keep)
    if archive_teams and args.teams:
        _archive_incoming(Path(args.teams), stamp, args.keep)

    if current_sig:
        _save_state(state)
    print("Done.")


if __name__ == "__main__":
    main()
