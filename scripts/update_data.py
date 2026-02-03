import argparse
import hashlib
import json
import re
from datetime import date
from pathlib import Path
from typing import List, Tuple

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
ROSE_PATH = DATA_DIR / "rose_fantaportoscuso.csv"
QUOT_PATH = DATA_DIR / "quotazioni.csv"
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
            changes.append(f"+ {row.get('Giocatore','')} ({row.get('Squadra','')})")
        else:
            prev_row = prev_map[name]
            if str(prev_row.get("PrezzoAttuale", "")) != str(row.get("PrezzoAttuale", "")):
                changes.append(
                    f"* {row.get('Giocatore','')} prezzo {prev_row.get('PrezzoAttuale','')} -> {row.get('PrezzoAttuale','')}"
                )
            if str(prev_row.get("Squadra", "")) != str(row.get("Squadra", "")) and row.get("Squadra", ""):
                changes.append(
                    f"* {row.get('Giocatore','')} squadra {prev_row.get('Squadra','')} -> {row.get('Squadra','')}"
                )

    for name, row in prev_map.items():
        if not name:
            continue
        if name not in curr_map:
            changes.append(f"- {row.get('Giocatore','')} ({row.get('Squadra','')})")
    return changes


def _diff_rose(prev: pd.DataFrame, curr: pd.DataFrame) -> list[str]:
    def _norm_player(name: str) -> str:
        base = str(name or "").strip()
        if not base:
            return ""
        base = re.sub(r"\s*\*\s*$", "", base)
        return base.strip().lower()

    changes = []
    prev_keys = {(str(r.get("Team", "")).strip().lower(), _norm_player(r.get("Giocatore", ""))): r for _, r in prev.iterrows()}
    curr_keys = {(str(r.get("Team", "")).strip().lower(), _norm_player(r.get("Giocatore", ""))): r for _, r in curr.iterrows()}

    for key, row in curr_keys.items():
        team, name = key
        if not team or not name:
            continue
        if key not in prev_keys:
            changes.append(f"+ {row.get('Team','')}: {row.get('Giocatore','')}")
        else:
            prev_row = prev_keys[key]
            if str(prev_row.get("PrezzoAcquisto", "")) != str(row.get("PrezzoAcquisto", "")):
                changes.append(
                    f"* {row.get('Team','')} {row.get('Giocatore','')} acquisto {prev_row.get('PrezzoAcquisto','')} -> {row.get('PrezzoAcquisto','')}"
                )
            if str(prev_row.get("PrezzoAttuale", "")) != str(row.get("PrezzoAttuale", "")):
                changes.append(
                    f"* {row.get('Team','')} {row.get('Giocatore','')} attuale {prev_row.get('PrezzoAttuale','')} -> {row.get('PrezzoAttuale','')}"
                )

    for key, row in prev_keys.items():
        team, name = key
        if not team or not name:
            continue
        if key not in curr_keys:
            changes.append(f"- {row.get('Team','')}: {row.get('Giocatore','')}")
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


def _load_market_history() -> list[dict]:
    if not HIST_MARKET.exists():
        return []
    items = []
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
    quot_map = {}
    for _, row in quot_df.iterrows():
        name = str(row.get("Giocatore", "")).strip()
        if not name:
            continue
        quot_map[name.lower()] = {
            "PrezzoAttuale": row.get("PrezzoAttuale", row.get("QuotazioneAttuale", "")),
            "Squadra": row.get("Squadra", ""),
        }

    updated = rose_df.copy()
    for idx, row in updated.iterrows():
        name = str(row.get("Giocatore", "")).strip().lower()
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
        print("Nessun nuovo file da aggiornare.")
        return

    stamp = args.date
    state = _load_state()
    last_sig = state.get("last_signature", {})
    current_sig = {}

    updated_quot = False
    updated_rose = False
    updated_teams = False

    if args.quotazioni:
        quot_path = Path(args.quotazioni)
        current_sig["quotazioni"] = _file_signature(quot_path)
        if last_sig.get("quotazioni") == current_sig["quotazioni"]:
            print("Update già eseguito (quotazioni).")
        else:
            prev_quot = pd.read_csv(QUOT_PATH) if QUOT_PATH.exists() else pd.DataFrame()
            new_quot = _read_input(quot_path)
            if not all(col in new_quot.columns for col in ["Giocatore", "PrezzoAttuale"]):
                new_quot = _parse_quotazioni_listone(quot_path)
            _validate_columns(new_quot, ["Giocatore", "PrezzoAttuale"], "Quotazioni")
            _archive_current(QUOT_PATH, HIST_QUOT, stamp, "quotazioni", args.keep)
            _write_csv(new_quot, QUOT_PATH)
            if not prev_quot.empty:
                diff_lines = _diff_quotazioni(prev_quot, new_quot)
                _write_diff(diff_lines, stamp, "quotazioni")
            updated_quot = True
            state.setdefault("last_signature", {})["quotazioni"] = current_sig["quotazioni"]

    if args.rose:
        rose_path = Path(args.rose)
        current_sig["rose"] = _file_signature(rose_path)
        if last_sig.get("rose") == current_sig["rose"]:
            print("Update già eseguito (rose).")
        else:
            prev_rose = pd.read_csv(ROSE_PATH) if ROSE_PATH.exists() else pd.DataFrame()
            new_rose = _read_input(rose_path)
            required_cols = ["Team", "Giocatore", "Ruolo", "Squadra", "PrezzoAcquisto", "PrezzoAttuale"]
            if not all(col in new_rose.columns for col in required_cols) and rose_path.suffix.lower() in {
                ".xlsx",
                ".xls",
            }:
                try:
                    new_rose = _parse_rose_nuovo(rose_path)
                except ValueError:
                    new_rose = _parse_squadre_master(rose_path)
            _validate_columns(new_rose, required_cols, "Rose")
            _archive_current(ROSE_PATH, HIST_ROSE, stamp, "rose_fantaportoscuso", args.keep)
            _write_csv(new_rose, ROSE_PATH)
            if not prev_rose.empty:
                diff_lines = _diff_rose(prev_rose, new_rose)
                _write_diff(diff_lines, stamp, "rose")
                market_changes = _build_market_diff(prev_rose, new_rose, stamp)
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

    if args.teams:
        teams_path = Path(args.teams)
        current_sig["teams"] = _file_signature(teams_path)
        if last_sig.get("teams") == current_sig["teams"]:
            print("Update già eseguito (teams).")
        else:
            prev_teams = pd.read_csv(TEAMS_PATH) if TEAMS_PATH.exists() else pd.DataFrame()
            new_teams = _read_input(teams_path)
            new_teams = _normalize_teams_columns(new_teams)
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
            _validate_columns(new_teams, required_cols, "Teams")
            _archive_current(TEAMS_PATH, HIST_TEAMS, stamp, "teams", args.keep)
            _write_csv(new_teams, TEAMS_PATH)
            updated_teams = True
            state.setdefault("last_signature", {})["teams"] = current_sig["teams"]

    if args.sync_rose and QUOT_PATH.exists() and ROSE_PATH.exists():
        rose_df = pd.read_csv(ROSE_PATH)
        quot_df = pd.read_csv(QUOT_PATH)
        updated = _sync_rose_with_quotazioni(rose_df, quot_df)
        _write_csv(updated, ROSE_PATH)

    if updated_quot and args.quotazioni:
        _archive_incoming(Path(args.quotazioni), stamp, args.keep)
    if updated_rose and args.rose:
        _archive_incoming(Path(args.rose), stamp, args.keep)
    if updated_teams and args.teams:
        _archive_incoming(Path(args.teams), stamp, args.keep)

    if current_sig:
        _save_state(state)
    print("Done.")


if __name__ == "__main__":
    main()
