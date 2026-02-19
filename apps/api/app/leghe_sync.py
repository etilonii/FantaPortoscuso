from __future__ import annotations

import csv
import json
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from html import unescape as html_unescape
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener

import http.cookiejar


LEGHE_BASE_URL = "https://leghe.fantacalcio.it/"
FANTACALCIO_BASE_URL = "https://www.fantacalcio.it"
FANTACALCIO_LOGIN_PAGE_URL = f"{FANTACALCIO_BASE_URL}/login"
FANTACALCIO_LOGIN_API_URL = f"{FANTACALCIO_BASE_URL}/api/v1/User/login"
FANTACALCIO_EXCEL_PRICES_ENDPOINT = "/api/v1/Excel/prices/20/1"
FANTACALCIO_EXCEL_STATS_ENDPOINT = "/api/v1/Excel/stats/20/1"
FANTACALCIO_QUOTAZIONI_BASE_URL = "https://www.fantacalcio.it/quotazioni-fantacalcio"
FANTACALCIO_STATS_BASE_URL = "https://www.fantacalcio.it/statistiche-serie-a"
KICKEST_CLEANSHEET_URL = "https://www.kickest.it/it/serie-a/statistiche/giocatori/clean-sheet"

TEAM_ABBR_MAP = {
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

ROLE_CLASSIC_MAP = {
    "p": "P",
    "d": "D",
    "c": "C",
    "a": "A",
}

ROLE_IMPORT_MAP = {
    "POR": "P",
    "P": "P",
    "DIF": "D",
    "D": "D",
    "CEN": "C",
    "C": "C",
    "ATT": "A",
    "A": "A",
}

STATS_INCOMING_SPECS: tuple[tuple[str, str, str, bool], ...] = (
    ("gol", "Gol fatti", "gol", False),
    ("assist", "Assist", "ass", False),
    ("ammonizioni", "Ammonizioni", "amm", False),
    ("espulsioni", "Espulsioni", "esp", False),
    ("autogol", "Autogol", "autogol", False),
    ("rigoriparati", "Rigori parati", "rp", False),
    ("gol_subiti", "Gol subiti", "gs", True),
    ("rigorisegnati", "Rigori segnati", "rigori_segnati", False),
    ("rigorisbagliati", "Rigori sbagliati", "rigori_sbagliati", False),
    ("partite", "Partite giocate", "pg", False),
    ("mediavoto", "Mediavoto", "mv", False),
    ("fantamedia", "Fantamedia", "mfv", False),
)

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class LegheContext:
    alias: str
    app_key: str
    competition_id: int | None
    competition_name: str | None
    current_turn: int | None
    last_calculated_matchday: int | None
    suggested_formations_matchday: int | None


_logger = logging.getLogger(__name__)


class LegheSyncError(RuntimeError):
    pass


def _repo_root() -> Path:
    here = Path(__file__).resolve()
    for base in [here.parent, *here.parents]:
        candidate = base / "data"
        if candidate.is_dir():
            return base
    return Path.cwd()


def _data_dir() -> Path:
    return _repo_root() / "data"


def _today_stamp() -> str:
    return date.today().strftime("%Y-%m-%d")


def _season_slug_for(dt: datetime) -> str:
    if dt.month >= 7:
        start_year = dt.year
        end_short = str(dt.year + 1)[2:4]
        return f"{start_year}-{end_short}"
    prev_year = dt.year - 1
    curr_short = str(dt.year)[2:4]
    return f"{prev_year}-{curr_short}"


def _strip_html_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = html_unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_number(value: str, *, default: float = 0.0) -> float:
    raw = str(value or "").strip()
    if not raw:
        return default
    raw = raw.replace("\xa0", "").replace(" ", "")
    # Handle common locale formats:
    # - Italian decimals: 6,82
    # - EN decimals: 6.82
    # - Thousand separators: 1.234,56 or 1,234.56
    if "," in raw and "." in raw:
        if raw.rfind(",") > raw.rfind("."):
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(",", "")
    elif "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    elif raw.count(".") > 1:
        raw = raw.replace(".", "")
    elif raw.count(".") == 1:
        left, right = raw.split(".", 1)
        if left.lstrip("-").isdigit() and right.isdigit() and len(right) == 3:
            raw = left + right

    raw = re.sub(r"[^0-9.\-]", "", raw)
    if raw in {"", "-", ".", "-."}:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _parse_int_number(value: str, *, default: int = 0) -> int:
    return int(round(_parse_number(value, default=float(default))))


def _extract_row_col_text(body: str, key: str) -> str:
    match = re.search(
        rf'<td[^>]*data-col-key="{re.escape(key)}"[^>]*>(.*?)</td>',
        body,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match is None:
        return ""
    return _strip_html_text(match.group(1))


def _parse_rigori_cell(value: str) -> tuple[int, int]:
    raw = str(value or "").strip()
    match = re.search(r"(\d+)\s*/\s*(\d+)", raw)
    if match is None:
        scored = _parse_int_number(raw, default=0)
        return max(0, scored), 0
    scored = max(0, int(match.group(1)))
    attempted = max(0, int(match.group(2)))
    missed = max(0, attempted - scored)
    return scored, missed


def _normalize_stats_header(value: object) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    text = text.replace(".", "").replace(":", "")
    return text


def _pick_stats_column(columns: dict[str, str], aliases: list[str], *, startswith: bool = False) -> str:
    for alias in aliases:
        key = _normalize_stats_header(alias)
        if key in columns:
            return columns[key]
    if startswith:
        for alias in aliases:
            key = _normalize_stats_header(alias)
            for col_key, col_name in columns.items():
                if col_key.startswith(key):
                    return col_name
    return ""


def _build_cookie_opener() -> tuple[object, http.cookiejar.CookieJar]:
    jar = http.cookiejar.CookieJar()
    opener = build_opener(HTTPCookieProcessor(jar))
    return opener, jar


def _fantacalcio_login(
    opener,
    *,
    username: str,
    password: str,
) -> dict[str, object]:
    try:
        _http_read_bytes(
            opener,
            FANTACALCIO_LOGIN_PAGE_URL,
            headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
            timeout_seconds=20,
        )
    except Exception:
        # Best effort warm-up for cookies; do not fail hard here.
        pass

    payload = json.dumps({"username": username, "password": password}).encode("utf-8")
    body, _ = _http_read_bytes(
        opener,
        FANTACALCIO_LOGIN_API_URL,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Referer": FANTACALCIO_LOGIN_PAGE_URL,
            "Origin": FANTACALCIO_BASE_URL,
            "X-Requested-With": "XMLHttpRequest",
        },
        data=payload,
        timeout_seconds=30,
    )

    try:
        parsed = json.loads(body.decode("utf-8", errors="replace"))
    except Exception as exc:
        return {
            "ok": False,
            "error": f"Fantacalcio login parse error: {exc}",
            "response": {},
        }

    if not isinstance(parsed, dict):
        return {
            "ok": False,
            "error": "Fantacalcio login response non valida",
            "response": {},
        }

    success = bool(parsed.get("success"))
    if success:
        return {"ok": True, "response": parsed}

    errors = parsed.get("errors") if isinstance(parsed.get("errors"), list) else []
    error_msg = "; ".join(str(item.get("message") or "") for item in errors if isinstance(item, dict)).strip()
    return {
        "ok": False,
        "error": error_msg or "Fantacalcio login failed",
        "response": parsed,
    }


def _download_fantacalcio_excel_authenticated(
    opener,
    *,
    endpoint_path: str,
    referer: str,
    out_path: Path,
) -> dict[str, object]:
    endpoint = str(endpoint_path or "").strip()
    if not endpoint.startswith("/"):
        endpoint = f"/{endpoint}"
    url = f"{FANTACALCIO_BASE_URL}{endpoint}"
    body, resp_headers = _http_read_bytes(
        opener,
        url,
        method="GET",
        headers={
            "Accept": (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
                "application/vnd.ms-excel,application/octet-stream,*/*"
            ),
            "Referer": referer,
            "Origin": FANTACALCIO_BASE_URL,
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout_seconds=45,
    )
    if not _looks_like_xlsx(resp_headers, body):
        snippet = body[:200].decode("utf-8", errors="replace")
        return {
            "ok": False,
            "warning": f"Excel endpoint non ha restituito XLSX: {snippet}",
            "url": url,
            "path": str(out_path),
        }

    _atomic_write_bytes(out_path, body)
    return {
        "ok": True,
        "url": url,
        "path": str(out_path),
        "bytes": int(len(body)),
        "content_type": str(resp_headers.get("content-type") or ""),
    }


def _extract_fantacalcio_stats_rows_from_xlsx(path: Path) -> list[dict[str, object]]:
    try:
        import pandas as pd
    except Exception as exc:
        raise LegheSyncError(f"Pandas non disponibile per parsing stats xlsx: {exc}") from exc

    if not path.exists():
        return []

    sheets = pd.read_excel(path, sheet_name=None, header=None)
    if not isinstance(sheets, dict) or not sheets:
        raise LegheSyncError("File stats xlsx senza fogli leggibili.")

    selected_sheet: str | int | None = None
    header_row: int | None = None
    for sheet_name, raw in sheets.items():
        if raw is None or raw.empty:
            continue
        for i in range(min(20, len(raw))):
            row = [_normalize_stats_header(v) for v in raw.iloc[i].tolist()]
            if "nome" in row and "squadra" in row:
                selected_sheet = sheet_name
                header_row = i
                break
        if selected_sheet is not None:
            break

    if selected_sheet is None or header_row is None:
        raise LegheSyncError("Header row Nome/Squadra non trovato nel file stats xlsx.")

    frame = pd.read_excel(path, sheet_name=selected_sheet, header=header_row)
    frame = frame.rename(columns={col: str(col).strip() for col in frame.columns})
    columns = {_normalize_stats_header(col): str(col) for col in frame.columns}

    col_id = _pick_stats_column(columns, ["Id", "ID"])
    col_role = _pick_stats_column(columns, ["Ruolo", "R"])
    col_name = _pick_stats_column(columns, ["Nome"])
    col_team = _pick_stats_column(columns, ["Squadra"])
    if not col_name or not col_team:
        raise LegheSyncError("Colonne Nome/Squadra mancanti nel file stats xlsx.")

    stats_cols = {
        "gol": _pick_stats_column(columns, ["Gol fatti", "Gf"]),
        "ass": _pick_stats_column(columns, ["Assist", "Ass"]),
        "amm": _pick_stats_column(columns, ["Ammonizioni", "Amm"]),
        "esp": _pick_stats_column(columns, ["Espulsioni", "Esp"]),
        "autogol": _pick_stats_column(columns, ["Autogol", "Au"]),
        "gs": _pick_stats_column(columns, ["Gol subiti", "Gs"]),
        "rp": _pick_stats_column(columns, ["Rigori parati", "Rp"]),
        "rigori_segnati": _pick_stats_column(columns, ["Rigori segnati", "R+", "R +"], startswith=True),
        "rigori_sbagliati": _pick_stats_column(columns, ["Rigori sbagliati", "R-", "R -"], startswith=True),
        "pg": _pick_stats_column(columns, ["Partite giocate", "Presenze", "Pv"]),
        "mv": _pick_stats_column(columns, ["Mediavoto", "Mv"]),
        "mfv": _pick_stats_column(columns, ["Fantamedia", "Fm"]),
    }

    out: list[dict[str, object]] = []
    for idx, row in frame.iterrows():
        player_name = str(row.get(col_name, "")).strip() if col_name else ""
        if not player_name:
            continue

        role_raw = str(row.get(col_role, "")).strip().upper() if col_role else ""
        role = ROLE_IMPORT_MAP.get(role_raw, role_raw[:1] if role_raw else "")

        team_raw = str(row.get(col_team, "")).strip() if col_team else ""
        team_upper = team_raw.upper()
        team = TEAM_ABBR_MAP.get(team_upper, team_raw.title() if team_raw else "")

        item: dict[str, object] = {
            "ID": int(_parse_number(str(row.get(col_id, idx + 1) if col_id else idx + 1), default=float(idx + 1))),
            "Giocatore": player_name,
            "Posizione": role,
            "Squadra": team,
        }
        for key, col_name_stat in stats_cols.items():
            if not col_name_stat:
                item[key] = 0
                continue
            value = row.get(col_name_stat, 0)
            if key in {"mv", "mfv"}:
                item[key] = round(float(_parse_number(str(value), default=0.0)), 2)
            else:
                item[key] = int(_parse_number(str(value), default=0.0))
        out.append(item)

    out.sort(
        key=lambda row: (
            str(row.get("Posizione") or ""),
            str(row.get("Giocatore") or "").lower(),
        )
    )
    return out


def _write_stats_bundle_files(
    *,
    stat_rows: list[dict[str, object]],
    out_dir: Path,
    stamp: str,
) -> list[dict[str, object]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    generated_files: list[dict[str, object]] = []
    for prefix, label, source_key, goalkeepers_only in STATS_INCOMING_SPECS:
        path = out_dir / f"{prefix}_{stamp}.csv"
        out_rows: list[dict[str, object]] = []
        for row in stat_rows:
            if goalkeepers_only and str(row.get("Posizione") or "").upper() != "P":
                continue
            value = row.get(source_key, 0)
            numeric_value = _parse_number(value, default=0.0)
            if numeric_value <= 0:
                continue
            rendered_value: object
            if label in {"Mediavoto", "Fantamedia"}:
                rendered_value = round(float(numeric_value), 2)
            else:
                rendered_value = int(round(float(numeric_value)))
            out_rows.append(
                {
                    "ID": len(out_rows) + 1,
                    "Giocatore": str(row.get("Giocatore") or ""),
                    "Posizione": str(row.get("Posizione") or ""),
                    "Squadra": str(row.get("Squadra") or ""),
                    label: rendered_value,
                }
            )

        fields = ["ID", "Giocatore", "Posizione", "Squadra", label]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(out_rows)
        generated_files.append(
            {
                "prefix": prefix,
                "label": label,
                "path": str(path),
                "rows": int(len(out_rows)),
            }
        )
    return generated_files


def _extract_kickest_cleansheet_rows_from_html(source: str) -> list[dict[str, object]]:
    if not source:
        return []

    block_match = re.search(
        r"function\s+getRawData\s*\(\)\s*\{[\s\S]*?return\s*(\[[\s\S]*?\])\s*;\s*\}",
        source,
        flags=re.IGNORECASE,
    )
    if block_match is None:
        return []

    try:
        raw_rows = json.loads(block_match.group(1))
    except Exception:
        return []

    if not isinstance(raw_rows, list):
        return []

    out: list[dict[str, object]] = []
    for entry in raw_rows:
        if not isinstance(entry, dict):
            continue

        position_id = str(entry.get("position_id") or "").strip()
        position_name = str(entry.get("position") or "").strip().lower()
        is_goalkeeper = position_id == "1" or "goalkeeper" in position_name
        if not is_goalkeeper:
            continue

        clean_sheet_value = _parse_int_number(entry.get("tot"), default=0)
        if clean_sheet_value <= 0:
            continue

        player_name = str(entry.get("display_name") or entry.get("match_name") or "").strip()
        if not player_name:
            first_name = str(entry.get("first_name") or "").strip()
            last_name = str(entry.get("last_name") or "").strip()
            player_name = " ".join(part for part in [first_name, last_name] if part).strip()
        if not player_name:
            continue

        team_raw = str(entry.get("team_name") or entry.get("team_code") or "").strip()
        team_upper = team_raw.upper()
        team = TEAM_ABBR_MAP.get(team_upper, team_raw.title() if team_raw else "")

        out.append(
            {
                "Giocatore": player_name,
                "Posizione": "P",
                "Squadra": team,
                "cleansheet": clean_sheet_value,
            }
        )

    out.sort(
        key=lambda row: (
            -int(row.get("cleansheet") or 0),
            str(row.get("Giocatore") or "").lower(),
        )
    )
    for idx, row in enumerate(out, start=1):
        row["ID"] = idx
    return out


def download_kickest_cleansheet_csv(
    *,
    date_stamp: str | None = None,
    out_dir: Path | None = None,
) -> dict[str, object]:
    stamp = (date_stamp or _today_stamp()).strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", stamp):
        raise LegheSyncError(f"date_stamp non valido (atteso YYYY-MM-DD): {stamp}")

    resolved_out_dir = out_dir or (_data_dir() / "incoming" / "stats")
    resolved_out_dir.mkdir(parents=True, exist_ok=True)
    out_path = resolved_out_dir / f"cleansheet_{stamp}.csv"

    opener = build_opener()
    body, _ = _http_read_bytes(
        opener,
        KICKEST_CLEANSHEET_URL,
        headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://www.kickest.it/",
        },
        timeout_seconds=45,
    )
    source = body.decode("utf-8", errors="replace")
    rows = _extract_kickest_cleansheet_rows_from_html(source)
    if not rows:
        return {
            "ok": False,
            "warning": "No cleansheet rows parsed from Kickest source",
            "url": KICKEST_CLEANSHEET_URL,
            "path": str(out_path),
            "rows": 0,
        }

    fields = ["ID", "Giocatore", "Posizione", "Squadra", "Cleansheet"]
    rendered_rows = [
        {
            "ID": int(row.get("ID") or idx + 1),
            "Giocatore": str(row.get("Giocatore") or ""),
            "Posizione": str(row.get("Posizione") or "P"),
            "Squadra": str(row.get("Squadra") or ""),
            "Cleansheet": int(_parse_number(row.get("cleansheet"), default=0.0)),
        }
        for idx, row in enumerate(rows)
    ]
    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rendered_rows)

    return {
        "ok": True,
        "url": KICKEST_CLEANSHEET_URL,
        "path": str(out_path),
        "rows": int(len(rendered_rows)),
        "source": "kickest_html",
    }


def _extract_fantacalcio_quotazioni_rows_from_xlsx(path: Path) -> list[dict[str, object]]:
    try:
        import pandas as pd
    except Exception as exc:
        raise LegheSyncError(f"Pandas non disponibile per parsing quotazioni xlsx: {exc}") from exc

    if not path.exists():
        return []

    raw = pd.read_excel(path, header=None)
    header_row: int | None = None
    for i in range(min(20, len(raw))):
        row = [_normalize_stats_header(v) for v in raw.iloc[i].tolist()]
        if "nome" in row and ("qta" in row or "prezzoattuale" in row):
            header_row = i
            break
    if header_row is None:
        raise LegheSyncError("Header row Nome/Qt.A non trovato nel file quotazioni xlsx.")

    frame = pd.read_excel(path, header=header_row)
    frame = frame.rename(columns={col: str(col).strip() for col in frame.columns})
    columns = {_normalize_stats_header(col): str(col) for col in frame.columns}

    col_name = _pick_stats_column(columns, ["Nome", "Giocatore"])
    col_team = _pick_stats_column(columns, ["Squadra", "Team"])
    col_role = _pick_stats_column(columns, ["R", "Ruolo"])
    col_qi = _pick_stats_column(columns, ["Qt.I", "Qti", "PrezzoIniziale"])
    col_qa = _pick_stats_column(columns, ["Qt.A", "Qta", "PrezzoAttuale"])

    if not col_name or not col_qa:
        raise LegheSyncError("Colonne Nome/Qt.A mancanti nel file quotazioni xlsx.")

    out: list[dict[str, object]] = []
    for _, row in frame.iterrows():
        player_name = str(row.get(col_name, "")).strip()
        if not player_name:
            continue

        qa = _parse_number(str(row.get(col_qa, "")), default=0.0)
        if qa <= 0:
            continue
        qi = _parse_number(str(row.get(col_qi, "")), default=qa) if col_qi else qa

        role_raw = str(row.get(col_role, "")).strip().upper() if col_role else ""
        role = ROLE_IMPORT_MAP.get(role_raw, role_raw[:1] if role_raw else "")

        team_raw = str(row.get(col_team, "")).strip() if col_team else ""
        team_upper = team_raw.upper()
        team = TEAM_ABBR_MAP.get(team_upper, team_raw.title() if team_raw else "")

        out.append(
            {
                "Giocatore": player_name,
                "Squadra": team,
                "Ruolo": role,
                "PrezzoIniziale": int(round(max(0.0, qi))),
                "PrezzoAttuale": int(round(max(0.0, qa))),
            }
        )

    out.sort(
        key=lambda row: (
            str(row.get("Ruolo") or ""),
            str(row.get("Giocatore") or "").lower(),
        )
    )
    return out


def _extract_fantacalcio_quotazioni_rows_from_html(source: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if not source:
        return rows

    row_pattern = re.compile(
        r'<tr\s+class="player-row"(?P<head>[^>]*)>(?P<body>.*?)</tr>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in row_pattern.finditer(source):
        head = str(match.group("head") or "")
        body = str(match.group("body") or "")

        role_match = re.search(r'data-filter-role-classic="([a-z])"', head, flags=re.IGNORECASE)
        role_raw = str(role_match.group(1) if role_match else "").strip().lower()
        role = ROLE_CLASSIC_MAP.get(role_raw, role_raw.upper()[:1] if role_raw else "")

        name_match = re.search(r'<th[^>]*class="player-name"[^>]*>.*?<span>(.*?)</span>', body, flags=re.DOTALL)
        name = _strip_html_text(name_match.group(1) if name_match else "")
        if not name:
            continue

        team_match = re.search(r'<td[^>]*data-col-key="sq"[^>]*>(.*?)</td>', body, flags=re.DOTALL)
        team_abbr = _strip_html_text(team_match.group(1) if team_match else "").upper()
        team = TEAM_ABBR_MAP.get(team_abbr, team_abbr.title() if team_abbr else "")

        qi_match = re.search(r'<td[^>]*data-col-key="c_qi"[^>]*>(.*?)</td>', body, flags=re.DOTALL)
        qa_match = re.search(r'<td[^>]*data-col-key="c_qa"[^>]*>(.*?)</td>', body, flags=re.DOTALL)
        qi = _parse_number(_strip_html_text(qi_match.group(1) if qi_match else ""), default=0.0)
        qa = _parse_number(_strip_html_text(qa_match.group(1) if qa_match else ""), default=0.0)
        if qa <= 0:
            continue

        rows.append(
            {
                "Giocatore": name,
                "Squadra": team,
                "Ruolo": role,
                "PrezzoIniziale": int(round(qi)),
                "PrezzoAttuale": int(round(qa)),
            }
        )

    rows.sort(
        key=lambda row: (
            str(row.get("Ruolo") or ""),
            str(row.get("Giocatore") or "").lower(),
        )
    )
    return rows


def _extract_fantacalcio_stats_rows_from_html(source: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if not source:
        return rows

    row_pattern = re.compile(
        r'<tr\s+class="player-row"(?P<head>[^>]*)>(?P<body>.*?)</tr>',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in row_pattern.finditer(source):
        head = str(match.group("head") or "")
        body = str(match.group("body") or "")

        role_match = re.search(r'data-filter-role-classic="([a-z])"', head, flags=re.IGNORECASE)
        role_raw = str(role_match.group(1) if role_match else "").strip().lower()
        role = ROLE_CLASSIC_MAP.get(role_raw, role_raw.upper()[:1] if role_raw else "")

        name_match = re.search(r'<th[^>]*class="player-name"[^>]*>.*?<span>(.*?)</span>', body, flags=re.DOTALL)
        name = _strip_html_text(name_match.group(1) if name_match else "")
        if not name:
            continue

        team_abbr = _extract_row_col_text(body, "sq").upper()
        if not team_abbr:
            continue

        rig_scored, rig_missed = _parse_rigori_cell(_extract_row_col_text(body, "rig"))
        rows.append(
            {
                "Giocatore": name,
                "Posizione": role,
                "Squadra": team_abbr,
                "pg": _parse_int_number(_extract_row_col_text(body, "pg"), default=0),
                "mv": _parse_number(_extract_row_col_text(body, "mv"), default=0.0),
                "mfv": _parse_number(_extract_row_col_text(body, "mfv"), default=0.0),
                "gol": _parse_int_number(_extract_row_col_text(body, "gol"), default=0),
                "gs": _parse_int_number(_extract_row_col_text(body, "gs"), default=0),
                "autogol": 0,
                "rigori_segnati": rig_scored,
                "rigori_sbagliati": rig_missed,
                "rp": _parse_int_number(_extract_row_col_text(body, "rp"), default=0),
                "ass": _parse_int_number(_extract_row_col_text(body, "ass"), default=0),
                "amm": _parse_int_number(_extract_row_col_text(body, "amm"), default=0),
                "esp": _parse_int_number(_extract_row_col_text(body, "esp"), default=0),
            }
        )

    rows.sort(
        key=lambda row: (
            str(row.get("Posizione") or ""),
            str(row.get("Giocatore") or "").lower(),
        )
    )
    return rows


def download_fantacalcio_stats_csv_bundle(
    *,
    season_slug: str | None = None,
    date_stamp: str | None = None,
    out_dir: Path | None = None,
    username: str | None = None,
    password: str | None = None,
) -> dict[str, object]:
    now = datetime.now(tz=timezone.utc)
    resolved_slug = (season_slug or _season_slug_for(now)).strip()
    if not re.match(r"^\d{4}-\d{2}$", resolved_slug):
        raise LegheSyncError(f"season_slug non valido (atteso YYYY-YY): {resolved_slug}")

    stamp = (date_stamp or _today_stamp()).strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", stamp):
        raise LegheSyncError(f"date_stamp non valido (atteso YYYY-MM-DD): {stamp}")

    resolved_out_dir = out_dir or (_data_dir() / "incoming" / "stats")
    url = f"{FANTACALCIO_STATS_BASE_URL}/{resolved_slug}"
    resolved_out_dir.mkdir(parents=True, exist_ok=True)

    warnings: list[str] = []
    source_kind = "html_public"
    auth_attempted = bool((username or "").strip() and (password or "").strip())
    auth_ok = False
    stat_rows: list[dict[str, object]] = []
    xlsx_path = resolved_out_dir / f"statistiche_{stamp}.xlsx"

    if auth_attempted:
        opener, _ = _build_cookie_opener()
        login_result = _fantacalcio_login(
            opener,
            username=str(username or "").strip(),
            password=str(password or "").strip(),
        )
        auth_ok = bool(login_result.get("ok"))
        if not auth_ok:
            warnings.append(str(login_result.get("error") or "Fantacalcio login failed"))
        else:
            downloaded_xlsx = _download_fantacalcio_excel_authenticated(
                opener,
                endpoint_path=FANTACALCIO_EXCEL_STATS_ENDPOINT,
                referer=url,
                out_path=xlsx_path,
            )
            if bool(downloaded_xlsx.get("ok")):
                try:
                    stat_rows = _extract_fantacalcio_stats_rows_from_xlsx(xlsx_path)
                    if stat_rows:
                        source_kind = "xlsx_authenticated"
                except Exception as exc:
                    warnings.append(f"stats xlsx parse failed: {exc}")
            else:
                warnings.append(str(downloaded_xlsx.get("warning") or "stats xlsx download failed"))

    if not stat_rows:
        opener = build_opener()
        body, _ = _http_read_bytes(
            opener,
            url,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": f"{FANTACALCIO_BASE_URL}/",
            },
            timeout_seconds=45,
        )
        source = body.decode("utf-8", errors="replace")
        stat_rows = _extract_fantacalcio_stats_rows_from_html(source)

    if not stat_rows:
        result: dict[str, object] = {
            "ok": False,
            "warning": "No stats rows parsed from sources",
            "url": url,
            "season_slug": resolved_slug,
            "source": source_kind,
            "auth_attempted": auth_attempted,
            "auth_ok": auth_ok,
            "rows": 0,
            "files": [],
        }
        if warnings:
            result["warnings"] = warnings
        return result

    generated_files = _write_stats_bundle_files(
        stat_rows=stat_rows,
        out_dir=resolved_out_dir,
        stamp=stamp,
    )
    try:
        kickest_result = download_kickest_cleansheet_csv(
            date_stamp=stamp,
            out_dir=resolved_out_dir,
        )
        if bool(kickest_result.get("ok")):
            generated_files.append(
                {
                    "prefix": "cleansheet",
                    "label": "Cleansheet",
                    "path": str(kickest_result.get("path") or ""),
                    "rows": int(kickest_result.get("rows") or 0),
                    "source": "kickest_html",
                }
            )
        else:
            warnings.append(
                str(
                    kickest_result.get("warning")
                    or "kickest cleansheet download failed"
                )
            )
    except Exception as exc:
        warnings.append(f"kickest cleansheet fetch failed: {exc}")
    result = {
        "ok": True,
        "url": url,
        "season_slug": resolved_slug,
        "source": source_kind,
        "auth_attempted": auth_attempted,
        "auth_ok": auth_ok,
        "xlsx_path": str(xlsx_path) if source_kind == "xlsx_authenticated" else None,
        "rows": int(len(stat_rows)),
        "files": generated_files,
    }
    if warnings:
        result["warnings"] = warnings
    return result


def download_fantacalcio_quotazioni_csv(
    *,
    season_slug: str | None = None,
    date_stamp: str | None = None,
    out_path: Path | None = None,
    username: str | None = None,
    password: str | None = None,
) -> dict[str, object]:
    now = datetime.now(tz=timezone.utc)
    resolved_slug = (season_slug or _season_slug_for(now)).strip()
    if not re.match(r"^\d{4}-\d{2}$", resolved_slug):
        raise LegheSyncError(f"season_slug non valido (atteso YYYY-YY): {resolved_slug}")

    stamp = (date_stamp or _today_stamp()).strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", stamp):
        raise LegheSyncError(f"date_stamp non valido (atteso YYYY-MM-DD): {stamp}")

    resolved_out = out_path or (_data_dir() / "incoming" / "quotazioni" / f"quotazioni_{stamp}.csv")
    url = f"{FANTACALCIO_QUOTAZIONI_BASE_URL}/{resolved_slug}"
    warnings: list[str] = []
    source_kind = "html_public"
    auth_attempted = bool((username or "").strip() and (password or "").strip())
    auth_ok = False
    rows: list[dict[str, object]] = []
    xlsx_path = resolved_out.with_suffix(".xlsx")

    if auth_attempted:
        opener, _ = _build_cookie_opener()
        login_result = _fantacalcio_login(
            opener,
            username=str(username or "").strip(),
            password=str(password or "").strip(),
        )
        auth_ok = bool(login_result.get("ok"))
        if not auth_ok:
            warnings.append(str(login_result.get("error") or "Fantacalcio login failed"))
        else:
            _ensure_parent(xlsx_path)
            downloaded_xlsx = _download_fantacalcio_excel_authenticated(
                opener,
                endpoint_path=FANTACALCIO_EXCEL_PRICES_ENDPOINT,
                referer=url,
                out_path=xlsx_path,
            )
            if bool(downloaded_xlsx.get("ok")):
                try:
                    rows = _extract_fantacalcio_quotazioni_rows_from_xlsx(xlsx_path)
                    if rows:
                        source_kind = "xlsx_authenticated"
                except Exception as exc:
                    warnings.append(f"quotazioni xlsx parse failed: {exc}")
            else:
                warnings.append(str(downloaded_xlsx.get("warning") or "quotazioni xlsx download failed"))

    if not rows:
        opener = build_opener()
        body, _ = _http_read_bytes(
            opener,
            url,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Referer": f"{FANTACALCIO_BASE_URL}/",
            },
            timeout_seconds=45,
        )
        source = body.decode("utf-8", errors="replace")
        rows = _extract_fantacalcio_quotazioni_rows_from_html(source)

    if not rows:
        result: dict[str, object] = {
            "ok": False,
            "warning": "No quotazioni rows parsed from sources",
            "url": url,
            "season_slug": resolved_slug,
            "source": source_kind,
            "auth_attempted": auth_attempted,
            "auth_ok": auth_ok,
            "path": str(resolved_out),
            "rows": 0,
        }
        if warnings:
            result["warnings"] = warnings
        return result

    _ensure_parent(resolved_out)
    fields = ["Giocatore", "Squadra", "Ruolo", "PrezzoIniziale", "PrezzoAttuale"]
    with resolved_out.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    result = {
        "ok": True,
        "url": url,
        "season_slug": resolved_slug,
        "source": source_kind,
        "auth_attempted": auth_attempted,
        "auth_ok": auth_ok,
        "xlsx_path": str(xlsx_path) if source_kind == "xlsx_authenticated" else None,
        "path": str(resolved_out),
        "rows": int(len(rows)),
    }
    if warnings:
        result["warnings"] = warnings
    return result


def _http_read_bytes(
    opener,
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
    timeout_seconds: int = 30,
) -> tuple[bytes, dict[str, str]]:
    request_headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.7,en;q=0.6",
        # Avoid gzip so urllib doesn't need to transparently decompress.
        "Accept-Encoding": "identity",
        **(headers or {}),
    }
    req = Request(url, data=data, method=method, headers=request_headers)
    try:
        with opener.open(req, timeout=timeout_seconds) as resp:
            body = resp.read()
            resp_headers = {k.lower(): str(v) for k, v in dict(resp.headers).items()}
            return body, resp_headers
    except HTTPError as exc:
        try:
            body = exc.read()  # type: ignore[attr-defined]
        except Exception:
            _logger.debug("Failed to read HTTP error body for %s", url, exc_info=True)
            body = b""
        snippet = body[:400].decode("utf-8", errors="replace")
        raise LegheSyncError(f"HTTP {exc.code} for {url} | body={snippet}") from exc
    except URLError as exc:
        raise LegheSyncError(f"Network error for {url}: {exc}") from exc


def _parse_leghe_context(html: str, *, alias: str) -> LegheContext:
    app_key_match = re.search(r'authAppKey\s*:\s*"([^"]+)"', html)
    if not app_key_match:
        raise LegheSyncError("Impossibile trovare authAppKey nella pagina Leghe.")
    app_key = app_key_match.group(1).strip()

    competition_id: int | None = None
    comp_match = re.search(r'competitionId\s*:\s*"?(?P<id>\d+)"?', html)
    if comp_match:
        try:
            competition_id = int(comp_match.group("id"))
        except (TypeError, ValueError):
            competition_id = None

    competition_name: str | None = None
    idx = html.find("currentCompetition")
    if idx >= 0:
        window = html[idx : idx + 20000]
        name_match = re.search(r"\"nome\"\s*:\s*\"([^\"]+)\"", window)
        if name_match:
            competition_name = name_match.group(1).strip()

    current_turn: int | None = None
    current_turn_match = re.search(r'currentTurn\s*:\s*"?(?P<turn>\d+)"?', html)
    if current_turn_match:
        try:
            current_turn = int(current_turn_match.group("turn"))
        except (TypeError, ValueError):
            current_turn = None

    last_calculated: int | None = None
    last_match = re.search(r"ultima giornata calcolata\s*(\d+)", html, re.IGNORECASE)
    if last_match:
        try:
            last_calculated = int(last_match.group(1))
        except (TypeError, ValueError):
            last_calculated = None

    suggested: int | None = None
    if last_calculated is not None:
        suggested = max(1, last_calculated + 1)
    elif current_turn is not None:
        suggested = max(1, current_turn)

    return LegheContext(
        alias=alias,
        app_key=app_key,
        competition_id=competition_id,
        competition_name=competition_name,
        current_turn=current_turn,
        last_calculated_matchday=last_calculated,
        suggested_formations_matchday=suggested,
    )


def fetch_leghe_context(opener, *, alias: str) -> LegheContext:
    url = f"{LEGHE_BASE_URL}{alias}/formazioni"
    body, _ = _http_read_bytes(opener, url, headers={"Accept": "text/html"})
    html = body.decode("utf-8", errors="replace")
    return _parse_leghe_context(html, alias=alias)


def download_formazioni_context_html(
    opener,
    *,
    alias: str,
    out_path: Path,
) -> dict[str, object]:
    url = f"{LEGHE_BASE_URL}{alias}/formazioni"
    body, resp_headers = _http_read_bytes(
        opener,
        url,
        headers={
            "Accept": "text/html",
            "Referer": f"{LEGHE_BASE_URL}{alias}/",
        },
        timeout_seconds=30,
    )
    html = body.decode("utf-8", errors="replace")
    if "authAppKey" not in html:
        snippet = html[:300].replace("\n", " ")
        raise LegheSyncError(f"HTML formazioni senza authAppKey: {snippet}")
    _atomic_write_bytes(out_path, body)
    return {
        "ok": True,
        "path": str(out_path),
        "bytes": int(len(body)),
        "content_type": str(resp_headers.get("content-type") or ""),
    }


def leghe_login(
    opener,
    *,
    alias: str,
    app_key: str,
    username: str,
    password: str,
) -> dict[str, Any]:
    url = f"{LEGHE_BASE_URL}api/v1/v1_utente/login?alias_lega={alias}"
    payload = json.dumps({"username": username, "password": password}).encode("utf-8")
    body, resp_headers = _http_read_bytes(
        opener,
        url,
        method="PUT",
        headers={
            "Content-Type": "application/json",
            "app_key": app_key,
            "Accept": "application/json",
            "Referer": f"{LEGHE_BASE_URL}{alias}/",
            "Origin": LEGHE_BASE_URL.rstrip("/"),
            "X-Requested-With": "XMLHttpRequest",
        },
        data=payload,
        timeout_seconds=30,
    )
    content_type = resp_headers.get("content-type", "")
    if "application/json" in content_type.lower():
        try:
            parsed = json.loads(body.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            parsed = {"raw": body[:400].decode("utf-8", errors="replace")}
        return parsed if isinstance(parsed, dict) else {"response": parsed}
    return {"raw": body[:400].decode("utf-8", errors="replace")}


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    _ensure_parent(path)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(payload)
    tmp.replace(path)


def _looks_like_xlsx(resp_headers: dict[str, str], body: bytes) -> bool:
    ctype = str(resp_headers.get("content-type") or "").lower()
    if "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in ctype:
        return True
    if "application/vnd.ms-excel" in ctype:
        return True
    # XLSX is a zip file.
    if body.startswith(b"PK\x03\x04"):
        return True
    # Legacy XLS is OLE compound document.
    if body.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"):
        return True
    return False


def download_leghe_excel(
    opener,
    *,
    url: str,
    app_key: str,
    referer: str,
    out_path: Path,
) -> dict[str, object]:
    body, resp_headers = _http_read_bytes(
        opener,
        url,
        method="GET",
        headers={
            "app_key": app_key,
            "Referer": referer,
        },
        timeout_seconds=60,
    )
    if not _looks_like_xlsx(resp_headers, body):
        snippet = body[:400].decode("utf-8", errors="replace")
        raise LegheSyncError(f"Download non XLSX: url={url} content_type={resp_headers.get('content-type')} body={snippet}")
    _atomic_write_bytes(out_path, body)
    return {
        "ok": True,
        "path": str(out_path),
        "bytes": int(len(body)),
        "content_type": str(resp_headers.get("content-type") or ""),
    }


def download_leghe_service_json(
    opener,
    *,
    url: str,
    app_key: str,
    referer: str,
    timeout_seconds: int = 30,
) -> dict[str, object]:
    body, resp_headers = _http_read_bytes(
        opener,
        url,
        method="GET",
        headers={
            "app_key": app_key,
            "Referer": referer,
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout_seconds=timeout_seconds,
    )
    raw = body.decode("utf-8", errors="replace")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        snippet = raw[:400].replace("\n", " ")
        raise LegheSyncError(f"Download JSON non valido: url={url} body={snippet}") from exc

    if not isinstance(parsed, dict):
        raise LegheSyncError(f"Download JSON non valido: url={url} payload non-object")

    parsed.setdefault("_content_type", str(resp_headers.get("content-type") or ""))
    return parsed


def fetch_leghe_formazioni_service_payloads(
    *,
    alias: str,
    username: str,
    password: str,
    competition_id: int | None = None,
    matchday: int | None = None,
    team_ids: list[int] | None = None,
) -> dict[str, object]:
    opener, jar = _build_leghe_opener()
    context = fetch_leghe_context(opener, alias=alias)

    leghe_login(
        opener,
        alias=alias,
        app_key=context.app_key,
        username=username,
        password=password,
    )

    resolved_competition_id = int(competition_id or 0) or context.competition_id
    if not resolved_competition_id:
        raise LegheSyncError("competition_id mancante: non posso leggere le formazioni via service.")

    resolved_matchday = (
        int(matchday or 0)
        or context.current_turn
        or context.suggested_formations_matchday
        or context.last_calculated_matchday
    )
    if not resolved_matchday:
        raise LegheSyncError("matchday non disponibile: non posso leggere le formazioni via service.")

    referer = f"{LEGHE_BASE_URL}{alias}/formazioni"
    attempts: list[dict[str, object]] = []
    payloads: list[dict[str, object]] = []
    seen_payload_keys: set[str] = set()

    def _register_payload(label: str, parsed: dict[str, object]) -> None:
        compact = json.dumps(parsed, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        if compact in seen_payload_keys:
            return
        seen_payload_keys.add(compact)
        payloads.append({"source": label, "payload": parsed})

    def _try(label: str, url: str) -> None:
        try:
            parsed = download_leghe_service_json(
                opener,
                url=url,
                app_key=context.app_key,
                referer=referer,
                timeout_seconds=30,
            )
            success_flag = bool(parsed.get("success")) if isinstance(parsed, dict) else False
            attempts.append(
                {
                    "ok": True,
                    "source": label,
                    "url": url,
                    "success": success_flag,
                }
            )
            _register_payload(label, parsed)
        except Exception as exc:
            attempts.append(
                {
                    "ok": False,
                    "source": label,
                    "url": url,
                    "warning": str(exc),
                }
            )

    base_params = {
        "alias_lega": alias,
        "id_comp": int(resolved_competition_id),
    }
    visualizza_params = {
        **base_params,
        "giornata_lega": int(resolved_matchday),
    }
    lista_params = {
        **base_params,
        "giornata": int(resolved_matchday),
    }

    _try(
        "visualizza_all",
        f"{LEGHE_BASE_URL}servizi/V1_LegheFormazioni/Visualizza?{urlencode(visualizza_params)}",
    )
    _try(
        "lista",
        f"{LEGHE_BASE_URL}servizi/V1_LegheFormazioni/lista?{urlencode(lista_params)}",
    )
    _try(
        "live_visualizza_all",
        f"{LEGHE_BASE_URL}servizi/V1_LegheLive/Visualizza?{urlencode({**base_params, 'id_squadra': 0})}",
    )

    for team_id in team_ids or []:
        if int(team_id or 0) <= 0:
            continue
        params = {
            **visualizza_params,
            "id_squadra": int(team_id),
        }
        _try(
            f"visualizza_team_{int(team_id)}",
            f"{LEGHE_BASE_URL}servizi/V1_LegheFormazioni/Visualizza?{urlencode(params)}",
        )

    return {
        "ok": True,
        "alias": alias,
        "matchday": int(resolved_matchday),
        "competition_id": int(resolved_competition_id),
        "context": {
            "current_turn": context.current_turn,
            "last_calculated_matchday": context.last_calculated_matchday,
            "suggested_formations_matchday": context.suggested_formations_matchday,
        },
        "cookies": len(list(jar)),
        "attempts": attempts,
        "payloads": payloads,
    }


def _build_leghe_opener() -> tuple[object, http.cookiejar.CookieJar]:
    jar = http.cookiejar.CookieJar()
    opener = build_opener(HTTPCookieProcessor(jar))
    return opener, jar


def _season_for(dt: datetime) -> str:
    # Same logic as scripts/update-all.ps1
    if dt.month >= 7:
        start_year = dt.year
        end_short = str(dt.year + 1)[2:4]
        return f"{start_year}-{end_short}"
    prev_year = dt.year - 1
    curr_short = str(dt.year)[2:4]
    return f"{prev_year}-{curr_short}"


def _write_status(payload: dict[str, object]) -> None:
    path = _data_dir() / "status.json"
    _ensure_parent(path)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)


def _run_subprocess(argv: list[str], *, cwd: Path) -> dict[str, object]:
    started = datetime.now(tz=timezone.utc)
    proc = subprocess.run(
        argv,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        check=False,
        env={
            **os.environ,
            # Make sure scripts behave consistently.
            "PYTHONUNBUFFERED": "1",
        },
    )
    ended = datetime.now(tz=timezone.utc)
    return {
        "argv": argv,
        "returncode": int(proc.returncode),
        "stdout": (proc.stdout or "")[-4000:],
        "stderr": (proc.stderr or "")[-4000:],
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "duration_seconds": (ended - started).total_seconds(),
    }


def _unique_positive_ints(values: list[int | None]) -> list[int]:
    ordered: list[int] = []
    seen: set[int] = set()
    for value in values:
        if value is None:
            continue
        current = int(value)
        if current <= 0 or current in seen:
            continue
        seen.add(current)
        ordered.append(current)
    return ordered


def _build_formazioni_matchday_candidates(
    *,
    context: LegheContext,
    preferred_matchday: int | None,
) -> list[int]:
    base_values = _unique_positive_ints(
        [
            preferred_matchday,
            context.current_turn,
            context.suggested_formations_matchday,
            context.last_calculated_matchday,
            (context.current_turn - 1) if context.current_turn else None,
            (context.suggested_formations_matchday - 1)
            if context.suggested_formations_matchday
            else None,
        ]
    )
    if not base_values:
        return []

    max_seed = max(base_values)
    fallback_tail = [value for value in range(max_seed - 1, max(max_seed - 6, 0), -1)]
    return _unique_positive_ints([*base_values, *fallback_tail])


def _xlsx_formazioni_rows_count(path: Path) -> int:
    return int(_inspect_formazioni_xlsx(path).get("rows") or 0)


def _inspect_formazioni_xlsx(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"rows": 0, "lineup_frames": 0, "sheets": []}

    try:
        import pandas as pd
    except Exception:
        return {"rows": 0, "lineup_frames": 0, "sheets": []}

    try:
        sheets = pd.read_excel(path, sheet_name=None)
    except Exception:
        return {"rows": 0, "lineup_frames": 0, "sheets": []}

    if not isinstance(sheets, dict):
        return {"rows": 0, "lineup_frames": 0, "sheets": []}

    lineup_frames = []
    fallback_frames = []
    sheet_debug: list[dict[str, object]] = []
    for sheet_name, frame in sheets.items():
        if frame is None or frame.empty:
            sheet_debug.append(
                {
                    "name": str(sheet_name),
                    "rows": 0,
                    "columns": [],
                    "has_team": False,
                    "has_lineup": False,
                }
            )
            continue

        fallback_frames.append(frame)
        columns = {
            normalize
            for normalize in (
                re.sub(r"[^a-z0-9]+", "", str(column or "").strip().lower())
                for column in frame.columns
            )
            if normalize
        }
        has_team = bool(columns.intersection({"team", "squadra", "fantateam", "fantasquadra", "teamname"}))
        has_lineup = bool(
            columns.intersection(
                {
                    "portiere",
                    "difensori",
                    "centrocampisti",
                    "attaccanti",
                    "titolare1",
                    "starter1",
                }
            )
        )
        sheet_debug.append(
            {
                "name": str(sheet_name),
                "rows": int(len(frame.index)),
                "columns": sorted(list(columns))[:30],
                "has_team": bool(has_team),
                "has_lineup": bool(has_lineup),
            }
        )
        if has_team and has_lineup:
            lineup_frames.append(frame)

    frames_to_scan = lineup_frames or fallback_frames
    if not frames_to_scan:
        return {
            "rows": 0,
            "lineup_frames": int(len(lineup_frames)),
            "sheets": sheet_debug,
        }

    row_count = 0
    for frame in frames_to_scan:
        current = frame.fillna("")
        for _, row in current.iterrows():
            values = {
                re.sub(r"[^a-z0-9]+", "", str(key or "").strip().lower()): str(value or "").strip()
                for key, value in row.to_dict().items()
                if key is not None
            }
            if not any(values.values()):
                continue

            if lineup_frames:
                team_value = (
                    values.get("team")
                    or values.get("squadra")
                    or values.get("fantateam")
                    or values.get("fantasquadra")
                    or values.get("teamname")
                )
                lineup_value = (
                    values.get("portiere")
                    or values.get("difensori")
                    or values.get("centrocampisti")
                    or values.get("attaccanti")
                    or values.get("titolare1")
                    or values.get("starter1")
                )
                if team_value and lineup_value:
                    row_count += 1
            else:
                if sum(1 for value in values.values() if value) >= 3:
                    row_count += 1

    return {
        "rows": int(row_count),
        "lineup_frames": int(len(lineup_frames)),
        "sheets": sheet_debug,
    }


def download_leghe_formazioni_xlsx_with_fallback(
    opener,
    *,
    alias: str,
    app_key: str,
    competition_id: int,
    competition_name: str,
    referer: str,
    out_path: Path,
    context: LegheContext,
    preferred_matchday: int | None = None,
) -> dict[str, object]:
    if not competition_id:
        return {"ok": False, "warning": "competition_id not available", "attempts": []}

    candidates = _build_formazioni_matchday_candidates(
        context=context,
        preferred_matchday=preferred_matchday,
    )
    if not candidates:
        return {"ok": False, "warning": "formations_matchday not available", "attempts": []}

    attempts: list[dict[str, object]] = []
    for matchday in candidates:
        params = {
            "alias_lega": alias,
            "id_competizione": int(competition_id),
            "giornata": int(matchday),
            "nome_competizione": competition_name,
            "dummy": 5,
        }
        form_url = f"{LEGHE_BASE_URL}servizi/V1_LegheFormazioni/excel?{urlencode(params)}"
        try:
            downloaded = download_leghe_excel(
                opener,
                url=form_url,
                app_key=app_key,
                referer=referer,
                out_path=out_path,
            )
        except LegheSyncError as exc:
            attempts.append(
                {
                    "ok": False,
                    "matchday": int(matchday),
                    "warning": str(exc),
                }
            )
            continue

        inspected = _inspect_formazioni_xlsx(out_path)
        rows = int(inspected.get("rows") or 0)
        attempts.append(
            {
                "ok": True,
                "matchday": int(matchday),
                "rows": int(rows),
                "bytes": int(downloaded.get("bytes") or 0),
                "lineup_frames": int(inspected.get("lineup_frames") or 0),
            }
        )
        if rows > 0:
            return {
                **downloaded,
                "ok": True,
                "selected_matchday": int(matchday),
                "rows": int(rows),
                "attempts": attempts,
            }

    return {
        "ok": False,
        "warning": "XLSX formazioni non disponibile o vuoto per le giornate candidate.",
        "path": str(out_path) if out_path.exists() else "",
        "selected_matchday": None,
        "rows": 0,
        "attempts": attempts,
    }


def refresh_formazioni_context_from_leghe(
    *,
    alias: str,
    out_path: Path,
    username: str | None = None,
    password: str | None = None,
    out_xlsx_path: Path | None = None,
    competition_id: int | None = None,
    competition_name: str | None = None,
    formations_matchday: int | None = None,
) -> dict[str, object]:
    opener, _ = _build_leghe_opener()
    context = fetch_leghe_context(opener, alias=alias)

    if username and password:
        leghe_login(
            opener,
            alias=alias,
            app_key=context.app_key,
            username=username,
            password=password,
        )

    downloaded = download_formazioni_context_html(
        opener,
        alias=alias,
        out_path=out_path,
    )

    xlsx_result: dict[str, object] | None = None
    if out_xlsx_path is not None:
        resolved_competition_id = int(competition_id or 0) or context.competition_id
        resolved_competition_name = (competition_name or context.competition_name or alias).strip()
        if resolved_competition_id:
            xlsx_result = download_leghe_formazioni_xlsx_with_fallback(
                opener,
                alias=alias,
                app_key=context.app_key,
                competition_id=int(resolved_competition_id),
                competition_name=resolved_competition_name,
                referer=f"{LEGHE_BASE_URL}{alias}/formazioni",
                out_path=out_xlsx_path,
                context=context,
                preferred_matchday=int(formations_matchday or 0) or None,
            )
        else:
            xlsx_result = {
                "ok": False,
                "warning": "competition_id not available",
            }

    return {
        "ok": True,
        "alias": alias,
        "context": {
            "competition_id": context.competition_id,
            "competition_name": context.competition_name,
            "current_turn": context.current_turn,
            "last_calculated_matchday": context.last_calculated_matchday,
            "suggested_formations_matchday": context.suggested_formations_matchday,
        },
        "downloaded": downloaded,
        "formazioni_xlsx": xlsx_result,
    }


def run_leghe_sync_and_pipeline(
    *,
    alias: str,
    username: str,
    password: str,
    date_stamp: str | None = None,
    competition_id: int | None = None,
    competition_name: str | None = None,
    formations_matchday: int | None = None,
    download_rose: bool = True,
    download_classifica: bool = True,
    download_formazioni: bool = True,
    download_formazioni_xlsx: bool = True,
    fetch_quotazioni: bool = False,
    quotazioni_season_slug: str | None = None,
    fetch_global_stats: bool = False,
    stats_season_slug: str | None = None,
    run_pipeline: bool = True,
) -> dict[str, object]:
    stamp = (date_stamp or _today_stamp()).strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", stamp):
        raise LegheSyncError(f"date_stamp non valido (atteso YYYY-MM-DD): {stamp}")

    update_id = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    now = datetime.now(tz=timezone.utc)
    steps: dict[str, str] = {"rose": "pending", "stats": "pending", "strength": "pending"}

    _write_status(
        {
            "last_update": now.isoformat().replace("+00:00", "Z"),
            "result": "running",
            "message": "Aggiornamento automatico (Leghe) in corso...",
            "season": _season_for(now),
            "update_id": update_id,
            "steps": steps,
        }
    )

    opener, jar = _build_leghe_opener()
    context: LegheContext | None = None
    downloaded: dict[str, dict[str, object]] = {}
    pipeline_runs: list[dict[str, object]] = []
    pipeline_warnings: list[str] = []
    effective_formations_matchday: int | None = formations_matchday

    try:
        context = fetch_leghe_context(opener, alias=alias)
        if competition_id is None:
            competition_id = context.competition_id
        if competition_name is None:
            competition_name = context.competition_name
        if formations_matchday is None:
            formations_matchday = context.suggested_formations_matchday
        effective_formations_matchday = formations_matchday

        leghe_login(
            opener,
            alias=alias,
            app_key=context.app_key,
            username=username,
            password=password,
        )

        data_dir = _data_dir()

        if download_rose:
            rose_url = f"{LEGHE_BASE_URL}servizi/v1_legheSquadra/excel?alias_lega={alias}"
            out_path = data_dir / "incoming" / "rose" / "rose.xlsx"
            downloaded["rose"] = download_leghe_excel(
                opener,
                url=rose_url,
                app_key=context.app_key,
                referer=f"{LEGHE_BASE_URL}{alias}/rose",
                out_path=out_path,
            )

        if download_classifica:
            if not competition_id:
                raise LegheSyncError("competition_id mancante: non posso scaricare la classifica.")
            comp_name = (competition_name or alias).strip()
            class_url = (
                f"{LEGHE_BASE_URL}servizi/v1_legheCompetizione/excelClassifica?"
                f"{urlencode({'tipo': 1, 'alias_lega': alias, 'id_competizione': competition_id, 'nome_competizione': comp_name})}"
            )
            out_path = data_dir / "incoming" / "classifica" / "classifica.xlsx"
            downloaded["classifica"] = download_leghe_excel(
                opener,
                url=class_url,
                app_key=context.app_key,
                referer=f"{LEGHE_BASE_URL}{alias}/classifica",
                out_path=out_path,
            )

        if download_formazioni:
            # Primary source for "formazioni live" in this project:
            # refresh the context HTML used by appkey payload extraction.
            html_path = data_dir / "tmp" / "formazioni_page.html"
            downloaded["formazioni_html"] = download_formazioni_context_html(
                opener,
                alias=alias,
                out_path=html_path,
            )

            stamped_html_path = data_dir / "tmp" / f"formazioni_{stamp}.html"
            downloaded["formazioni_html_stamped"] = download_formazioni_context_html(
                opener,
                alias=alias,
                out_path=stamped_html_path,
            )

        if download_formazioni and download_formazioni_xlsx:
            if not competition_id:
                raise LegheSyncError("competition_id mancante: non posso scaricare le formazioni.")
            out_path = data_dir / "incoming" / "formazioni" / "formazioni.xlsx"
            downloaded["formazioni_xlsx"] = download_leghe_formazioni_xlsx_with_fallback(
                opener,
                alias=alias,
                app_key=context.app_key,
                competition_id=int(competition_id),
                competition_name=(competition_name or context.competition_name or alias).strip(),
                referer=f"{LEGHE_BASE_URL}{alias}/formazioni",
                out_path=out_path,
                context=context,
                preferred_matchday=int(formations_matchday or 0) or None,
            )
            selected_matchday = downloaded["formazioni_xlsx"].get("selected_matchday")
            if isinstance(selected_matchday, int) and selected_matchday > 0:
                effective_formations_matchday = selected_matchday

        root = _repo_root()

        def _write_running_status(message: str) -> None:
            _write_status(
                {
                    "last_update": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                    "result": "running",
                    "message": message,
                    "season": _season_for(datetime.now(tz=timezone.utc)),
                    "update_id": update_id,
                    "matchday": int(effective_formations_matchday) if effective_formations_matchday else None,
                    "steps": steps,
                }
            )

        def _run_pipeline_step(
            argv: list[str],
            *,
            label: str,
            fatal: bool = True,
        ) -> bool:
            run_item = _run_subprocess(argv, cwd=root)
            pipeline_runs.append(run_item)
            if int(run_item.get("returncode") or 0) == 0:
                return True
            if fatal:
                raise LegheSyncError(f"{label} failed (rc={run_item.get('returncode')})")
            pipeline_warnings.append(f"{label} failed (rc={run_item.get('returncode')})")
            return False

        if fetch_quotazioni:
            try:
                quotazioni_result = download_fantacalcio_quotazioni_csv(
                    season_slug=quotazioni_season_slug,
                    date_stamp=stamp,
                    username=username,
                    password=password,
                )
                downloaded["quotazioni"] = dict(quotazioni_result)
                if not bool(quotazioni_result.get("ok")):
                    pipeline_warnings.append("fetch_quotazioni non ha prodotto righe utilizzabili")
            except Exception as exc:
                pipeline_warnings.append(f"fetch_quotazioni failed: {exc}")

        if fetch_global_stats:
            try:
                global_stats_result = download_fantacalcio_stats_csv_bundle(
                    season_slug=stats_season_slug,
                    date_stamp=stamp,
                    username=username,
                    password=password,
                )
                downloaded["global_stats"] = dict(global_stats_result)
                if not bool(global_stats_result.get("ok")):
                    pipeline_warnings.append("fetch_global_stats non ha prodotto righe utilizzabili")
            except Exception as exc:
                pipeline_warnings.append(f"fetch_global_stats failed: {exc}")

        if run_pipeline:

            # 1) classifica + rose/quotazioni (+ market)
            steps["rose"] = "running"
            _write_running_status("Aggiornamento in corso: Rose/Quotazioni...")
            _run_pipeline_step(
                [
                    sys.executable,
                    str(root / "scripts" / "pipeline_v2.py"),
                    "--domains",
                    "classifica",
                    "--date",
                    stamp,
                ],
                label="pipeline_v2 classifica",
                fatal=True,
            )
            _run_pipeline_step(
                [
                    sys.executable,
                    str(root / "scripts" / "update_data.py"),
                    "--auto",
                    "--date",
                    stamp,
                    "--keep",
                    "5",
                ],
                label="update_data",
                fatal=True,
            )
            steps["rose"] = "ok"

            # 2) statistiche (stats/*.csv + statistiche_giocatori.csv + eventuali update DB csv)
            steps["stats"] = "running"
            _write_running_status("Aggiornamento in corso: Statistiche...")
            _run_pipeline_step(
                [
                    sys.executable,
                    str(root / "scripts" / "clean_stats_batch.py"),
                ],
                label="clean_stats_batch",
                fatal=True,
            )
            steps["stats"] = "ok"

            # 3) forza squadra / XI / report premium
            steps["strength"] = "running"
            _write_running_status("Aggiornamento in corso: Forza squadra e XI...")
            _run_pipeline_step(
                [
                    sys.executable,
                    str(root / "scripts" / "update_fixtures.py"),
                ],
                label="update_fixtures",
                fatal=False,
            )
            sync_seriea_cmd = [
                sys.executable,
                str(root / "scripts" / "sync_seriea_live_context.py"),
                "--season",
                _season_slug_for(now),
            ]
            if effective_formations_matchday is not None:
                sync_seriea_cmd.extend(["--round", str(int(effective_formations_matchday))])
            _run_pipeline_step(
                sync_seriea_cmd,
                label="sync_seriea_live_context",
                fatal=False,
            )
            _run_pipeline_step(
                [
                    sys.executable,
                    str(root / "scripts" / "build_player_tiers.py"),
                ],
                label="build_player_tiers",
                fatal=True,
            )
            _run_pipeline_step(
                [
                    sys.executable,
                    str(root / "scripts" / "build_team_strength_ranking.py"),
                    "--snapshot",
                    "--snapshot-date",
                    stamp,
                ],
                label="build_team_strength_ranking",
                fatal=True,
            )
            steps["strength"] = "ok"
        else:
            if fetch_quotazioni:
                steps["rose"] = "running"
                _write_running_status("Aggiornamento in corso: Applicazione Quotazioni...")
                quot_ok = bool((downloaded.get("quotazioni") or {}).get("ok"))
                if quot_ok:
                    applied = _run_pipeline_step(
                        [
                            sys.executable,
                            str(root / "scripts" / "update_data.py"),
                            "--auto",
                            "--date",
                            stamp,
                            "--keep",
                            "5",
                        ],
                        label="update_data",
                        fatal=False,
                    )
                    steps["rose"] = "ok" if applied else "error"
                else:
                    steps["rose"] = "error"

            if fetch_global_stats:
                steps["stats"] = "running"
                _write_running_status("Aggiornamento in corso: Applicazione Statistiche...")
                stats_ok = bool((downloaded.get("global_stats") or {}).get("ok"))
                if stats_ok:
                    cleaned = _run_pipeline_step(
                        [
                            sys.executable,
                            str(root / "scripts" / "clean_stats_batch.py"),
                        ],
                        label="clean_stats_batch",
                        fatal=False,
                    )
                    steps["stats"] = "ok" if cleaned else "error"
                else:
                    steps["stats"] = "error"

        steps.setdefault("stats", "pending")
        _write_status(
            {
                "last_update": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                "result": "ok",
                "message": "Aggiornamento completato con successo.",
                "season": _season_for(datetime.now(tz=timezone.utc)),
                "update_id": update_id,
                "matchday": int(effective_formations_matchday) if effective_formations_matchday else None,
                "steps": steps,
            }
        )

        return {
            "ok": True,
            "alias": alias,
            "date": stamp,
            "update_id": update_id,
            "context": {
                "competition_id": competition_id,
                "competition_name": competition_name,
                "current_turn": context.current_turn if context else None,
                "last_calculated_matchday": context.last_calculated_matchday if context else None,
                "suggested_formations_matchday": context.suggested_formations_matchday if context else None,
                "effective_formations_matchday": int(effective_formations_matchday)
                if effective_formations_matchday
                else None,
            },
            "cookies": len(list(jar)),
            "downloaded": downloaded,
            "pipeline": pipeline_runs,
            "warnings": pipeline_warnings,
        }
    except Exception as exc:
        # Best-effort: mark status as error.
        for key, value in list(steps.items()):
            if value == "running":
                steps[key] = "error"
        _write_status(
            {
                "last_update": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                "result": "error",
                "message": f"Errore update: {exc}",
                "season": _season_for(datetime.now(tz=timezone.utc)),
                "update_id": update_id,
                "steps": steps,
            }
        )
        raise
