from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPCookieProcessor, Request, build_opener

import http.cookiejar


LEGHE_BASE_URL = "https://leghe.fantacalcio.it/"

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
    if not path.exists():
        return 0

    try:
        import pandas as pd
    except Exception:
        return 0

    try:
        sheets = pd.read_excel(path, sheet_name=None)
    except Exception:
        return 0

    if not isinstance(sheets, dict):
        return 0

    lineup_frames = []
    fallback_frames = []
    for frame in sheets.values():
        if frame is None or frame.empty:
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
        if has_team and has_lineup:
            lineup_frames.append(frame)

    frames_to_scan = lineup_frames or fallback_frames
    if not frames_to_scan:
        return 0

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

    return row_count


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

        rows = _xlsx_formazioni_rows_count(out_path)
        attempts.append(
            {
                "ok": True,
                "matchday": int(matchday),
                "rows": int(rows),
                "bytes": int(downloaded.get("bytes") or 0),
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

        if run_pipeline:
            # 1) classifica -> pipeline_v2 (legacy data/classifica.csv)
            steps["strength"] = "running"
            _write_status(
                {
                    "last_update": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                    "result": "running",
                    "message": "Aggiornamento in corso: Classifica...",
                    "season": _season_for(datetime.now(tz=timezone.utc)),
                    "update_id": update_id,
                    "steps": steps,
                }
            )
            pipeline_runs.append(
                _run_subprocess(
                    [sys.executable, str(root / "scripts" / "pipeline_v2.py"), "--domains", "classifica", "--date", stamp],
                    cwd=root,
                )
            )
            if pipeline_runs[-1]["returncode"] != 0:
                raise LegheSyncError(f"pipeline_v2 classifica failed (rc={pipeline_runs[-1]['returncode']})")
            steps["strength"] = "ok"

            # 2) rose -> update_data (legacy data/rose_fantaportoscuso.csv + market_latest.json)
            steps["rose"] = "running"
            _write_status(
                {
                    "last_update": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                    "result": "running",
                    "message": "Aggiornamento in corso: Rose...",
                    "season": _season_for(datetime.now(tz=timezone.utc)),
                    "update_id": update_id,
                    "steps": steps,
                }
            )
            pipeline_runs.append(
                _run_subprocess(
                    [
                        sys.executable,
                        str(root / "scripts" / "update_data.py"),
                        "--auto",
                        "--date",
                        stamp,
                        "--keep",
                        "5",
                    ],
                    cwd=root,
                )
            )
            if pipeline_runs[-1]["returncode"] != 0:
                raise LegheSyncError(f"update_data failed (rc={pipeline_runs[-1]['returncode']})")
            steps["rose"] = "ok"

            # 3) report: tiers + strength ranking (fast, helps UI)
            _write_status(
                {
                    "last_update": datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                    "result": "running",
                    "message": "Aggiornamento in corso: Report...",
                    "season": _season_for(datetime.now(tz=timezone.utc)),
                    "update_id": update_id,
                    "steps": steps,
                }
            )
            pipeline_runs.append(
                _run_subprocess([sys.executable, str(root / "scripts" / "build_player_tiers.py")], cwd=root)
            )
            if pipeline_runs[-1]["returncode"] != 0:
                raise LegheSyncError(f"build_player_tiers failed (rc={pipeline_runs[-1]['returncode']})")
            pipeline_runs.append(
                _run_subprocess(
                    [
                        sys.executable,
                        str(root / "scripts" / "build_team_strength_ranking.py"),
                        "--snapshot",
                        "--snapshot-date",
                        stamp,
                    ],
                    cwd=root,
                )
            )
            if pipeline_runs[-1]["returncode"] != 0:
                raise LegheSyncError(f"build_team_strength_ranking failed (rc={pipeline_runs[-1]['returncode']})")

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
