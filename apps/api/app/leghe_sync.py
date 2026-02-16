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

    return LegheContext(
        alias=alias,
        app_key=app_key,
        competition_id=competition_id,
        competition_name=competition_name,
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

    try:
        context = fetch_leghe_context(opener, alias=alias)
        if competition_id is None:
            competition_id = context.competition_id
        if competition_name is None:
            competition_name = context.competition_name
        if formations_matchday is None:
            formations_matchday = context.suggested_formations_matchday

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
            if not formations_matchday:
                raise LegheSyncError("formations_matchday mancante: non posso scaricare le formazioni.")
            params = {
                "alias_lega": alias,
                "id_competizione": competition_id,
                "giornata": int(formations_matchday),
                "nome_competizione": alias,
                "dummy": 5,
            }
            form_url = f"{LEGHE_BASE_URL}servizi/V1_LegheFormazioni/excel?{urlencode(params)}"
            out_path = data_dir / "incoming" / "formazioni" / "formazioni.xlsx"
            try:
                downloaded["formazioni_xlsx"] = download_leghe_excel(
                    opener,
                    url=form_url,
                    app_key=context.app_key,
                    referer=f"{LEGHE_BASE_URL}{alias}/formazioni",
                    out_path=out_path,
                )
            except LegheSyncError as exc:
                # Non-fatal: HTML appkey source is enough for live formations.
                downloaded["formazioni_xlsx"] = {
                    "ok": False,
                    "warning": str(exc),
                }

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
                "matchday": int(formations_matchday) if formations_matchday else None,
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
                "last_calculated_matchday": context.last_calculated_matchday if context else None,
                "suggested_formations_matchday": context.suggested_formations_matchday if context else None,
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
