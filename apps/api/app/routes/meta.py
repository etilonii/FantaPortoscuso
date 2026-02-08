import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter


router = APIRouter(prefix="/meta", tags=["meta"])


def _resolve_data_dir() -> Path:
    here = Path(__file__).resolve()
    for base in [here.parent, *here.parents]:
        candidate = base / "data"
        if candidate.is_dir():
            return candidate

    cwd_candidate = Path.cwd() / "data"
    if cwd_candidate.is_dir():
        return cwd_candidate

    return Path(__file__).resolve().parent / "data"


DATA_DIR = _resolve_data_dir()
STATUS_PATH = DATA_DIR / "status.json"
CORE_DATA_FILES = {
    "rose": DATA_DIR / "rose_fantaportoscuso.csv",
    "stats": DATA_DIR / "statistiche_giocatori.csv",
    "strength": DATA_DIR / "classifica.csv",
    "quotazioni": DATA_DIR / "quotazioni.csv",
}


def _normalize_payload(raw: dict, fallback: dict) -> dict:
    result = str(raw.get("result", "")).strip().lower()
    if result in {"ok", "success"}:
        normalized_result = "ok"
    elif result == "running":
        normalized_result = "running"
    else:
        normalized_result = "error"

    payload = {
        "last_update": str(raw.get("last_update") or fallback["last_update"]),
        "result": normalized_result,
        "message": str(raw.get("message") or fallback["message"]),
    }

    season = raw.get("season")
    if season not in (None, ""):
        payload["season"] = str(season)

    matchday = raw.get("matchday")
    if matchday not in (None, ""):
        try:
            payload["matchday"] = int(matchday)
        except (TypeError, ValueError):
            pass

    update_id = raw.get("update_id")
    if update_id not in (None, ""):
        payload["update_id"] = str(update_id)

    raw_steps = raw.get("steps")
    if isinstance(raw_steps, dict):
        allowed = {"pending", "running", "ok", "error"}
        steps = {}
        for key in ("rose", "stats", "strength"):
            value = str(raw_steps.get(key, "")).strip().lower()
            if value in allowed:
                steps[key] = value
        if steps:
            payload["steps"] = steps

    return payload


def _build_data_files_status(fallback: dict) -> dict:
    file_status = {}
    mtimes = []
    for key, path in CORE_DATA_FILES.items():
        ok = path.exists() and path.is_file() and path.stat().st_size > 0
        file_status[key] = "ok" if ok else "error"
        if ok:
            mtimes.append(path.stat().st_mtime)

    if not mtimes:
        return fallback

    latest_dt = datetime.fromtimestamp(max(mtimes), tz=timezone.utc)
    return {
        "last_update": latest_dt.isoformat().replace("+00:00", "Z"),
        "result": "ok",
        "message": "Stato derivato dai file dati correnti",
        "steps": {
            "rose": file_status["rose"],
            "stats": file_status["stats"],
            "strength": file_status["strength"],
        },
    }


@router.get("/data-status")
def data_status():
    fallback = {
        "last_update": "",
        "result": "error",
        "message": "Nessun aggiornamento dati disponibile",
    }

    if STATUS_PATH.exists():
        try:
            raw = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return _normalize_payload(raw, fallback)
        except Exception:
            pass

    return _build_data_files_status(fallback)
