import json
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from ..deps import get_db
from ..models import MaintenanceState


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
CORE_DATA_FILE_CANDIDATES = {
    "rose": (DATA_DIR / "rose_fantaportoscuso.csv",),
    "stats": (
        DATA_DIR / "runtime" / "statistiche_giocatori.csv",
        DATA_DIR / "statistiche_giocatori.csv",
    ),
    "strength": (DATA_DIR / "classifica.csv",),
    "quotazioni": (DATA_DIR / "quotazioni.csv",),
}


def _default_maintenance_payload() -> dict:
    return {
        "enabled": False,
        "message": "",
        "retry_after_minutes": 10,
        "updated_at": "",
        "updated_by_key": None,
    }


def _serialize_maintenance(record: MaintenanceState | None) -> dict:
    if record is None:
        return _default_maintenance_payload()
    return {
        "enabled": bool(record.enabled),
        "message": str(record.message or ""),
        "retry_after_minutes": max(1, int(record.retry_after_minutes or 10)),
        "updated_at": record.updated_at.isoformat() if record.updated_at else "",
        "updated_by_key": str(record.updated_by_key) if record.updated_by_key else None,
    }


def _first_valid_data_file(paths: tuple[Path, ...]) -> Path | None:
    for path in paths:
        try:
            if path.exists() and path.is_file() and path.stat().st_size > 0:
                return path
        except Exception:
            continue
    return None


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
        for key in ("rose", "stats", "strength", "quotazioni"):
            value = str(raw_steps.get(key, "")).strip().lower()
            if value in allowed:
                steps[key] = value
        if steps:
            payload["steps"] = steps

    return payload


def _build_data_files_status(fallback: dict) -> dict:
    file_status = {}
    mtimes = []
    for key, candidates in CORE_DATA_FILE_CANDIDATES.items():
        selected = _first_valid_data_file(candidates)
        ok = selected is not None
        file_status[key] = "ok" if ok else "error"
        if ok:
            mtimes.append(selected.stat().st_mtime)

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
            "quotazioni": file_status["quotazioni"],
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
            # status.json may include BOM when produced by some editors/scripts.
            raw = json.loads(STATUS_PATH.read_text(encoding="utf-8-sig"))
            if isinstance(raw, dict):
                return _normalize_payload(raw, fallback)
        except Exception:
            pass

    return _build_data_files_status(fallback)


@router.get("/maintenance")
def maintenance_status(db: Session = Depends(get_db)):
    record = db.query(MaintenanceState).order_by(MaintenanceState.id.asc()).first()
    return _serialize_maintenance(record)
