import json
from datetime import datetime
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


@router.get("/data-status")
def data_status():
    fallback = {
        "last_update": datetime.utcnow().isoformat(),
        "result": "error",
        "message": "Nessun aggiornamento dati disponibile",
    }

    if not STATUS_PATH.exists():
        return fallback

    try:
        raw = json.loads(STATUS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {
            **fallback,
            "message": "Nessun aggiornamento dati disponibile",
        }

    if not isinstance(raw, dict):
        return fallback

    result = str(raw.get("result", "")).strip().lower()
    normalized_result = "ok" if result == "ok" else "error"

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

    return payload
