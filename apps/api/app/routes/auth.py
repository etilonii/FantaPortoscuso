import hashlib
import json
import secrets
from datetime import timedelta
from pathlib import Path
from datetime import datetime

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..backup import run_backup_fail_fast
from ..auth_utils import access_key_from_bearer, extract_bearer_token
from ..config import (
    BACKUP_DIR,
    BACKUP_KEEP_LAST,
    DATABASE_URL,
    KEY_LENGTH,
)
from ..deps import get_db
from ..models import AccessKey, DeviceSession, KeyReset, RefreshToken, TeamKey
from ..auth_tokens import (
    REFRESH_TOKEN_TTL,
    create_access_token,
    create_refresh_token,
    decode_access_token,
    hash_refresh_token,
)
from ..schemas import (
    AdminKeyResponse,
    AdminKeyItem,
    ImportKeysRequest,
    ImportTeamKeysRequest,
    KeyCreateResponse,
    KeyDeleteRequest,
    KeyNoteRequest,
    KeyResetUsageResponse,
    LoginRequest,
    LoginResponse,
    LogoutRequest,
    PingRequest,
    RefreshRequest,
    RefreshResponse,
    ResetKeyRequest,
    SetAdminRequest,
    TeamKeyRequest,
    TeamKeyItem,
    TeamKeyDeleteRequest,
)


router = APIRouter(prefix="/auth", tags=["auth"])


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
LAST_UPDATE_PATH = DATA_DIR / "history" / "last_update.json"
LAST_STATS_UPDATE_PATH = DATA_DIR / "history" / "last_stats_update.json"
MARKET_LATEST_PATH = DATA_DIR / "market_latest.json"

MAX_KEY_RESETS_PER_SEASON = 3
RESET_COOLDOWN_HOURS = 24


def _current_season(now: datetime | None = None) -> str:
    ref = now or datetime.utcnow()
    # La stagione parte ad agosto.
    start_year = ref.year if ref.month >= 8 else ref.year - 1
    end_year = (start_year + 1) % 100
    return f"{start_year}-{end_year:02d}"


def _backup_or_500(prefix: str) -> None:
    try:
        run_backup_fail_fast(
            DATABASE_URL,
            BACKUP_DIR,
            BACKUP_KEEP_LAST,
            prefix=prefix,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=f"Backup failed: {exc}") from exc


def _generate_key() -> str:
    # token_hex produces 2 chars per byte
    byte_len = max(4, (KEY_LENGTH + 1) // 2)
    return secrets.token_hex(byte_len)[:KEY_LENGTH].lower()


def _ua_hash(user_agent: str) -> str:
    return hashlib.sha256(user_agent.encode("utf-8")).hexdigest()


def _issue_tokens(db: Session, access_key: AccessKey, device_id: str | None) -> dict:
    access_token, access_expires_at = create_access_token(
        key_value=access_key.key,
        is_admin=bool(access_key.is_admin),
        device_id=device_id,
    )
    refresh_token_raw = create_refresh_token()
    refresh_expires_at = datetime.utcnow() + REFRESH_TOKEN_TTL

    refresh_record = RefreshToken(
        key_id=access_key.id,
        token_hash=hash_refresh_token(refresh_token_raw),
        device_id=device_id,
        expires_at=refresh_expires_at,
        last_used_at=datetime.utcnow(),
    )
    db.add(refresh_record)
    db.commit()

    return {
        "access_token": access_token,
        "access_expires_at": access_expires_at.isoformat(),
        "refresh_token": refresh_token_raw,
        "refresh_expires_at": refresh_expires_at.isoformat(),
    }


@router.post("/keys", response_model=KeyCreateResponse)
def create_key(db: Session = Depends(get_db)):
    for _ in range(5):
        key = _generate_key()
        if not db.query(AccessKey).filter(AccessKey.key == key).first():
            record = AccessKey(
                key=key,
                used=False,
            )
            db.add(record)
            db.commit()
            return KeyCreateResponse(key=key)
    raise HTTPException(status_code=500, detail="Failed to generate key")


@router.post("/admin/bootstrap", response_model=AdminKeyResponse)
def bootstrap_admin_key(db: Session = Depends(get_db)):
    existing = db.query(AccessKey).filter(AccessKey.is_admin.is_(True)).first()
    if existing:
        raise HTTPException(status_code=409, detail="Admin key already exists")
    for _ in range(5):
        key = _generate_key()
        if not db.query(AccessKey).filter(AccessKey.key == key).first():
            record = AccessKey(
                key=key,
                used=False,
                is_admin=True,
            )
            db.add(record)
            db.commit()
            return AdminKeyResponse(key=key)
    raise HTTPException(status_code=500, detail="Failed to generate admin key")


def _require_admin_key(
    x_admin_key: str | None,
    db: Session,
    authorization: str | None = None,
) -> AccessKey:
    bearer_record = access_key_from_bearer(authorization, db)
    if bearer_record is not None:
        if not bearer_record.is_admin:
            raise HTTPException(status_code=403, detail="Permessi admin richiesti")
        return bearer_record

    if not x_admin_key:
        raise HTTPException(status_code=401, detail="Admin key richiesta")
    key_value = x_admin_key.strip().lower()
    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record or not record.is_admin:
        raise HTTPException(status_code=403, detail="Admin key non valida")
    if not record.used:
        raise HTTPException(status_code=403, detail="Admin key non ancora attivata")
    return record



def _key_reset_usage(db: Session, key_value: str, season: str) -> tuple[int, datetime | None, bool]:
    used = db.query(KeyReset).filter(KeyReset.key == key_value, KeyReset.season == season).count()
    last_reset = (
        db.query(KeyReset)
        .filter(KeyReset.key == key_value, KeyReset.season == season)
        .order_by(KeyReset.reset_at.desc())
        .first()
    )
    last_reset_at = last_reset.reset_at if last_reset else None
    cooldown_blocked = False
    if last_reset_at and last_reset_at >= datetime.utcnow() - timedelta(hours=RESET_COOLDOWN_HOURS):
        cooldown_blocked = True
    return used, last_reset_at, cooldown_blocked

@router.get("/admin/keys", response_model=list[AdminKeyItem])
def list_keys(
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db, authorization)
    season = _current_season()
    keys = db.query(AccessKey).order_by(AccessKey.created_at.desc()).all()
    now = datetime.utcnow()
    sessions = db.query(DeviceSession).all()
    last_seen_map = {}
    online_keys = set()
    device_count_map = {}
    team_map = {item.key: item.team for item in db.query(TeamKey).all()}
    for s in sessions:
        if not s.key:
            continue
        device_count_map[s.key] = device_count_map.get(s.key, 0) + 1
        last_seen = s.last_seen_at
        if last_seen:
            current = last_seen_map.get(s.key)
            if not current or last_seen > current:
                last_seen_map[s.key] = last_seen
            if last_seen >= now - timedelta(minutes=5):
                online_keys.add(s.key)

    reset_rows = (
        db.query(
            KeyReset.key.label("key"),
            func.count(KeyReset.id).label("used"),
            func.max(KeyReset.reset_at).label("last_reset_at"),
        )
        .filter(KeyReset.season == season)
        .group_by(KeyReset.key)
        .all()
    )
    reset_map = {row.key: row for row in reset_rows}
    items: list[AdminKeyItem] = []
    for k in keys:
        items.append(
            AdminKeyItem(
                key=k.key,
                used=k.used,
                is_admin=k.is_admin,
                device_id=k.device_id,
                device_count=device_count_map.get(k.key, 0),
                team=team_map.get(k.key),
                note=k.note,
                created_at=k.created_at.isoformat() if k.created_at else None,
                used_at=k.used_at.isoformat() if k.used_at else None,
                last_seen_at=last_seen_map.get(k.key).isoformat()
                if last_seen_map.get(k.key)
                else None,
                online=k.key in online_keys,
                reset_used=int(getattr(reset_map.get(k.key), "used", 0) or 0),
                reset_limit=MAX_KEY_RESETS_PER_SEASON,
                reset_season=season,
                reset_cooldown_blocked=(
                    bool(getattr(reset_map.get(k.key), "last_reset_at", None))
                    and getattr(reset_map.get(k.key), "last_reset_at")
                    >= now - timedelta(hours=RESET_COOLDOWN_HOURS)
                ),
            )
        )
    return items


@router.post("/admin/keys", response_model=KeyCreateResponse)
def create_key_admin(
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db, authorization)
    for _ in range(5):
        key = _generate_key()
        if not db.query(AccessKey).filter(AccessKey.key == key).first():
            record = AccessKey(
                key=key,
                used=False,
                is_admin=False,
            )
            db.add(record)
            db.commit()
            return KeyCreateResponse(key=key)
    raise HTTPException(status_code=500, detail="Failed to generate key")


@router.delete("/admin/keys")
def delete_key_admin(
    payload: KeyDeleteRequest,
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    admin_record = _require_admin_key(x_admin_key, db, authorization)
    _backup_or_500("delete-key")
    key_value = payload.key.strip().lower()
    if not key_value:
        raise HTTPException(status_code=400, detail="Key non valida")

    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record:
        raise HTTPException(status_code=404, detail="Key non trovata")

    if record.is_admin:
        admin_count = db.query(AccessKey).filter(AccessKey.is_admin.is_(True)).count()
        if admin_count <= 1:
            raise HTTPException(status_code=400, detail="Impossibile eliminare l'ultima key admin")

    if record.key == admin_record.key:
        raise HTTPException(status_code=400, detail="Impossibile eliminare la key admin in uso")

    db.query(RefreshToken).filter(RefreshToken.key_id == record.id).delete(synchronize_session=False)
    db.query(DeviceSession).filter(DeviceSession.key == key_value).delete(synchronize_session=False)
    db.query(TeamKey).filter(TeamKey.key == key_value).delete(synchronize_session=False)
    db.query(KeyReset).filter(KeyReset.key == key_value).delete(synchronize_session=False)
    db.delete(record)
    db.commit()
    return {"status": "ok", "key": key_value}


@router.post("/admin/key-note")
def set_key_note_admin(
    payload: KeyNoteRequest,
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db, authorization)
    key_value = payload.key.strip().lower()
    if not key_value:
        raise HTTPException(status_code=400, detail="Key non valida")

    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record:
        raise HTTPException(status_code=404, detail="Key non trovata")

    note_value = (payload.note or "").strip()
    record.note = note_value[:255] if note_value else None
    db.add(record)
    db.commit()
    return {"status": "ok", "key": key_value, "note": record.note}


@router.post("/admin/set-admin")
def set_admin_flag(
    payload: SetAdminRequest,
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db, authorization)
    key_value = payload.key.strip().lower()
    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record:
        raise HTTPException(status_code=404, detail="Key non trovata")
    record.is_admin = bool(payload.is_admin)
    db.add(record)
    db.commit()
    return {"status": "ok", "key": key_value, "is_admin": record.is_admin}


@router.post("/admin/team-key")
def set_team_key(
    payload: TeamKeyRequest,
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db, authorization)
    key_value = payload.key.strip().lower()
    team_value = payload.team.strip()
    if not key_value or not team_value:
        raise HTTPException(status_code=400, detail="Key o team non validi")
    record = db.query(TeamKey).filter(TeamKey.key == key_value).first()
    if record:
        record.team = team_value
        db.add(record)
    else:
        db.add(TeamKey(key=key_value, team=team_value))
    db.commit()
    return {"status": "ok", "key": key_value, "team": team_value}


@router.get("/admin/team-keys", response_model=list[TeamKeyItem])
def list_team_keys(
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db, authorization)
    items = db.query(TeamKey).order_by(TeamKey.team.asc()).all()
    return [TeamKeyItem(key=item.key, team=item.team) for item in items]


@router.delete("/admin/team-key")
def delete_team_key(
    payload: TeamKeyDeleteRequest,
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db, authorization)
    key_value = payload.key.strip().lower()
    if not key_value:
        raise HTTPException(status_code=400, detail="Key non valida")
    record = db.query(TeamKey).filter(TeamKey.key == key_value).first()
    if not record:
        raise HTTPException(status_code=404, detail="Associazione non trovata")
    db.delete(record)
    db.commit()
    return {"status": "ok", "key": key_value}


@router.get("/session")
def session_info(
    x_access_key: str | None = Header(default=None, alias="X-Access-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    db: Session = Depends(get_db),
):
    record = access_key_from_bearer(authorization, db)
    if record is None:
        key_value = str(x_access_key or "").strip().lower()
        if not key_value:
            raise HTTPException(status_code=401, detail="Key richiesta")
        record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
        if not record or not record.used:
            raise HTTPException(status_code=401, detail="Key non valida")
        if getattr(record, "blocked_at", None) is not None:
            raise HTTPException(status_code=403, detail="Key sospesa")

    team_row = db.query(TeamKey).filter(TeamKey.key == record.key).first()
    team_name = team_row.team if team_row else None
    return {
        "status": "ok",
        "key": record.key,
        "is_admin": bool(record.is_admin),
        "team": team_name,
    }


@router.get("/admin/status")
def admin_status(
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db, authorization)
    status = {"data": {}, "market": {}, "auth": {}}

    if LAST_UPDATE_PATH.exists():
        try:
            status["data"]["last_update"] = json.loads(
                LAST_UPDATE_PATH.read_text(encoding="utf-8")
            )
        except Exception:
            status["data"]["last_update"] = {}
    else:
        status["data"]["last_update"] = {}

    if LAST_STATS_UPDATE_PATH.exists():
        try:
            status["data"]["last_stats_update"] = json.loads(
                LAST_STATS_UPDATE_PATH.read_text(encoding="utf-8")
            )
        except Exception:
            status["data"]["last_stats_update"] = {}
    else:
        status["data"]["last_stats_update"] = {}

    if MARKET_LATEST_PATH.exists():
        try:
            market = json.loads(MARKET_LATEST_PATH.read_text(encoding="utf-8"))
        except Exception:
            market = {}
        if isinstance(market, dict):
            items = market.get("items", [])
            teams = market.get("teams", [])
        elif isinstance(market, list):
            items = market
            teams = []
        else:
            items = []
            teams = []
        dates = []
        for item in items:
            date = (item or {}).get("date")
            if date:
                dates.append(date)
        for team in teams:
            date = (team or {}).get("last_date")
            if date:
                dates.append(date)
        status["market"] = {
            "items": len(items),
            "teams": len(teams),
            "latest_date": sorted(dates)[-1] if dates else None,
        }
    else:
        status["market"] = {"items": 0, "teams": 0, "latest_date": None}

    now = datetime.utcnow()
    sessions = db.query(DeviceSession).all()
    last_seen = None
    online_count = 0
    for s in sessions:
        if s.last_seen_at:
            if not last_seen or s.last_seen_at > last_seen:
                last_seen = s.last_seen_at
            if s.last_seen_at >= now - timedelta(minutes=5):
                online_count += 1
    status["auth"] = {
        "last_seen_at": last_seen.isoformat() if last_seen else None,
        "online_devices": online_count,
    }

    return status


@router.post("/admin/import-keys")
def import_keys(
    payload: ImportKeysRequest,
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db, authorization)
    _backup_or_500("import-keys")
    inserted = 0
    for raw_key in payload.keys:
        key_value = raw_key.strip().lower()
        if not key_value:
            continue
        exists = db.query(AccessKey).filter(AccessKey.key == key_value).first()
        if exists:
            exists.is_admin = bool(payload.is_admin)
            db.add(exists)
            continue
        record = AccessKey(
            key=key_value,
            used=False,
            is_admin=bool(payload.is_admin),
        )
        db.add(record)
        inserted += 1
    db.commit()
    return {"imported": inserted}


@router.post("/admin/import-team-keys")
def import_team_keys(
    payload: ImportTeamKeysRequest,
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db, authorization)
    _backup_or_500("import-team-keys")
    inserted = 0
    for item in payload.items:
        key_value = item.key.strip().lower()
        team_value = item.team.strip()
        if not key_value or not team_value:
            continue
        record = db.query(TeamKey).filter(TeamKey.key == key_value).first()
        if record:
            record.team = team_value
            db.add(record)
            continue
        db.add(TeamKey(key=key_value, team=team_value))
        inserted += 1
    db.commit()
    return {"imported": inserted}


@router.get("/admin/reset-usage", response_model=KeyResetUsageResponse)
def key_reset_usage_admin(
    key: str,
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db, authorization)
    key_value = key.strip().lower()
    if not key_value:
        raise HTTPException(status_code=400, detail="Key non valida")
    season = _current_season()
    used, last_reset_at, cooldown_blocked = _key_reset_usage(db, key_value, season)
    return KeyResetUsageResponse(
        key=key_value,
        season=season,
        used=used,
        limit=MAX_KEY_RESETS_PER_SEASON,
        last_reset_at=last_reset_at.isoformat() if last_reset_at else None,
        cooldown_blocked=cooldown_blocked,
    )

@router.post("/admin/reset-key")
def reset_key_admin(
    payload: ResetKeyRequest,
    x_admin_key: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    admin_record = _require_admin_key(x_admin_key, db, authorization)
    _backup_or_500("reset-key")
    key_value = payload.key.strip().lower()
    if not key_value:
        raise HTTPException(status_code=400, detail="Key non valida")

    season = _current_season()
    used, last_reset_at, cooldown_blocked = _key_reset_usage(db, key_value, season)
    if used >= MAX_KEY_RESETS_PER_SEASON:
        raise HTTPException(
            status_code=403,
            detail="Limite reset raggiunto: massimo 3 reset per stagione.",
        )
    if cooldown_blocked:
        raise HTTPException(
            status_code=403,
            detail="Reset gia' effettuato nelle ultime 24 ore. Riprova piu' tardi.",
        )

    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record:
        raise HTTPException(status_code=404, detail="Key non trovata")

    record.used = False
    record.device_id = None
    record.user_agent_hash = None
    record.ip_address = None
    record.used_at = None
    db.add(record)

    note_value = (payload.note or "").strip() or None
    db.add(
        KeyReset(
            key=key_value,
            season=season,
            reset_at=datetime.utcnow(),
            admin_key=admin_record.key,
            note=note_value,
        )
    )
    db.commit()
    return {"status": "ok", "used": used + 1, "limit": MAX_KEY_RESETS_PER_SEASON}


@router.post("/ping")
def ping(
    payload: PingRequest,
    request: Request,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    device_id = payload.device_id.strip()
    bearer_record = access_key_from_bearer(authorization, db)
    if bearer_record is not None:
        access_key = bearer_record
    else:
        key_value = payload.key.strip().lower()
        access_key = db.query(AccessKey).filter(AccessKey.key == key_value).first()
        if not access_key:
            raise HTTPException(status_code=401, detail="Key non valida")
        if not access_key.used:
            raise HTTPException(status_code=403, detail="Key non ancora attivata")
        if getattr(access_key, "blocked_at", None) is not None:
            raise HTTPException(status_code=403, detail="Key sospesa")
    session = db.query(DeviceSession).filter(DeviceSession.device_id == device_id).first()
    if session:
        session.last_seen_at = datetime.utcnow()
        session.ip_address = request.client.host if request.client else None
        db.add(session)
        db.commit()
    return {"status": "ok"}


@router.post("/login", response_model=LoginResponse)
def login(payload: LoginRequest, request: Request, db: Session = Depends(get_db)):
    key_value = payload.key.strip().lower()
    device_id = payload.device_id.strip()
    user_agent = request.headers.get("user-agent", "")
    ua_hash = _ua_hash(user_agent)
    ip = request.client.host if request.client else None

    access_key = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not access_key:
        raise HTTPException(status_code=401, detail="Key non valida")
    if getattr(access_key, "blocked_at", None) is not None:
        raise HTTPException(status_code=403, detail="Key sospesa")

    # First use binds the device
    if not access_key.used:
        access_key.used = True
        access_key.device_id = device_id
        access_key.user_agent_hash = ua_hash
        access_key.ip_address = ip
        access_key.used_at = datetime.utcnow()
        db.add(access_key)

        session = DeviceSession(
            device_id=device_id,
            key=key_value,
            user_agent_hash=ua_hash,
            ip_address=ip,
        )
        db.add(session)
        db.commit()
        tokens = _issue_tokens(db, access_key, device_id)
        return LoginResponse(
            status="ok",
            message="Accesso autorizzato e device collegato",
            is_admin=access_key.is_admin,
            warning="Accesso legacy con key: passa ai Bearer token.",
            **tokens,
        )

    # Already used: check device binding
    if access_key.is_admin:
        session = db.query(DeviceSession).filter(DeviceSession.device_id == device_id).first()
        if not session:
            admin_sessions = (
                db.query(DeviceSession).filter(DeviceSession.key == key_value).count()
            )
            if admin_sessions >= 2:
                raise HTTPException(
                    status_code=403, detail="Key admin gia' usata su 2 dispositivi"
                )
            session = DeviceSession(
                device_id=device_id,
                key=key_value,
                user_agent_hash=ua_hash,
                ip_address=ip,
            )
            db.add(session)
            db.commit()
            tokens = _issue_tokens(db, access_key, device_id)
            return LoginResponse(
                status="ok",
                message="Accesso autorizzato (admin)",
                is_admin=True,
                warning="Accesso legacy con key: passa ai Bearer token.",
                **tokens,
            )
    else:
        if access_key.device_id != device_id:
            raise HTTPException(status_code=403, detail="Key gia' legata ad altro dispositivo")

    session = db.query(DeviceSession).filter(DeviceSession.device_id == device_id).first()
    if session:
        session.last_seen_at = datetime.utcnow()
        session.ip_address = ip
        db.add(session)
        db.commit()

    tokens = _issue_tokens(db, access_key, device_id)
    return LoginResponse(
        status="ok",
        message="Accesso autorizzato",
        is_admin=access_key.is_admin,
        warning="Accesso legacy con key: passa ai Bearer token.",
        **tokens,
    )


@router.post("/refresh", response_model=RefreshResponse)
def refresh_tokens(payload: RefreshRequest, db: Session = Depends(get_db)):
    now = datetime.utcnow()
    token_hash = hash_refresh_token(payload.refresh_token.strip())
    record = (
        db.query(RefreshToken)
        .filter(
            RefreshToken.token_hash == token_hash,
            RefreshToken.revoked_at.is_(None),
            RefreshToken.expires_at > now,
        )
        .first()
    )
    if not record:
        raise HTTPException(status_code=401, detail="Refresh token non valido o scaduto")

    access_key = db.query(AccessKey).filter(AccessKey.id == record.key_id).first()
    if not access_key:
        raise HTTPException(status_code=401, detail="Refresh token non valido")
    if getattr(access_key, "blocked_at", None) is not None:
        raise HTTPException(status_code=403, detail="Key sospesa")

    record.revoked_at = now
    record.last_used_at = now
    db.add(record)
    db.commit()

    tokens = _issue_tokens(db, access_key, record.device_id)
    return RefreshResponse(
        access_token=tokens["access_token"],
        access_expires_at=tokens["access_expires_at"],
        refresh_token=tokens["refresh_token"],
        refresh_expires_at=tokens["refresh_expires_at"],
    )


@router.post("/logout")
def logout(payload: LogoutRequest, db: Session = Depends(get_db)):
    token_hash = hash_refresh_token(payload.refresh_token.strip())
    record = (
        db.query(RefreshToken)
        .filter(RefreshToken.token_hash == token_hash, RefreshToken.revoked_at.is_(None))
        .first()
    )
    if record:
        record.revoked_at = datetime.utcnow()
        db.add(record)
        db.commit()
    return {"status": "ok"}
