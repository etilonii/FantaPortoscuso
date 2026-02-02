import hashlib
import os
import secrets
from datetime import datetime

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy.orm import Session

from ..config import KEY_LENGTH
from ..deps import get_db
from ..models import AccessKey, DeviceSession, TeamKey
from ..schemas import (
    AdminKeyResponse,
    AdminKeyItem,
    ImportKeysRequest,
    ImportTeamKeysRequest,
    KeyCreateResponse,
    LoginRequest,
    LoginResponse,
    ResetKeyRequest,
    SetAdminRequest,
    TeamKeyRequest,
)


router = APIRouter(prefix="/auth", tags=["auth"])


def _generate_key() -> str:
    # token_hex produces 2 chars per byte
    byte_len = max(4, (KEY_LENGTH + 1) // 2)
    return secrets.token_hex(byte_len)[:KEY_LENGTH].lower()


def _ua_hash(user_agent: str) -> str:
    return hashlib.sha256(user_agent.encode("utf-8")).hexdigest()


@router.post("/keys", response_model=KeyCreateResponse)
def create_key(db: Session = Depends(get_db)):
    for _ in range(5):
        key = _generate_key()
        if not db.query(AccessKey).filter(AccessKey.key == key).first():
            record = AccessKey(key=key, used=False)
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
            record = AccessKey(key=key, used=False, is_admin=True)
            db.add(record)
            db.commit()
            return AdminKeyResponse(key=key)
    raise HTTPException(status_code=500, detail="Failed to generate admin key")


def _require_admin_key(x_admin_key: str | None, db: Session) -> AccessKey:
    if not x_admin_key:
        raise HTTPException(status_code=401, detail="Admin key richiesta")
    key_value = x_admin_key.strip().lower()
    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record or not record.is_admin:
        raise HTTPException(status_code=403, detail="Admin key non valida")
    if not record.used:
        raise HTTPException(status_code=403, detail="Admin key non ancora attivata")
    return record


@router.get("/admin/keys", response_model=list[AdminKeyItem])
def list_keys(
    x_admin_key: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db)
    keys = db.query(AccessKey).order_by(AccessKey.created_at.desc()).all()
    return [
        AdminKeyItem(
            key=k.key,
            used=k.used,
            is_admin=k.is_admin,
            device_id=k.device_id,
            created_at=k.created_at.isoformat() if k.created_at else None,
            used_at=k.used_at.isoformat() if k.used_at else None,
        )
        for k in keys
    ]


@router.post("/admin/keys", response_model=KeyCreateResponse)
def create_key_admin(
    x_admin_key: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db)
    for _ in range(5):
        key = _generate_key()
        if not db.query(AccessKey).filter(AccessKey.key == key).first():
            record = AccessKey(key=key, used=False, is_admin=False)
            db.add(record)
            db.commit()
            return KeyCreateResponse(key=key)
    raise HTTPException(status_code=500, detail="Failed to generate key")


@router.post("/admin/set-admin")
def set_admin_flag(
    payload: SetAdminRequest,
    x_admin_key: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db)
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
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db)
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


@router.post("/admin/import-keys")
def import_keys(
    payload: ImportKeysRequest,
    x_import_secret: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    secret = os.getenv("IMPORT_SECRET", "")
    if not secret or not x_import_secret or x_import_secret != secret:
        raise HTTPException(status_code=403, detail="Import secret non valido")
    inserted = 0
    for raw_key in payload.keys:
        key_value = raw_key.strip().lower()
        if not key_value:
            continue
        exists = db.query(AccessKey).filter(AccessKey.key == key_value).first()
        if exists:
            continue
        record = AccessKey(key=key_value, used=False, is_admin=payload.is_admin)
        db.add(record)
        inserted += 1
    db.commit()
    return {"imported": inserted}


@router.post("/admin/import-team-keys")
def import_team_keys(
    payload: ImportTeamKeysRequest,
    x_import_secret: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    secret = os.getenv("IMPORT_SECRET", "")
    if not secret or not x_import_secret or x_import_secret != secret:
        raise HTTPException(status_code=403, detail="Import secret non valido")
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


@router.post("/admin/reset-key-import")
def reset_key(
    payload: ResetKeyRequest,
    x_import_secret: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    secret = os.getenv("IMPORT_SECRET", "")
    if not secret or not x_import_secret or x_import_secret != secret:
        raise HTTPException(status_code=403, detail="Import secret non valido")
    key_value = payload.key.strip().lower()
    if not key_value:
        raise HTTPException(status_code=400, detail="Key non valida")
    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record:
        raise HTTPException(status_code=404, detail="Key non trovata")
    record.used = False
    record.device_id = None
    record.user_agent_hash = None
    record.ip_address = None
    record.used_at = None
    db.add(record)
    db.commit()
    return {"status": "ok"}


@router.post("/admin/reset-key")
def reset_key_admin(
    payload: ResetKeyRequest,
    x_admin_key: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    _require_admin_key(x_admin_key, db)
    key_value = payload.key.strip().lower()
    if not key_value:
        raise HTTPException(status_code=400, detail="Key non valida")
    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record:
        raise HTTPException(status_code=404, detail="Key non trovata")
    record.used = False
    record.device_id = None
    record.user_agent_hash = None
    record.ip_address = None
    record.used_at = None
    db.add(record)
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
        return LoginResponse(
            status="ok", message="Accesso autorizzato e device collegato", is_admin=access_key.is_admin
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
            return LoginResponse(status="ok", message="Accesso autorizzato (admin)", is_admin=True)
    else:
        if access_key.device_id != device_id:
            raise HTTPException(status_code=403, detail="Key gia' legata ad altro dispositivo")

    session = db.query(DeviceSession).filter(DeviceSession.device_id == device_id).first()
    if session:
        session.last_seen_at = datetime.utcnow()
        session.ip_address = ip
        db.add(session)
        db.commit()

    return LoginResponse(status="ok", message="Accesso autorizzato", is_admin=access_key.is_admin)
