from datetime import datetime

from fastapi import HTTPException
from sqlalchemy.orm import Session

from .auth_tokens import decode_access_token
from .models import AccessKey


def extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    value = authorization.strip()
    if not value.lower().startswith("bearer "):
        return None
    token = value[7:].strip()
    return token or None


def block_state_for_key(record: AccessKey, now: datetime | None = None) -> tuple[bool, bool]:
    blocked_at = getattr(record, "blocked_at", None)
    if blocked_at is None:
        return False, False

    ref = now or datetime.utcnow()
    blocked_until = getattr(record, "blocked_until", None)
    if blocked_until is not None and blocked_until <= ref:
        record.blocked_at = None
        record.blocked_until = None
        record.blocked_reason = None
        return False, True
    return True, False


def ensure_key_not_blocked(db: Session, record: AccessKey) -> None:
    is_blocked, changed = block_state_for_key(record)
    if changed:
        db.add(record)
        db.commit()
    if is_blocked:
        raise HTTPException(status_code=403, detail="Key sospesa")


def access_key_from_bearer(authorization: str | None, db: Session) -> AccessKey | None:
    token = extract_bearer_token(authorization)
    if not token:
        return None
    try:
        payload = decode_access_token(token)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc

    key_value = str(payload.get("sub", "")).strip().lower()
    if not key_value:
        raise HTTPException(status_code=401, detail="Token non valido")

    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record or not record.used:
        raise HTTPException(status_code=401, detail="Token non valido")
    ensure_key_not_blocked(db, record)
    return record
