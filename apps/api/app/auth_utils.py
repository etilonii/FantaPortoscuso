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
    return record
