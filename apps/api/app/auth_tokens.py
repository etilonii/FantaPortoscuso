import base64
import hashlib
import hmac
import json
import secrets
from datetime import datetime, timedelta

from .config import AUTH_SECRET


ACCESS_TOKEN_TTL = timedelta(hours=24)
REFRESH_TOKEN_TTL = timedelta(days=30)


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(raw: str) -> bytes:
    pad = "=" * ((4 - len(raw) % 4) % 4)
    return base64.urlsafe_b64decode((raw + pad).encode("ascii"))


def _sign(raw: str) -> str:
    digest = hmac.new(AUTH_SECRET.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).digest()
    return _b64url_encode(digest)


def create_access_token(
    *,
    key_value: str,
    is_admin: bool,
    device_id: str | None = None,
    now: datetime | None = None,
) -> tuple[str, datetime]:
    issued_at = now or datetime.utcnow()
    expires_at = issued_at + ACCESS_TOKEN_TTL
    payload = {
        "sub": key_value.strip().lower(),
        "adm": bool(is_admin),
        "dev": (device_id or "").strip(),
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    payload_raw = _b64url_encode(json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = _sign(payload_raw)
    return f"{payload_raw}.{signature}", expires_at


def decode_access_token(token: str) -> dict:
    if not token or "." not in token:
        raise ValueError("Token non valido")
    payload_raw, signature = token.split(".", 1)
    expected = _sign(payload_raw)
    if not hmac.compare_digest(signature, expected):
        raise ValueError("Firma token non valida")
    try:
        payload = json.loads(_b64url_decode(payload_raw).decode("utf-8"))
    except Exception as exc:
        raise ValueError("Payload token non valido") from exc
    exp = int(payload.get("exp", 0) or 0)
    if exp <= int(datetime.utcnow().timestamp()):
        raise ValueError("Token scaduto")
    return payload


def create_refresh_token() -> str:
    return secrets.token_urlsafe(48)


def hash_refresh_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()

