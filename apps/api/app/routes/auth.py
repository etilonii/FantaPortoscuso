import hashlib
import json
import secrets
from urllib.parse import urljoin
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
    STRIPE_SECRET_KEY,
    STRIPE_WEBHOOK_SECRET,
    STRIPE_PUBLISHABLE_KEY,
    BILLING_PUBLIC_BASE_URL,
    BILLING_SUCCESS_PATH,
    BILLING_CANCEL_PATH,
    STRIPE_ENABLED,
)
from ..deps import get_db
from ..models import AccessKey, DeviceSession, KeyReset, RefreshToken, TeamKey, BillingPayment
from ..subscriptions import (
    PLAN_BASE,
    PLAN_PREMIUM,
    PLAN_TRIAL,
    CYCLE_TRIAL,
    CYCLE_MONTHLY,
    CYCLE_SEASON9,
    TRIAL_DURATION,
    normalize_plan,
    normalize_cycle,
    schedule_plan_change,
    set_manual_suspension,
    subscription_price_eur,
    subscription_price_catalog_eur,
    subscription_block_message,
    subscription_snapshot,
)
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
    SetSubscriptionRequest,
    ToggleSubscriptionBlockRequest,
    BillingCheckoutRequest,
    BillingCheckoutResponse,
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


def _subscription_payload(db: Session, access_key: AccessKey) -> dict:
    snapshot, changed = subscription_snapshot(access_key)
    if changed:
        db.add(access_key)
        db.commit()
        snapshot, _ = subscription_snapshot(access_key)
    return snapshot


def _get_stripe_sdk():
    if not STRIPE_ENABLED or not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Pagamenti non configurati lato server")
    try:
        import stripe  # type: ignore
    except Exception as exc:  # pragma: no cover - runtime dependency
        raise HTTPException(status_code=503, detail="Stripe SDK non disponibile sul server") from exc
    stripe.api_key = STRIPE_SECRET_KEY
    return stripe


def _resolve_access_key_for_billing(
    authorization: str | None,
    x_access_key: str | None,
    db: Session,
) -> AccessKey:
    key_value = ""
    token = extract_bearer_token(authorization)
    if token:
        try:
            payload = decode_access_token(token)
            key_value = str(payload.get("sub", "")).strip().lower()
        except ValueError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc
    if not key_value:
        key_value = str(x_access_key or "").strip().lower()
    if not key_value:
        raise HTTPException(status_code=401, detail="Key richiesta")

    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record:
        raise HTTPException(status_code=401, detail="Key non valida")
    if not record.used:
        raise HTTPException(status_code=403, detail="Key non ancora attivata")
    return record


def _normalize_public_path(raw_path: str | None, fallback: str) -> str:
    value = str(raw_path or "").strip()
    if not value:
        return fallback
    if not value.startswith("/"):
        return fallback
    if value.startswith("//"):
        return fallback
    return value


def _make_public_url(request: Request, path: str, *, add_session_placeholder: bool = False) -> str:
    clean_path = _normalize_public_path(path, "/")
    base = str(BILLING_PUBLIC_BASE_URL or "").strip()
    if not base:
        base = request.headers.get("origin") or str(request.base_url).rstrip("/")
    final = urljoin(base.rstrip("/") + "/", clean_path.lstrip("/"))
    if add_session_placeholder:
        sep = "&" if "?" in final else "?"
        final = f"{final}{sep}session_id={{CHECKOUT_SESSION_ID}}"
    return final


def _record_billing_payment(
    db: Session,
    *,
    provider_event_id: str,
    session_payload: dict,
    key_value: str,
    plan_tier: str,
    billing_cycle: str,
    raw_payload: dict,
) -> tuple[BillingPayment, bool]:
    existing = (
        db.query(BillingPayment)
        .filter(BillingPayment.provider_event_id == provider_event_id)
        .first()
    )
    if existing:
        return existing, False

    amount_total = session_payload.get("amount_total")
    amount_eur = None
    if amount_total is not None:
        try:
            amount_eur = float(amount_total) / 100.0
        except (TypeError, ValueError):
            amount_eur = None

    serializable_payload = raw_payload
    if hasattr(raw_payload, "to_dict_recursive"):
        try:
            serializable_payload = raw_payload.to_dict_recursive()
        except Exception:
            serializable_payload = {"raw": str(raw_payload)}

    payment = BillingPayment(
        provider="stripe",
        provider_event_id=provider_event_id,
        checkout_session_id=str(session_payload.get("id") or "") or None,
        key=key_value,
        plan_tier=plan_tier,
        billing_cycle=billing_cycle,
        amount_eur=amount_eur,
        currency=str(session_payload.get("currency") or "").lower() or None,
        payment_status=str(session_payload.get("payment_status") or "").lower() or None,
        customer_email=str(session_payload.get("customer_details", {}).get("email") or "")
        or None,
        raw_payload=json.dumps(serializable_payload, ensure_ascii=False, default=str),
        applied_at=None,
    )
    db.add(payment)
    return payment, True


def _apply_paid_checkout_session(
    db: Session,
    *,
    session_payload: dict,
    provider_event_id: str,
    raw_payload: dict,
) -> dict:
    metadata = session_payload.get("metadata") or {}
    key_value = str(metadata.get("key") or session_payload.get("client_reference_id") or "").strip().lower()
    plan_tier = normalize_plan(str(metadata.get("plan_tier") or ""))
    billing_cycle = normalize_cycle(str(metadata.get("billing_cycle") or ""), plan_tier)
    payment_status = str(session_payload.get("payment_status") or "").strip().lower()

    if not key_value:
        return {"applied": False, "reason": "missing_key"}
    if plan_tier not in {PLAN_BASE, PLAN_PREMIUM}:
        return {"applied": False, "reason": "invalid_plan"}
    if billing_cycle not in {CYCLE_MONTHLY, CYCLE_SEASON9}:
        return {"applied": False, "reason": "invalid_cycle"}
    if payment_status != "paid":
        return {"applied": False, "reason": "not_paid", "key": key_value}

    payment, inserted = _record_billing_payment(
        db,
        provider_event_id=provider_event_id,
        session_payload=session_payload,
        key_value=key_value,
        plan_tier=plan_tier,
        billing_cycle=billing_cycle,
        raw_payload=raw_payload,
    )
    if not inserted:
        return {"applied": bool(payment.applied_at), "reason": "duplicate_event", "key": key_value}

    record = db.query(AccessKey).filter(AccessKey.key == key_value).first()
    if not record:
        db.commit()
        return {"applied": False, "reason": "unknown_key", "key": key_value}

    schedule_plan_change(
        record,
        target_plan=plan_tier,
        billing_cycle=billing_cycle,
        force_immediate=True,
    )
    set_manual_suspension(record, False)
    payment.applied_at = datetime.utcnow()
    db.add(record)
    db.add(payment)
    db.commit()
    return {"applied": True, "reason": "ok", "key": key_value}


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
                plan_tier=PLAN_TRIAL,
                billing_cycle=CYCLE_TRIAL,
                plan_expires_at=None,
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
                plan_tier=PLAN_PREMIUM,
                billing_cycle="season9",
                plan_expires_at=None,
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
    changed_any = False
    for k in keys:
        sub, changed = subscription_snapshot(k, now)
        changed_any = changed_any or changed
        items.append(
            AdminKeyItem(
                key=k.key,
                used=k.used,
                is_admin=k.is_admin,
                subscription=sub,
                device_id=k.device_id,
                device_count=device_count_map.get(k.key, 0),
                team=team_map.get(k.key),
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
    if changed_any:
        db.commit()
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
                plan_tier=PLAN_TRIAL,
                billing_cycle=CYCLE_TRIAL,
                plan_expires_at=None,
            )
            db.add(record)
            db.commit()
            return KeyCreateResponse(key=key)
    raise HTTPException(status_code=500, detail="Failed to generate key")


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

    sub = _subscription_payload(db, record)
    if not record.is_admin and sub.get("status") != "active":
        raise HTTPException(
            status_code=402,
            detail={
                "code": "subscription_blocked",
                "message": subscription_block_message(sub),
                "subscription": sub,
            },
        )

    team_row = db.query(TeamKey).filter(TeamKey.key == record.key).first()
    team_name = team_row.team if team_row else None
    return {
        "status": "ok",
        "key": record.key,
        "is_admin": bool(record.is_admin),
        "team": team_name,
        "subscription": sub,
    }


@router.get("/billing/catalog")
def billing_catalog():
    return {
        "status": "ok",
        "enabled": bool(STRIPE_ENABLED and STRIPE_SECRET_KEY),
        "publishable_key": STRIPE_PUBLISHABLE_KEY or None,
        "price_catalog_eur": subscription_price_catalog_eur(),
    }


@router.post("/billing/checkout", response_model=BillingCheckoutResponse)
def create_billing_checkout(
    payload: BillingCheckoutRequest,
    request: Request,
    x_access_key: str | None = Header(default=None, alias="X-Access-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    db: Session = Depends(get_db),
):
    record = _resolve_access_key_for_billing(authorization, x_access_key, db)
    if record.is_admin:
        raise HTTPException(status_code=403, detail="Le key admin non usano checkout")

    plan_tier = normalize_plan(payload.plan_tier)
    billing_cycle = normalize_cycle(payload.billing_cycle, plan_tier)
    if plan_tier not in {PLAN_BASE, PLAN_PREMIUM}:
        raise HTTPException(status_code=400, detail="Piano non acquistabile")
    if billing_cycle not in {CYCLE_MONTHLY, CYCLE_SEASON9}:
        raise HTTPException(status_code=400, detail="Ciclo non acquistabile")

    amount_eur = subscription_price_eur(plan_tier, billing_cycle)
    if not amount_eur or amount_eur <= 0:
        raise HTTPException(status_code=400, detail="Prezzo non disponibile per il piano selezionato")

    stripe = _get_stripe_sdk()
    success_path = _normalize_public_path(payload.success_path, BILLING_SUCCESS_PATH)
    cancel_path = _normalize_public_path(payload.cancel_path, BILLING_CANCEL_PATH)
    success_url = _make_public_url(request, success_path, add_session_placeholder=True)
    cancel_url = _make_public_url(request, cancel_path, add_session_placeholder=False)

    tier_label = "Premium" if plan_tier == PLAN_PREMIUM else "Base"
    cycle_label = "9 mesi" if billing_cycle == CYCLE_SEASON9 else "Mensile"
    try:
        session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            success_url=success_url,
            cancel_url=cancel_url,
            client_reference_id=record.key,
            metadata={
                "key": record.key,
                "plan_tier": plan_tier,
                "billing_cycle": billing_cycle,
            },
            line_items=[
                {
                    "quantity": 1,
                    "price_data": {
                        "currency": "eur",
                        "unit_amount": int(round(float(amount_eur) * 100)),
                        "product_data": {
                            "name": f"FantaPortoscuso {tier_label}",
                            "description": f"Piano {tier_label} - {cycle_label}",
                        },
                    },
                }
            ],
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Errore creazione checkout Stripe: {exc}") from exc

    checkout_url = str(getattr(session, "url", "") or "")
    session_id = str(getattr(session, "id", "") or "")
    if not checkout_url or not session_id:
        raise HTTPException(status_code=502, detail="Checkout Stripe non disponibile")

    return BillingCheckoutResponse(
        status="ok",
        checkout_url=checkout_url,
        session_id=session_id,
        publishable_key=STRIPE_PUBLISHABLE_KEY or None,
    )


@router.get("/billing/verify")
def verify_billing_checkout(
    session_id: str,
    x_access_key: str | None = Header(default=None, alias="X-Access-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
    db: Session = Depends(get_db),
):
    record = _resolve_access_key_for_billing(authorization, x_access_key, db)
    stripe = _get_stripe_sdk()
    clean_session_id = str(session_id or "").strip()
    if not clean_session_id:
        raise HTTPException(status_code=400, detail="session_id mancante")

    try:
        session_obj = stripe.checkout.Session.retrieve(clean_session_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Sessione Stripe non trovata: {exc}") from exc

    session_payload = dict(session_obj or {})
    metadata = session_payload.get("metadata") or {}
    owner_key = str(metadata.get("key") or session_payload.get("client_reference_id") or "").strip().lower()
    if owner_key and owner_key != record.key and not record.is_admin:
        raise HTTPException(status_code=403, detail="Sessione checkout non associata a questa key")

    provider_event_id = f"stripe_session_paid:{clean_session_id}"
    result = _apply_paid_checkout_session(
        db,
        session_payload=session_payload,
        provider_event_id=provider_event_id,
        raw_payload={"source": "verify", "session": session_payload},
    )
    snapshot = _subscription_payload(db, record)
    return {"status": "ok", "result": result, "subscription": snapshot}


@router.post("/billing/webhook")
async def stripe_billing_webhook(request: Request, db: Session = Depends(get_db)):
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=503, detail="Webhook Stripe non configurato")
    stripe = _get_stripe_sdk()

    payload = await request.body()
    signature = request.headers.get("stripe-signature") or ""
    if not signature:
        raise HTTPException(status_code=400, detail="Header Stripe-Signature mancante")
    try:
        event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=signature,
            secret=STRIPE_WEBHOOK_SECRET,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Firma webhook non valida: {exc}") from exc

    event_type = str(event.get("type") or "").strip()
    result = {"applied": False, "reason": "ignored_event"}
    if event_type in {"checkout.session.completed", "checkout.session.async_payment_succeeded"}:
        session_payload = dict(((event.get("data") or {}).get("object") or {}))
        session_id = str(session_payload.get("id") or "").strip()
        provider_event_id = f"stripe_session_paid:{session_id}" if session_id else str(event.get("id") or "")
        result = _apply_paid_checkout_session(
            db,
            session_payload=session_payload,
            provider_event_id=provider_event_id,
            raw_payload=event,
        )

    return {"status": "ok", "event_type": event_type, "result": result}


@router.post("/admin/subscription")
def set_subscription_plan(
    payload: SetSubscriptionRequest,
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

    schedule = schedule_plan_change(
        record,
        target_plan=payload.plan_tier,
        billing_cycle=payload.billing_cycle,
        force_immediate=bool(payload.force_immediate),
    )
    db.add(record)
    db.commit()
    snapshot = _subscription_payload(db, record)
    return {
        "status": "ok",
        "key": key_value,
        "schedule": {
            **schedule,
            "effective_at": (
                schedule["effective_at"].isoformat()
                if isinstance(schedule.get("effective_at"), datetime)
                else schedule.get("effective_at")
            ),
        },
        "subscription": snapshot,
    }


@router.post("/admin/subscription/block")
def set_subscription_block(
    payload: ToggleSubscriptionBlockRequest,
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

    set_manual_suspension(record, bool(payload.blocked), payload.reason)
    db.add(record)
    db.commit()
    snapshot = _subscription_payload(db, record)
    return {"status": "ok", "key": key_value, "subscription": snapshot}


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
            if exists.is_admin:
                exists.plan_tier = PLAN_PREMIUM
                exists.billing_cycle = "season9"
                exists.plan_expires_at = None
                exists.blocked_at = None
                exists.blocked_reason = None
            db.add(exists)
            continue
        if payload.is_admin:
            record = AccessKey(
                key=key_value,
                used=False,
                is_admin=True,
                plan_tier=PLAN_PREMIUM,
                billing_cycle="season9",
                plan_expires_at=None,
            )
        else:
            record = AccessKey(
                key=key_value,
                used=False,
                is_admin=False,
                plan_tier=PLAN_TRIAL,
                billing_cycle=CYCLE_TRIAL,
                plan_expires_at=None,
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
        subscription = _subscription_payload(db, access_key)
        if not access_key.is_admin and subscription.get("status") != "active":
            raise HTTPException(
                status_code=402,
                detail={
                    "code": "subscription_blocked",
                    "message": subscription_block_message(subscription),
                    "subscription": subscription,
                },
            )
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

    subscription = _subscription_payload(db, access_key)
    if not access_key.is_admin and subscription.get("status") != "active":
        raise HTTPException(
            status_code=402,
            detail={
                "code": "subscription_blocked",
                "message": subscription_block_message(subscription),
                "subscription": subscription,
            },
        )

    # First use binds the device
    if not access_key.used:
        access_key.used = True
        access_key.device_id = device_id
        access_key.user_agent_hash = ua_hash
        access_key.ip_address = ip
        access_key.used_at = datetime.utcnow()
        if str(access_key.plan_tier or "").strip().lower() == PLAN_TRIAL and access_key.plan_expires_at is None:
            access_key.plan_expires_at = datetime.utcnow() + TRIAL_DURATION
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
            subscription=subscription,
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
                subscription=subscription,
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
        subscription=subscription,
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
    subscription = _subscription_payload(db, access_key)
    if not access_key.is_admin and subscription.get("status") != "active":
        raise HTTPException(
            status_code=402,
            detail={
                "code": "subscription_blocked",
                "message": subscription_block_message(subscription),
                "subscription": subscription,
            },
        )

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
