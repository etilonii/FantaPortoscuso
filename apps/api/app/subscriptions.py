from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Tuple


PLAN_TRIAL = "trial"
PLAN_BASE = "base"
PLAN_PREMIUM = "premium"
PLAN_VALUES = {PLAN_TRIAL, PLAN_BASE, PLAN_PREMIUM}

CYCLE_TRIAL = "trial"
CYCLE_MONTHLY = "monthly"
CYCLE_SEASON9 = "season9"
CYCLE_VALUES = {CYCLE_TRIAL, CYCLE_MONTHLY, CYCLE_SEASON9}

TRIAL_DURATION = timedelta(days=3)
MONTHLY_DURATION = timedelta(days=30)

PLAN_PRICE_EUR = {
    PLAN_TRIAL: {
        CYCLE_TRIAL: 0.0,
    },
    PLAN_BASE: {
        CYCLE_MONTHLY: 5.0,
        CYCLE_SEASON9: 37.99,
    },
    PLAN_PREMIUM: {
        CYCLE_MONTHLY: 10.0,
        CYCLE_SEASON9: 57.99,
    },
}


ALL_FEATURES = (
    "home",
    "quotazioni",
    "rose",
    "listone",
    "plusvalenze",
    "top_acquisti",
    "mercato",
    "statistiche_giocatori",
    "formazioni",
    "schede_giocatori",
    "dark_light",
    "classifica_lega",
    "formazioni_live",
    "mercato_live",
    "formazione_consigliata",
    "tier_list",
    "potenza_squadra_titolari",
    "potenza_squadra_totale",
    "classifica_potenza",
    "classifica_reale_lega",
    "classifica_fixtures_seriea",
    "predictions_campionato_fixtures",
)

TRIAL_FEATURES = {
    "home",
    "rose",
    "listone",
    "statistiche_giocatori",
    "top_acquisti",
    "classifica_lega",
    "dark_light",
}

BASE_FEATURES = {
    "home",
    "quotazioni",
    "rose",
    "listone",
    "plusvalenze",
    "top_acquisti",
    "mercato",
    "statistiche_giocatori",
    "formazioni",
    "schede_giocatori",
    "dark_light",
    "classifica_lega",
}

PREMIUM_FEATURES = {
    *BASE_FEATURES,
    "formazioni_live",
    "mercato_live",
    "formazione_consigliata",
    "tier_list",
    "potenza_squadra_titolari",
    "potenza_squadra_totale",
    "classifica_potenza",
    "classifica_reale_lega",
    "classifica_fixtures_seriea",
    "predictions_campionato_fixtures",
}


def utcnow() -> datetime:
    return datetime.utcnow()


def normalize_plan(value: str | None) -> str:
    raw = str(value or "").strip().lower()
    if raw in PLAN_VALUES:
        return raw
    return PLAN_TRIAL


def normalize_cycle(value: str | None, plan_tier: str) -> str:
    plan_tier = normalize_plan(plan_tier)
    raw = str(value or "").strip().lower()
    if plan_tier == PLAN_TRIAL:
        return CYCLE_TRIAL
    if raw in CYCLE_VALUES and raw != CYCLE_TRIAL:
        return raw
    return CYCLE_MONTHLY


def transition_delay_hours(current_plan: str, target_plan: str) -> int:
    current = normalize_plan(current_plan)
    target = normalize_plan(target_plan)
    if current == PLAN_TRIAL and target in {PLAN_BASE, PLAN_PREMIUM}:
        return 0
    if current == PLAN_BASE and target == PLAN_PREMIUM:
        return 0
    if current == PLAN_PREMIUM and target in {PLAN_PREMIUM, PLAN_BASE}:
        return 48
    return 12


def subscription_price_eur(plan_tier: str | None, billing_cycle: str | None) -> float | None:
    plan = normalize_plan(plan_tier)
    cycle = normalize_cycle(billing_cycle, plan)
    prices = PLAN_PRICE_EUR.get(plan, {})
    value = prices.get(cycle)
    if value is None:
        return None
    return float(value)


def subscription_price_catalog_eur() -> Dict[str, Dict[str, float]]:
    return {
        plan: {cycle: float(price) for cycle, price in cycle_map.items()}
        for plan, cycle_map in PLAN_PRICE_EUR.items()
    }


def features_for_plan(plan_tier: str, *, is_admin: bool = False) -> Dict[str, bool]:
    if is_admin:
        return {name: True for name in ALL_FEATURES}

    plan = normalize_plan(plan_tier)
    if plan == PLAN_PREMIUM:
        enabled = PREMIUM_FEATURES
    elif plan == PLAN_BASE:
        enabled = BASE_FEATURES
    else:
        enabled = TRIAL_FEATURES
    return {name: name in enabled for name in ALL_FEATURES}


def _activate_plan(access_key: Any, plan_tier: str, billing_cycle: str, now: datetime) -> None:
    plan = normalize_plan(plan_tier)
    cycle = normalize_cycle(billing_cycle, plan)
    access_key.plan_tier = plan
    access_key.billing_cycle = cycle
    access_key.pending_plan_tier = None
    access_key.pending_billing_cycle = None
    access_key.pending_effective_at = None

    if plan == PLAN_TRIAL:
        access_key.plan_expires_at = now + TRIAL_DURATION
    elif cycle == CYCLE_MONTHLY:
        access_key.plan_expires_at = now + MONTHLY_DURATION
    else:
        access_key.plan_expires_at = None

    access_key.blocked_at = None
    access_key.blocked_reason = None


def ensure_subscription_defaults(access_key: Any, now: datetime | None = None) -> bool:
    now = now or utcnow()
    changed = False

    if getattr(access_key, "is_admin", False):
        if normalize_plan(getattr(access_key, "plan_tier", None)) != PLAN_PREMIUM:
            access_key.plan_tier = PLAN_PREMIUM
            changed = True
        if getattr(access_key, "billing_cycle", None) != CYCLE_SEASON9:
            access_key.billing_cycle = CYCLE_SEASON9
            changed = True
        if getattr(access_key, "plan_expires_at", None) is not None:
            access_key.plan_expires_at = None
            changed = True
        for field_name, target in (
            ("pending_plan_tier", None),
            ("pending_billing_cycle", None),
            ("pending_effective_at", None),
            ("blocked_at", None),
            ("blocked_reason", None),
        ):
            if getattr(access_key, field_name, None) is not target:
                setattr(access_key, field_name, target)
                changed = True
        return changed

    plan = normalize_plan(getattr(access_key, "plan_tier", None))
    if plan != getattr(access_key, "plan_tier", None):
        access_key.plan_tier = plan
        changed = True

    normalized_cycle = normalize_cycle(getattr(access_key, "billing_cycle", None), plan)
    if normalized_cycle != getattr(access_key, "billing_cycle", None):
        access_key.billing_cycle = normalized_cycle
        changed = True

    if (
        plan == PLAN_TRIAL
        and bool(getattr(access_key, "used", False))
        and getattr(access_key, "plan_expires_at", None) is None
    ):
        access_key.plan_expires_at = now + TRIAL_DURATION
        changed = True

    pending_plan_raw = getattr(access_key, "pending_plan_tier", None)
    pending_plan = normalize_plan(pending_plan_raw) if pending_plan_raw else None
    if pending_plan and pending_plan != pending_plan_raw:
        access_key.pending_plan_tier = pending_plan
        changed = True
    pending_cycle_raw = getattr(access_key, "pending_billing_cycle", None)
    if pending_plan:
        pending_cycle = normalize_cycle(pending_cycle_raw, pending_plan)
        if pending_cycle != pending_cycle_raw:
            access_key.pending_billing_cycle = pending_cycle
            changed = True
    else:
        if pending_cycle_raw is not None:
            access_key.pending_billing_cycle = None
            changed = True
        if getattr(access_key, "pending_effective_at", None) is not None:
            access_key.pending_effective_at = None
            changed = True

    pending_effective_at = getattr(access_key, "pending_effective_at", None)
    if pending_plan and pending_effective_at and pending_effective_at <= now:
        _activate_plan(
            access_key,
            pending_plan,
            getattr(access_key, "pending_billing_cycle", None) or CYCLE_MONTHLY,
            now,
        )
        changed = True

    return changed


def schedule_plan_change(
    access_key: Any,
    target_plan: str,
    billing_cycle: str | None = None,
    *,
    force_immediate: bool = False,
    now: datetime | None = None,
) -> Dict[str, Any]:
    now = now or utcnow()
    ensure_subscription_defaults(access_key, now)
    plan = normalize_plan(target_plan)
    cycle = normalize_cycle(billing_cycle, plan)
    current_plan = normalize_plan(getattr(access_key, "plan_tier", None))

    if force_immediate:
        _activate_plan(access_key, plan, cycle, now)
        return {
            "scheduled": False,
            "effective_at": now,
            "delay_hours": 0,
            "target_plan_tier": plan,
            "target_billing_cycle": cycle,
        }

    delay_hours = transition_delay_hours(current_plan, plan)
    effective_at = now + timedelta(hours=delay_hours)
    access_key.pending_plan_tier = plan
    access_key.pending_billing_cycle = cycle
    access_key.pending_effective_at = effective_at
    if plan in {PLAN_BASE, PLAN_PREMIUM}:
        access_key.blocked_at = None
        access_key.blocked_reason = None
    return {
        "scheduled": True,
        "effective_at": effective_at,
        "delay_hours": delay_hours,
        "target_plan_tier": plan,
        "target_billing_cycle": cycle,
    }


def set_manual_suspension(access_key: Any, blocked: bool, reason: str | None = None) -> None:
    if bool(blocked):
        access_key.blocked_at = utcnow()
        clean_reason = str(reason or "").strip()
        access_key.blocked_reason = clean_reason or "manual_suspension"
    else:
        access_key.blocked_at = None
        access_key.blocked_reason = None


def subscription_snapshot(access_key: Any, now: datetime | None = None) -> Tuple[Dict[str, Any], bool]:
    now = now or utcnow()
    changed = ensure_subscription_defaults(access_key, now)

    plan_tier = normalize_plan(getattr(access_key, "plan_tier", None))
    billing_cycle = normalize_cycle(getattr(access_key, "billing_cycle", None), plan_tier)
    if billing_cycle != getattr(access_key, "billing_cycle", None):
        access_key.billing_cycle = billing_cycle
        changed = True

    blocked_reason = None
    status = "active"
    expires_at = getattr(access_key, "plan_expires_at", None)
    blocked_at = getattr(access_key, "blocked_at", None)

    if blocked_at is not None:
        status = "blocked"
        blocked_reason = str(getattr(access_key, "blocked_reason", "") or "manual_suspension")
    elif expires_at is not None and expires_at <= now:
        status = "blocked"
        blocked_reason = "trial_expired" if plan_tier == PLAN_TRIAL else "plan_expired"

    pending_plan = getattr(access_key, "pending_plan_tier", None)
    pending_cycle = getattr(access_key, "pending_billing_cycle", None)
    pending_effective_at = getattr(access_key, "pending_effective_at", None)

    seconds_to_expiry = None
    if expires_at is not None:
        seconds_to_expiry = max(0, int((expires_at - now).total_seconds()))

    seconds_to_pending = None
    if pending_effective_at is not None:
        seconds_to_pending = max(0, int((pending_effective_at - now).total_seconds()))

    features = (
        {name: False for name in ALL_FEATURES}
        if status == "blocked"
        else features_for_plan(plan_tier, is_admin=bool(getattr(access_key, "is_admin", False)))
    )

    payload: Dict[str, Any] = {
        "plan_tier": plan_tier,
        "billing_cycle": billing_cycle,
        "current_price_eur": subscription_price_eur(plan_tier, billing_cycle),
        "pending_price_eur": (
            subscription_price_eur(pending_plan, pending_cycle) if pending_plan else None
        ),
        "price_catalog_eur": subscription_price_catalog_eur(),
        "status": status,
        "blocked_reason": blocked_reason,
        "plan_expires_at": expires_at.isoformat() if expires_at is not None else None,
        "seconds_to_expiry": seconds_to_expiry,
        "pending_plan_tier": pending_plan,
        "pending_billing_cycle": pending_cycle,
        "pending_effective_at": (
            pending_effective_at.isoformat() if pending_effective_at is not None else None
        ),
        "seconds_to_pending": seconds_to_pending,
        "features": features,
    }
    return payload, changed


def subscription_block_message(snapshot: Dict[str, Any]) -> str:
    reason = str(snapshot.get("blocked_reason") or "").strip().lower()
    if reason == "trial_expired":
        return "Trial terminato: rinnova la key per continuare a usare il sito."
    if reason == "plan_expired":
        return "Piano scaduto: rinnova la key per continuare a usare il sito."
    if reason == "manual_suspension":
        return "Key sospesa: rinnova o contatta l'amministratore."
    if reason:
        return f"Key bloccata: {reason}."
    return "Key bloccata: rinnova per continuare a usare il sito."


def can_use_feature(snapshot: Dict[str, Any], feature_name: str) -> bool:
    if str(snapshot.get("status", "active")) != "active":
        return False
    features = snapshot.get("features", {})
    if not isinstance(features, dict):
        return False
    return bool(features.get(feature_name))
