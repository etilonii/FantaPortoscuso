import os


def get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None and value != "" else default


def get_env_bool(name: str, default: bool) -> bool:
    raw_default = "1" if default else "0"
    raw = get_env(name, raw_default).strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def get_env_int(name: str, default: int, min_value: int | None = None) -> int:
    raw = get_env(name, str(default)).strip()
    try:
        parsed = int(raw)
    except (TypeError, ValueError):
        parsed = int(default)
    if min_value is not None and parsed < min_value:
        parsed = min_value
    return parsed


def get_env_optional_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    clean = str(raw).strip()
    if not clean:
        return None
    try:
        return int(clean)
    except (TypeError, ValueError):
        return None


def get_env_optional(name: str) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return None
    clean = str(raw).strip()
    return clean or None


APP_NAME = get_env("APP_NAME", "FantaPortoscuso API")
DATABASE_URL = get_env("DATABASE_URL", "sqlite:///./data/db/app.db")
KEY_LENGTH = int(get_env("KEY_LENGTH", "8"))
AUTH_SECRET = get_env("AUTH_SECRET", "fp-dev-secret-change-me")
RATE_LIMIT_REQUESTS = int(get_env("RATE_LIMIT_REQUESTS", "120"))
RATE_LIMIT_WINDOW_SECONDS = int(get_env("RATE_LIMIT_WINDOW_SECONDS", "60"))
BACKUP_DIR = get_env("BACKUP_DIR", "./data/backups")
BACKUP_KEEP_LAST = int(get_env("BACKUP_KEEP_LAST", "20"))
AUTO_LIVE_IMPORT_ENABLED = get_env_bool("AUTO_LIVE_IMPORT_ENABLED", True)
AUTO_LIVE_IMPORT_INTERVAL_HOURS = get_env_int("AUTO_LIVE_IMPORT_INTERVAL_HOURS", 12, min_value=1)
AUTO_LIVE_IMPORT_ON_START = get_env_bool("AUTO_LIVE_IMPORT_ON_START", True)
AUTO_LIVE_IMPORT_ROUND = get_env_optional_int("AUTO_LIVE_IMPORT_ROUND")
AUTO_LIVE_IMPORT_SEASON = get_env("AUTO_LIVE_IMPORT_SEASON", "").strip()

STRIPE_SECRET_KEY = get_env("STRIPE_SECRET_KEY", "").strip()
STRIPE_WEBHOOK_SECRET = get_env("STRIPE_WEBHOOK_SECRET", "").strip()
STRIPE_PUBLISHABLE_KEY = get_env("STRIPE_PUBLISHABLE_KEY", "").strip()
STRIPE_ENABLED = bool(STRIPE_SECRET_KEY)
BILLING_PUBLIC_BASE_URL = get_env_optional("BILLING_PUBLIC_BASE_URL")
BILLING_SUCCESS_PATH = get_env("BILLING_SUCCESS_PATH", "/?billing=success")
BILLING_CANCEL_PATH = get_env("BILLING_CANCEL_PATH", "/?billing=cancel")
