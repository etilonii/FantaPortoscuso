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

# Leghe Fantacalcio sync (download XLSX + run local pipeline)
AUTO_LEGHE_SYNC_ENABLED = get_env_bool("AUTO_LEGHE_SYNC_ENABLED", False)
AUTO_LEGHE_SYNC_INTERVAL_HOURS = get_env_int("AUTO_LEGHE_SYNC_INTERVAL_HOURS", 12, min_value=1)
AUTO_LEGHE_SYNC_ON_START = get_env_bool("AUTO_LEGHE_SYNC_ON_START", False)

LEGHE_ALIAS = get_env_optional("LEGHE_ALIAS")
LEGHE_USERNAME = get_env_optional("LEGHE_USERNAME")
LEGHE_PASSWORD = get_env_optional("LEGHE_PASSWORD")
LEGHE_COMPETITION_ID = get_env_optional_int("LEGHE_COMPETITION_ID")
LEGHE_COMPETITION_NAME = get_env_optional("LEGHE_COMPETITION_NAME")
LEGHE_FORMATIONS_MATCHDAY = get_env_optional_int("LEGHE_FORMATIONS_MATCHDAY")
