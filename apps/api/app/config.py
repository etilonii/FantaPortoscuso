import os


def get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None and value != "" else default


APP_NAME = get_env("APP_NAME", "FantaPortoscuso API")
DATABASE_URL = get_env("DATABASE_URL", "sqlite:///./data/db/app.db")
KEY_LENGTH = int(get_env("KEY_LENGTH", "8"))
AUTH_SECRET = get_env("AUTH_SECRET", "fp-dev-secret-change-me")
RATE_LIMIT_REQUESTS = int(get_env("RATE_LIMIT_REQUESTS", "120"))
RATE_LIMIT_WINDOW_SECONDS = int(get_env("RATE_LIMIT_WINDOW_SECONDS", "60"))
BACKUP_DIR = get_env("BACKUP_DIR", "./data/backups")
BACKUP_KEEP_LAST = int(get_env("BACKUP_KEEP_LAST", "20"))
