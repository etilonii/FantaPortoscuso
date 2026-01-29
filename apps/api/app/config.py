import os


def get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None and value != "" else default


APP_NAME = get_env("APP_NAME", "FantaPortoscuso API")
DATABASE_URL = get_env("DATABASE_URL", "sqlite:///./app.db")
KEY_LENGTH = int(get_env("KEY_LENGTH", "8"))
