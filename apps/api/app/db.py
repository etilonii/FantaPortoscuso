from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from .config import DATABASE_URL


def _normalize_database_url(raw_url: str) -> str:
    url = str(raw_url or "").strip()
    if url.startswith("postgres://"):
        return "postgresql+psycopg2://" + url[len("postgres://") :]
    if url.startswith("postgresql://") and "+psycopg" not in url and "+psycopg2" not in url:
        return "postgresql+psycopg2://" + url[len("postgresql://") :]
    return url


DATABASE_URL_NORMALIZED = _normalize_database_url(DATABASE_URL)
IS_SQLITE = DATABASE_URL_NORMALIZED.startswith("sqlite")

engine_kwargs: dict[str, object] = {"pool_pre_ping": True}
connect_args = {"check_same_thread": False} if IS_SQLITE else {}
if not IS_SQLITE:
    engine_kwargs.update(
        {
            "pool_recycle": 1800,
            "pool_size": 5,
            "max_overflow": 10,
            "pool_timeout": 30,
        }
    )

engine = create_engine(DATABASE_URL_NORMALIZED, connect_args=connect_args, **engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
