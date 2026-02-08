from __future__ import annotations

import importlib.util
import logging
from pathlib import Path

from sqlalchemy import text


logger = logging.getLogger(__name__)
MIGRATIONS_DIR = Path(__file__).resolve().parent


def ensure_migrations_table(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version VARCHAR(64) PRIMARY KEY,
                    applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )


def get_applied_versions(engine) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT version FROM schema_migrations"))
        return {str(row[0]) for row in rows}


def _load_migration_modules() -> list[tuple[str, object]]:
    modules: list[tuple[str, object]] = []
    for path in sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.py")):
        version = path.stem
        spec = importlib.util.spec_from_file_location(f"app_migration_{version}", path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Cannot load migration module: {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if not hasattr(module, "upgrade"):
            raise RuntimeError(f"Migration {version} missing upgrade()")
        modules.append((version, module))
    return modules


def apply_pending_migrations(engine) -> None:
    ensure_migrations_table(engine)
    applied = get_applied_versions(engine)
    for version, module in _load_migration_modules():
        if version in applied:
            continue
        logger.info("Applying migration %s", version)
        try:
            with engine.begin() as conn:
                module.upgrade(conn)
                conn.execute(
                    text(
                        "INSERT INTO schema_migrations (version, applied_at) "
                        "VALUES (:version, CURRENT_TIMESTAMP)"
                    ),
                    {"version": version},
                )
        except Exception:
            logger.error("Migration failed: %s", version, exc_info=True)
            raise
        logger.info("Migration applied: %s", version)
