from __future__ import annotations

import secrets
import shutil
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote


def backup_database(db_path: str, backup_dir: str, prefix: str = "app") -> str:
    src = Path(db_path).expanduser()
    if not src.is_absolute():
        src = (Path.cwd() / src).resolve()

    if not src.exists():
        raise RuntimeError(f"Database file non trovato: {src}")
    if not src.is_file():
        raise RuntimeError(f"Percorso database non valido: {src}")

    target_dir = Path(backup_dir).expanduser()
    if not target_dir.is_absolute():
        target_dir = (Path.cwd() / target_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    short_id = secrets.token_hex(3)
    final_path = target_dir / f"{prefix}-{stamp}-{short_id}.sqlite"
    tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")

    try:
        shutil.copy2(src, tmp_path)
        if not tmp_path.exists() or tmp_path.stat().st_size <= 0:
            raise RuntimeError("Backup temporaneo non valido (file assente o vuoto)")
        tmp_path.replace(final_path)
        if not final_path.exists() or final_path.stat().st_size <= 0:
            raise RuntimeError("Backup finale non valido (file assente o vuoto)")
    except Exception as exc:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        if isinstance(exc, RuntimeError):
            raise
        raise RuntimeError(f"Copia backup fallita: {exc}") from exc

    return str(final_path)


def enforce_backup_retention(backup_dir: str, keep_last: int = 20, prefix: str = "app") -> None:
    target_dir = Path(backup_dir).expanduser()
    if not target_dir.is_absolute():
        target_dir = (Path.cwd() / target_dir).resolve()
    if not target_dir.exists():
        return

    keep = max(0, int(keep_last))
    backups = sorted(
        (p for p in target_dir.glob(f"{prefix}-*.sqlite") if p.is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for old in backups[keep:]:
        old.unlink(missing_ok=True)


def sqlite_db_path_from_url(
    database_url: str,
    base_dir: str | Path | None = None,
) -> str:
    if not database_url.startswith("sqlite:///"):
        raise RuntimeError("DATABASE_URL non supportato per backup")

    raw_path = unquote(database_url.replace("sqlite:///", "", 1)).strip()
    if not raw_path or raw_path == ":memory:":
        raise RuntimeError("DATABASE_URL sqlite non valido per backup")

    parsed_path = Path(raw_path).expanduser()
    if parsed_path.is_absolute():
        return str(parsed_path)

    base_path = Path(base_dir).expanduser() if base_dir is not None else Path.cwd()
    if not base_path.is_absolute():
        base_path = (Path.cwd() / base_path).resolve()
    return str((base_path / parsed_path).resolve())


def run_backup_fail_fast(
    database_url: str,
    backup_dir: str,
    keep_last: int,
    prefix: str,
    base_dir: str | Path | None = None,
) -> str:
    db_path = sqlite_db_path_from_url(database_url, base_dir=base_dir)
    backup_path = backup_database(db_path, backup_dir, prefix=prefix)
    enforce_backup_retention(backup_dir, keep_last=keep_last, prefix=prefix)
    return backup_path

