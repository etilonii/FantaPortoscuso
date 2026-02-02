import argparse
import datetime
import os
import sqlite3


def _read_keys(path: str) -> list[str]:
    keys: list[str] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            value = line.strip().lower()
            if value:
                keys.append(value)
    return keys


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS access_keys (
            id INTEGER PRIMARY KEY,
            key TEXT UNIQUE,
            used BOOLEAN NOT NULL DEFAULT 0,
            is_admin BOOLEAN NOT NULL DEFAULT 0,
            device_id TEXT,
            user_agent_hash TEXT,
            ip_address TEXT,
            created_at TEXT,
            used_at TEXT
        )
        """
    )
    conn.commit()


def import_keys(db_path: str, keys: list[str], is_admin: bool) -> int:
    if not keys:
        return 0
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        _ensure_table(conn)
        now = datetime.datetime.utcnow().isoformat()
        rows = [(k, 0, 1 if is_admin else 0, now) for k in keys]
        conn.executemany(
            "INSERT OR IGNORE INTO access_keys (key, used, is_admin, created_at) VALUES (?, ?, ?, ?)",
            rows,
        )
        conn.commit()
        return conn.total_changes
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Import access keys into access_keys table.")
    parser.add_argument(
        "--db",
        default="/app/data/db/app.db",
        help="SQLite DB path (default: /app/data/db/app.db)",
    )
    parser.add_argument("--keys", default="", help="Comma-separated keys to import.")
    parser.add_argument("--file", default="", help="Path to a file with one key per line.")
    parser.add_argument("--admin", action="store_true", help="Mark imported keys as admin.")
    args = parser.parse_args()

    keys: list[str] = []
    if args.keys:
        keys.extend([k.strip().lower() for k in args.keys.split(",") if k.strip()])
    if args.file:
        keys.extend(_read_keys(args.file))
    keys = list(dict.fromkeys(keys))

    changed = import_keys(args.db, keys, args.admin)
    print(f"Imported {len(keys)} keys. DB changes: {changed}.")


if __name__ == "__main__":
    main()
