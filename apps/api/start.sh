#!/usr/bin/env sh
set -e

DB_PATH="/app/data/db/app.db"
SEED_PATH="/app/seed/app.db.seed"

if [ ! -f "$DB_PATH" ] && [ -f "$SEED_PATH" ]; then
  cp "$SEED_PATH" "$DB_PATH"
fi

PORT="${PORT:-8001}"
exec uvicorn apps.api.app.main:app --host 0.0.0.0 --port "$PORT"
