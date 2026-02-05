#!/usr/bin/env sh
set -e

DB_PATH="/app/data/db/app.db"
SEED_PATH="/app/seed/app.db.seed"
SEED_DB_DIR="/app/seed/db"
SEED_HISTORY_DIR="/app/seed/history"

mkdir -p /app/data/db

if [ ! -f "$DB_PATH" ] && [ -f "$SEED_PATH" ]; then
  cp "$SEED_PATH" "$DB_PATH"
fi

if [ -d "$SEED_DB_DIR" ]; then
  for seed_file in "$SEED_DB_DIR"/*.csv; do
    [ -f "$seed_file" ] || continue
    target="/app/data/db/$(basename "$seed_file")"
    if [ ! -s "$target" ]; then
      cp "$seed_file" "$target"
    fi
  done
fi

# Seed history data if volume is empty (needed for starred QA fallback).
if [ -d "$SEED_HISTORY_DIR" ]; then
  for hist_dir in "$SEED_HISTORY_DIR"/*; do
    [ -d "$hist_dir" ] || continue
    target="/app/data/history/$(basename "$hist_dir")"
    if [ ! -d "$target" ] || [ -z "$(ls -A "$target" 2>/dev/null)" ]; then
      mkdir -p "$target"
      cp -r "$hist_dir"/* "$target"/ 2>/dev/null || true
    fi
  done
fi

PORT="${PORT:-8001}"
exec uvicorn apps.api.app.main:app --host 0.0.0.0 --port "$PORT"
