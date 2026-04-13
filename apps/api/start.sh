#!/usr/bin/env sh
set -e

DATA_ROOT="/app/data"
SEED_ROOT="/app/seed"
DB_PATH="/app/data/db/app.db"

warn() {
  echo "WARN: $1" >&2
}

safe_mkdir() {
  mkdir -p "$1" || warn "mkdir failed: $1"
}

safe_copy() {
  src="$1"
  dst="$2"
  cp "$src" "$dst" || warn "copy failed: $src -> $dst"
}

seed_missing_files() {
  src_root="$1"
  dst_root="$2"

  [ -d "$src_root" ] || return 0

  find "$src_root" -type d | while IFS= read -r src_dir; do
    rel_path="${src_dir#"$src_root"}"
    [ -n "$rel_path" ] || continue
    safe_mkdir "$dst_root$rel_path"
  done

  find "$src_root" -type f | while IFS= read -r src_file; do
    rel_path="${src_file#"$src_root"}"
    target="$dst_root$rel_path"
    if [ ! -e "$target" ]; then
      safe_mkdir "$(dirname "$target")"
      safe_copy "$src_file" "$target"
    fi
  done
}

safe_mkdir "$DATA_ROOT"
safe_mkdir "$DATA_ROOT/db"
safe_mkdir "$DATA_ROOT/backups"

# Populate an empty or partial persistent volume from the image snapshot.
seed_missing_files "$SEED_ROOT" "$DATA_ROOT"

if [ ! -f "$DB_PATH" ]; then
  warn "database file not found at $DB_PATH; a new SQLite database will be created on first write"
fi

PORT="${PORT:-8001}"
exec uvicorn apps.api.app.main:app --host 0.0.0.0 --port "$PORT"
