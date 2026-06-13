#!/usr/bin/env bash
#
# Weekly off-Neon logical backup of the CCAS database.
#
# Neon's free plan keeps only 24h of PITR history, so a bad write not caught
# within a day is unrecoverable. This script takes a compressed logical dump to
# local disk as the disaster-recovery floor. It is read-only (pg_dump only) and
# excludes the rebuildable cache / diagnostics tables to keep dumps small.
#
# Connection: reuses the app's existing config. DATABASE_URL (JDBC form) takes
# priority; otherwise the DB_* fields are used. No backup-specific secrets. The
# password is always passed via PGPASSWORD, never in argv (so it can't leak
# through `ps` / /proc/<pid>/cmdline to other local users).
#
# Env knobs:
#   CCAS_BACKUP_DIR     output directory      (default: ~/ccas-backups)
#   CCAS_BACKUP_RETAIN  dumps to keep         (default: 6)
#
# Restore a dump with (needs pg_restore >= the pg_dump that wrote it):
#   pg_restore --no-owner --no-privileges -d <target-conn> ccas-<stamp>.dump
#
set -euo pipefail

BACKUP_DIR="${CCAS_BACKUP_DIR:-$HOME/ccas-backups}"
RETAIN="${CCAS_BACKUP_RETAIN:-6}"

# Data excluded from the dump (rebuilt from the Chess.com API on next run).
# --exclude-table-data keeps each table's DDL but drops its rows, so a restore
# recreates the empty tables and the app refills them.
EXCLUDE_DATA=(api_response_cache api_response_body api_fetch_failure)

# Percent-decode a URI component (%20 -> space, +-> space).
urldecode() {
  local s="${1//+/ }"
  printf '%b' "${s//%/\\x}"
}

# Resolve a libpq conninfo string from the app's env, with the password routed
# to PGPASSWORD rather than the URI/argv.
if [[ -n "${DATABASE_URL:-}" ]]; then
  # The app stores DATABASE_URL as a JDBC URI:
  #   jdbc:postgresql://host/db?user=...&password=...&sslmode=require
  # Stripping the "jdbc:" prefix yields a libpq URI pg_dump accepts. Pull the
  # password query param out into PGPASSWORD so it never reaches argv.
  CONN="${DATABASE_URL#jdbc:}"
  if [[ "$CONN" == *\?* ]]; then
    base="${CONN%%\?*}"
    query="${CONN#*\?}"
    kept=()
    IFS='&' read -ra params <<<"$query"
    for kv in "${params[@]}"; do
      if [[ "$kv" == password=* ]]; then
        export PGPASSWORD="$(urldecode "${kv#password=}")"
      else
        kept+=("$kv")
      fi
    done
    if ((${#kept[@]})); then
      CONN="$base?$(IFS='&'; echo "${kept[*]}")"
    else
      CONN="$base"
    fi
  fi
else
  : "${DB_HOST:?set DATABASE_URL or DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD}"
  : "${DB_NAME:?DB_NAME required when DATABASE_URL is unset}"
  : "${DB_USER:?DB_USER required when DATABASE_URL is unset}"
  : "${DB_PASSWORD:?DB_PASSWORD required when DATABASE_URL is unset}"
  CONN="postgresql://${DB_HOST}:${DB_PORT:-5432}/${DB_NAME}?user=${DB_USER}&sslmode=require"
  export PGPASSWORD="${DB_PASSWORD}"
fi

mkdir -p "$BACKUP_DIR"

STAMP="$(date -u +%Y%m%d-%H%M%S)"
OUT="$BACKUP_DIR/ccas-$STAMP.dump"
TMP="$OUT.tmp"

# Dump to a temp file and only promote it to the final name on success, so a
# failed/partial dump never masquerades as a valid backup (or evicts a good one
# via the retention prune below).
trap 'rm -f "$TMP"' EXIT

excludes=()
for t in "${EXCLUDE_DATA[@]}"; do
  excludes+=(--exclude-table-data="$t")
done

pg_dump "$CONN" \
  --format=custom \
  --no-owner \
  --no-privileges \
  "${excludes[@]}" \
  --file="$TMP"

mv "$TMP" "$OUT"

echo "wrote $OUT ($(du -h "$OUT" | cut -f1))"

# Retention: keep the newest $RETAIN dumps, delete the rest.
while IFS= read -r old; do
  rm -f "$old"
  echo "pruned $old"
done < <(ls -1t "$BACKUP_DIR"/ccas-*.dump 2>/dev/null | tail -n "+$((RETAIN + 1))")
