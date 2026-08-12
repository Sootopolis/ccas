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

# Percent-decode, one character at a time. Only the two hex digits of an escape
# are ever handed to printf's '%b': the whole-string form ("${s//%/\\x}") would
# also expand any backslash the password itself contains, and would turn a '%'
# that is not an escape into an invalid \x. Bytes are appended individually, so
# a multi-byte UTF-8 escape (%C3%A9) reassembles correctly.
urldecode_pct() {
  local s="$1" out="" c
  while [[ -n "$s" ]]; do
    c="${s:0:1}"
    if [[ "$c" == "%" && "${s:1:2}" =~ ^[0-9A-Fa-f]{2}$ ]]; then
      out+="$(printf '%b' "\\x${s:1:2}")"
      s="${s:3}"
    else
      out+="$c"
      s="${s:1}"
    fi
  done
  printf '%s' "$out"
}

# A query-string value, matching how pgjdbc reads `?password=` out of a JDBC URL
# (URLDecoder rules, so '+' is a space).
urldecode() {
  urldecode_pct "${1//+/ }"
}

# A userinfo component. RFC 3986 has no plus-is-space rule there, so a '+' in the
# password must survive as a literal plus.
urldecode_userinfo() {
  urldecode_pct "$1"
}

# Resolve a libpq conninfo string from the app's env, with the password routed
# to PGPASSWORD rather than the URI/argv.
if [[ -n "${DATABASE_URL:-}" ]]; then
  # DATABASE_URL may be in either accepted form (the app normalises both — see
  # PostgresClient.normalizeJdbcUrl):
  #   jdbc:postgresql://host/db?user=...&password=...&sslmode=require
  #   postgresql://user:pass@host/db?sslmode=require       (what providers hand out)
  # Stripping the "jdbc:" prefix yields a libpq URI pg_dump accepts. Credentials
  # are pulled out of whichever position they sit in, into PGPASSWORD, so the
  # password never reaches argv (where `ps` would expose it to other local users).
  CONN="${DATABASE_URL#jdbc:}"

  # Userinfo form: split the authority at its last '@' (a password may contain an
  # unescaped one), keeping the username in the URI and routing the password out.
  authority="${CONN#*://}"
  authority="${authority%%\?*}"
  if [[ "$authority" == *@* ]]; then
    scheme="${CONN%%://*}"
    query=""
    [[ "$CONN" == *\?* ]] && query="?${CONN#*\?}"
    userinfo="${authority%@*}"
    hostpath="${authority##*@}"
    if [[ "$userinfo" == *:* ]]; then
      export PGPASSWORD="$(urldecode_userinfo "${userinfo#*:}")"
      userinfo="${userinfo%%:*}"
    fi
    CONN="${scheme}://${userinfo}@${hostpath}${query}"
  fi

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
