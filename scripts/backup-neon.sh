#!/usr/bin/env bash
#
# Weekly off-Neon logical backup of the CCAS database.
#
# Neon's free plan keeps only 24h of PITR history, so a bad write not caught
# within a day is unrecoverable. This script takes a compressed logical dump to
# local disk as the disaster-recovery floor. It is read-only (pg_dump only) and
# excludes the rebuildable cache / diagnostics tables to keep dumps small.
#
# Connection: the server's own settings, read the way the server reads them, so
# the dump is of the database the server uses. No backup-specific secrets.
#
# Restore a dump with (needs pg_restore >= the pg_dump that wrote it):
#   pg_restore --no-owner --no-privileges -d <target-conn> ccas-<stamp>.dump
#
set -euo pipefail

# A dump is the whole database: owner-only, like the ccas.env it connects with.
umask 077

BACKUP_DIR="${CCAS_BACKUP_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/ccas/backups}"
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

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

# Every occurrence of $2 in $1 becomes $3, left to right. A loop rather than
# ${1//…/…}, whose handling of backslashes in the replacement varies by bash
# version, and macOS runs this under 3.2.
replace_all() {
  local s="$1" out=""
  while [[ "$s" == *"$2"* ]]; do
    out+="${s%%"$2"*}$3"
    s="${s#*"$2"}"
  done
  printf '%s' "$out$s"
}

# ServerEnvFile.unquote, step for step.
unquote() {
  local v="$1"
  if [[ ${#v} -ge 2 && "$v" == \"*\" ]]; then
    v="${v:1:${#v}-2}"
    v="$(replace_all "$v" '\"' '"')"
    v="$(replace_all "$v" '\\' '\')"
  elif [[ ${#v} -ge 2 && "$v" == \'*\' ]]; then
    v="${v:1:${#v}-2}"
  fi
  printf '%s' "$v"
}

# The libpq name for a URL parameter, or nothing to drop it: pg_dump refuses one
# it does not know, and a JDBC URL may carry pgjdbc's. pgjdbc names libpq has an
# equivalent for are renamed; libpq's own pass through.
libpq_key() {
  case "$1" in
    channelBinding) echo channel_binding ;;
    connectTimeout) echo connect_timeout ;;
    # libpq accepts `ssl=true` in a URI for JDBC's sake, and no other value.
    ssl) [[ "$2" == true ]] && echo ssl ;;
    host | hostaddr | port | dbname | user | passfile | require_auth | \
      channel_binding | connect_timeout | client_encoding | options | \
      application_name | fallback_application_name | keepalives | \
      keepalives_idle | keepalives_interval | keepalives_count | \
      tcp_user_timeout | replication | gssencmode | sslmode | requiressl | \
      sslnegotiation | sslcompression | sslcert | sslkey | sslpassword | \
      sslcertmode | sslrootcert | sslcrl | sslcrldir | sslsni | requirepeer | \
      ssl_min_protocol_version | ssl_max_protocol_version | krbsrvname | \
      gsslib | gssdelegation | service | target_session_attrs | \
      load_balance_hosts) echo "$1" ;;
  esac
  return 0
}

# Each connection key the environment leaves blank takes its value from the
# server's ccas.env: the precedence ServerEnvOverlay gives the server. The file
# is parsed, never `source`d: the `&` in a JDBC URL is a shell control operator.
CONN_KEYS=(DATABASE_URL DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD)
ENV_FILE="${XDG_CONFIG_HOME:-$HOME/.config}/ccas/ccas.env"
if [[ -r "$ENV_FILE" ]]; then
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$(trim "$line")" == \#* || "$line" != *=* ]] && continue
    key="$(trim "${line%%=*}")"
    [[ "$key" == "export "* ]] && key="$(trim "${key#export }")"
    # Last assignment wins, as in ServerEnvFile.toMap.
    if [[ " ${CONN_KEYS[*]} " == *" $key "* ]]; then
      printf -v "file_$key" '%s' "$(unquote "$(trim "${line#*=}")")"
    fi
  done <"$ENV_FILE"
  for key in "${CONN_KEYS[@]}"; do
    file_var="file_$key"
    if [[ -z "$(trim "${!key:-}")" && -n "$(trim "${!file_var:-}")" ]]; then
      printf -v "$key" '%s' "${!file_var}"
    fi
  done
fi

# Stops on a DATABASE_URL that would put a piece of the password where something
# prints it; names only the shape, never a piece of the URL.
reject_url() {
  echo "backup-neon.sh: DATABASE_URL has $1; percent-encode any & or ? in the password" >&2
  exit 1
}

# Resolve a libpq conninfo string, with the password routed to PGPASSWORD rather
# than the URI, so it never reaches argv (where `ps` / /proc/<pid>/cmdline would
# expose it to other local users).
if [[ -n "${DATABASE_URL:-}" ]]; then
  # DATABASE_URL may be in either accepted form (the app normalises both — see
  # PostgresClient.normalizeJdbcUrl):
  #   jdbc:postgresql://host/db?user=...&password=...&sslmode=require
  #   postgresql://user:pass@host/db?sslmode=require       (what providers hand out)
  # Without "jdbc:", either is a libpq URI once its password is lifted out and
  # its parameters are given libpq's names (below).
  CONN="${DATABASE_URL#jdbc:}"

  # Userinfo form: split the authority at its last '@' (a password may contain an
  # unescaped one), keeping the username in the URI and routing the password out.
  authority="${CONN#*://}"
  # A '?' before the '@' is inside the password: cut there, the rest of it would
  # land in the host and port, which pg_dump echoes when it rejects them.
  [[ "${authority%%/*}" == *\?*@* ]] && reject_url "a '?' before its '@'"
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
    dropped=0
    IFS='&' read -ra params <<<"$query"
    # The ${a[@]+…} guard: bash before 4.4 calls an empty array unbound under -u.
    for kv in ${params[@]+"${params[@]}"}; do
      [[ -n "$kv" ]] || continue
      # A parameter always has an '='; a part without one is most likely a piece
      # of a password holding an unescaped '&'.
      [[ "$kv" == *=* ]] || reject_url "a query part with no '='"
      name="${kv%%=*}"
      if [[ "$name" == password ]]; then
        export PGPASSWORD="$(urldecode "${kv#password=}")"
      else
        libpq_name="$(libpq_key "$name" "${kv#*=}")"
        if [[ "$libpq_name" == ssl ]]; then
          # First: libpq rewrites it to sslmode=require in place, and pgjdbc lets
          # an explicit sslmode win wherever it sits.
          kept=("ssl=true" ${kept[@]+"${kept[@]}"})
        elif [[ -n "$libpq_name" ]]; then
          kept+=("$libpq_name${kv#"$name"}")
        else
          dropped=$((dropped + 1))
        fi
      fi
    done
    # A count, not names: the same malformed password could split into a
    # name-shaped piece, and anything echoed here reaches the cron log.
    if ((dropped)); then
      echo "backup-neon.sh: dropped $dropped URL parameter(s) pg_dump would reject" >&2
    fi
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

# Name what was dumped: a shell exporting another DATABASE_URL (direnv, say)
# beats ccas.env, and a dump of the wrong database looks like any other.
dumped="${CONN#*://}"
dumped="${dumped%%\?*}"
dumped="${dumped##*@}"
echo "wrote $OUT ($(du -h "$OUT" | cut -f1)) from $dumped"

# Retention: keep the newest $RETAIN dumps, delete the rest.
while IFS= read -r old; do
  rm -f "$old"
  echo "pruned $old"
done < <(ls -1t "$BACKUP_DIR"/ccas-*.dump 2>/dev/null | tail -n "+$((RETAIN + 1))")
