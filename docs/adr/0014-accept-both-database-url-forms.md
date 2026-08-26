# Accept both database URL forms, and lift credentials out of either

**Status:** Accepted, 2026-08-13 (#210).

## Context

Every managed Postgres provider — Neon, Heroku, Render, Supabase, Railway — hands out the **libpq**
connection URI from PostgreSQL's own documentation:
`postgresql://user:pass@host/db?sslmode=require`.

pgjdbc accepts neither half of that. It requires the `jdbc:` subprotocol, and it requires credentials
as `?user=` / `?password=` query parameters rather than in userinfo position. Pasting a provider URL
verbatim failed at boot inside Hikari with `RuntimeException: Failed to get driver instance for
jdbcUrl=…` — a message that names the driver rather than the URL shape, so it points away from the
actual problem.

## Decision

`PostgresClient.normalizeJdbcUrl` accepts both forms and converts, returning a JDBC URL plus
separately-carried credentials.

**Credentials are always lifted out of the URL, for both forms.** Hikari echoes the `jdbcUrl` into
that failure message, so a password embedded in the URL reaches the log on any driver-level failure.
`hikariConfig.setUsername` / `setPassword` are equivalent as far as pgjdbc is concerned and keep the
secret out of the string.

**Percent-decoding differs by position, deliberately.** Userinfo follows RFC 3986 — percent-escapes
only, `+` is a literal plus. Query values follow what pgjdbc itself applies to them: `URLDecoder`,
where `+` decodes to a space. Decoding a lifted query credential any other way would silently change
the password of an existing, working `jdbc:` configuration. This asymmetry looks like an
inconsistency and is not one; do not unify it.

Scheme matching is case-insensitive (RFC 3986 §3.1); the rest of the URL keeps the case it was
written in.

## Consequences

- Non-Postgres `jdbc:` URLs pass through untouched, as does any `jdbc:` URL this cannot improve on.
  Anything that is neither form returns `Left` with an actionable, credential-free message.
- `ccas config set DATABASE_URL` warns — and still writes — when the value is neither form.
- `scripts/backup-neon.sh` parses both forms too, routing the password through `PGPASSWORD` so it
  never reaches `argv`.
