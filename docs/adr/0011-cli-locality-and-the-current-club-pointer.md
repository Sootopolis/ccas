# CLI commands are either wholly local or wholly server-backed

**Status:** Accepted, 2026-06-25 (#86, #123). Club-pointer half amended by #176.

## Context

`ccas` (`ccas.cli.Main`) is a zio-cli binary. Some of what it does needs a running server; some of it
is a local file edit. If a command group mixes the two, the user hits an availability trap: with the
server down, `ccas club add` fails while `ccas club use` succeeds, and nothing in the UI explains the
difference.

## Decision

Every command is either a `CliCommand.ServerCommand` — carrying `--server`, dispatched by
`Dispatcher` through `CcasApiClient` — or a local command handled in-process by `Main.execute`, with
no `--server` and no dispatch.

**Every group is locality-homogeneous.** `server` and `config` are wholly local; `blacklist`,
`schedule` and `club` are wholly server-backed. Preserve this when adding commands.

That is why `use-club` is a top-level leaf and not `ccas club use`: grouping it would make `club` the
first mixed group. The shape follows `git switch` and `kubectl config use-context`. The `club` group's
help text points at `use-club` so discoverability does not depend on tree position.

Local does not mean network-free. `server status` / `down`, and `up --detach`'s readiness wait, probe
fixed loopback `/health` endpoints via `HealthProbe`; `use-club` on a *set* makes a short best-effort
`/api/managed-clubs` fetch to refresh the completion cache, but its guaranteed effect — the local
pointer write — still lands offline. `config` touches nothing.

### `current_club` and `managed_club` are orthogonal, not hierarchical

`current_club` is a *per-device local pointer* in the CLI config. `managed_club` is a *shared DB row*
naming the clubs this deployment runs CCAS for. `ClubResolver.single` never consults the managed set
— only `--all` expands from it, via `ClubResolver.multi` — so a `current_club` naming an unmanaged
club still resolves and still submits jobs. Job submission likewise gates on the `club` row, not
managed status (#177 tracks closing that gap). Because unmanaging leaves the `club` row intact,
`ccas club remove` clears `current_club` when it names the removed club, so the next bare command
fails loudly on `ClubResolver.NoClubError` rather than silently running against a disowned club.

### `current_club` is id-authoritative; the slug is a display label

It is stored as `<id>:<slug>` (parsed and rendered by `CurrentClubRef`) once the CLI knows the club's
stable Chess.com id — a bare slug otherwise (offline set, or a club the server could not resolve an
id for), backfilled on the next successful command.

`ClubResolver.single` / `multi` return a `ClubTarget(clubId, slug)`. Club-scoped job requests carry
the optional `clubId`, and the server resolves through `ClubResolution.resolve`
(`ccas.analysis.apps`), which wraps `Club.resolveByIdOrSlug` — id first, slug fallback — into a typed
`ClubVerdict`: `Known(club)` / `NotLocal(slug)` / `Problematic(slug)` (tombstoned). A non-`Known`
verdict maps to a typed `ClubProblem` (`ccas.utils.errors`, snake_case wire) carried on
`ClubJobResult` / `JobResult` alongside the human `error` string.

This is what makes a renamed `current_club` still resolve: the id never moves when Chess.com renames
the slug, so the stale slug the CLI echoed no longer 404s at submission (#176). The submit response
returns the canonical `{clubId, canonicalSlug}`, and the CLI rewrites `current_club` when that
differs from what is stored, so display names self-heal.

## Consequences

- Anything comparing or displaying `current_club` — the `club list` `*` marker, the stale-current
  hint, `use-club` show/clear — goes through `CurrentClubRef`, matching by id when the pointer has
  one. Blacklist and recruitment-report stay slug-keyed, since they do their own server-side rename
  recovery, so they use the target's display slug rather than the id.
- `--all` and an explicit `--club <slug>` carry no id; id resolution is scoped to `current_club` for
  now. A slug-only target therefore still resolves by exact slug *before* any rename recovery can
  run, so `ClubSlugRenameResolver` stays unreachable from that path (#176 / #180).
- **Club targeting is by option, never by positional.** `--club <slug>` (comma-separated on `membership`
  and `history`), `--all` on those two for every managed club, and otherwise the config's
  `current_club`. The remaining positionals are `<username>...` on the blacklist commands, the
  `<slug>` of `club add` / `remove`, the optional `[slug]` of `use-club`, and the optional `[run-id]`
  on `recruit --report`.
- **`ServerEnvFile` is modelled as an ordered list of lines**, so setting or unsetting one key leaves
  every other key, blank line, comment and even the line ordering untouched. A line with no `=`, and
  any comment or blank, is preserved verbatim, so a malformed line never crashes parsing. Values
  split on the FIRST `=`, which is what keeps a JDBC URL's `&?=:` intact, and an optional surrounding
  quote pair is stripped on read and re-added on write only when the value needs it. Writes go
  through a temp-file-then-rename that creates the temp file owner-only (0600) and carries that mode
  across the rename, so `DATABASE_URL` and `DB_PASSWORD` are never world-readable.
- **Two config files, deliberately separate.** `${XDG_CONFIG_HOME:-~/.config}/ccas/config.conf` holds
  CLI *client* settings (`api_url`, `default_clubs`, `log_dir`, `current_club`) — read by `CliConfig`
  via zio-config, written by `ConfigWriter` as a surgical line edit, since zio-config has no HOCON
  writer. `${XDG_CONFIG_HOME:-~/.config}/ccas/ccas.env` holds what the *server* needs to boot
  (`CCAS_CONTACT_EMAIL`, `DATABASE_URL`, …) and is what `ccas config get|set|unset|list|path`
  manages, applied at boot by `ServerEnvOverlay`. `ccas config` does **not** see `current_club`; that
  is by contract, not a bug.
- Both paths resolve through `XdgPaths`, which reads environment variables — so any code a test must
  exercise against a temp path needs an explicit-path form (see `CompletionCache`'s `…In` variants)
  rather than relying on rebinding the environment.
- **Completion cache.** `${XDG_CACHE_HOME:-~/.cache}/ccas/{clubs.txt,recent-jobs.txt}`, read by the
  generated scripts with a bare `cat` — no JVM, no network. `Dispatcher.refreshClubsCache` repopulates
  it after any successful server-touching command, gated on a 6h TTL, sourced from
  `/api/managed-clubs` (falling back to `/api/clubs` only when nothing is managed yet, since
  `/api/clubs` is every club ever ingested and a few history crawls push that into the thousands).
  `CompletionCache.invalidate` bypasses the TTL when the managed set changes (`club add` / `remove`)
  and when a submit result carries a typed `ClubProblem`. `Dispatcher.missingClub` branches on that
  typed field; the legacy `error.startsWith("Club not found")` string survives only as a fallback for
  a server predating it.
