# Running on Neon

What changes when the database is a [Neon](https://neon.tech) compute that suspends when idle, as production's is. Everything else about configuring CCAS is in the [README](../README.md), section Configuration.

## Letting a compute scale to zero

Neon suspends a compute after 5 minutes with no active query, and the pool's defaults allow that: they keep no idle connection, send no keepalive ping, and retire a connection soon after it goes idle. Raising `DB_POOL_MIN_IDLE` above 0 or setting `DB_POOL_KEEPALIVE_TIME` keeps the compute awake for as long as the server runs, spending the free tier's monthly compute hours (#420). The cost of the defaults is a cold-start delay on the next query and the occasional transient `08xxx` reconnect, which the socket-timeout and transient-retry hardening absorbs. Expect benign "idle" warnings if a command sits at a confirmation prompt. An idle server's scheduler reads the database now and then, not at every poll ([ADR 0028](adr/0028-the-scheduler-reads-only-when-a-schedule-can-be-due.md)).

A waking compute can also outlast `ccas server up --detach`'s readiness wait; raise it with `ready_timeout_seconds` (README, CLI config file).

## Backups

Neon's free plan retains only 24h of point-in-time recovery. [`scripts/backup-neon.sh`](../scripts/backup-neon.sh) takes a weekly compressed logical dump to local disk as the disaster-recovery floor. It is read-only (`pg_dump` only) and reads the server's connection config the way the server does — `DATABASE_URL` in either form, otherwise the `DB_*` fields, each from the environment and otherwise from `ccas.env` — so it dumps the database the server uses, with nothing to export. The rebuildable cache / diagnostics tables (`api_response_cache`, `api_response_body`, `api_fetch_failure`) are dumped schema-only (`--exclude-table-data`) to keep dumps small.

`CCAS_BACKUP_DIR` sets where the `.dump` files go, and `CCAS_BACKUP_RETAIN` how many of the most recent to keep; the script defines their defaults near its top.

Run it once manually, then schedule weekly. Sunday 04:00, cron's local time; cron's own `PATH` is `/usr/bin:/bin`, so add the directory holding the `pg_dump` that `command -v pg_dump` finds, and set any `CCAS_BACKUP_*` variable on the crontab line too:

```cron
PATH=/opt/homebrew/bin:/usr/bin:/bin
0 4 * * 0 /path/to/ccas/scripts/backup-neon.sh >> ~/.local/share/ccas/backups/backup.log 2>&1
```

A weekly cadence is one compute wake per week — negligible against the free plan's monthly compute allowance.

### Restoring

Restore a dump with `pg_restore` (use a version **≥** the `pg_dump` that wrote the file — a custom-format archive can't be read by an older `pg_restore`), as the role the app connects with. Restored as anyone else, the objects belong to that user, and the app fails on them with permission errors. Into a fresh local database:

```bash
createdb -O ccas <db>
pg_restore --no-owner --no-privileges --role=ccas -d <db> ccas-<stamp>.dump   # errors about pg_stat_statements are harmless
```

`--role` needs a superuser or a member of `ccas`. Into any other database, connect as the app's role instead and drop `--role`: `pg_restore --no-owner --no-privileges -d <conn-as-app-role> ccas-<stamp>.dump`.
