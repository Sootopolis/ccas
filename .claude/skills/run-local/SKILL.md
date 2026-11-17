---
name: run-local
description: Run this working tree's `ccas` against a local server on a throwaway database and drive its interactive commands from Claude's shell, which has no terminal. Use for the by-hand checks a change's report lists, when the user asks Claude to run them, or to see a CLI change working end to end.
---

# Run the working tree's `ccas` locally

Every step here stays on this machine's Postgres and the `wip` server port. Nothing reaches production, as long as each client command passes `--server`. From a shell that carries `wip`'s environment, a client command without `--server` goes to `127.0.0.1:8080`, which is production's server.

## 1. Build this tree

`sbt --server -batch stage`, then run `target/universal/stage/bin/ccas`. Check the date on `target/universal/stage/lib/ccas.ccas-*.jar`; the `bin/` script keeps its old date. The `ccas` on `PATH` runs the base worktree's build, which lacks this tree's changes.

## 2. A throwaway database

A copy of nothing, so the developer's local `ccas` database is untouched and needs no clean-up:

1. `createdb ccas_handcheck`.
2. Hand it to the role the server connects as, which owns `ccas`. `psql -d ccas_handcheck -Atc "select pg_get_userbyid(datdba) from pg_database where datname = 'ccas'"` names it. Then `ALTER DATABASE ccas_handcheck OWNER TO <role>` and `ALTER SCHEMA public OWNER TO <role>`. Without the schema, boot fails with `permission denied for schema public`.
3. `psql -d ccas_handcheck -c "CREATE EXTENSION btree_gist"`, which `club_name` needs.

## 3. The server

From a shell carrying `wip`'s environment (`printenv DIRENV_DIR` names the `wip` tree, and `SERVER_PORT` is 8081), check that `DATABASE_URL` points at `localhost:5432/ccas`. Use a `case` that prints only whether it matches; never print the URL. Then:

```
DATABASE_URL="${DATABASE_URL/localhost:5432\/ccas/localhost:5432/ccas_handcheck}" target/universal/stage/bin/ccas server up --detach
```

First, `lsof -nP -iTCP:8081 -sTCP:LISTEN` shows whether a server already holds the port. If one does, note what it serves (`curl -s http://127.0.0.1:8081/health/ready`) and stop it with `server down --port 8081`. The started line must name `db localhost:5432/ccas_handcheck`. A failed boot leaves its cause in `~/.local/state/ccas/logs/server-8081.log`; `grep level=ERROR` it.

## 4. A club

The server never fetches a club it has not seen, so seed a made-up one. Use an id and slug no real club uses:

```
psql -d ccas_handcheck -c "INSERT INTO club (club_id, created, slug, name) VALUES (999001, now(), 'handcheck-club', 'Handcheck Club')" -c "INSERT INTO club_name (club_id, slug, since) VALUES (999001, 'handcheck-club', now() - interval '1 day')"
```

Recruit runs that get past their questions call Chess.com, so keep to the questions, or decline them.

## 5. A CLI of its own

Commands rewrite the club completion cache, and some read `current_club`. Run the client with `XDG_CONFIG_HOME`, `XDG_CACHE_HOME` and `XDG_STATE_HOME` pointed at directories in the scratchpad, through a wrapper script there that `exec`s `target/universal/stage/bin/ccas`. The developer's own config, current club and cache then stay as they were. Pass `--server http://127.0.0.1:8081`. To test a command run against the default server, write `api_url = "http://127.0.0.1:8081"` to `$XDG_CONFIG_HOME/ccas/config.conf` and leave `--server` out. Keep the server itself on the real directories, since it reads `ccas.env` from them.

## 6. Interactive commands

`script -q /dev/null <command>` gives the command a terminal, so `System.console()` is non-null. Answers piped in all at once are lost: the ones sent before the JVM reads are discarded, and `script`'s closing `^D` then ends the input. Pace them, and hold stdin open until the command has finished:

```
{ sleep 4; echo; sleep 0.2; echo 25; sleep 0.2; …; sleep 1; echo y; sleep 4; } \
  | script -q /dev/null <wrapper> recruitment-criteria set --server http://127.0.0.1:8081 --club handcheck-club \
  | tr -d '\r' | sed 's/\x1b\[[0-9;]*[A-Za-z]//g'
```

To test Ctrl-C, start the command from Python with `preexec_fn` resetting SIGINT to `SIG_DFL`, and write `\x03` to `script`'s stdin. A command started from Claude's shell inherits SIGINT as ignored, so a `kill -INT` or a typed ^C does nothing.

The Bash tool's display capitalises the first word of a failing command's output: an `error:` line shows as `Error:`. Check the bytes (`| od -c`) before calling it a bug.

## 7. Clean up

`target/universal/stage/bin/ccas server down --port 8081`, then `dropdb ccas_handcheck`. If step 3 stopped a server, start it again as it was. The server log keeps the runs. Say so in the report, along with what was run and what it printed.
