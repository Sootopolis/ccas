# Job logs and reports live in one directory per machine

**Status:** Accepted, 2026-10-23 (#344).

## Context

CCAS writes two kinds of file that outlive the process writing them. The first is per-job logs, which a server writes and `ccas logs` serves until the retention sweep removes them. The second is app reports, which `OutputFile` archives on each new write. The apps' own mains write reports (`HistoryApp.run` and its siblings), and a server writes one only when it stops a History job midway.

Both defaulted to paths relative to the launch directory, `logs/jobs` and `out/`. On 2026-10-23 production's job logs from the previous fortnight were split between the base worktree and the home directory, because one boot ran from there. Each server answers a finished job's log from the other directory as `Expired` (`JobRunner.logStream`), and its sweep never reaches the other copy.

Moving the defaults under the XDG base directories fixes the launch-directory problem. That's `JobRunner.LogDir` and `OutputFile.Root`, both resolved by `ccas.utils.AppDir`. It leaves a question: a machine can run two servers, one per database (one per worktree, say), and #331 had just given each port its own pid file and server log. Should job logs and reports be split per server too?

## Decision

**No: every process on a machine shares one job-log directory and one report directory.**

- **Per port** ties the files to the wrong thing. The port names a process, but a job's log belongs to the `job_run` row in its database, and a report to its club. Boot production on another port once (its usual one busy, say) and its logs split again. That is the defect this fixes, with a different trigger. An app main has no port at all.
- **Per database** names the right owner, but there is no stable name to key on. The URL isn't one: a pooled and a direct host reach the same database, and `DATABASE_URL` and the `DB_*` fields spell it differently. A key would have to be an id stored in the database itself. That is a stored-data change, made only to spare the one machine that runs two databases an override it can set once.
- **Shared** is safe for job logs, because nothing about a log file needs the directory to say whose it is. Job ids are ULIDs, so files from two databases never collide, and `logStream` opens a file only for an id in its own database's `job_run`.

To keep the processes for two databases apart, the operator sets `JOB_LOGS_DIR` and `CCAS_OUTPUT_DIR` in the environment of one of them.

## Consequences

- **A sweep applies its own window to every log in the directory.** `FileSink.sweepBefore` deletes by modification time and spares only the jobs running in its own process. The window is that server's `job_log_retention_days`, so when two databases set different windows, the shorter one wins for both.
- **A report archives the previous one, whichever database it came from.** Reports for the same club and app, run against two databases, move each other's latest report into `archive/`. Nothing is deleted.
- **Nothing defaults to the launch directory any more, but a relative setting is still honoured.** It resolves against the launch directory. The job-log directory resolves once, at boot, with a warning saying where it landed. The report directory resolves on each write, and the `Output written to` line names the absolute path. A relative `XDG_*_HOME` or `user.home` is ignored, as the XDG specification requires of the former.
- **The defaults rely on the operator to keep two databases apart.** Before, each worktree's runs kept to its own `logs/jobs` and `out/` only by being started from that worktree: a server from its shell, an app main from an IDE whose working directory is the worktree. Now one worktree's processes must be given `JOB_LOGS_DIR` and `CCAS_OUTPUT_DIR`. An app main reads only its process environment, not `ccas.env`, so for an IDE launch that means the run configuration.
