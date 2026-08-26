# A job's log sink degrades and retries rather than switching off

**Status:** Accepted, 2026-06-26 (#52).

## Context

Each job writes its log to `${logDir}/<jobId>.log` through a `FileSink`, tee'd to the server console.
The first version disabled file logging permanently on the first write failure. That is the wrong
shape for the failures that actually occur: a `logDir` that has not appeared yet, a disk that fills
and is then freed, permissions that are fixed a minute later. All of them clear on their own, and all
of them silently cost the rest of the job's log.

Retrying every line is equally wrong — a permanent cause like disk-full would reopen the writer on
every line and put one stack trace per line on stderr.

## Decision

On a write failure the sink enters a **suppressed** state, counts the dropped line, and periodically
reopens a fresh writer in `CREATE, APPEND` mode, gated by `retryAfterLines` lines or
`retryAfterNanos` elapsed, whichever comes first. So a permanent cause costs one reopen per gate
rather than one per line, and stderr sees one trace per failure *episode*.

On recovery the sink writes a `resumed after N dropped line(s)` marker; on `close` it records an
`N log line(s) dropped` summary if any were lost. Both flow through the file-tail logs endpoint
(#47), and the running total is exposed via `droppedLineCount`.

`FileSink.make` returns a sink even when the initial open fails — one that starts suppressed and
retries on its first write — so the job runs and the stdout tee keeps working regardless. `logDir`
itself is assumed to exist; `JobRunner.live` creates it once at server startup.

## Consequences

- **Two failure modes are out of scope by construction.** A file deleted under an already-open fd
  never fails at all — Linux keeps writing to the unlinked inode — so the data is silently lost with
  no signal to react to. And a delete-then-recreate desyncs the file-tail offset. Both are
  limitations, not bugs to be caught here.
- ANSI escapes are stripped from the file but not from the stdout tee, so `cat job.log` is readable
  while an operator watching the console keeps colour.
- The `BufferedWriter` is opened once and held for the job's lifetime, closed by `JobRunner` from its
  terminal-status finaliser. Each write appends one line and flushes, so the file-tail endpoint sees
  lines as they land, guarded by a per-sink monitor because `BufferedWriter` is not thread-safe and
  one sink is shared across all of a job's forked fibers via `JobLogSink.currentSink.locally`.
- The file write runs **outside** `ProgressDisplay`'s render lock, so a slow disk never stalls bar
  redraws.
