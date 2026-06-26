package ccas.server.jobs

import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, NoSuchFileException, Path, StandardOpenOption}
import java.time.{Duration, Instant}

import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

import zio.{UIO, ZIO}

import ccas.utils.JobLogSink

/** Per-job file-backed sink. `writeConsoleSync` tees the formatted log line to `System.out` (so the server console
  * keeps showing every job's output); `writeFileSync` appends the same line, stripped of ANSI escapes, to
  * `${logDir}/<jobId>.log`.
  *
  * Stripping ANSI from the file (but not the stdout tee) keeps `cat job.log` human-readable while preserving colour
  * for an interactive operator watching the server console.
  *
  * The underlying [[java.io.BufferedWriter]] is opened once in [[FileSink.make]] and held open for the job's lifetime;
  * `JobRunner` closes it via [[close]] from its terminal-status finaliser. Each `writeFileSync` writes one line and
  * flushes (so the file-tail logs endpoint sees lines as they land), guarded by a per-sink monitor because
  * `BufferedWriter` is not thread-safe and the same `FileSink` is shared across all of a job's forked fibers (via
  * `JobLogSink.currentSink.locally`). The file write runs *outside* `ProgressDisplay`'s render lock, so a slow disk
  * never stalls bar redraws.
  *
  * '''Write-failure handling (issue #52).''' A write failure no longer permanently disables file logging. Instead the
  * sink enters a *suppressed* state, counts the dropped line, and periodically retries by reopening a fresh writer in
  * `CREATE, APPEND` mode (gated by [[retryAfterLines]] lines or [[retryAfterNanos]] elapsed, whichever first — so a
  * permanent cause like disk-full doesn't reopen on every line, and stderr sees one trace per failure episode rather
  * than per line). On recovery it writes a `resumed after N dropped line(s)` marker, then continues; on [[close]] a
  * `N log line(s) dropped` summary is recorded if any were lost. Both markers flow through the file-tail logs endpoint
  * (#47), and the running total is exposed via [[droppedLineCount]]. This recovers transient failures: an initial open
  * failure that later clears (the `logDir` reappears), and `write`/`flush` failures that later clear (disk space freed,
  * perms restored).
  *
  * Out of scope, by construction: a file deleted under an already-open fd (Linux keeps writing to the unlinked inode,
  * so no failure ever fires and the data is silently lost), and the file-tail offset desync that follows a
  * delete-then-recreate. Both are documented limitations, not handled here.
  *
  * `logDir` is assumed to already exist; `JobRunner.live` creates it once at server startup. If opening the writer
  * fails, [[FileSink.make]] still returns a sink — one that starts suppressed and retries on its first write — so the
  * job runs and the stdout tee keeps working regardless.
  */
final class FileSink private[jobs] (
    path: Path,
    openWriter: () => BufferedWriter,
    initialWriter: Option[BufferedWriter],
    nowNanos: () => Long,
    retryAfterLines: Long,
    retryAfterNanos: Long
) extends JobLogSink {

  // Serialises access to the non-thread-safe BufferedWriter — and to every `var` below — across the job's concurrent
  // fibers. Distinct from `ProgressDisplay`'s render lock (that's the point of #53: file IO no longer contends with bar
  // redraws). `writeFileSync` is invoked outside `ProgressDisplay.lock` and only ever takes `fileLock`, so the two
  // locks never nest in either order — no deadlock is possible.
  private val fileLock = new Object

  // All mutable state lives under `fileLock`. `writer` is None while suppressed (a failed open/write) or never opened.
  private var writer: Option[BufferedWriter]    = initialWriter
  private var closed: Boolean                   = false
  private var droppedLines: Long                = 0L                    // lifetime total → accessor + final summary
  private var suppressedDrops: Long             = 0L                    // gap of the current episode → resume marker
  private var linesSinceLastAttempt: Long       = 0L                    // count-gate; reset on every reopen attempt
  private var lastAttemptNanos: Long            = nowNanos()           // time-gate anchor; init so it can't trip line 1
  private var failureLogged: Boolean            = initialWriter.isEmpty // stderr once per episode (make already logged)

  /** Lifetime count of log lines dropped because the file could not be written. Read under the lock. */
  private[jobs] def droppedLineCount: Long = fileLock.synchronized(droppedLines)

  override def writeConsoleSync(line: String): Unit = System.out.println(line)

  override def writeFileSync(line: String): Unit =
    fileLock.synchronized {
      if (!closed) {
        writer match {
          case Some(w) => writeOrSuppress(w, line)
          case None    => retryOrDrop(line)
        }
      }
    }

  // Writer is healthy: write + flush. On failure, release the broken writer's fd, enter the suppressed state, count the
  // line that failed, and log one stderr trace for this episode.
  private def writeOrSuppress(w: BufferedWriter, line: String): Unit =
    try {
      w.write(JobLogSink.stripAnsi(line))
      w.write("\n")
      w.flush()
    } catch {
      case NonFatal(t) =>
        // `closeQuietly` calls `BufferedWriter.close()`, which flushes; if the failure was transient and cleared by
        // now, the failing line may actually land on disk despite being counted dropped (so the next resume marker can
        // over-report by 1). Benign and rare — the alternative (abandon the fd without closing) leaks it.
        closeQuietly(w)
        writer = None
        droppedLines += 1
        suppressedDrops += 1
        linesSinceLastAttempt = 0
        lastAttemptNanos = nowNanos()
        if (!failureLogged) {
          System.err.println(s"[FileSink] write to $path failed; retrying periodically, suppressing further errors until it recovers.")
          t.printStackTrace(System.err)
          failureLogged = true
        }
    }

  // Suppressed: when the retry gate opens, try to reopen and resume; otherwise just drop the line. The line that
  // triggers a successful reopen is written (not counted); a line whose reopen attempt fails is dropped (counted once).
  private def retryOrDrop(line: String): Unit = {
    linesSinceLastAttempt += 1
    val gateOpen = linesSinceLastAttempt >= retryAfterLines || (nowNanos() - lastAttemptNanos) >= retryAfterNanos
    if (gateOpen) {
      lastAttemptNanos = nowNanos()
      try {
        val w = openWriter()
        w.write(resumeMarker)
        w.write(JobLogSink.stripAnsi(line))
        w.write("\n")
        w.flush()
        writer = Some(w)
        suppressedDrops = 0
        linesSinceLastAttempt = 0
        failureLogged = false
      } catch {
        case NonFatal(_) =>
          // Gate consumed; stay suppressed silently (the count + final summary surface the magnitude, not stderr spam).
          linesSinceLastAttempt = 0
          droppedLines += 1
          suppressedDrops += 1
      }
    } else {
      droppedLines += 1
      suppressedDrops += 1
    }
  }

  /** Flush and close the underlying writer; if any lines were dropped, record a summary first. Marks the sink closed so
    * any late `writeFileSync` is a clean no-op (no reopen, no count). Idempotent and error-swallowing. `JobRunner` calls
    * this from the job's terminal-status finaliser before firing the completion promise, so a tailing client observes a
    * fully-written, closed file.
    */
  def close(): UIO[Unit] =
    ZIO.succeed {
      fileLock.synchronized {
        if (!closed) {
          closed = true
          writer match {
            case Some(w)                       => closeWithSummary(w)
            case None if droppedLines > 0      => recordSummaryWhileSuppressed()
            case None                          => ()
          }
          writer = None
        }
      }
    }

  private def closeWithSummary(w: BufferedWriter): Unit = {
    if (droppedLines > 0) {
      try w.write(summaryLine)
      catch { case NonFatal(_) => () }
    }
    try {
      w.flush()
      w.close()
    } catch {
      case NonFatal(_) => ()
    }
  }

  // Suppressed at close but lines were dropped: try one ungated reopen purely to record the summary in the file (so it
  // surfaces via the #47 tail); fall back to stderr if even that fails.
  private def recordSummaryWhileSuppressed(): Unit =
    try {
      val w = openWriter()
      w.write(summaryLine)
      w.flush()
      w.close()
    } catch {
      case NonFatal(_) => System.err.println(summaryLine.trim)
    }

  // `suppressedDrops == 0` only on an initial open failure that clears before any line is dropped (recovery from a
  // mid-job write failure always has ≥1). Phrase it as a plain start rather than "resumed after 0 dropped line(s)".
  private def resumeMarker: String =
    if (suppressedDrops == 0) "[FileSink] file logging started\n"
    else s"[FileSink] file logging resumed after $suppressedDrops dropped line(s)\n"

  private def summaryLine: String = s"[FileSink] $droppedLines log line(s) dropped due to write failures\n"

  private def closeQuietly(w: BufferedWriter): Unit =
    try w.close()
    catch { case NonFatal(_) => () }
}

object FileSink {

  // Reopen cadence: retry the file write at most once per this many dropped lines, or once per this elapsed time,
  // whichever comes first. Compiled-in (not HOCON / app_setting) — surfacing them as tunables is a possible follow-up.
  private val DefaultRetryLines: Long = 50L
  private val DefaultRetryNanos: Long = Duration.ofSeconds(5).toNanos

  /** Create a sink writing to `${logDir}/<jobId>.log`, opening the held-open [[java.io.BufferedWriter]] now. Caller is
    * responsible for ensuring `logDir` exists (typically done one-shot in `JobRunner.live`). An open failure is folded
    * into a suppressed sink (logged once to stderr) rather than failing the effect, so a transient disk problem never
    * kills the job — the stdout tee still works, and the sink retries the file on its next write.
    */
  def make(logDir: Path, jobId: String): UIO[FileSink] = {
    val path = logDir.resolve(s"$jobId.log")
    val open = () =>
      Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    ZIO
      .attemptBlocking(open())
      .fold(
        t => suppressedSink(path, open, t),
        w => build(path, open, Some(w))
      )
  }

  private def build(path: Path, open: () => BufferedWriter, initial: Option[BufferedWriter]): FileSink =
    new FileSink(path, open, initial, () => System.nanoTime(), DefaultRetryLines, DefaultRetryNanos)

  private def suppressedSink(path: Path, open: () => BufferedWriter, cause: Throwable): FileSink = {
    System.err.println(
      s"[FileSink] open $path failed; file logging suppressed, will retry. (${cause.getClass.getSimpleName}: ${cause.getMessage})"
    )
    // A missing logDir is the expected benign case — the message above is enough. Reserve the stack trace for the
    // genuinely unexpected (perms revoked, IO errors).
    cause match {
      case _: NoSuchFileException => ()
      case _                      => cause.printStackTrace(System.err)
    }
    build(path, open, None)
  }

  /** Delete `*.log` files in `logDir` whose last-modified time is before `cutoff`, returning the count removed.
    * Best-effort: a file that vanishes mid-sweep or can't be stat'd / deleted is skipped, never failing the sweep.
    * Called one-shot from `JobRunner.live` at startup, mirroring the cache-retention sweep in `Tables.ensureTables`.
    * The `*.log` glob means non-log files in the directory are left untouched.
    */
  def sweepBefore(logDir: Path, cutoff: Instant): ZIO[Any, Throwable, Int] =
    ZIO.attemptBlocking {
      val stream = Files.newDirectoryStream(logDir, "*.log")
      val stale =
        try stream.iterator().asScala.filter(olderThan(cutoff)).toList
        finally stream.close()
      stale.count(deleteQuietly)
    }

  // A file that vanished or can't be stat'd is treated as not-old (left alone); any non-fatal IO error is swallowed so
  // one bad file never aborts the whole sweep.
  private def olderThan(cutoff: Instant)(path: Path): Boolean =
    try Files.getLastModifiedTime(path).toInstant.isBefore(cutoff)
    catch { case NonFatal(_) => false }

  private def deleteQuietly(path: Path): Boolean =
    try Files.deleteIfExists(path)
    catch { case NonFatal(_) => false }
}
