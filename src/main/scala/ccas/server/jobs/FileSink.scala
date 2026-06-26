package ccas.server.jobs

import java.io.BufferedWriter
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, NoSuchFileException, Path, StandardOpenOption}
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

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
  * `logDir` is assumed to already exist; `JobRunner.live` creates it once at server startup. If opening the writer
  * fails, [[FileSink.make]] still returns a sink — one with no writer (`writeFailed` latched) — so the job runs and the
  * stdout tee keeps working; only the file logging is lost.
  */
final class FileSink private (path: Path, writer: Option[BufferedWriter]) extends JobLogSink {

  // Latches on open failure (writer == None) or on the first write failure; once set, `writeFileSync` short-circuits so
  // a permanently-unwritable file (disk full, perms revoked mid-run) doesn't drown stderr with the same trace on every
  // log line. The stdout tee keeps working regardless — operators still see job output on the server console.
  private val writeFailed = new AtomicBoolean(writer.isEmpty)

  // Serialises access to the non-thread-safe BufferedWriter across the job's concurrent fibers. Distinct from
  // `ProgressDisplay`'s render lock — that's the whole point of #53: file IO no longer contends with bar redraws.
  private val fileLock = new Object

  override def writeConsoleSync(line: String): Unit = System.out.println(line)

  override def writeFileSync(line: String): Unit =
    writer.foreach { w =>
      fileLock.synchronized {
        // Re-check the latch *inside* the lock so a write racing `close()` (which sets the latch under the same lock)
        // short-circuits cleanly instead of hitting an already-closed writer and logging a spurious failure.
        if (!writeFailed.get) {
          try {
            w.write(JobLogSink.stripAnsi(line))
            w.write("\n")
            w.flush()
          } catch {
            case t: Throwable =>
              if (writeFailed.compareAndSet(false, true)) {
                System.err.println(s"[FileSink] write to $path failed; suppressing further FileSink errors for this job.")
                t.printStackTrace(System.err)
              }
          }
        }
      }
    }

  /** Flush and close the underlying writer, then latch so any late `writeFileSync` short-circuits cleanly. Idempotent
    * and error-swallowing; a no-op when the writer never opened. `JobRunner` calls this from the job's terminal-status
    * finaliser before firing the completion promise, so a tailing client observes a fully-written, closed file.
    */
  def close(): UIO[Unit] =
    ZIO.succeed {
      fileLock.synchronized {
        writer.foreach(w =>
          try {
            w.flush()
            w.close()
          } catch {
            case _: Throwable => ()
          }
        )
        writeFailed.set(true)
      }
    }
}

object FileSink {

  /** Create a sink writing to `${logDir}/<jobId>.log`, opening the held-open [[java.io.BufferedWriter]] now. Caller is
    * responsible for ensuring `logDir` exists (typically done one-shot in `JobRunner.live`). An open failure is folded
    * into a file-disabled sink (logged once to stderr) rather than failing the effect, so a transient disk problem
    * never kills the job — the stdout tee still works.
    */
  def make(logDir: Path, jobId: String): UIO[FileSink] = {
    val path = logDir.resolve(s"$jobId.log")
    ZIO
      .attemptBlocking(
        Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
      )
      .fold(t => fileDisabled(path, t), w => new FileSink(path, Some(w)))
  }

  private def fileDisabled(path: Path, cause: Throwable): FileSink = {
    System.err.println(
      s"[FileSink] open $path failed; file logging disabled for this job. (${cause.getClass.getSimpleName}: ${cause.getMessage})"
    )
    // A missing logDir is the expected benign case — the message above is enough. Reserve the stack trace for the
    // genuinely unexpected (perms revoked, IO errors).
    cause match {
      case _: NoSuchFileException => ()
      case _                      => cause.printStackTrace(System.err)
    }
    new FileSink(path, None)
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
