package ccas.server.jobs

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.concurrent.atomic.AtomicBoolean

import zio.{UIO, ZIO}

import ccas.utils.JobLogSink

/** Per-job file-backed sink. Each `writeSync` tees the formatted log line to `System.out` (so the server console
  * keeps showing every job's output) and appends the same line, stripped of ANSI escapes, to `${logDir}/<jobId>.log`.
  *
  * Stripping ANSI from the file (but not the stdout tee) keeps `cat job.log` human-readable while preserving colour
  * for an interactive operator watching the server console.
  *
  * The file is opened, written, and closed on every `writeSync` call. Acceptable for expected log volumes (CCAS apps
  * emit at most a few hundred lines per job) but worth replacing with a per-job `BufferedWriter` held open for the
  * job's lifetime when the file-tail logs endpoint lands in #47 — that issue already needs lifecycle management for
  * the same writer.
  *
  * `logDir` is assumed to already exist; `JobRunner.live` creates it once at server startup.
  */
final class FileSink private (path: Path) extends JobLogSink {

  // Latches on the first write failure; subsequent failures are silently dropped so a permanently-unwritable file
  // (disk full, perms revoked mid-run) doesn't drown stderr with the same trace on every log line. Stdout tee keeps
  // working regardless — operators still see job output on the server console.
  private val writeFailed = new AtomicBoolean(false)

  override def writeSync(line: String): Unit = {
    System.out.println(line)
    try {
      val bytes = (JobLogSink.stripAnsi(line) + "\n").getBytes(StandardCharsets.UTF_8)
      Files.write(path, bytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND): Unit
    } catch {
      case t: Throwable =>
        if (writeFailed.compareAndSet(false, true)) {
          System.err.println(s"[FileSink] write to $path failed; suppressing further FileSink errors for this job.")
          t.printStackTrace(System.err)
        }
    }
  }
}

object FileSink {

  /** Create a sink writing to `${logDir}/<jobId>.log`. Caller is responsible for ensuring `logDir` exists (typically
    * done one-shot in `JobRunner.live`).
    */
  def make(logDir: Path, jobId: String): UIO[FileSink] =
    ZIO.succeed(new FileSink(logDir.resolve(s"$jobId.log")))
}
