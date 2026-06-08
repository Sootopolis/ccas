package ccas.server.jobs

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import zio.stream.ZStream
import zio.{Chunk, Duration, Promise, Ref, UIO, ZIO}

import ccas.api.misc.subtypes.JobRunId

/** Streams a previously-submitted job's log file `${logDir}/<jobId>.log` (written by [[FileSink]]) as a sequence of
  * lines, emitting them as they're appended and closing when the job is terminal and the tail has reached EOF. The
  * abstraction boundary for callers is [[JobRunner.logStream]], which returns the resulting `ZStream`; this is its sole
  * backing implementation. An SSE / WebSocket variant would just reframe the same line stream at the route, so no
  * transport interface is introduced here.
  *
  * The "is this job still running?" signal comes from `completions`, the in-process registry of completion promises
  * maintained by [[JobRunner]]. A job present in the map is live → tail until its promise fires; a job absent from the
  * map (already terminal, or — on a recycled host — never ours) is treated as drained, so the stream reads whatever is
  * on disk and ends. A missing log file (job logged nothing yet, or the disk was wiped) reads as empty rather than
  * failing, so the worst case is an immediate empty close, never a hang.
  *
  * The tailer re-reads the whole file each tick. CCAS jobs emit at most a few hundred lines, so this is cheap and
  * sidesteps byte-offset / partial-line bookkeeping. An offset-incremental tail (alongside the held-open `BufferedWriter`
  * in #53) is the upgrade path when log volumes grow.
  */
final class FileTail(
  logDir: Path,
  completions: Ref[Map[JobRunId, Promise[Nothing, Unit]]],
  pollInterval: Duration
) {

  def subscribe(jobId: JobRunId): ZStream[Any, Throwable, String] =
    ZStream.unwrap {
      completions.get.map { running =>
        val terminal: UIO[Boolean] = running.get(jobId).fold[UIO[Boolean]](ZIO.succeed(true))(_.isDone)
        tail(logDir.resolve(s"${JobRunId.unwrap(jobId)}.log"), terminal)
      }
    }

  // `emitted` = number of complete lines already produced. Each step emits new lines if any; otherwise, if the job is
  // terminal, it does ONE final re-read (to catch last-moment lines written between the read above and the terminal
  // check) and then ends. The promise fires only after `FileSink`'s final synchronous writes, so `done == true`
  // guarantees the file is fully flushed. While the job is still running, no second read happens — the step just sleeps
  // and retries.
  private def tail(path: Path, terminal: UIO[Boolean]): ZStream[Any, Throwable, String] =
    ZStream.unfoldChunkZIO(0) { emitted =>
      def emitFrom(lines: Vector[String]): Option[(Chunk[String], Int)] =
        Some(Chunk.fromIterable(lines.drop(emitted)) -> lines.size)

      def step: ZIO[Any, Throwable, Option[(Chunk[String], Int)]] =
        readCompleteLines(path).flatMap { lines =>
          if (lines.size > emitted) { ZIO.succeed(emitFrom(lines)) }
          else {
            terminal.flatMap { done =>
              if (done) { readCompleteLines(path).map(settled => if (settled.size > emitted) { emitFrom(settled) } else { None }) }
              else { ZIO.sleep(pollInterval) *> step }
            }
          }
        }
      step
    }

  // Complete lines = everything up to the last newline. A non-newline-terminated tail is a partial line caught
  // mid-write (`FileSink` always appends "<line>\n"); drop it this tick and pick it up once the newline lands. A
  // missing file reads as empty.
  private def readCompleteLines(path: Path): ZIO[Any, Throwable, Vector[String]] =
    ZIO.attemptBlocking {
      if (!Files.exists(path)) { Vector.empty }
      else {
        val content = Files.readString(path, StandardCharsets.UTF_8)
        content.split("\n", -1).dropRight(1).toVector
      }
    }
}
