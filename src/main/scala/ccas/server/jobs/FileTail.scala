package ccas.server.jobs

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.Arrays

import zio.stream.ZStream
import zio.{Chunk, Duration, Promise, Ref, UIO, ZIO}

import ccas.api.misc.subtypes.JobRunId

/** Streams a submitted job's log file `${logDir}/<jobId>.log` (written by [[FileSink]]) as lines, emitting them as
  * they are appended and closing once the job is terminal and the tail has reached EOF. Callers go through
  * [[JobRunner.logStream]]; this is its sole backing implementation.
  *
  * "Is this job still running?" comes from `completions`, [[JobRunner]]'s in-process registry of completion
  * promises: present means live, so tail until the promise fires; absent means already terminal — or never ours, on
  * a recycled host — so read what is on disk and end. A missing log file reads as empty rather than failing, so the
  * worst case is an immediate empty close, never a hang.
  *
  * The tailer is offset-incremental, reading only bytes appended since the last tick and carrying a partial line in
  * a byte buffer. Splitting on the `'\n'` byte is UTF-8-safe — `0x0A` never appears inside a multibyte sequence —
  * so a char torn at a read boundary always sits after the last newline and stays buffered. It reads through its own
  * [[FileChannel]], so [[FileSink]]'s held-open writer never blocks it.
  */
final class FileTail(
  logDir: Path,
  completions: Ref[Map[JobRunId, Promise[Nothing, Unit]]],
  pollInterval: Duration
) {

  // `offset` = total bytes consumed from the file so far. `partial` = raw bytes after the last '\n' seen, carried
  // across ticks until their line completes. `doneAndDrained` = set once the terminal-final read has emitted, so the
  // next iteration ends instead of re-reading (avoids a redundant read / empty-chunk stall).
  private final case class TailState(offset: Long, partial: Chunk[Byte], doneAndDrained: Boolean)
  private object TailState {
    val initial: TailState = TailState(0L, Chunk.empty, doneAndDrained = false)
  }

  def subscribe(jobId: JobRunId): ZStream[Any, Throwable, String] =
    ZStream.unwrap {
      completions.get.map { running =>
        val terminal: UIO[Boolean] = running.get(jobId).fold[UIO[Boolean]](ZIO.succeed(true))(_.isDone)
        tail(logDir.resolve(s"${JobRunId.unwrap(jobId)}.log"), terminal)
      }
    }

  // Each step reads only the bytes appended since the tracked offset and emits any newly-completed lines. If none
  // appeared and the job is terminal, it does ONE final re-read (to catch last-moment lines written between the read
  // above and the terminal check) and then ends. The promise fires only after `FileSink.close()` flushes and closes
  // the held-open writer, so `done == true` guarantees the file is fully flushed. While the job is still running, an
  // idle tick just sleeps and retries — no empty chunk is ever emitted.
  private def tail(path: Path, terminal: UIO[Boolean]): ZStream[Any, Throwable, String] =
    ZStream.unfoldChunkZIO(TailState.initial) { state =>
      def step(s: TailState): ZIO[Any, Throwable, Option[(Chunk[String], TailState)]] =
        if (s.doneAndDrained) { ZIO.succeed(None) }
        else {
          readAppended(path, s).flatMap { case (lines, next) =>
            if (lines.nonEmpty) { ZIO.succeed(Some(Chunk.fromIterable(lines) -> next)) }
            else {
              terminal.flatMap { done =>
                if (done) {
                  // The file is flushed + closed, so this final read reaches true EOF.
                  readAppended(path, next).map { case (settled, finalState) =>
                    if (settled.nonEmpty) { Some(Chunk.fromIterable(settled) -> finalState.copy(doneAndDrained = true)) }
                    else { None }
                  }
                }
                else { ZIO.sleep(pollInterval) *> step(next) }
              }
            }
          }
        }
      step(state)
    }

  // Reads the bytes appended since `state.offset` and splits out newly-completed lines. Complete lines = everything up
  // to the last newline; a non-newline-terminated tail is a partial line caught mid-write (`FileSink` always appends
  // "<line>\n") and is carried in `partial` until its newline lands. Offset advances by the bytes *actually* read, so a
  // short read (writer mid-flush) is never skipped. A missing file reads as empty.
  private def readAppended(path: Path, state: TailState): ZIO[Any, Throwable, (Vector[String], TailState)] =
    ZIO.attemptBlocking {
      if (!Files.exists(path)) { (Vector.empty, state) }
      else {
        val channel = FileChannel.open(path, StandardOpenOption.READ)
        try {
          val size = channel.size()
          if (size <= state.offset) { (Vector.empty, state) } // no new bytes (or impossible truncation — ignore)
          else {
            val toRead = (size - state.offset).toInt // CCAS logs are small; fits an Int.
            val buf    = ByteBuffer.allocate(toRead)
            var n      = 0
            while (buf.hasRemaining && n != -1) {
              n = channel.read(buf, state.offset + buf.position()) // absolute-positioned; channel position untouched
            }
            val appended  = Arrays.copyOf(buf.array(), buf.position()) // only the bytes actually read
            val newOffset = state.offset + appended.length
            val combined  = (state.partial ++ Chunk.fromArray(appended)).toArray
            val lastNl    = combined.lastIndexOf('\n'.toByte)
            if (lastNl < 0) { (Vector.empty, TailState(newOffset, Chunk.fromArray(combined), state.doneAndDrained)) }
            else {
              val text      = new String(combined, 0, lastNl + 1, StandardCharsets.UTF_8)
              val lines     = text.split("\n", -1).dropRight(1).toVector
              val remainder = Chunk.fromArray(Arrays.copyOfRange(combined, lastNl + 1, combined.length))
              (lines, TailState(newOffset, remainder, state.doneAndDrained))
            }
          }
        }
        finally { channel.close() }
      }
    }
}
