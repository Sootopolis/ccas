package ccas.utils

import java.util.regex.Pattern

import zio.{FiberRef, UIO, Unsafe, ZIO}

/** Pluggable target for log lines emitted by [[ProgressDisplay]]'s `ZLogger`.
  *
  * The `ZLogger` callback is synchronous, so the contract is two sync writes: `writeConsoleSync` (the terminal tee,
  * which [[ProgressDisplay]] runs under its render lock, interleaved with the bar clear/redraw) and `writeFileSync` (a
  * sink's durable per-job target, which [[ProgressDisplay]] runs *outside* the lock so file IO never stalls bar
  * redraws). The ZIO-friendly `write` wrapper composes both for callers outside the logger.
  *
  * The currently-active sink is carried in [[JobLogSink.currentSink]], a `FiberRef` whose value the logger reads from
  * its `context` map. `JobRunner.submit` overrides this `FiberRef` for the duration of each job (via
  * `currentSink.locally(FileSink(...))`) so per-job output lands in `${job-logs.directory}/<jobId>.log` while keeping a
  * stdout tee. All other code paths (CLI apps, tests, the long-lived server console) inherit the default
  * [[StdoutSink]] (console-only, no file target) and preserve current behaviour.
  */
trait JobLogSink {

  /** Write `line` to the terminal (the stdout tee). [[ProgressDisplay.logAboveBarsSync]] invokes this *inside* its
    * render lock, interleaved with the bar clear/redraw, so it must stay cheap — a single `println`.
    */
  def writeConsoleSync(line: String): Unit

  /** Write `line` to the sink's durable per-job target (a file, for [[ccas.server.jobs.FileSink]]).
    * [[ProgressDisplay.logAboveBarsSync]] invokes this *outside* the render lock, so a slow disk never stalls bar
    * redraws. Defaults to a no-op: only file-backed sinks override it; [[StdoutSink]] and the non-job code paths have
    * no second destination.
    */
  def writeFileSync(line: String): Unit = ()

  /** ZIO-friendly wrapper for callers outside the synchronous logger. Writes the durable file target first (so the
    * record survives even if the console write throws), then the console tee.
    *
    * Note: this does NOT acquire [[ProgressDisplay]]'s render lock, so its `writeConsoleSync` can interleave with an
    * active bar redraw and tear terminal output. Safe only for off-logger / test compose paths that have no live bars;
    * the production logging path goes through [[ProgressDisplay.logAboveBarsSync]], which holds the lock for the tee.
    */
  final def write(line: String): UIO[Unit] = ZIO.succeed { writeFileSync(line); writeConsoleSync(line) }
}

object JobLogSink {

  /** Default sink — writes to `System.out`, console-only (no file target). Preserves prior behaviour for non-job code
    * paths. Declared before `currentSink` so the FiberRef's initial value resolves directly to the singleton's
    * identity.
    */
  object StdoutSink extends JobLogSink {
    override def writeConsoleSync(line: String): Unit = System.out.println(line)
  }

  /** Per-fiber sink override. `ProgressDisplay.asZLogger` resolves the active sink via
    * `context.getOrDefault(JobLogSink.currentSink)` so the lookup is sync and inherits across forked children.
    */
  val currentSink: FiberRef[JobLogSink] =
    Unsafe.unsafe(implicit u => FiberRef.unsafe.make[JobLogSink](StdoutSink))

  // CSI escape: ESC `[` <params> <final letter>. Built programmatically (Pattern.quote on a char-from-int) to avoid
  // embedding a raw ESC byte in the source file (see feedback_scala_esc_in_strings).
  private val Esc: String     = Character.toString(0x1B)
  private val AnsiCsi: Pattern = Pattern.compile(Pattern.quote(Esc) + "\\[[0-9;]*[a-zA-Z]")

  /** Remove ANSI CSI escapes from `s`. Used by file-backed sinks so `cat job.log` is readable. */
  def stripAnsi(s: String): String = AnsiCsi.matcher(s).replaceAll("")
}
