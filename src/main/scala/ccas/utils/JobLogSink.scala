package ccas.utils

import java.util.regex.Pattern

import zio.{FiberRef, UIO, Unsafe, ZIO}

/** Pluggable target for log lines emitted by [[ProgressDisplay]]'s `ZLogger`.
  *
  * The `ZLogger` callback is synchronous, so the primary contract is `writeSync`. The ZIO-friendly `write` wrapper is
  * provided for callers outside the logger that want to compose log emission as an effect.
  *
  * The currently-active sink is carried in [[JobLogSink.currentSink]], a `FiberRef` whose value the logger reads from
  * its `context` map. `JobRunner.submit` overrides this `FiberRef` for the duration of each job (via
  * `currentSink.locally(FileSink(...))`) so per-job output lands in `${job-logs.directory}/<jobId>.log` while keeping a
  * stdout tee. All other code paths (CLI apps, tests, the long-lived server console) inherit the default
  * [[StdoutSink]] and preserve current behaviour.
  */
trait JobLogSink {
  def writeSync(line: String): Unit
  final def write(line: String): UIO[Unit] = ZIO.succeed(writeSync(line))
}

object JobLogSink {

  /** Default sink — writes to `System.out`. Preserves prior behaviour for non-job code paths. Declared before
    * `currentSink` so the FiberRef's initial value resolves directly to the singleton's identity.
    */
  object StdoutSink extends JobLogSink {
    override def writeSync(line: String): Unit = System.out.println(line)
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
