package ccas.utils

import java.io.PrintStream
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import zio.{Cause, FiberId, FiberRef, FiberRefs, LogLevel, LogSpan, Ref, Runtime, Scope, Trace, UIO, URIO, URLayer, ZIO, ZLayer, ZLogger}

/** Manages progress bars rendered as a single line on stdout, overwritten in-place via `\r`.
  *
  * State (`bars`) is guarded by a Java intrinsic monitor (`lock`) so the synchronous `ZLogger` callback installed by
  * `ProgressDisplay.live` and the ZIO-effect entry points (`render`, `removeBar`, `finishAllSync`) all serialise their
  * stdout writes through the same mutex. JVM monitors are reentrant, so a logger callback fired from a thread that
  * already holds the lock proceeds without deadlock. Bar redraws go directly to the injected `out` stream (`System.out`
  * in production; a capture buffer under test), not through `zio.Console`, because `ZLogger.apply` is synchronous and
  * cannot run a ZIO effect. The injected `err` stream receives the last-resort stack trace if a log message thunk
  * throws inside the `ZLogger` callback.
  *
  * `logAboveBarsSync` routes the active `JobLogSink`'s file write (`writeFileSync`) *outside* the lock — a per-job
  * `FileSink` serialises its own `BufferedWriter`, so file IO never contends here (#53). Only the terminal-visible
  * sequence stays inside the lock: `clear → writeConsoleSync (stdout tee) → redraw`, kept atomic across fibers so a bar
  * redraw can't tear across a log line. The default `StdoutSink`'s `writeConsoleSync` is a single `println`, so the
  * critical section stays short.
  *
  * When `enabled` is `false` the bar list is still tracked (so `addBarScoped` finalisers behave consistently across
  * modes) but the bar-redraw side-effects are suppressed. `logAboveBarsSync` still routes its line through the active
  * `JobLogSink` (the default `StdoutSink` writes the log line to `out`); only the bar dance is skipped — suitable for
  * server / non-interactive mode.
  */
final class ProgressDisplay private[utils] (
  private val enabled: Boolean,
  private val out: PrintStream,
  private val err: PrintStream
) {

  private val lock                                  = new Object
  private var bars: List[ProgressDisplay.BarState]  = Nil
  private val idGen                                 = new AtomicInteger(0)

  // ---------------------------------------------------------------------------
  // Bar lifecycle
  // ---------------------------------------------------------------------------

  /** Create a new progress bar appended to the bottom of the display. */
  def addBar: UIO[ProgressBar] = ZIO.succeed {
    val id = idGen.getAndIncrement()
    lock.synchronized { bars = bars :+ ProgressDisplay.BarState(id, 0, "") }
    new ProgressBar(id, this)
  }

  /** Create a scoped progress bar — automatically removed when the scope closes. */
  def addBarScoped: ZIO[Scope, Nothing, ProgressBar] =
    ZIO.acquireRelease(addBar)(_.finish)

  // ---------------------------------------------------------------------------
  // Rendering (called by ProgressBar)
  //
  // The `enabled` check guards stdout writes only — the `bars` list is mutated unconditionally so
  // `addBarScoped` finalisers stay consistent (otherwise disabled-mode bars would leak in `bars`).
  // ---------------------------------------------------------------------------

  /** Update one bar's output and re-render the whole display. */
  private[utils] def render(barId: Int, output: String, lineCount: Int): UIO[Unit] = ZIO.succeed {
    lock.synchronized {
      bars = bars.map(b =>
        if (b.id == barId) ProgressDisplay.BarState(barId, lineCount, output) else b
      )
      drawAllSync()
    }
  }

  /** Remove a bar from the display. */
  private[utils] def removeBar(barId: Int): UIO[Unit] = ZIO.succeed {
    lock.synchronized {
      clearLineSync()
      bars = bars.filterNot(_.id == barId)
      drawAllSync()
    }
  }

  /** Finish all bars — erase without redraw. Called from the live layer's release block. */
  private[utils] def finishAllSync(): Unit =
    lock.synchronized {
      clearLineSync()
      bars = Nil
    }

  /** VisibleForTesting — number of bars currently tracked. Disabled-mode tests use this to verify the list doesn't
    * leak. Not intended for production callers; production code should treat the bar collection as opaque.
    */
  private[utils] def barCount: Int = lock.synchronized(bars.size)

  // ---------------------------------------------------------------------------
  // Logging — called synchronously by the custom ZLogger
  // ---------------------------------------------------------------------------

  /** Print a log message above the progress bars without disrupting them.
    *
    * The durable file write (`sink.writeFileSync`) runs first and *outside* `lock` — it touches an independent per-job
    * file, never the terminal, so it needs no serialisation with the bar dance and a slow disk can't stall redraws.
    * File-first also means the record survives even if the locked terminal sequence throws. The terminal-visible
    * sequence — `clear → writeConsoleSync (stdout tee) → redraw` — runs under `lock` so the bar list stays consistent
    * with the printed state.
    */
  private[utils] def logAboveBarsSync(sink: JobLogSink, msg: String): Unit = {
    sink.writeFileSync(msg)
    lock.synchronized {
      clearLineSync()
      sink.writeConsoleSync(msg)
      drawAllSync()
    }
  }

  // ---------------------------------------------------------------------------
  // ZLogger — installed by `live` via `Runtime.removeDefaultLoggers ++ ZIO.withLoggerScoped`. Companion-visible
  // (`private[ProgressDisplay]`) so the layer's `withLoggerScoped` call can reach it without exposing the logger
  // to the rest of the package — production callers should only install it via `live`.
  //
  // Note: `Instant.now()` reads the wall clock, not `Clock.instant`, so timestamps in tests under `TestClock` won't
  // advance with `TestClock.adjust`. Acceptable for an interactive CLI formatter.
  // ---------------------------------------------------------------------------

  private[ProgressDisplay] val asZLogger: ZLogger[String, Any] = new ZLogger[String, Any] {
    override def apply(
      trace: Trace,
      fiberId: FiberId,
      level: LogLevel,
      message: () => String,
      cause: Cause[Any],
      context: FiberRefs,
      spans: List[LogSpan],
      annotations: Map[String, String]
    ): Any = {
      val threshold = context.getOrDefault(FiberRef.currentLogLevel)
      if (level >= threshold) {
        try {
          val sink = context.getOrDefault(JobLogSink.currentSink)
          logAboveBarsSync(sink, ProgressDisplay.format(level, message(), Instant.now(), cause, spans, annotations))
        } catch { case t: Throwable => t.printStackTrace(err) }
      }
    }
  }

  /** Run `zio` (and every fiber it forks) with the runtime's default loggers removed and this display's `asZLogger`
    * installed — the same `{asZLogger}` end-state `live` establishes (idempotent when already in it, e.g. the
    * scheduler path), but applied inline to one effect rather than a layer scope. `JobRunner` wraps each
    * job with this so a job submitted from a fiber that never entered the `live` logger scope — e.g. a zio-http
    * request handler, which carries only the runtime's default loggers — still routes its `ZIO.log*` through
    * `asZLogger` into the per-job `JobLogSink` file ([[ccas.server.jobs.FileSink]]). Without it, `.forkIn` inheritance
    * leaves such jobs logging via the default logger and the per-job file stays empty (#132). Removing the defaults
    * (not merely adding `asZLogger`) keeps those jobs from also emitting a second, structured copy to the server
    * console, so HTTP-submitted jobs log identically to scheduler-submitted ones.
    */
  def installLogger[R, E, A](zio: ZIO[R, E, A]): ZIO[R, E, A] =
    ZIO.scoped[R] {
      for {
        _ <- Runtime.removeDefaultLoggers.build
        _ <- ZIO.withLoggerScoped(asZLogger)
        a <- zio
      } yield a
    }

  // ---------------------------------------------------------------------------
  // Internal rendering — single-line, \r-based (no cursor-up needed). The `enabled` check lives in these
  // helpers so callers don't repeat it; both also short-circuit on empty bar lists. Always called from inside
  // a `lock.synchronized` block so the read of `bars` is consistent.
  // ---------------------------------------------------------------------------

  private def clearLineSync(): Unit =
    if (enabled && bars.exists(_.lineCount > 0)) out.print("\r\u001b[K")

  private def drawAllSync(): Unit =
    if (enabled) {
      val parts = bars.filter(_.lineCount > 0).map(_.lastOutput.trim)
      if (parts.nonEmpty) out.print("\r" + parts.mkString("  ") + "\u001b[K")
    }
}

object ProgressDisplay {

  private case class BarState(id: Int, lineCount: Int, lastOutput: String)

  /** Synchronous factory — no IO, just allocates the lock + state. Use `live` instead in production code; `make` is
    * intended for tests that need a noop / quiet display without spinning up the layer machinery.
    *
    * @param enabled
    *   `true` for stdout rendering. `false` suppresses every stdout side-effect — bar lifecycle (`addBar`/`removeBar`)
    *   still tracks state correctly so that `addBarScoped` finalisers behave the same in both modes.
    */
  def make(enabled: Boolean): ProgressDisplay = makeWith(enabled, System.out, System.err)

  /** Test-only factory that injects the bar-redraw (`out`) and defect (`err`) streams so suites can capture output
    * without mutating process-global `System.out` / `System.err`. Production code uses [[make]] / [[live]].
    */
  private[utils] def makeWith(enabled: Boolean, out: PrintStream, err: PrintStream): ProgressDisplay =
    new ProgressDisplay(enabled, out, err)

  /** Provides a `ProgressDisplay` service AND replaces ZIO's default console logger with the progress-display
    * `ZLogger` for the layer's scope. ZIO's default console logger is therefore inactive while this layer is alive —
    * no double output, no need for a separate `bootstrap = Runtime.removeDefaultLoggers` override.
    *
    * On scope close, the default-logger removal and the custom-logger registration are both reverted (both helpers
    * are scope-bound under the hood) and `acquireRelease` runs `finishAllSync()` to wipe any remaining bars.
    *
    * @param showProgress
    *   `true` for interactive CLI use (renders bars + log lines on stdout). `false` for server / non-interactive mode
    *   (suppresses bar rendering; `ZIO.log*` still fires through the custom formatter, just without bar dance).
    */
  def live(showProgress: Boolean): URLayer[Scope, ProgressDisplay] =
    liveWith(showProgress, System.out, System.err)

  /** Test-only variant of [[live]] that injects the bar-redraw (`out`) and defect (`err`) streams, so suites that
    * exercise the installed `ZLogger` (e.g. a throwing message thunk routed to `err`) can capture without swapping
    * process-global streams.
    */
  private[utils] def liveWith(showProgress: Boolean, out: PrintStream, err: PrintStream): URLayer[Scope, ProgressDisplay] =
    Runtime.removeDefaultLoggers ++ ZLayer.scoped {
      for {
        d <- ZIO.acquireRelease(
          ZIO.succeed(makeWith(showProgress, out, err))
        )(d => ZIO.succeed(d.finishAllSync()))
        _ <- ZIO.withLoggerScoped(d.asZLogger)
      } yield d
    }

  // ---------------------------------------------------------------------------
  // Companion accessors
  // ---------------------------------------------------------------------------

  def progressBar: URIO[ProgressDisplay & Scope, ProgressBar] =
    ZIO.serviceWithZIO[ProgressDisplay](_.addBarScoped)

  /** Run `action` on each item in parallel, showing a progress bar. The `label` function receives the current count
    * and total to produce the bar text (e.g. `(n, total) => s"  Processing: $n/$total"`).
    */
  def foreachParProgress[R, E, A](items: Iterable[A], parallelism: Int)(label: (Int, Int) => String)(
    action: A => ZIO[R, E, Any]
  ): ZIO[R & ProgressDisplay & Scope, E, Unit] = {
    val total = items.size
    for {
      bar     <- progressBar
      counter <- Ref.make(0)
      _ <- ZIO.foreachParDiscard(items) { item =>
        action(item) *> counter.updateAndGet(_ + 1).flatMap(n => bar.print(n, total, label(n, total)))
      }.withParallelism(parallelism)
    } yield ()
  }

  // ---------------------------------------------------------------------------
  // Log source attribution — a reserved log-annotation key rendered by `format` as a bracketed prefix
  // (`[INFO HH:mm:ss] [<src>] msg`) so interleaved console output shows which app/job/component emitted
  // each line. `sourced` sets it via `ZIO.logAnnotate` (core ZIO, inherited by forked children), so all
  // of an effect's `ZIO.log*` — and any fibers it forks — carry the tag. Global logs (rate-limit,
  // scheduler) set their own `src` to stay attributable even when running on a job's fiber.
  // ---------------------------------------------------------------------------

  /** Reserved log-annotation key: `format` renders its value as the bracketed source prefix and drops it from the
    * trailing `k=v` set, so no other call site should annotate with this key. Set it via [[sourced]].
    */
  val LogSource: String = "src"

  def sourced[R, E, A](src: String)(zio: ZIO[R, E, A]): ZIO[R, E, A] =
    ZIO.logAnnotate(LogSource, src)(zio)

  // Matches the leading `[LEVEL HH:mm:ss] ` bracket (group 1) followed by the `[<src>] ` prefix that `format`
  // inserts. Bracket contents never contain `]` (level labels, times, and source tags — kind/slug, `#id`,
  // `rate-limit`, `scheduler` — are all `]`-free), so `[^\]]+` is unambiguous.
  private val SourceTagPrefix: Pattern = Pattern.compile("^(\\[[^\\]]+\\] )\\[[^\\]]+\\] ")

  /** Strip the bracketed source prefix (`[<src>] `) that `format` inserts after the `[LEVEL HH:mm:ss]` bracket, so a
    * per-job log file — and the CLI that streams it — isn't cluttered by a tag that only earns its keep on the
    * multiplexed server console. Scoped to the per-job file path ([[ccas.server.jobs.FileSink]]), where every line
    * carries the tag; a line whose message merely starts with a second `[...]` token is left intact because only the
    * source tag directly abutting the level bracket is removed.
    */
  def stripSourceTag(line: String): String = SourceTagPrefix.matcher(line).replaceFirst("$1")

  // ---------------------------------------------------------------------------
  // Private formatter — ANSI colours, [LEVEL HH:mm:ss] msg, optional cause / spans / annotations
  // ---------------------------------------------------------------------------

  private val Reset  = "\u001b[0m"
  private val Cyan   = "\u001b[36m"
  private val Green  = "\u001b[32m"
  private val Yellow = "\u001b[33m"
  private val Red    = "\u001b[31m"

  private def colorFor(level: LogLevel): String = level match {
    case LogLevel.Debug   => Cyan
    case LogLevel.Info    => Green
    case LogLevel.Warning => Yellow
    case LogLevel.Error   => Red
    case LogLevel.Fatal   => Red
    case _                => Reset
  }

  private val TimeFormat = DateTimeFormatter.ofPattern("HH:mm:ss")
  private val Zone       = ZoneId.systemDefault()

  /** Upper bound for `cause.prettyPrint` — a multi-MB Cause tree would otherwise corrupt bar redraw and flood
    * stdout. Sized for interactive CLI; full causes still reach any structured sink that consumes `Cause` directly
    * (none today, but worth preserving for future log aggregators).
    */
  private val MaxCauseChars = 2048

  private def formatTime(when: Instant): String =
    when.atZone(Zone).toLocalTime.format(TimeFormat)

  private def format(
    level: LogLevel,
    msg: String,
    when: Instant,
    cause: Cause[Any],
    spans: List[LogSpan],
    annotations: Map[String, String]
  ): String = {
    val srcTag = annotations.get(LogSource).fold("")(s => s" [$s]")
    val base   = s"${colorFor(level)}[${level.label} ${formatTime(when)}]$Reset$srcTag $msg"
    val sb     = new StringBuilder(base)
    if (!cause.isEmpty) {
      sb.append('\n')
      val rendered = cause.prettyPrint
      sb.append(rendered.take(MaxCauseChars))
      if (rendered.length > MaxCauseChars) sb.append("\n... [cause truncated]")
    }
    if (spans.nonEmpty) {
      val now = when.toEpochMilli
      sb.append("  spans=")
      sb.append(spans.map(s => s"${s.label}=${now - s.startTime}ms").mkString(" "))
    }
    val rest = annotations - LogSource // `src` is rendered as the prefix above; don't duplicate it here
    if (rest.nonEmpty) {
      sb.append("  ")
      sb.append(rest.map { case (k, v) => s"$k=$v" }.mkString(" "))
    }
    sb.toString
  }
}
