package ccas.utils

import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId}
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern

import io.netty.handler.timeout.ReadTimeoutException
import zio.stream.SubscriptionRef
import zio.{Cause, FiberId, FiberRef, FiberRefs, LogLevel, LogSpan, Ref, Runtime, Scope, Trace, UIO, Unsafe, URIO, URLayer, ZIO, ZLayer, ZLogger}

/** Manages progress bars rendered one-per-line on stdout; a redraw moves the cursor up over the block and repaints.
  *
  * `bars` is guarded by a Java intrinsic monitor, so the synchronous `ZLogger` callback installed by
  * [[ProgressDisplay.live]] and the ZIO entry points all serialise their stdout writes through one mutex. JVM
  * monitors are reentrant, so a callback fired from a thread already holding it proceeds without deadlock. Redraws
  * go straight to the injected `out` rather than through `zio.Console`, because `ZLogger.apply` is synchronous and
  * cannot run an effect; `err` takes the last-resort stack trace if a message thunk throws inside the callback.
  *
  * `logAboveBarsSync` routes the active `JobLogSink`'s file write OUTSIDE the lock — a per-job `FileSink` serialises
  * its own writer, so file IO never contends here (#53). Only `clear -> writeConsoleSync -> redraw` stays inside,
  * kept atomic so a redraw can't tear across a log line.
  *
  * With `enabled = false` the bar list is still tracked, so `addBarScoped` finalisers behave the same across modes,
  * but the redraw side-effects are suppressed — server / non-interactive mode.
  */
final class ProgressDisplay private[utils] (
  private val enabled: Boolean,
  private val out: PrintStream,
  private val err: PrintStream,
  private val globalChannel: Option[ProgressDisplay.BarChannel]
) {

  private val lock                                  = new Object
  private var bars: List[ProgressDisplay.BarState]  = Nil
  private val idGen                                 = new AtomicInteger(0)
  // Physical terminal lines the last draw occupied — so a redraw moves the cursor back to the top of the bar block and
  // repaints it. Only touched inside `lock.synchronized`.
  private var lastDrawnLines: Int = 0

  /** Current snapshot of the process-wide global bar channel — the bars created outside any job's
    * [[ProgressDisplay.currentChannel]] scope (e.g. the shared `ChessComClient` API gauge). `progressStream` samples this
    * (merged with a job's own channel) to build each `/progress` frame so the gauge shows alongside a job's app bars.
    * Read-only (`.get`), so no caller can mutate the shared gauge state; empty when the display has no global channel
    * (the sync test factory / server console).
    */
  private[ccas] def globalBarSnapshot: UIO[Map[Int, BarSnapshot]] =
    globalChannel.fold(ZIO.succeed(Map.empty[Int, BarSnapshot]))(_.get)

  // ---------------------------------------------------------------------------
  // Bar lifecycle
  // ---------------------------------------------------------------------------

  /** Create a new progress bar appended to the bottom of the display.
    *
    * The bar's publish target is captured **now**, from the creating fiber's [[ProgressDisplay.currentChannel]] (a job's
    * channel when created inside a job effect; otherwise the shared [[globalChannel]]). Capturing at creation — not at
    * each `render` — keeps a bar's snapshots flowing to one channel for its whole life, even though later updates may run
    * on other fibers (e.g. the API gauge is created outside any job but updated from job fibers).
    */
  def addBar: UIO[ProgressBar] =
    ProgressDisplay.currentChannel.get.map { jobChannel =>
      val id     = idGen.getAndIncrement()
      val target = jobChannel.orElse(globalChannel)
      lock.synchronized { bars = bars :+ ProgressDisplay.BarState(id, 0, "", 0, 0, "", target) }
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

  /** Update one bar's output and re-render the whole display, then republish the bar's channel from `bars`.
    *
    * `output` is the already-rendered line (block bar + percentage) used for the local terminal draw; the raw
    * `current` / `total` / `text` are stored so [[publishChannel]] can emit an unrendered [[BarSnapshot]] (the following
    * CLI re-renders it at its own width). Publishing runs outside the terminal lock (a `SubscriptionRef` write is an
    * effect) but re-reads `bars` under the lock at publish time, so it can't diverge from local state.
    */
  private[utils] def render(barId: Int, output: String, lineCount: Int, current: Int, total: Int, text: String): UIO[Unit] =
    ZIO.succeed {
      lock.synchronized {
        val target = bars.find(_.id == barId).flatMap(_.target)
        bars = bars.map(b =>
          if (b.id == barId) b.copy(lineCount = lineCount, lastOutput = output, current = current, total = total, text = text)
          else b
        )
        clearBlockSync()
        drawAllSync()
        target
      }
    }.flatMap(publishChannel)

  /** Remove a bar from the display, then republish its channel from `bars` (the bar is now gone, so it drops out). */
  private[utils] def removeBar(barId: Int): UIO[Unit] =
    ZIO.succeed {
      lock.synchronized {
        val target = bars.find(_.id == barId).flatMap(_.target)
        clearBlockSync()
        bars = bars.filterNot(_.id == barId)
        drawAllSync()
        target
      }
    }.flatMap(publishChannel)

  // Republish `target`'s snapshot by re-deriving it from `bars` (the lock-guarded source of truth) at publish time and
  // `set`-ing the whole map — NOT an incremental delta. So even when concurrent render/removeBar publishes for the same
  // channel reorder relative to their locked mutations, the last publish to run re-reads the final `bars` and the
  // channel converges to it (no sticky ghost bar). No-op when the bar has no channel (sync `make` / server console).
  private def publishChannel(target: Option[ProgressDisplay.BarChannel]): UIO[Unit] =
    ZIO.foreachDiscard(target) { channel =>
      ZIO.succeed {
        lock.synchronized {
          bars.collect {
            case b if b.lineCount > 0 && b.target.exists(_ eq channel) =>
              b.id -> BarSnapshot(b.id, b.current, b.total, b.text)
          }.toMap
        }
      }.flatMap(channel.set)
    }

  /** Finish all bars — erase without redraw. Called from the live layer's release block. */
  private[utils] def finishAllSync(): Unit =
    lock.synchronized {
      clearBlockSync()
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
      clearBlockSync()
      sink.writeConsoleSync(msg)
      drawAllSync()
    }
  }

  /** Print a log line above the bars on this display's own `out` stream — the CLI-follow analog of [[logAboveBarsSync]]
    * (which routes through a [[JobLogSink]]). The CLI `JobFollower` uses this to interleave each streamed job-log line
    * above the bars it renders from `/api/jobs/{id}/progress`. Clears the bar block, prints the line, redraws — all under
    * the render lock, so it can't tear against a concurrent bar [[render]] on the same display. When `enabled` is false
    * (bars suppressed / non-TTY), it degrades to a plain `out.println(line)`.
    */
  private[ccas] def logLineAboveBars(line: String): UIO[Unit] =
    ZIO.succeed {
      lock.synchronized {
        clearBlockSync()
        out.println(line)
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
          // Force the message thunk once, inside the try so a throwing thunk stays caught (see `testLoggerSwallowsThrow`),
          // and reuse it for both the read-idle-reap filter and the formatter.
          val msg = message()
          if (!ProgressDisplay.isBenignReadIdleReap(level, msg, cause)) {
            val sink = context.getOrDefault(JobLogSink.currentSink)
            logAboveBarsSync(sink, ProgressDisplay.format(level, msg, Instant.now(), cause, spans, annotations))
          }
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

  // Internal rendering, multi-line: each bar draws on its own line; a redraw moves the cursor up over the block
  // (CSI n A), clears to end of screen (CSI J), then repaints. The `enabled` check lives in these helpers so callers
  // don't repeat it, and they are always called inside `lock.synchronized` so `bars` and `lastDrawnLines` stay
  // consistent. ANSI cursor control was already assumed, so this needs no capability a non-TTY display hasn't
  // already gated out. `Esc` is `27.toChar` rather than a unicode escape only to keep the interpolations below
  // free of escape noise — both avoid a raw control byte.

  private val Esc: String = 27.toChar.toString

  // The visible bars as physical lines: each bar's output split on any embedded newlines, trimmed, and truncated to the
  // detected terminal width so a too-wide bar can't wrap onto a second row and desync the cursor-up count.
  private def barLinesSync(): List[String] =
    bars.filter(_.lineCount > 0).flatMap(_.lastOutput.split("\n", -1)).map(l => truncateToWidth(l.trim))

  // Move the cursor to column 0 of the FIRST bar line — up `lastDrawnLines - 1` rows — when a block is currently drawn.
  private def moveToBlockTopSync(): Unit =
    if (lastDrawnLines > 0) {
      out.print("\r")
      if (lastDrawnLines > 1) { out.print(s"$Esc[${lastDrawnLines - 1}A") }
    }

  // Erase the currently-drawn bar block, leaving the cursor at the top of where it was (so a log line prints there and
  // the bars redraw below it). Resets the line count.
  private def clearBlockSync(): Unit =
    if (enabled && lastDrawnLines > 0) {
      moveToBlockTopSync()
      out.print(s"$Esc[J") // clear from cursor to end of screen
      lastDrawnLines = 0
    }

  // Draw every visible bar one per line and record how many physical lines that took. Each line is `\r`-anchored and
  // cleared to end of line (CSI K); lines are separated by `\n`. Assumes the block region is already clear (callers
  // pair this with `clearBlockSync`), so a shrinking block leaves no orphaned rows.
  private def drawAllSync(): Unit =
    if (enabled) {
      val lines = barLinesSync()
      if (lines.nonEmpty) {
        out.print(lines.mkString("\r", s"$Esc[K\n\r", s"$Esc[K"))
        lastDrawnLines = lines.size
      }
    }

  // Best-effort terminal width, resolved once at construction (the window is stable enough over a short interactive run).
  // Prefer an exported COLUMNS, else ask the controlling terminal directly — bash does NOT export COLUMNS to a child
  // JVM, so it's usually absent and a fixed 80 would mis-truncate on a narrower window — else fall back to 80. Computed
  // eagerly (not lazily on first draw) so the `sttyCols` sub-process spawn runs here, off the render lock, rather than
  // inside `lock.synchronized` on the first bar. Only an `enabled` display probes; the server / non-TTY path stays at 80.
  private val terminalWidth: Int =
    if (enabled) { Option(System.getenv("COLUMNS")).flatMap(_.toIntOption).filter(_ > 0).orElse(sttyCols).getOrElse(80) }
    else { 80 }

  // The controlling terminal's column count via `stty size` (prints "<rows> <cols>"), reading /dev/tty so it reflects
  // the real window even when this process's stdin/stdout are redirected. Any failure (no tty, stty absent) yields None.
  private def sttyCols: Option[Int] =
    try {
      val proc   = new ProcessBuilder("sh", "-c", "stty size < /dev/tty 2>/dev/null").start()
      val output = new String(proc.getInputStream.readAllBytes(), StandardCharsets.UTF_8).trim
      proc.waitFor()
      output.split("\\s+").lift(1).flatMap(_.toIntOption).filter(_ > 0)
    } catch { case _: Throwable => None }

  // Truncate a bar line to the detected width so it can't wrap onto a second physical row and desync the cursor-up count.
  private def truncateToWidth(line: String): String =
    if (line.length <= terminalWidth) { line } else { line.take(terminalWidth - 1) } // leave the last column, avoid wrap
}

object ProgressDisplay {

  /** A per-job (or the shared global) bar channel: the current set of that scope's bars keyed by bar id, published as a
    * latest-wins snapshot. `GET /api/jobs/{id}/progress` streams `.changes`; removal is a key drop.
    */
  type BarChannel = SubscriptionRef[Map[Int, BarSnapshot]]

  /** Per-fiber active bar channel — the job-scoped destination for bars created on this fiber (and, via FiberRef
    * inheritance, its forked children). [[ccas.server.jobs.JobRunner]] sets it per job with `currentChannel.locally`,
    * exactly as it does [[JobLogSink.currentSink]] for log lines, so a job's app bars land in that job's channel.
    * `None` (the default) routes a bar to the display's shared global channel instead.
    */
  val currentChannel: FiberRef[Option[BarChannel]] =
    Unsafe.unsafe(implicit u => FiberRef.unsafe.make[Option[BarChannel]](None))

  // `lastOutput` is the rendered line for the local terminal draw; `current`/`total`/`text` are the raw fields mirrored
  // into `target` (this bar's channel) as an unrendered `BarSnapshot`. `target` is captured at `addBar` and fixed for
  // the bar's life. `lineCount > 0` marks a bar that has actually been printed (drawn + snapshot-eligible).
  private case class BarState(
    id: Int,
    lineCount: Int,
    lastOutput: String,
    current: Int,
    total: Int,
    text: String,
    target: Option[BarChannel]
  )

  /** Synchronous factory — no IO, just allocates the lock + state. Use `live` instead in production code; `make` is
    * intended for tests that need a noop / quiet display without spinning up the layer machinery.
    *
    * @param enabled
    *   `true` for stdout rendering. `false` suppresses every stdout side-effect — bar lifecycle (`addBar`/`removeBar`)
    *   still tracks state correctly so that `addBarScoped` finalisers behave the same in both modes.
    */
  def make(enabled: Boolean): ProgressDisplay = makeWith(enabled, System.out, System.err, None)

  /** Test-only factory that injects the bar-redraw (`out`) and defect (`err`) streams so suites can capture output
    * without mutating process-global `System.out` / `System.err`, and the optional global bar channel. Production code
    * uses [[make]] / [[live]].
    */
  private[ccas] def makeWith(
    enabled: Boolean,
    out: PrintStream,
    err: PrintStream,
    globalChannel: Option[BarChannel]
  ): ProgressDisplay =
    new ProgressDisplay(enabled, out, err, globalChannel)

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
        channel <- SubscriptionRef.make(Map.empty[Int, BarSnapshot])
        d <- ZIO.acquireRelease(
          ZIO.succeed(makeWith(showProgress, out, err, Some(channel)))
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

  /** zio-http's `ServerInboundHandler.exceptionCaught` logs every fatal channel exception under this exact message. */
  private val FatalNettyExceptionMessage = "Fatal exception in Netty"

  /** True for the benign WARN zio-http emits when the server's read-idle reaper closes a live streaming follow.
    *
    * `CcasServer` sets `Server.Config#idleTimeout`, which installs a read-only Netty `ReadTimeoutHandler`. A job-log /
    * progress follow (`GET /api/jobs/{id}/{logs,progress}`) is write-only server→client once the request is sent, so the
    * read timer is never reset and the handler reaps every live follow on schedule — surfacing as zio-http's
    * `ZIO.logWarningCause("Fatal exception in Netty", Cause.die(ReadTimeoutException))`. The follow's client transparently
    * reconnects (#161), so the reap is benign transport noise, not a failure, and we drop exactly this WARN.
    *
    * The match is deliberately over-specified — the exact zio-http message AND a cause that is *solely* a
    * `ReadTimeoutException` defect (no typed failure, no other defect). This scopes it to the server reaper rather than
    * any bare read-timeout WARN elsewhere, and makes both coordinates fail *open*: if a zio-http upgrade renames the
    * message or wraps the exception, the filter simply stops matching and the (still benign) WARN reappears — noise
    * returns, no genuine signal is ever hidden.
    */
  private def isBenignReadIdleReap(level: LogLevel, message: String, cause: Cause[Any]): Boolean =
    level == LogLevel.Warning &&
      message == FatalNettyExceptionMessage &&
      cause.failures.isEmpty &&
      cause.defects.nonEmpty &&
      cause.defects.forall(_.isInstanceOf[ReadTimeoutException])

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
