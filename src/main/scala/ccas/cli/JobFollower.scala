package ccas.cli

import zio.*

import ccas.server.routes.JobRoutes.{CancelResult, ClubJobResult, JobResult, JobStatusResponse}

/** Drives a submitted job to completion by **following its log stream** (`GET /api/jobs/{id}/logs`), printing each
  * line as it arrives. The server holds the response open until the job is terminal and the tail reaches EOF, so the
  * stream closing means "job finished" — at which point one `GET /api/jobs/{id}` resolves the exit code.
  *
  * A silent job phase longer than the server's 60s read-idle timeout gets its follow connection reaped mid-stream (the
  * keepalive from #152 resets the *client's* read timer but not the server's `ReadTimeoutHandler` — #161). The detached
  * job keeps running server-side, so a drop is a transport artefact, not the end of the work: we transparently
  * reconnect and resume tailing instead of surfacing it as a failure. Only when reconnects are exhausted do we fall
  * back to the "reattach with `ccas logs`" hint.
  *
  * `maxWait`, `reconnectBackoff`, and `maxReconnects` are injected (no defaults) so tests run fast; production wires
  * generous values. Exit codes: 0 when the job reaches `Completed`, 1 when it reaches `Failed`, could not be submitted,
  * did not finish within `maxWait`, or the follow was lost past `maxReconnects` reconnects.
  */
final class JobFollower(
  api: CcasApiClient,
  maxWait: Duration,
  reconnectBackoff: Duration,
  maxReconnects: Int,
  showProgress: Boolean
) {

  def followJob(jobId: String): Task[Int] =
    followWith(jobId, line => Console.printLine(line).orDie, bars = showProgress)

  // Follow a job, optionally rendering its live progress bars above the streamed log lines. When not `bars`, log lines
  // go straight to `sink` (the pre-bars behaviour: `--stdout` routes to stderr, everything else stdout) and the reconnect
  // notice to stderr. When `bars`, delegate to `withBars`. `interpret` maps the raw follow outcome to an exit code.
  private def followWith(jobId: String, sink: String => UIO[Unit], bars: Boolean): Task[Int] =
    if (!bars) { followResult(jobId, sink, stderrNotice).flatMap(interpret(jobId, _)) }
    else { withBars(jobId) }

  // Bars branch: draw progress bars from `/api/jobs/{id}/progress` above the log lines. The progress consumer runs on a
  // scope-owned fiber (`forkScoped`, so it can't leak in the window between fork and cleanup registration) and
  // auto-reconnects across the server's ~60s read-idle drop (#161) just like the log follow — otherwise the bars would
  // freeze partway through a long job. `onExit` interrupts that fiber and clears the bars BEFORE `interpret` prints any
  // terminal status, so the final message lands on a quiescent terminal (not jammed onto a bar line). The one-time
  // reconnect notice for the *log* follow routes through `renderer.logLine` (the render lock), so it can't tear a live
  // bar redraw either. Both log lines and bars write through the one display's lock, so they never interleave mid-line.
  private def withBars(jobId: String): Task[Int] =
    ZIO.scoped {
      for {
        renderer <- ClientProgressRenderer.make
        progress <- consumeProgress(jobId, renderer).forkScoped
        result <- followResult(jobId, renderer.logLine, renderer.logLine)
          .onExit(_ => progress.interrupt *> renderer.clear)
        code <- interpret(jobId, result)
      } yield code
    }

  // Consume the progress stream, reconnecting on a mid-stream drop — the same ~60s read-idle reap the log follow handles
  // (#161) — so bars stay live for the whole job. Frames are latest-wins, so a reconnect needs no replay: the next frame
  // re-syncs every bar. A clean end (the server closes the stream at job completion) or any non-drop error stops without
  // retrying; the parallel log follow surfaces any real failure. Bounded by this fiber's scope (interrupted in `withBars`
  // once the follow ends), so the reconnect loop can't outlive the job.
  private def consumeProgress(jobId: String, renderer: ClientProgressRenderer): UIO[Unit] =
    api
      .streamProgress(s"/api/jobs/$jobId/progress")(renderer.render)
      .catchAll {
        case StreamDropped(_) => ZIO.sleep(reconnectBackoff) *> consumeProgress(jobId, renderer)
        case _                => ZIO.unit
      }

  // Run the log follow (reconnecting across drops, bounded by `maxWait`) and return the raw outcome without printing
  // a terminal status: Some(Right)=EOF/terminal, Some(Left)=gave up after maxReconnects, None=timed out. `notice`
  // sinks the one-time reconnect message.
  //
  // Ctrl-C during a follow cancels the server job (#170). `.onInterrupt` is attached OUTSIDE `.timeout` on purpose,
  // so a genuine `maxWait` expiry — which interrupts `follow` internally and returns `None` — does NOT trip it; only
  // a real interruption of this whole effect does. It fires while the CLI's `Client` scope is still open, so the POST
  // lands during teardown. Deliberately not scoped over `interpret`: at EOF the job is already terminal.
  private def followResult(
    jobId: String,
    onLine: String => UIO[Unit],
    notice: String => UIO[Unit]
  ): Task[Option[Either[String, Unit]]] =
    Ref.make(0).flatMap(printed =>
      follow(jobId, onLine, printed, notice).timeout(maxWait).onInterrupt(cancelOnInterrupt(jobId, notice))
    )

  // Best-effort cancel of the followed job when the follow is interrupted (Ctrl-C). The notice routes through the
  // follow's `notice` sink — the render-lock-safe `renderer.logLine` in bars mode, stderr otherwise — so it can't tear a
  // live bar line (this finalizer runs BEFORE `withBars`' `renderer.clear`, which is the outer `.onExit`). It emits
  // first so the operator sees it even if the POST is slow or fails; the POST itself is `.ignore`d — a 404 (the job
  // already reached a terminal state before the interrupt landed) or a transport error during teardown must not turn a
  // clean Ctrl-C into a stack trace. Cancellation is best-effort/asynchronous server-side (an in-flight blocking
  // statement runs to completion first), so this requests the cancel and returns — it does not wait for `Cancelled`.
  private def cancelOnInterrupt(jobId: String, notice: String => UIO[Unit]): UIO[Unit] =
    notice(s"$jobId: cancelling on interrupt…") *>
      api.postEmpty[CancelResult](s"/api/jobs/$jobId/cancel").ignore

  // Map the follow outcome to an exit code, printing the terminal status. In bars mode this runs AFTER the bars are
  // cleared (see `withBars`), so the message can't be jammed onto a live bar line.
  private def interpret(jobId: String, result: Option[Either[String, Unit]]): Task[Int] =
    result match {
      case Some(Right(()))   => finalExitCode(jobId)
      case Some(Left(cause)) => streamLost(jobId, cause).as(1)
      case None              => stillRunning(jobId).as(1)
    }

  private val stderrNotice: String => UIO[Unit] = line => Console.printLineError(line).orDie

  /** Follow the log stream, auto-reconnecting across a mid-stream drop (#161). Each (re)connect replays the log from
    * the start of the file (`FileTail.subscribe` begins at offset 0), so [[skipReplay]] drops the lines already shown
    * and only genuinely new lines print. `Right(())` = the stream reached EOF (the job is terminal); `Left(cause)` = we
    * gave up after `maxReconnects` drops. A non-drop error (e.g. a 404 for a GC'd job) propagates unchanged.
    */
  private def follow(
    jobId: String,
    onLine: String => UIO[Unit],
    printed: Ref[Int],
    notice: String => UIO[Unit]
  ): Task[Either[String, Unit]] = {
    def attempt(reconnects: Int): Task[Either[String, Unit]] =
      for {
        startPrinted <- printed.get
        seen         <- Ref.make(0)
        result <- api
          .streamLines(s"/api/jobs/$jobId/logs")(skipReplay(onLine, printed, seen, startPrinted))
          .foldZIO(
            {
              case StreamDropped(cause) => afterDrop(jobId, cause, reconnects, notice, () => attempt(reconnects + 1))
              case other                => ZIO.fail(other)
            },
            _ => ZIO.succeed(Right(()))
          )
      } yield result
    attempt(0)
  }

  // Wrap `onLine` so a reconnected stream's replayed prefix (the `startPrinted` lines already shown) is skipped and
  // only new lines print. Line N is stable across reconnects because the log file is append-only. `printed` tracks the
  // running total shown so the next reconnect knows where to resume; `seen` is this attempt's line counter.
  private def skipReplay(
    onLine: String => UIO[Unit],
    printed: Ref[Int],
    seen: Ref[Int],
    startPrinted: Int
  ): String => UIO[Unit] =
    line =>
      seen.updateAndGet(_ + 1).flatMap { n =>
        ZIO.unlessDiscard(n <= startPrinted)(onLine(line) *> printed.update(_ + 1))
      }

  // A drop while following: the detached job is still running (or just finished with its tail not yet drained to us).
  // Reconnect after a short backoff — a still-running job's re-follow resumes live tailing (bounded overall by
  // `maxWait`); a terminal job's re-follow replays to EOF and returns `Right`. `maxReconnects` is a *cumulative*
  // busy-loop backstop over the whole follow (not consecutive) for a stream that redrops instantly — `maxWait` is the
  // real wall-clock bound, and is sized far above any real job's silent-gap count. One-time notice per follow.
  private def afterDrop(
    jobId: String,
    cause: String,
    reconnects: Int,
    notice: String => UIO[Unit],
    retry: () => Task[Either[String, Unit]]
  ): Task[Either[String, Unit]] =
    if (reconnects >= maxReconnects) {
      ZIO.succeed(Left(cause))
    } else {
      for {
        _      <- ZIO.whenDiscard(reconnects == 0)(notice(reconnectMessage(jobId)))
        _      <- ZIO.sleep(reconnectBackoff)
        result <- retry()
      } yield result
    }

  private def reconnectMessage(jobId: String): String =
    s"$jobId: log stream dropped on a silent phase — reconnecting (the job keeps running)…"

  private def streamLost(jobId: String, cause: String): UIO[Unit] =
    Console
      .printLineError(
        s"$jobId: lost the log stream ($cause). The job keeps running on the server — " +
          s"reattach with 'ccas logs $jobId' or check 'ccas jobs'."
      )
      .orDie

  private def stillRunning(jobId: String): UIO[Unit] =
    Console
      .printLineError(s"$jobId: still running after ${maxWait.toMinutes}m — check 'ccas jobs' / 'ccas logs $jobId'")
      .orDie

  // The stream closing tells us the job finished but not whether it succeeded; read the terminal status once to decide.
  // An unexpected status (server adds a new terminal kind, or a stale Running) exits non-zero rather than passing.
  private def finalExitCode(jobId: String): Task[Int] =
    api.getJson[JobStatusResponse](s"/api/jobs/$jobId").flatMap { job =>
      job.status match {
        case "Completed" => ZIO.succeed(0)
        case "Failed"    => Console.printLineError(s"$jobId failed: ${job.error.getOrElse("unknown error")}").orDie.as(1)
        // A followed job cancelled out from under us (e.g. `ccas cancel` from another terminal): a clean terminal
        // outcome, not an "unexpected status". Non-zero — the job did not complete.
        case "Cancelled" => Console.printLineError(s"$jobId was cancelled").orDie.as(1)
        case other       => Console.printLineError(s"$jobId: unexpected status '$other'").orDie.as(1)
      }
    }

  /** Single-job submit result (recruit, matchref). The POST returns HTTP 200 even on failure, so branch on the body. */
  def handleSingle(label: String, result: JobResult): Task[Int] =
    (result.error, result.jobId) match {
      case (Some(err), _) => Console.printLineError(s"$label: $err").orDie.as(1)
      case (_, Some(id)) =>
        CompletionCache.appendJob(id) *> Console.printLine(s"$label submitted: $id").orDie *> followJob(id)
      case _ => Console.printLineError(s"$label: server returned no job id").orDie.as(1)
    }

  /** Recruit submit + result delivery. When `logsToStderr`, the submit notice and streamed job logs go to stderr so
    * stdout carries only the bare-username payload (`ccas recruit --stdout | wl-copy`, and the non-interactive
    * deferred-report path); the interactive confirm flow keeps both on stdout. `onComplete` runs only when the job
    * reaches `Completed`, receiving the job id so the caller can fetch/confirm and render the usernames; it is
    * skipped on failure/timeout/no-id.
    */
  def handleRecruit(
    label: String,
    result: JobResult,
    logsToStderr: Boolean,
    onComplete: String => Task[Unit]
  ): Task[Int] =
    (result.error, result.jobId) match {
      case (Some(err), _) => Console.printLineError(s"$label: $err").orDie.as(1)
      case (_, Some(id)) =>
        val notice: UIO[Unit] =
          (if (logsToStderr) { Console.printLineError(s"$label submitted: $id") }
           else { Console.printLine(s"$label submitted: $id") }).orDie
        val onLine: String => UIO[Unit] =
          if (logsToStderr) { line => Console.printLineError(line).orDie }
          else { line => Console.printLine(line).orDie }
        // `--stdout` (logsToStderr) keeps stdout clean for the username payload, so no bars there; an interactive run
        // gets bars when the terminal supports them.
        for {
          _    <- CompletionCache.appendJob(id)
          _    <- notice
          code <- followWith(id, onLine, bars = showProgress && !logsToStderr)
          _    <- ZIO.whenDiscard(code == 0)(onComplete(id))
        } yield code
      case _ => Console.printLineError(s"$label: server returned no job id").orDie.as(1)
    }

  /** Club-scoped single-job submit result (stats). */
  def handleClubSingle(result: ClubJobResult): Task[Int] =
    (result.error, result.jobId) match {
      case (Some(err), _) => Console.printLineError(s"${result.clubSlug}: $err").orDie.as(1)
      case (_, Some(id)) =>
        CompletionCache.appendJob(id) *> Console.printLine(s"${result.clubSlug} submitted: $id").orDie *> followJob(id)
      case _ => Console.printLineError(s"${result.clubSlug}: server returned no job id").orDie.as(1)
    }

  /** Batch submit result (membership, history): follow each club's job in turn, fail overall if any job failed. */
  def handleBatch(results: List[ClubJobResult]): Task[Int] =
    ZIO.foreach(results)(handleClubSingle).map(codes => if (codes.forall(_ == 0)) { 0 } else { 1 })
}
