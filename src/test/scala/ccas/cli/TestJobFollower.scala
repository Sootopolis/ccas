package ccas.cli

import zio.*
import zio.json.*
import zio.test.{assertTrue, Spec, TestAspect, TestConsole, ZIOSpecDefault}

import ccas.server.routes.JobRoutes.{ClubJobResult, JobResult, JobStatusResponse}

/** Exercises log-stream following and submit-result handling against a scripted stub client — no socket, no DB. */
object TestJobFollower extends ZIOSpecDefault {

  /** How the stub's `streamLines` behaves. `Attempts` models the server tail replaying from offset 0 on every
    * (re)connect: each entry is `(lines this attempt replays from the start, whether it then drops)`; the last entry
    * should have `drop = false` so the follow finishes at EOF.
    */
  private sealed trait StreamBehaviour
  private final case class ReplayOnce(lines: List[String])                extends StreamBehaviour
  private case object Hang                                                extends StreamBehaviour
  private final case class AlwaysFail(err: Throwable)                       extends StreamBehaviour
  private final case class Attempts(ref: Ref[List[(List[String], Boolean)]]) extends StreamBehaviour
  // Stay open until `gate` completes (a progress-reconnect signal), then reach EOF — models a /logs stream that
  // outlives the /progress reconnect so the test can observe it.
  private final case class AwaitThenComplete(gate: Promise[Nothing, Unit]) extends StreamBehaviour

  /** Stub that replays a fixed `JobStatusResponse` body for `getJson`, drives `streamLines` per [[StreamBehaviour]], and
    * runs `progress` (re-evaluated per call, so a Ref-backed effect can script a drop-then-reconnect) for `streamProgress`.
    */
  private final class StubApi(script: Ref[List[String]], behaviour: StreamBehaviour, progress: Task[Unit])
      extends CcasApiClient {
    override def getJson[Resp: JsonDecoder](path: String): Task[Resp] =
      script.modify {
        case head :: tail => (head, tail)
        case Nil          => ("", Nil)
      }.flatMap(s => ZIO.fromEither(s.fromJson[Resp]).mapError(m => CliError(s"stub decode failed: $m", 1)))

    override def streamLines(path: String)(onLine: String => UIO[Unit]): Task[Unit] =
      behaviour match {
        case ReplayOnce(lines) => ZIO.foreachDiscard(lines)(onLine)
        case Hang              => ZIO.never
        case AlwaysFail(err)     => ZIO.fail(err)
        case AwaitThenComplete(gate) => gate.await
        case Attempts(ref) =>
          ref.modify {
            case head :: tail => (Some(head), tail)
            case Nil          => (None, Nil)
          }.flatMap {
            case Some((lines, drop)) =>
              ZIO.foreachDiscard(lines)(onLine) *>
                (if (drop) { ZIO.fail(StreamDropped("idle timeout")) } else { ZIO.unit })
            case None => ZIO.unit // ran out of scripted attempts → clean EOF
          }
      }

    override def postJson[Req: JsonEncoder, Resp: JsonDecoder](path: String, body: Req): Task[Resp] =
      ZIO.die(new UnsupportedOperationException("postJson"))
    override def postEmpty[Resp: JsonDecoder](path: String): Task[Resp] =
      ZIO.die(new UnsupportedOperationException("postEmpty"))
    override def postUnit[Req: JsonEncoder](path: String, body: Req): Task[Unit] = ZIO.unit
    override def delete(path: String): Task[Unit]                                = ZIO.unit

    // Rendering is exercised in TestClientProgressRenderer; here `progress` scripts the transport (default ZIO.unit =
    // one clean pass; a Ref-backed effect scripts a drop-then-reconnect). onFrame is unused — no frames are emitted.
    override def streamProgress(path: String)(onFrame: ccas.utils.ProgressSnapshot => UIO[Unit]): Task[Unit] = progress
  }

  /** Stub that records every `postEmpty` (the cancel POST) path and drives `streamLines` via `stream`, so an interrupt
    * test can assert whether following a job fired the cancel-on-interrupt POST (#170). `getJson` replays a fixed status.
    * `postEmpty` returns a valid `CancelResult` body decoded through the caller's own `Resp` decoder (the follower
    * `.ignore`s it anyway), so no `CancelResult` codec is needed here.
    */
  private final class CancelRecordingApi(
    status: String,
    cancelled: Ref[List[String]],
    stream: (String => UIO[Unit]) => Task[Unit]
  ) extends CcasApiClient {
    override def getJson[Resp: JsonDecoder](path: String): Task[Resp] =
      ZIO.fromEither(statusJson(status, None).fromJson[Resp]).mapError(m => CliError(s"stub decode failed: $m", 1))
    override def streamLines(path: String)(onLine: String => UIO[Unit]): Task[Unit] = stream(onLine)
    override def streamProgress(path: String)(onFrame: ccas.utils.ProgressSnapshot => UIO[Unit]): Task[Unit] = ZIO.unit
    override def postEmpty[Resp: JsonDecoder](path: String): Task[Resp] =
      cancelled.update(path :: _) *>
        ZIO.fromEither("""{"jobId":"job-1"}""".fromJson[Resp]).mapError(m => CliError(s"stub decode failed: $m", 1))
    override def postJson[Req: JsonEncoder, Resp: JsonDecoder](path: String, body: Req): Task[Resp] =
      ZIO.die(new UnsupportedOperationException("postJson"))
    override def postUnit[Req: JsonEncoder](path: String, body: Req): Task[Unit] = ZIO.unit
    override def delete(path: String): Task[Unit]                                = ZIO.unit
  }

  private def statusJson(status: String, error: Option[String]): String =
    JobStatusResponse("job-1", "Membership", status, None, "2026-01-01T00:00:00Z", None, error, "Cli").toJson

  // Fast reconnect tuning for tests: negligible backoff, tiny cap so the give-up path resolves quickly. Bars off by
  // default (plain log lines, asserted via TestConsole); `barsFollower` flips `showProgress` on for the bars-path test.
  private def follower(api: CcasApiClient, maxWait: Duration): JobFollower =
    JobFollower(api, maxWait, reconnectBackoff = 1.milli, maxReconnects = 3, showProgress = false)

  private def barsFollower(api: CcasApiClient, maxWait: Duration): JobFollower =
    JobFollower(api, maxWait, reconnectBackoff = 1.milli, maxReconnects = 3, showProgress = true)

  private def followerWith(status: String, error: Option[String], logLines: List[String]): UIO[JobFollower] =
    Ref
      .make(List(statusJson(status, error)))
      .map(ref => follower(new StubApi(ref, ReplayOnce(logLines), ZIO.unit), 1.minute))

  override def spec: Spec[Any, Throwable] = suite("TestJobFollower")(
    test("followJob streams the log lines and returns 0 when the job completes") {
      for {
        follower <- followerWith("Completed", None, List("hello", "world"))
        code   <- follower.followJob("job-1")
      } yield assertTrue(code == 0)
    },
    test("followJob returns 1 when the job fails") {
      for {
        follower <- followerWith("Failed", Some("boom"), List("partial output"))
        code   <- follower.followJob("job-1")
      } yield assertTrue(code == 1)
    },
    test("followJob returns 1 when the job was cancelled") {
      for {
        follower <- followerWith("Cancelled", Some("Cancelled by operator"), List("partial output"))
        code     <- follower.followJob("job-1")
      } yield assertTrue(code == 1)
    },
    test("followJob with progress bars on forks the progress consumer, follows, and completes cleanly") {
      // showProgress = true takes the bars branch: build a client ProgressDisplay, fork the (empty) /progress consumer,
      // follow the log stream through the display, then tear the consumer down and clear bars on completion. Asserts the
      // follow still resolves to 0 — i.e. the bars wiring doesn't break or hang the log follow.
      for {
        ref  <- Ref.make(List(statusJson("Completed", None)))
        code <- barsFollower(new StubApi(ref, ReplayOnce(List("log line")), ZIO.unit), 1.minute).followJob("job-1")
      } yield assertTrue(code == 0)
    },
    test("bars mode reconnects the /progress stream after a mid-stream drop") {
      // The server reaps /progress at its read-idle timeout just like /logs (#161); the consumer must reconnect or the
      // bars freeze for the rest of the job. /progress drops on its 1st subscribe, then on reconnect signals `reconnected`
      // and ends; /logs stays open until that signal, so the reconnect is observed before the follow finishes.
      for {
        reconnected  <- Promise.make[Nothing, Unit]
        progressCall <- Ref.make(0)
        progress = progressCall.updateAndGet(_ + 1).flatMap(n =>
          if (n == 1) { ZIO.fail(StreamDropped("read-idle reap")) }
          else { reconnected.succeed(()).unit }
        )
        ref   <- Ref.make(List(statusJson("Completed", None)))
        code  <- barsFollower(new StubApi(ref, AwaitThenComplete(reconnected), progress), 1.minute).followJob("job-1")
        calls <- progressCall.get
      } yield assertTrue(code == 0, calls >= 2)
    },
    test("bars mode times out cleanly and still reports 'still running'") {
      // Bug-fix guard: on the timeout path the bars are cleared (onExit) BEFORE interpret prints the terminal status, so
      // the follow resolves to 1 and the message is emitted rather than lost/hung.
      for {
        ref  <- Ref.make(List(statusJson("Running", None)))
        code <- barsFollower(new StubApi(ref, Hang, ZIO.unit), 30.millis).followJob("job-1")
        err  <- TestConsole.outputErr
      } yield assertTrue(code == 1, err.exists(_.contains("still running")))
    },
    test("followJob times out when the log stream never ends") {
      for {
        ref <- Ref.make(List(statusJson("Running", None)))
        code <- follower(new StubApi(ref, Hang, ZIO.unit), 30.millis).followJob("job-1")
      } yield assertTrue(code == 1)
    },
    test("followJob reconnects across a mid-stream drop and resumes without re-printing seen lines") {
      for {
        // Attempt 1 shows a,b then drops; attempt 2 replays a,b (skipped as already-shown) and finishes with c,d.
        attempts <- Ref.make(List(List("a", "b") -> true, List("a", "b", "c", "d") -> false))
        ref      <- Ref.make(List(statusJson("Completed", None)))
        code     <- follower(new StubApi(ref, Attempts(attempts), ZIO.unit), 1.minute).followJob("job-1")
        out      <- TestConsole.output
        err      <- TestConsole.outputErr
      } yield assertTrue(
        code == 0,
        // Each real line printed exactly once, in order — no duplicated replay prefix.
        out.map(_.stripLineEnd) == Vector("a", "b", "c", "d"),
        // A one-time reconnect notice landed on stderr; the give-up "reattach" message did not.
        err.exists(_.contains("reconnecting")),
        !err.exists(_.contains("lost the log stream"))
      )
    },
    test("followJob gives up with a reattach hint after exhausting reconnects") {
      for {
        ref <- Ref.make(List(statusJson("Running", None)))
        // Always drops → never reaches EOF → give up after maxReconnects and surface the reattach hint.
        code <- follower(new StubApi(ref, AlwaysFail(StreamDropped("idle timeout")), ZIO.unit), 1.minute).followJob("job-1")
        err  <- TestConsole.outputErr
      } yield assertTrue(
        code == 1,
        err.exists(line => line.contains("ccas logs job-1") && line.contains("keeps running"))
      )
    },
    test("an interrupt during a follow cancels the server job (#170)") {
      for {
        cancelled <- Ref.make(List.empty[String])
        started   <- Promise.make[Nothing, Unit]
        // The stream signals it is live, then hangs — so the follow is unambiguously inside `streamLines` (its
        // onInterrupt installed) when we interrupt. `Running` status is irrelevant; interpret never runs.
        api = new CancelRecordingApi("Running", cancelled, _ => started.succeed(()) *> ZIO.never)
        fiber     <- follower(api, 1.minute).followJob("job-1").fork
        _         <- started.await
        _         <- fiber.interrupt
        recorded  <- cancelled.get
      } yield assertTrue(recorded.exists(p => p.contains("job-1") && p.contains("cancel")))
    },
    test("an interrupt during the post-follow recruit confirm does not cancel (#170)") {
      for {
        cancelled <- Ref.make(List.empty[String])
        inConfirm <- Promise.make[Nothing, Unit]
        // The stream completes immediately (job terminal) so the follow returns before we interrupt; the interrupt lands
        // in the `onComplete` (confirm) phase, which is OUTSIDE the follow's cancel-on-interrupt scope. No cancel fires.
        api = new CancelRecordingApi("Completed", cancelled, _ => ZIO.unit)
        fiber <- follower(api, 1.minute)
          .handleRecruit("recruit", JobResult(Some("job-1"), None), logsToStderr = false, _ => inConfirm.succeed(()) *> ZIO.never)
          .fork
        _        <- inConfirm.await
        _        <- fiber.interrupt
        recorded <- cancelled.get
      } yield assertTrue(recorded.isEmpty)
    },
    test("handleSingle short-circuits on a submission error") {
      for {
        follower <- followerWith("Completed", None, Nil)
        code   <- follower.handleSingle("recruit", JobResult(None, Some("Club not found")))
      } yield assertTrue(code == 1)
    },
    test("handleRecruit runs onComplete on success and routes logs to stderr when piping") {
      for {
        follower <- followerWith("Completed", None, List("hello"))
        ran      <- Ref.make(false)
        code     <- follower.handleRecruit("recruit", JobResult(Some("job-1"), None), logsToStderr = true, _ => ran.set(true))
        didRun   <- ran.get
        out      <- TestConsole.output
        err      <- TestConsole.outputErr
      } yield assertTrue(
        code == 0,
        didRun,
        // stdout stays clean for the pipe; the submit notice and log line land on stderr.
        !out.exists(_.contains("hello")),
        err.exists(_.contains("hello")),
        err.exists(_.contains("recruit submitted: job-1"))
      )
    },
    test("handleRecruit skips onComplete when the job fails") {
      for {
        follower <- followerWith("Failed", Some("boom"), List("partial"))
        ran      <- Ref.make(false)
        code     <- follower.handleRecruit("recruit", JobResult(Some("job-1"), None), logsToStderr = false, _ => ran.set(true))
        didRun   <- ran.get
      } yield assertTrue(code == 1, !didRun)
    },
    test("handleRecruit short-circuits on a submission error without following") {
      for {
        follower <- followerWith("Completed", None, Nil)
        ran      <- Ref.make(false)
        code     <- follower.handleRecruit("recruit", JobResult(None, Some("Club not found")), logsToStderr = true, _ => ran.set(true))
        didRun   <- ran.get
      } yield assertTrue(code == 1, !didRun)
    },
    test("handleBatch fails overall if any job fails to submit") {
      for {
        follower <- followerWith("Completed", None, Nil)
        code <- follower.handleBatch(
          List(
            ClubJobResult("a", Some("job-1"), None),
            ClubJobResult("b", None, Some("Club not found"))
          )
        )
      } yield assertTrue(code == 1)
    }
  ) @@ TestAspect.withLiveClock
}
