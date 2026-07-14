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

  /** Stub that replays a fixed `JobStatusResponse` body for `getJson` and drives `streamLines` per [[StreamBehaviour]]. */
  private final class StubApi(script: Ref[List[String]], behaviour: StreamBehaviour) extends CcasApiClient {
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
  }

  private def statusJson(status: String, error: Option[String]): String =
    JobStatusResponse("job-1", "Membership", status, None, "2026-01-01T00:00:00Z", None, error, "Cli").toJson

  // Fast reconnect tuning for tests: negligible backoff, tiny cap so the give-up path resolves quickly.
  private def follower(api: CcasApiClient, maxWait: Duration): JobFollower =
    JobFollower(api, maxWait, reconnectBackoff = 1.milli, maxReconnects = 3)

  private def followerWith(status: String, error: Option[String], logLines: List[String]): UIO[JobFollower] =
    Ref
      .make(List(statusJson(status, error)))
      .map(ref => follower(new StubApi(ref, ReplayOnce(logLines)), 1.minute))

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
    test("followJob times out when the log stream never ends") {
      for {
        ref <- Ref.make(List(statusJson("Running", None)))
        code <- follower(new StubApi(ref, Hang), 30.millis).followJob("job-1")
      } yield assertTrue(code == 1)
    },
    test("followJob reconnects across a mid-stream drop and resumes without re-printing seen lines") {
      for {
        // Attempt 1 shows a,b then drops; attempt 2 replays a,b (skipped as already-shown) and finishes with c,d.
        attempts <- Ref.make(List(List("a", "b") -> true, List("a", "b", "c", "d") -> false))
        ref      <- Ref.make(List(statusJson("Completed", None)))
        code     <- follower(new StubApi(ref, Attempts(attempts)), 1.minute).followJob("job-1")
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
        code <- follower(new StubApi(ref, AlwaysFail(StreamDropped("idle timeout"))), 1.minute).followJob("job-1")
        err  <- TestConsole.outputErr
      } yield assertTrue(
        code == 1,
        err.exists(line => line.contains("ccas logs job-1") && line.contains("keeps running"))
      )
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
