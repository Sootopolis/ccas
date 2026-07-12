package ccas.cli

import zio.*
import zio.json.*
import zio.test.{assertTrue, Spec, TestAspect, TestConsole, ZIOSpecDefault}

import ccas.server.routes.JobRoutes.{ClubJobResult, JobResult, JobStatusResponse}

/** Exercises log-stream following and submit-result handling against a scripted stub client — no socket, no DB. */
object TestJobFollower extends ZIOSpecDefault {

  /** Stub that replays a fixed `JobStatusResponse` body for `getJson` and a fixed list of log lines for `streamLines`
    * (or hangs forever to exercise the follow timeout, or fails with `streamError` to exercise a mid-stream drop).
    */
  private final class StubApi(
    script: Ref[List[String]],
    logLines: List[String],
    streamHangs: Boolean,
    streamError: Option[Throwable]
  ) extends CcasApiClient {
    override def getJson[Resp: JsonDecoder](path: String): Task[Resp] =
      script.modify {
        case head :: tail => (head, tail)
        case Nil          => ("", Nil)
      }.flatMap(s => ZIO.fromEither(s.fromJson[Resp]).mapError(m => CliError(s"stub decode failed: $m", 1)))

    override def streamLines(path: String)(onLine: String => UIO[Unit]): Task[Unit] =
      streamError match {
        case Some(err)             => ZIO.fail(err)
        case None if streamHangs   => ZIO.never
        case None                  => ZIO.foreachDiscard(logLines)(onLine)
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

  private def followerWith(status: String, error: Option[String], logLines: List[String]): UIO[JobFollower] =
    Ref
      .make(List(statusJson(status, error)))
      .map(ref => JobFollower(new StubApi(ref, logLines, streamHangs = false, streamError = None), 1.minute))

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
        follower = JobFollower(new StubApi(ref, Nil, streamHangs = true, streamError = None), 30.millis)
        code <- follower.followJob("job-1")
      } yield assertTrue(code == 1)
    },
    test("followJob reports a mid-stream drop with a reattach hint instead of failing") {
      for {
        ref <- Ref.make(List(statusJson("Running", None)))
        follower = JobFollower(new StubApi(ref, Nil, streamHangs = false, streamError = Some(StreamDropped("idle timeout"))), 1.minute)
        code <- follower.followJob("job-1")
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
