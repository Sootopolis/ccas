package ccas.server.jobs

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import scala.jdk.CollectionConverters.*

import com.typesafe.config.ConfigFactory

import ccas.utils.sql.PostgresClient
import zio.{durationInt, Promise, Ref, Scope, ZIO, ZLayer}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, JobRunId}
import ccas.server.ServerTables
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.errors.ConflictException
import ccas.utils.sql.{FreshSchemaLayer, TestDbCleanup}
import ccas.utils.ProgressDisplay

object TestJobRunner extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestJobRunner")(
    testSubmitSucceeds,
    testSubmitRecordsFailed,
    testSubmitRejectsDuplicate,
    testConcurrentSubmitConflict,
    testSubmitAllowsDifferentClub,
    testSubmitAllowsDifferentKind,
    testStatusUnknown,
    testRecentJobsOrdered,
    testSubmitWritesJobLog,
    testLogStreamReplaysCompletedJob,
    testLogStreamUnknownReturnsNone,
    testLogStreamTailsLiveJob,
    testLogStreamCarriesPartialLineAcrossTicks,
    testLogStreamJoinsMultibyteCharAcrossTicks,
    testLogStreamReassemblesCjkAndEmojiTornMidChar
  ).provideShared(
    FreshSchemaLayer("test_job_runner", onInit = ServerTables.ensureTables),
    TestChessComClientSupport.dummyLayer,
    JobRunner.live,
    // `Scope.default` is the global, never-closing scope. Fine for a single suite — the ZLogger swap and
    // `finishAllSync` finalizer would only matter if multiple suites shared this layer. Revisit if that changes.
    Scope.default >>> ProgressDisplay.live(showProgress = false)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)

  private object Times {
    val t0: java.time.Instant = java.time.Instant.parse("2025-06-01T00:00:00Z")
  }

  private val clubIdA = ClubId(200)
  private val clubIdB = ClubId(201)
  private val clubIdC = ClubId(202)

  private val deleteAllJobRuns = for {
    _ <- TestDbCleanup.clearJobRuns
    _ <- Club.upsert(Club(clubIdA, Times.t0, ClubSlug("club-a"), "Club A", None, None, None))
    _ <- Club.upsert(Club(clubIdB, Times.t0, ClubSlug("club-b"), "Club B", None, None, None))
    _ <- Club.upsert(Club(clubIdC, Times.t0, ClubSlug("club-c"), "Club C", None, None, None))
    _ <- Club.upsert(Club(ClubId(203), Times.t0, ClubSlug("test-club"), "Test Club", None, None, None))
    _ <- Club.upsert(Club(ClubId(204), Times.t0, ClubSlug("dup-club"), "Dup Club", None, None, None))
  } yield ()

  private def awaitStatus(
    runner: JobRunner,
    id: JobRunId,
    maxWait: zio.Duration = 10.seconds
  ): ZIO[ccas.utils.sql.PostgresClient, Throwable, JobRun] =
    runner.status(id).flatMap {
      case Some(job) if job.status != JobRunStatus.Running => ZIO.succeed(job)
      case _                                               => ZIO.sleep(100.millis) *> awaitStatus(runner, id, maxWait)
    }.timeoutFail(new Exception(s"Job $id did not complete in time"))(maxWait)

  // --- Tests ---

  private def testSubmitSucceeds = test("submit succeeds and completes") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      id     <- runner.submit(JobKind.Recruitment, Some(ClubId(203)), None, RunTrigger.Cli, _ => ZIO.unit)
      job    <- awaitStatus(runner, id)
    } yield assertTrue(
      job.status == JobRunStatus.Completed,
      job.completedAt.isDefined
    )
  }

  private def testSubmitRecordsFailed = test("submit records Failed on effect failure") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      id     <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, _ => ZIO.fail(new Exception("boom")))
      job    <- awaitStatus(runner, id)
    } yield assertTrue(
      job.status == JobRunStatus.Failed,
      job.error.contains("boom")
    )
  }

  private def testSubmitRejectsDuplicate = test("submit rejects duplicate running job") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      _      <- runner.submit(JobKind.Recruitment, Some(ClubId(204)), None, RunTrigger.Cli, _ => ZIO.never)
      result <- runner.submit(JobKind.Recruitment, Some(ClubId(204)), None, RunTrigger.Cli, _ => ZIO.unit).either
    } yield assertTrue(
      result.isLeft,
      result.left.exists(_.isInstanceOf[ConflictException])
    )
  }

  private def testConcurrentSubmitConflict = test("concurrent submits for same kind/club produce exactly one winner") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      gate   <- zio.Promise.make[Nothing, Unit]
      fibers <- ZIO.foreach(List.fill(5)(()))(
        _ => (gate.await *> runner.submit(JobKind.Recruitment, Some(ClubId(204)), None, RunTrigger.Cli, _ => ZIO.never).either).fork
      )
      _       <- gate.succeed(())
      results <- ZIO.foreach(fibers)(_.join)
      successes = results.count(_.isRight)
      conflicts = results.count(_.left.exists(_.isInstanceOf[ConflictException]))
    } yield assertTrue(
      successes == 1,
      conflicts == results.size - 1
    )
  }

  private def testSubmitAllowsDifferentClub = test("submit allows same kind with different club") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      _      <- runner.submit(JobKind.Recruitment, Some(clubIdA), None, RunTrigger.Cli, _ => ZIO.never)
      id2    <- runner.submit(JobKind.Recruitment, Some(clubIdB), None, RunTrigger.Cli, _ => ZIO.unit)
    } yield assertTrue(JobRunId.unwrap(id2).nonEmpty)
  }

  private def testSubmitAllowsDifferentKind = test("submit allows different kind with same club") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      _      <- runner.submit(JobKind.Recruitment, Some(clubIdC), None, RunTrigger.Cli, _ => ZIO.never)
      id2    <- runner.submit(JobKind.Membership, Some(clubIdC), None, RunTrigger.Cli, _ => ZIO.unit)
    } yield assertTrue(JobRunId.unwrap(id2).nonEmpty)
  }

  private def testStatusUnknown = test("status returns None for unknown id") {
    for {
      runner <- ZIO.service[JobRunner]
      result <- runner.status(JobRunId.wrap("nonexistent"))
    } yield assertTrue(result.isEmpty)
  }

  private def testSubmitWritesJobLog = test("submit routes job log lines into the per-job file") {
    val logDir = Paths.get(ConfigFactory.load().getString("job-logs.directory"))
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      id     <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, _ => ZIO.logInfo("hello from job"))
      _      <- awaitStatus(runner, id)
      path   =  logDir.resolve(s"${JobRunId.unwrap(id)}.log")
      lines  <- ZIO.attempt(Files.readAllLines(path).asScala.toList)
    } yield assertTrue(
      lines.exists(_.contains("hello from job")),
      // ANSI stripped — file should be readable. The formatter emits at least one ESC byte; stripped output has none.
      lines.forall(!_.contains(0x1B.toChar))
    )
  }

  private def testLogStreamReplaysCompletedJob = test("logStream replays a completed job's log and ends") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      id     <- runner.submit(
        JobKind.MatchRef,
        None,
        None,
        RunTrigger.Cli,
        _ => ZIO.logInfo("line one") *> ZIO.logInfo("line two")
      )
      _         <- awaitStatus(runner, id)
      streamOpt <- runner.logStream(id)
      stream    <- ZIO.fromOption(streamOpt).orElseFail(new Exception("expected a stream for a known job"))
      lines     <- stream.runCollect.map(_.toList)
    } yield assertTrue(
      lines.exists(_.contains("line one")),
      lines.exists(_.contains("line two"))
    )
  }

  private def testLogStreamUnknownReturnsNone = test("logStream returns None for unknown id") {
    for {
      runner <- ZIO.service[JobRunner]
      result <- runner.logStream(JobRunId.wrap("does-not-exist"))
    } yield assertTrue(result.isEmpty)
  }

  private def testLogStreamTailsLiveJob = test("logStream tails a running job and closes when it finishes") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      gate   <- Promise.make[Nothing, Unit]
      // The job logs a line, then blocks on the gate so it stays Running while we subscribe.
      id        <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, _ => ZIO.logInfo("streaming line") *> gate.await)
      streamOpt <- runner.logStream(id)
      stream    <- ZIO.fromOption(streamOpt).orElseFail(new Exception("expected a live stream"))
      collect   <- stream.runCollect.fork
      _         <- gate.succeed(())
      _         <- awaitStatus(runner, id)
      lines     <- collect.join.map(_.toList)
    } yield assertTrue(lines.exists(_.contains("streaming line")))
  }

  // Offset-incremental tail: a line written across two appends (no newline on the first) must be buffered and emitted
  // once complete, never as a torn fragment. Drives `FileTail` directly with a hand-built completions map so we control
  // the partial-then-complete timing; an uncompleted promise keeps the job "live" until we flip it terminal.
  private def testLogStreamCarriesPartialLineAcrossTicks =
    test("FileTail buffers a line split across two appends and emits it exactly once") {
      for {
        dir     <- ZIO.attempt(Files.createTempDirectory("ccas-filetail-partial"))
        jobId    = JobRunId.wrap("partial-line-job")
        path     = dir.resolve(s"${JobRunId.unwrap(jobId)}.log")
        promise <- Promise.make[Nothing, Unit]
        ref     <- Ref.make(Map(jobId -> promise)) // present + uncompleted => treated as live
        tail     = new FileTail(dir, ref, 20.millis) // small poll interval for a fast, deterministic test
        collect <- tail.subscribe(jobId).runCollect.fork
        _       <- appendBytes(path, "hello".getBytes(StandardCharsets.UTF_8)) // no trailing newline: stays buffered
        _       <- ZIO.sleep(200.millis)                                       // > pollInterval: a tick observes the partial
        _       <- appendBytes(path, " world\n".getBytes(StandardCharsets.UTF_8))
        _       <- ZIO.sleep(200.millis)  // let a live tick emit the joined line before we flip terminal
        _       <- promise.succeed(())    // job now terminal => stream drains and ends
        lines   <- collect.join.map(_.toList)
      } yield assertTrue(lines == List("hello world"))
    }

  // A multibyte UTF-8 char (é = 0xC3 0xA9) split across two appends must decode whole. Proves the byte-offset tail
  // never tears a char at a read boundary: splitting on the '\n' byte (0x0A, which never appears mid-sequence) always
  // lands on a char boundary, so the half-char stays buffered until its second byte arrives.
  private def testLogStreamJoinsMultibyteCharAcrossTicks =
    test("FileTail joins a multibyte UTF-8 char split across two appends") {
      for {
        dir     <- ZIO.attempt(Files.createTempDirectory("ccas-filetail-multibyte"))
        jobId    = JobRunId.wrap("multibyte-line-job")
        path     = dir.resolve(s"${JobRunId.unwrap(jobId)}.log")
        promise <- Promise.make[Nothing, Unit]
        ref     <- Ref.make(Map(jobId -> promise))
        tail     = new FileTail(dir, ref, 20.millis)
        collect <- tail.subscribe(jobId).runCollect.fork
        _       <- appendBytes(path, Array[Byte]('x'.toByte, 0xC3.toByte)) // "x" + lead byte of é, no newline
        _       <- ZIO.sleep(200.millis)
        _       <- appendBytes(path, Array[Byte](0xA9.toByte, '\n'.toByte)) // trailing byte of é + newline
        _       <- ZIO.sleep(200.millis)
        _       <- promise.succeed(())
        lines   <- collect.join.map(_.toList)
      } yield assertTrue(lines == List("x\u00e9")) // "x" + e-acute (U+00E9), decoded from bytes 0xC3 0xA9
    }

  // Club names carry Chinese (3-byte UTF-8) and emoji (4-byte UTF-8 / surrogate pair) glyphs, so log lines do too.
  // Split a line right through the middle of the 4-byte emoji (after only 2 of its bytes) across two appends: the tail
  // must still emit the line whole. Worst case for the byte-offset reader \u2014 the torn fragment is wider than the 2-byte
  // \u00e9 case and the break lands deep inside the sequence.
  private def testLogStreamReassemblesCjkAndEmojiTornMidChar =
    test("FileTail reassembles a Chinese + emoji line torn mid-emoji across appends") {
      val line  = "\u4e2d\u6587\ud83d\ude00" // U+4E2D U+6587 (3-byte CJK each) + grinning-face emoji U+1F600 (4-byte / surrogate pair)
      val bytes = line.getBytes(StandardCharsets.UTF_8) // 10 bytes total: 3 + 3 + 4
      for {
        dir     <- ZIO.attempt(Files.createTempDirectory("ccas-filetail-cjk"))
        jobId    = JobRunId.wrap("cjk-emoji-job")
        path     = dir.resolve(s"${JobRunId.unwrap(jobId)}.log")
        promise <- Promise.make[Nothing, Unit]
        ref     <- Ref.make(Map(jobId -> promise))
        tail     = new FileTail(dir, ref, 20.millis)
        collect <- tail.subscribe(jobId).runCollect.fork
        _       <- appendBytes(path, bytes.slice(0, 8)) // \u4e2d\u6587 + first 2 of the emoji's 4 bytes, no newline
        _       <- ZIO.sleep(200.millis)
        _       <- appendBytes(path, bytes.slice(8, bytes.length) ++ Array('\n'.toByte)) // last 2 emoji bytes + newline
        _       <- ZIO.sleep(200.millis)
        _       <- promise.succeed(())
        lines   <- collect.join.map(_.toList)
      } yield assertTrue(lines == List(line))
    }

  // Append raw bytes, flushing synchronously (open/write/close per call), so the on-disk state is settled before the
  // next poll observes it.
  private def appendBytes(path: Path, bytes: Array[Byte]): ZIO[Any, Throwable, Unit] =
    ZIO.attemptBlocking {
      Files.write(path, bytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
      ()
    }

  private def testRecentJobsOrdered = test("recentJobs returns ordered list") {
    for {
      _      <- deleteAllJobRuns
      runner <- ZIO.service[JobRunner]
      id1    <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, _ => ZIO.unit)
      _      <- awaitStatus(runner, id1)
      id2    <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, _ => ZIO.unit)
      _      <- awaitStatus(runner, id2)
      // Poll until all follow-up jobs settle (no Running jobs remain)
      _ <- runner.recentJobs(50).repeatUntil(_.forall(_.status != JobRunStatus.Running))
        .timeoutFail(new Exception("Follow-up jobs did not settle"))(10.seconds)
      recent <- runner.recentJobs(50)
    } yield assertTrue(
      recent.size >= 2,
      recent.head.startedAt.isAfter(recent.last.startedAt) || recent.head.startedAt == recent.last.startedAt
    )
  }
}
