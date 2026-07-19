package ccas.server.jobs

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import scala.jdk.CollectionConverters.*

import com.typesafe.config.ConfigFactory

import ccas.utils.sql.PostgresClient
import zio.{durationInt, Promise, Ref, ZIO, ZLayer}
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
    testConflictWhileCancelInFlight,
    testConcurrentSubmitConflict,
    testSubmitAllowsDifferentClub,
    testSubmitAllowsDifferentKind,
    testStatusUnknown,
    testCancelInterruptsRunningJob,
    testCancelUnknownReturnsFalse,
    testNonOperatorInterruptDoesNotMarkCancelled,
    testRecentJobsOrdered,
    testSubmitWritesJobLog,
    testLogStreamReplaysCompletedJob,
    testLogStreamUnknownReturnsNone,
    testLogStreamTailsLiveJob,
    testLogStreamCarriesPartialLineAcrossTicks,
    testLogStreamJoinsMultibyteCharAcrossTicks,
    testLogStreamReassemblesCjkAndEmojiTornMidChar,
    testProgressStreamTailsLiveJobAndCloses,
    testProgressStreamUnknownReturnsNone
  ).provideShared(
    FreshSchemaLayer("test_job_runner", onInit = ServerTables.ensureTables),
    TestChessComClientSupport.dummyLayer,
    JobRunner.live,
    // #132 regression seam: provide ProgressDisplay as a plain *service* with NO ambient ZLogger. `ProgressDisplay.live`
    // would install `asZLogger` into `currentLoggers` for the whole suite fiber tree, so every `submit` would inherit it
    // and fill the per-job file even on the unfixed code — masking the bug. `make` installs no logger, so each `submit`
    // here runs from the same default-logger context as a zio-http request handler (the condition #132 fails under).
    // The fix wraps each job in `display.installLogger`, so the per-job-log assertions (testSubmitWritesJobLog,
    // testLogStreamReplaysCompletedJob, testLogStreamTailsLiveJob) pass only with the fix applied. Caveat: without
    // `live`'s suite-wide default-logger removal, a test that logs at the test-fiber level (outside a job) hits the
    // default logger, not `asZLogger` — assert per-job-log behaviour only through `submit`.
    ZLayer.succeed(ProgressDisplay.make(enabled = false))
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

  // #170: a resubmit that races an in-flight cancel of the same (kind, club) must read the self-resolving "finishing
  // cancellation — retry in a moment" wording, not the baffling "already running". The job parks in an UNINTERRUPTIBLE
  // await so, after `cancel` registers it in `cancelRequested` and forks the interrupt, the interrupt stays pending: the
  // row is still Running AND the id sits in `cancelRequested` — the exact window `submit` special-cases. Releasing the
  // gate ends the uninterruptible region, the pending interrupt lands, and the job settles to Cancelled.
  private def testConflictWhileCancelInFlight =
    test("a resubmit racing an in-flight cancel gets the 'finishing cancellation' message") {
      for {
        _       <- deleteAllJobRuns
        runner  <- ZIO.service[JobRunner]
        started <- Promise.make[Nothing, Unit]
        gate    <- Promise.make[Nothing, Unit]
        id <- runner.submit(
          JobKind.Membership,
          Some(clubIdA),
          None,
          RunTrigger.Cli,
          _ => (started.succeed(()) *> gate.await).uninterruptible
        )
        _      <- started.await
        _      <- runner.cancel(id) // registers id in cancelRequested; the forked interrupt can't land (uninterruptible)
        result <- runner.submit(JobKind.Membership, Some(clubIdA), None, RunTrigger.Cli, _ => ZIO.unit).either
        _      <- gate.succeed(()) // release the await → pending interrupt lands → job settles Cancelled
        _      <- awaitStatus(runner, id)
      } yield assertTrue(
        result.left.exists {
          case e: ConflictException => e.getMessage.contains("finishing cancellation")
          case _                    => false
        }
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

  private def testCancelInterruptsRunningJob = test("cancel interrupts a running job and marks it Cancelled") {
    for {
      _       <- deleteAllJobRuns
      runner  <- ZIO.service[JobRunner]
      started <- Promise.make[Nothing, Unit]
      // The job announces it is running, then blocks forever — so it is unambiguously live when we cancel.
      id  <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, _ => started.succeed(()) *> ZIO.never)
      _   <- started.await
      cancelled <- runner.cancel(id)
      job       <- awaitStatus(runner, id) // returns once the job leaves Running (here: Cancelled via onInterrupt)
    } yield assertTrue(
      cancelled,
      job.status == JobRunStatus.Cancelled,
      job.completedAt.isDefined,
      job.error.exists(_.contains("Cancelled"))
    )
  }

  private def testCancelUnknownReturnsFalse = test("cancel returns false for unknown id") {
    for {
      runner <- ZIO.service[JobRunner]
      result <- runner.cancel(JobRunId.wrap("does-not-exist"))
    } yield assertTrue(!result)
  }

  // Guards the cancel-vs-shutdown distinction: the job interrupts ITSELF (stands in for the `layerScope` interrupt fired
  // at every in-flight job on server shutdown), so no operator `cancel` ran and no id is in `cancelRequested`. The
  // onInterrupt hook must therefore NOT write Cancelled — the row stays Running for the next boot's orphan sweep. Were
  // the gate absent, this would flip to Cancelled/"Cancelled by operator", mislabeling a shutdown as an operator cancel.
  private def testNonOperatorInterruptDoesNotMarkCancelled =
    test("a job interrupted without an operator cancel is left Running, not marked Cancelled") {
      for {
        _      <- deleteAllJobRuns
        runner <- ZIO.service[JobRunner]
        id     <- runner.submit(JobKind.MatchRef, None, None, RunTrigger.Cli, _ => ZIO.interrupt)
        // The fiber self-interrupts immediately; wait past its unwind + release, then confirm the row never left Running.
        _   <- ZIO.sleep(300.millis)
        job <- runner.status(id)
      } yield assertTrue(job.exists(j => j.status == JobRunStatus.Running && j.completedAt.isEmpty))
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

  // End-to-end for `progressStream`: submit → `currentChannel.locally` → a job bar publishes into the per-job channel →
  // the stream emits its snapshot → the job completes → `interruptWhen(promise)` closes the stream. We wait for a bar
  // frame *before* releasing the job (deterministic — otherwise the job could finish and drop the bar before the
  // subscriber observes it), then assert the stream ended on completion. Global-gauge merge is None here (the suite's
  // `ProgressDisplay.make` has no global channel), so this covers the `(None, Some(job))` path + the completion halt.
  private def testProgressStreamTailsLiveJobAndCloses =
    test("progressStream emits a running job's bar snapshots and closes when it finishes") {
      for {
        _      <- deleteAllJobRuns
        runner <- ZIO.service[JobRunner]
        gate   <- Promise.make[Nothing, Unit]
        id <- runner.submit(
          JobKind.MatchRef,
          None,
          None,
          RunTrigger.Cli,
          _ =>
            ZIO.scoped {
              for {
                bar <- ProgressDisplay.progressBar
                _   <- bar.print(1, 2, "working")
                _   <- gate.await // stay Running until the subscriber has seen the bar frame
              } yield ()
            }
        )
        streamOpt <- runner.progressStream(id)
        stream    <- ZIO.fromOption(streamOpt).orElseFail(new Exception("expected a live progress stream"))
        seen      <- Promise.make[Nothing, Unit]
        framesRef <- Ref.make(List.empty[String])
        collect <- stream
          .runForeach(f => framesRef.update(f :: _) *> ZIO.whenDiscard(f.contains("working"))(seen.succeed(()).unit))
          .fork
        _      <- seen.await.timeoutFail(new Exception("no bar frame observed"))(10.seconds)
        _      <- gate.succeed(())
        _      <- awaitStatus(runner, id)
        _      <- collect.join // returns only if the stream ended — proves close-on-completion
        frames <- framesRef.get
      } yield assertTrue(frames.exists(_.contains("\"text\":\"working\"")))
    }

  private def testProgressStreamUnknownReturnsNone = test("progressStream returns None for unknown id") {
    for {
      runner <- ZIO.service[JobRunner]
      result <- runner.progressStream(JobRunId.wrap("does-not-exist"))
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
