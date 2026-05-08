package ccas.utils.sql

import java.sql.SQLException
import java.time.{Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.ZIO

import ccas.analysis.tables.RunTrigger
import ccas.api.misc.subtypes.JobRunId
import ccas.server.jobs.{JobKind, JobRun, JobRunStatus}
import ccas.server.ServerTables
import ccas.utils.sql.PostgresClient.withTransaction

object TestWithTransaction extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestWithTransaction")(
    testCommitOnSuccess,
    testRollbackOnSqlException,
    testRollbackOnZioFail,
    testRollbackOnDefect,
    testConnectionSharing
  ).provideShared(
    FreshSchemaLayer("test_with_tx", onInit = ServerTables.ensureTables)
  ) @@ TestAspect.sequential

  private val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  private val idA = JobRunId.wrap("tx-test-a")
  private val idB = JobRunId.wrap("tx-test-b")

  private val runA = JobRun(idA, JobKind.Recruitment, None, RunTrigger.Cli, JobRunStatus.Running, None, t0, None, None)
  private val runB = JobRun(idB, JobKind.Membership, None, RunTrigger.Cli, JobRunStatus.Running, None, t0, None, None)

  private val deleteAll = TestDbCleanup.clearJobRuns

  // --- Tests ---

  private def testCommitOnSuccess = test("commit on success: both inserts are visible") {
    for {
      _ <- deleteAll
      _ <- withTransaction(JobRun.insert(runA) *> JobRun.insert(runB))
      a <- JobRun.selectId(idA)
      b <- JobRun.selectId(idB)
    } yield assertTrue(
      a.isDefined,
      b.isDefined
    )
  }

  private def testRollbackOnSqlException = test("rollback on SQLException: first insert is undone") {
    for {
      _      <- deleteAll
      result <- withTransaction(JobRun.insert(runA) *> JobRun.insert(runA)).exit
      a      <- JobRun.selectId(idA)
    } yield assertTrue(
      result.isFailure,
      a.isEmpty
    )
  }

  private def testRollbackOnZioFail = test("rollback on ZIO.fail: first insert is undone") {
    for {
      _      <- deleteAll
      result <- withTransaction(JobRun.insert(runA) *> ZIO.fail(new SQLException("simulated"))).exit
      a      <- JobRun.selectId(idA)
    } yield assertTrue(
      result.isFailure,
      a.isEmpty
    )
  }

  private def testRollbackOnDefect = test("rollback on defect: first insert is undone") {
    for {
      _      <- deleteAll
      result <- withTransaction(JobRun.insert(runA) *> ZIO.die(new RuntimeException("boom"))).exit
      a      <- JobRun.selectId(idA)
    } yield assertTrue(
      result.isFailure,
      a.isEmpty
    )
  }

  private def testConnectionSharing = test("connectZIO calls inside withTransaction share one connection") {
    for {
      _ <- deleteAll
      a <- withTransaction(JobRun.insert(runA) *> JobRun.selectId(idA))
    } yield assertTrue(
      a.isDefined,
      a.get.id == idA
    )
  }
}
