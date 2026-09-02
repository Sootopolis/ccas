package ccas.server

import zio.{RIO, UIO, URIO, ZIO, ZLayer}
import zio.stream.ZStream
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault, ZTestLogger}

import ccas.analysis.tables.{AppSetting, Club, RunTrigger}
import ccas.api.misc.subtypes.{ClubId, JobRunId}
import ccas.server.jobs.{JobKind, JobLogs, JobRun, JobRunner}
import ccas.utils.ProgressDisplay
import ccas.utils.client.{BodyStore, ChessComClient, TestChessComClientSupport}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}

/** Covers the composition in `CcasServer.retentionPass` rather than either sweep on its own: that both halves run in
  * one pass, cheap-and-bounded first, and that neither half's failure can skip the other or end the pass (#244).
  */
object TestRetentionPass extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRetentionPass")(
    testPassRunsBothHalvesFileSweepFirst,
    testTableHalfFailureStillRunsTheFileSweep,
    testFileHalfDefectStillRunsTheTableSweep
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private def testPassRunsBothHalvesFileSweepFirst =
    test("one pass sweeps job logs and then the API-diagnostics tables") {
      for {
        _       <- CcasServer.retentionPass
        logs    <- sweepLines
        fileIdx  = logs.indexWhere(_.contains("job log(s)"))
        tableIdx = logs.indexWhere(_.contains("cache rows"))
      } yield assertTrue(fileIdx >= 0, tableIdx >= 0, fileIdx < tableIdx)
    }.provide(
      FreshSchemaLayer("test_retention_pass_both", onInit = ServerTables.ensureTables),
      liveJobRunner,
      ZTestLogger.default
    )

  /** The table half needs `api_response_cache`; this schema deliberately holds only what `JobRunner` itself does, so
    * `Tables.retentionSweep` fails on a missing relation with no stubbing.
    */
  private def testTableHalfFailureStillRunsTheFileSweep =
    test("a failing table sweep is logged and does not skip the job-log sweep or end the pass") {
      for {
        exit <- CcasServer.retentionPass.exit
        logs <- sweepLines
        errs <- loggedMessages("Retention sweep failed")
      } yield assertTrue(exit.isSuccess, logs.exists(_.contains("job log(s)")), errs.nonEmpty)
    }.provide(
      FreshSchemaLayer(
        "test_retention_pass_broken",
        onInit = (Club.createTable *> JobRun.createTable *> AppSetting.createTable).unit
      ),
      liveJobRunner,
      ZTestLogger.default
    )

  /** `sweepLogs` absorbs its own failures, so only a defect can reach the job-log half's guard — hence a dying stub
    * where the table half gets a broken schema.
    */
  private def testFileHalfDefectStillRunsTheTableSweep =
    test("a job-log sweep that dies is logged and does not skip the table sweep or end the pass") {
      for {
        exit <- CcasServer.retentionPass.exit
        logs <- sweepLines
        errs <- loggedMessages("Job-log sweep failed")
      } yield assertTrue(exit.isSuccess, logs.exists(_.contains("cache rows")), errs.nonEmpty)
    }.provide(
      FreshSchemaLayer("test_retention_pass_dying", onInit = ServerTables.ensureTables),
      ZLayer.succeed[JobRunner](DyingJobRunner),
      ZTestLogger.default
    )

  private val sweepLines =
    ZTestLogger.logOutput.map(_.map(_.message()).filter(_.startsWith("Retention sweep:")))

  private def loggedMessages(fragment: String) =
    ZTestLogger.logOutput.map(_.map(_.message()).filter(_.contains(fragment)))

  // `ProgressDisplay.make` installs no ZLogger of its own, so the sweep's lines reach `ZTestLogger` (see TestJobRunner).
  private val liveJobRunner =
    ZLayer.makeSome[PostgresClient & BodyStore, JobRunner](
      TestChessComClientSupport.dummyLayer,
      ZLayer.succeed(ProgressDisplay.make(enabled = false)),
      JobRunner.live
    )

  private object DyingJobRunner extends JobRunner {
    private def unused: Nothing = throw new NotImplementedError("TestRetentionPass drives sweepLogs only")

    override def sweepLogs: URIO[PostgresClient, Int] = ZIO.die(new RuntimeException("simulated job-log sweep defect"))

    override def submit(
      kind: JobKind,
      clubId: Option[ClubId],
      params: Option[String],
      trigger: RunTrigger,
      effect: Option[JobRunId] => RIO[ProgressDisplay & ChessComClient & PostgresClient, Any]
    ): RIO[PostgresClient, JobRunId] = unused

    override def status(id: JobRunId): RIO[PostgresClient, Option[JobRun]] = unused
    override def cancel(id: JobRunId): UIO[Boolean]                        = unused
    override def recentJobs(limit: Int): RIO[PostgresClient, List[JobRun]] = unused

    override def logStream(id: JobRunId): RIO[PostgresClient, JobLogs] = unused

    override def progressStream(id: JobRunId): RIO[PostgresClient, Option[ZStream[Any, Throwable, String]]] = unused
  }
}
