package ccas.analysis.tables

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.utils.sql.FreshSchemaLayer

/** Retention lives in its own schema on purpose: `retentionSweep` deletes every row past the retention window, which
  * in a shared suite would reach across into another test's fixtures depending on list order.
  */
object TestRetentionSweep extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRetentionSweep")(
    testEnsureTablesLeavesRetentionToTheSweep
  ).provideShared(
    FreshSchemaLayer("test_retention_sweep", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private val url  = "https://api.chess.com/pub/club/stale-test"
  private val body = """{"name":"stale-only-body"}"""

  // Far outside any retention window, so the sweep's own cutoff is the only thing under test.
  private val staleAt: Instant =
    LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC).minus(Duration.ofDays(400))

  private def testEnsureTablesLeavesRetentionToTheSweep =
    test("ensureTables leaves expired rows alone; retentionSweep removes them") {
      for {
        _ <- ApiResponseCache.upsertWithBody(
               url = url, body = body, etag = None, lastModified = None,
               maxAgeSeconds = None, contentType = None, fetchedAt = staleAt
             )
        _           <- Tables.ensureTables
        afterEnsure <- ApiResponseCache.lookupMeta(url)
        _           <- Tables.retentionSweep
        afterSweep  <- ApiResponseCache.lookupMeta(url)
      } yield assertTrue(afterEnsure.isDefined, afterSweep.isEmpty)
      // Live clock: `retentionSweep` derives its cutoff from `Clock.instant`, and the default TestClock sits at the
      // epoch, which makes every row look fresh.
    } @@ TestAspect.withLiveClock
}
