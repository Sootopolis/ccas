package ccas.analysis.tables

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.ZIO
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

object TestApiFetchFailureSql extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestApiFetchFailureSql")(
    testInsertWithoutBody,
    testInsertWithBody,
    testInsertDeduplicatesBody,
    testSelectRecent,
    testDeleteBeforeCleansOrphans,
    testDeleteAllCleansOrphans
  ).provideShared(
    FreshSchemaLayer("test_api_fetch_failure", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(2))
    val t3: Instant = t0.plus(Duration.ofDays(3))
  }

  private def countBodies: ZIO[PostgresClient, Throwable, Int] =
    connectZIO(sql"SELECT COUNT(*) FROM api_response_body".query[Long].run().head.toInt)

  private def countFailures: ZIO[PostgresClient, Throwable, Int] =
    connectZIO(sql"SELECT COUNT(*) FROM api_fetch_failure".query[Long].run().head.toInt)

  private def testInsertWithoutBody = test("insert without response body") {
    val item = ApiFetchFailure(Times.t0, "https://example.com/a", "Timeout", Some("timed out"), None)
    for {
      rows    <- ApiFetchFailure.insert(item)
      bodies  <- countBodies
      results <- ApiFetchFailure.selectRecent(Times.t0)
    } yield assertTrue(
      rows == 1,
      bodies == 0,
      results.size == 1,
      results.head.url == "https://example.com/a",
      results.head.responseBody.isEmpty
    )
  }

  private def testInsertWithBody = test("insert with response body creates api_response_body row") {
    val item = ApiFetchFailure(Times.t1, "https://example.com/b", "Http429", None, Some("rate limited"))
    for {
      rows   <- ApiFetchFailure.insert(item)
      bodies <- countBodies
    } yield assertTrue(
      rows == 1,
      bodies == 1
    )
  }

  private def testInsertDeduplicatesBody = test("insert with same body reuses existing api_response_body") {
    val item = ApiFetchFailure(Times.t2, "https://example.com/c", "Http429", None, Some("rate limited"))
    for {
      _      <- ApiFetchFailure.insert(item)
      bodies <- countBodies
    } yield assertTrue(bodies == 1) // same body text, so still just 1 body row
  }

  private def testSelectRecent = test("selectRecent filters by time") {
    for {
      results <- ApiFetchFailure.selectRecent(Times.t1)
    } yield assertTrue(
      results.size == 2, // t1 and t2 items, not t0
      results.forall(_.occurredAt.compareTo(Times.t1) >= 0)
    )
  }

  private def testDeleteBeforeCleansOrphans = test("deleteBefore removes failures and orphaned bodies") {
    for {
      failuresBefore <- countFailures
      deleted        <- ApiFetchFailure.deleteBefore(Times.t2)
      failuresAfter  <- countFailures
      bodiesAfter    <- countBodies
    } yield assertTrue(
      deleted == 2, // t0 and t1 items
      failuresAfter == failuresBefore - 2,
      bodiesAfter == 1 // body for "rate limited" still referenced by t2 item
    )
  }

  private def testDeleteAllCleansOrphans = test("deleteAll removes all failures and bodies") {
    for {
      deleted     <- connectZIO(sql"DELETE FROM api_fetch_failure".update.run())
      _           <- connectZIO(sql"DELETE FROM api_response_body".update.run())
      failures    <- countFailures
      bodiesAfter <- countBodies
    } yield assertTrue(
      deleted == 1, // t2 item
      failures == 0,
      bodiesAfter == 0
    )
  }
}
