package ccas.utils.sql

import java.sql.SQLException

import com.augustnagro.magnum.sql
import zio.ZIO
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.utils.sql.PostgresClient.connectZIO

object TestPostgresClientRetry extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("PostgresClient retry")(
    testConnectRetries,
    testTransactRetries,
    testWithTransactionRetries,
    testNonTransientNotRetried
  ).provideShared(
    FreshSchemaLayer("test_retry")
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private def riggedClient(transient: Boolean): ZIO[PostgresClient, Nothing, (PostgresClient, FailOnceDataSource)] =
    ZIO.serviceWith[PostgresClient](FailOnceDataSource.rig(_, transient))

  private def testConnectRetries = test("connect retries on transient error and succeeds") {
    for {
      (client, failDs) <- riggedClient(transient = true)
      result           <- client.connect(sql"SELECT 1".query[Int].run().head)
    } yield assertTrue(
      result == 1,
      failDs.attempts.get == 2
    )
  }

  private def testTransactRetries = test("transact retries on transient error and succeeds") {
    for {
      (client, failDs) <- riggedClient(transient = true)
      result           <- client.transact(sql"SELECT 1".query[Int].run().head)
    } yield assertTrue(
      result == 1,
      failDs.attempts.get == 2
    )
  }

  private def testWithTransactionRetries = test("withTransaction retries entire transaction on transient error") {
    for {
      (client, failDs) <- riggedClient(transient = true)
      result <- client.withTransaction[Any, SQLException, Int] {
        connectZIO(sql"SELECT 1".query[Int].run().head)
      }
    } yield assertTrue(
      result == 1,
      failDs.attempts.get == 2
    )
  }

  private def testNonTransientNotRetried = test("non-transient error is not retried") {
    for {
      (client, failDs) <- riggedClient(transient = false)
      result           <- client.connect(sql"SELECT 1".query[Int].run().head).exit
    } yield assertTrue(
      result.isFailure,
      failDs.attempts.get == 1
    )
  }
}
