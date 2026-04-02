package ccas.utils.sql

import java.io.PrintWriter
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger as JLogger
import javax.sql.DataSource

import com.augustnagro.magnum.sql
import zio.{durationInt, ZIO}
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
    ZIO.service[PostgresClient].map { base =>
      val failDs = new FailOnceDataSource(base.transactor.dataSource, transient)
      val client = PostgresClient.fromDataSource(failDs, retryBaseDelay = 10.millis, retryMaxRetries = 3)
      (client, failDs)
    }

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
      result            <- client.connect(sql"SELECT 1".query[Int].run().head).exit
    } yield assertTrue(
      result.isFailure,
      failDs.attempts.get == 1
    )
  }

  /** DataSource wrapper that throws a SQLException on the first call to `getConnection`, then delegates. */
  private class FailOnceDataSource(delegate: DataSource, transient: Boolean) extends DataSource {
    val attempts                  = new AtomicInteger(0)
    private val failed            = new AtomicInteger(0)

    override def getConnection: java.sql.Connection = {
      attempts.incrementAndGet()
      if (failed.getAndIncrement() == 0) {
        val state = if (transient) "08006" else "23505"
        throw new SQLException("injected failure", state)
      }
      delegate.getConnection
    }

    override def getConnection(username: String, password: String): java.sql.Connection = getConnection
    override def getLogWriter: PrintWriter                                               = delegate.getLogWriter
    override def setLogWriter(out: PrintWriter): Unit                                    = delegate.setLogWriter(out)
    override def setLoginTimeout(seconds: Int): Unit                                     = delegate.setLoginTimeout(seconds)
    override def getLoginTimeout: Int                                                    = delegate.getLoginTimeout
    override def getParentLogger: JLogger                                                = delegate.getParentLogger
    override def unwrap[T](iface: Class[T]): T                                          = delegate.unwrap(iface)
    override def isWrapperFor(iface: Class[?]): Boolean                                  = delegate.isWrapperFor(iface)
  }
}
