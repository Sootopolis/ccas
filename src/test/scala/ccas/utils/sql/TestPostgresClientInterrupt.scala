package ccas.utils.sql

import java.io.PrintWriter
import java.sql.{Connection, SQLException}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.logging.Logger
import javax.sql.DataSource

import zio.*
import zio.test.*

/** Behavioural test for the #193 fix that the pure `isTransient` test can't reach: `PostgresClient.connect` runs on
  * `attemptBlockingInterrupt`, so interrupting a fiber parked in a blocking `getConnection` completes promptly (on
  * shutdown/cancel) instead of hanging until the connection wait returns on its own. With plain `attemptBlocking` the
  * interrupt below would never return and the `Live.live` timeout would fail the assertion.
  *
  * Uses a fake DataSource whose `getConnection` blocks forever, so no real Postgres is required. The real (Live) clock
  * is used because the block/interrupt happens on an OS thread in wall-clock time — a frozen TestClock would let a
  * regression hang the suite instead of failing it.
  */
object TestPostgresClientInterrupt extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PostgresClient interruptibility (#193)")(
    test("interrupting a fiber blocked in getConnection completes promptly (attemptBlockingInterrupt)") {
      val entered = new CountDownLatch(1)
      val client  = PostgresClient.fromDataSource(new BlockingDataSource(entered))
      for {
        fiber   <- client.connect(1).fork
        reached <- ZIO.attemptBlocking(entered.await(10, TimeUnit.SECONDS)).orDie // true once getConnection is blocking
        _       <- ZIO.dieMessage("getConnection was never reached").unless(reached)
        result  <- Live.live(fiber.interrupt.timeout(10.seconds))
      } yield assertTrue(result.exists(_.isInterrupted)) // exited via interruption; plain attemptBlocking would hang
    }
  )

  /** A DataSource whose `getConnection` blocks until the calling thread is interrupted. */
  private final class BlockingDataSource(entered: CountDownLatch) extends DataSource {
    override def getConnection: Connection = {
      entered.countDown()
      Thread.sleep(Long.MaxValue) // returns only via InterruptedException when the fiber is interrupted
      throw new SQLException("unreachable")
    }
    override def getConnection(username: String, password: String): Connection = getConnection
    override def getLogWriter: PrintWriter            = new PrintWriter(java.io.OutputStream.nullOutputStream)
    override def setLogWriter(out: PrintWriter): Unit = ()
    override def setLoginTimeout(seconds: Int): Unit  = ()
    override def getLoginTimeout: Int                 = 0
    override def getParentLogger: Logger              = Logger.getLogger("BlockingDataSource")
    override def unwrap[T](iface: Class[T]): T =
      if (iface.isInstance(this)) { iface.cast(this) }
      else { throw new SQLException(s"Cannot unwrap to ${iface.getName}") }
    override def isWrapperFor(iface: Class[?]): Boolean = iface.isInstance(this)
  }
}
