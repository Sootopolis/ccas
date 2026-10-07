package ccas.utils.sql

import java.io.{OutputStream, PrintWriter}
import java.lang.reflect.{Method, Proxy}
import java.sql.{Connection, SQLException}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger
import javax.sql.DataSource

import zio.*
import zio.test.*

/** Behavioural tests for #193 and #304: `connect`, `transact` and `withTransaction` check out on
  * `attemptBlockingInterrupt`, so interrupting a fiber parked in a blocking `getConnection` completes promptly (on
  * shutdown/cancel) instead of hanging until the connection wait returns on its own. With plain `attemptBlocking` the
  * interrupt would never return and the `Live.live` timeout would fail the assertion. Each asserts the exit is
  * interruption only, so a defect raised while releasing the checkout fails it too.
  *
  * Uses fake DataSources whose `getConnection` blocks until interrupted, so no real Postgres is required. The real
  * (Live) clock is used because the block/interrupt happens on an OS thread in wall-clock time — a frozen TestClock
  * would let a regression hang the suite instead of failing it.
  */
object TestPostgresClientInterrupt extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] = suite("PostgresClient interruptibility (#193, #304)")(
    test("interrupting a connect blocked in getConnection completes promptly") {
      val entered = new CountDownLatch(1)
      val client  = PostgresClient.fromDataSource(new BlockingDataSource(entered))
      for {
        result <- interruptInCheckout(entered, client.connect(1))
      } yield assertTrue(result.exists(_.isInterruptedOnly))
    },
    test("interrupting a transact blocked in getConnection completes promptly") {
      val entered = new CountDownLatch(1)
      val client  = PostgresClient.fromDataSource(new BlockingDataSource(entered))
      for {
        result <- interruptInCheckout(entered, client.transact(1))
      } yield assertTrue(result.exists(_.isInterruptedOnly))
    },
    test("interrupting a withTransaction blocked in getConnection completes promptly") {
      val entered = new CountDownLatch(1)
      val client  = PostgresClient.fromDataSource(new BlockingDataSource(entered))
      for {
        result <- interruptInCheckout(entered, client.withTransaction(ZIO.unit))
      } yield assertTrue(result.exists(_.isInterruptedOnly))
    },
    test("withTransaction closes a connection handed over just as the interrupt lands") {
      val entered = new CountDownLatch(1)
      val closed  = new AtomicBoolean(false)
      val client  = PostgresClient.fromDataSource(new HandOverOnInterruptDataSource(entered, closeRecording(closed)))
      for {
        result <- interruptInCheckout(entered, client.withTransaction(ZIO.unit))
      } yield assertTrue(
        result.exists(_.isInterruptedOnly),
        closed.get // else the pool would never get it back
      )
    }
  )

  /** Forks `effect`, waits until it is inside `getConnection`, then interrupts it. `None` if the interrupt has not
    * completed within 10 seconds.
    */
  private def interruptInCheckout[A](
    entered: CountDownLatch,
    effect: IO[SQLException, A]
  ): URIO[Live, Option[Exit[SQLException, A]]] =
    for {
      fiber   <- effect.fork
      reached <- ZIO.attemptBlocking(entered.await(10, TimeUnit.SECONDS)).orDie
      _       <- ZIO.whenDiscard(!reached)(ZIO.dieMessage("getConnection was never reached"))
      result  <- Live.live(fiber.interrupt.timeout(10.seconds))
    } yield result

  /** A Connection that records being closed and supports nothing else. */
  private def closeRecording(closed: AtomicBoolean): Connection =
    Proxy
      .newProxyInstance(
        getClass.getClassLoader,
        Array(classOf[Connection]),
        (_, method: Method, _) =>
          method.getName match {
            case "close" => closed.set(true); null
            case other   => throw new UnsupportedOperationException(other)
          }
      )
      .asInstanceOf[Connection] // safe: proxy implements Connection interface

  /** A DataSource whose `getConnection` blocks until the calling thread is interrupted. */
  private final class BlockingDataSource(entered: CountDownLatch) extends FakeDataSource {
    override def getConnection: Connection = {
      entered.countDown()
      Thread.sleep(Long.MaxValue) // returns only via InterruptedException when the fiber is interrupted
      throw new SQLException("unreachable")
    }
  }

  /** A DataSource whose `getConnection` blocks until the calling thread is interrupted, then returns `conn` anyway: the
    * pool handing over a connection at the moment the interrupt lands.
    */
  private final class HandOverOnInterruptDataSource(entered: CountDownLatch, conn: Connection) extends FakeDataSource {
    override def getConnection: Connection = {
      entered.countDown()
      try {
        Thread.sleep(Long.MaxValue)
      } catch {
        case _: InterruptedException => ()
      }
      conn
    }
  }

  private abstract class FakeDataSource extends DataSource {
    override def getConnection(username: String, password: String): Connection = getConnection
    override def getLogWriter: PrintWriter            = new PrintWriter(OutputStream.nullOutputStream)
    override def setLogWriter(out: PrintWriter): Unit = ()
    override def setLoginTimeout(seconds: Int): Unit  = ()
    override def getLoginTimeout: Int                 = 0
    override def getParentLogger: Logger              = Logger.getLogger(getClass.getSimpleName)
    override def unwrap[T](iface: Class[T]): T =
      if (iface.isInstance(this)) { iface.cast(this) }
      else { throw new SQLException(s"Cannot unwrap to ${iface.getName}") }
    override def isWrapperFor(iface: Class[?]): Boolean = iface.isInstance(this)
  }
}
