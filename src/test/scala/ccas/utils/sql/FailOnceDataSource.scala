package ccas.utils.sql

import java.io.PrintWriter
import java.sql.{Connection, SQLException}
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.Logger as JLogger
import javax.sql.DataSource

import zio.durationInt

/** DataSource wrapper that throws a SQLException on the first call to `getConnection`, then delegates. */
private[ccas] class FailOnceDataSource(delegate: DataSource, transient: Boolean) extends DataSource {
  val attempts       = new AtomicInteger(0)
  private val failed = new AtomicInteger(0)

  override def getConnection: Connection = {
    attempts.incrementAndGet()
    if (failed.getAndIncrement() == 0) {
      val state = if (transient) "08006" else "23505"
      throw new SQLException("injected failure", state)
    }
    delegate.getConnection
  }

  override def getConnection(username: String, password: String): Connection = getConnection
  override def getLogWriter: PrintWriter                                     = delegate.getLogWriter
  override def setLogWriter(out: PrintWriter): Unit                          = delegate.setLogWriter(out)
  override def setLoginTimeout(seconds: Int): Unit                           = delegate.setLoginTimeout(seconds)
  override def getLoginTimeout: Int                                          = delegate.getLoginTimeout
  override def getParentLogger: JLogger                                      = delegate.getParentLogger
  override def unwrap[T](iface: Class[T]): T                                 = delegate.unwrap(iface)
  override def isWrapperFor(iface: Class[?]): Boolean                        = delegate.isWrapperFor(iface)
}

private[ccas] object FailOnceDataSource {

  /** A client on `base`'s database whose first connection fails, and the data source that counts its attempts. */
  def rig(base: PostgresClient, transient: Boolean): (PostgresClient, FailOnceDataSource) = {
    val failDs = new FailOnceDataSource(base.transactor.dataSource, transient)
    val client = PostgresClient.fromDataSource(dataSource = failDs, retryBaseDelay = 10.millis, retryMaxRetries = 3)
    (client, failDs)
  }
}
