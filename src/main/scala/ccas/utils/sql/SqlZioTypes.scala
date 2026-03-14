package ccas.utils.sql

import java.io.PrintWriter
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.sql.{Connection, SQLException}
import java.util.logging.Logger as JLogger
import javax.sql.DataSource

import com.augustnagro.magnum.{connect, transact, DbCon, DbTx, Transactor}
import zio.{IO, ZIO, ZLayer}

object SqlZioTypes {
  type SqlTask[+A] = IO[SQLException, A]

  /** Run a block of raw SQL with a connection, as a ZIO effect. */
  def connectZIO[A](f: DbCon ?=> A): ZIO[Transactor, SQLException, A] =
    ZIO.serviceWithZIO[Transactor](xa => ZIO.attempt(connect(xa)(f)).refineToOrDie[SQLException])

  /** Run a block of raw SQL in a transaction, as a ZIO effect. */
  def transactZIO[A](f: DbTx ?=> A): ZIO[Transactor, SQLException, A] =
    ZIO.serviceWithZIO[Transactor](xa => ZIO.attempt(transact(xa)(f)).refineToOrDie[SQLException])

  /** Run a ZIO effect that uses `connectZIO` calls within a single JDBC transaction.
    * Every `connectZIO` inside `f` shares the same underlying connection.
    * Commits on success, rolls back on failure or interruption.
    */
  def withTransaction[A](f: ZIO[Transactor, SQLException, A]): ZIO[Transactor, SQLException, A] =
    ZIO.serviceWithZIO[Transactor] { xa =>
      ZIO.acquireReleaseWith(
        acquire = ZIO.attempt(xa.dataSource.getConnection).refineToOrDie[SQLException]
      )(
        release = con => ZIO.succeed(con.close())
      ) { con =>
        con.setAutoCommit(false)
        val proxy = noCloseProxy(con)
        val scopedXa = xa.copy(dataSource = SingleConnectionDataSource(proxy))
        f.provideLayer(ZLayer.succeed(scopedXa))
          .foldCauseZIO(
            failure = cause => ZIO.attempt(con.rollback()).ignore *> ZIO.failCause(cause),
            success = a => ZIO.attempt(con.commit()).refineToOrDie[SQLException].as(a)
          )
      }
    }

  /** Wraps a Connection in a Proxy whose `close()` is a no-op. */
  private def noCloseProxy(con: Connection): Connection =
    Proxy.newProxyInstance(
      con.getClass.getClassLoader,
      Array(classOf[Connection]),
      (_, method: Method, args: Array[AnyRef]) =>
        if method.getName == "close" then ()
        else if args == null then method.invoke(con)
        else method.invoke(con, args*)
    ).asInstanceOf[Connection]

  /** Minimal DataSource that always returns the same (proxied) connection. */
  private class SingleConnectionDataSource(con: Connection) extends DataSource {
    override def getConnection: Connection = con
    override def getConnection(username: String, password: String): Connection = con
    override def getLogWriter: PrintWriter = null
    override def setLogWriter(out: PrintWriter): Unit = ()
    override def setLoginTimeout(seconds: Int): Unit = ()
    override def getLoginTimeout: Int = 0
    override def getParentLogger: JLogger = JLogger.getLogger("SingleConnectionDataSource")
    override def unwrap[T](iface: Class[T]): T =
      if iface.isInstance(this) then iface.cast(this)
      else throw new SQLException(s"Cannot unwrap to ${iface.getName}")
    override def isWrapperFor(iface: Class[?]): Boolean = iface.isInstance(this)
  }
}
