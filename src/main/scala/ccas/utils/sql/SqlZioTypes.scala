package ccas.utils.sql

import java.sql.SQLException

import com.augustnagro.magnum.{connect, transact, DbCon, DbTx, Transactor}
import zio.{IO, ZIO}

object SqlZioTypes {
  type SqlTask[+A] = IO[SQLException, A]

  /** Run a block of raw SQL with a connection, as a ZIO effect. */
  def connectZIO[A](f: DbCon ?=> A): ZIO[Transactor, SQLException, A] =
    ZIO.serviceWithZIO[Transactor](xa => ZIO.attempt(connect(xa)(f)).refineToOrDie[SQLException])

  /** Run a block of raw SQL in a transaction, as a ZIO effect. */
  def transactZIO[A](f: DbTx ?=> A): ZIO[Transactor, SQLException, A] =
    ZIO.serviceWithZIO[Transactor](xa => ZIO.attempt(transact(xa)(f)).refineToOrDie[SQLException])
}
