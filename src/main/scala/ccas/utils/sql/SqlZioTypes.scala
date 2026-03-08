package ccas.utils.sql

import com.augustnagro.magnum.{DbTx, Transactor, transact}
import zio.{IO, ZIO}

import java.sql.SQLException

object SqlZioTypes {
  type SqlTask[+A] = IO[SQLException, A]

  /** Run a block of raw SQL in a transaction, as a ZIO effect. */
  def transactZIO[A](f: DbTx ?=> A): ZIO[Transactor, SQLException, A] =
    ZIO.serviceWithZIO[Transactor](xa =>
      ZIO.attempt(transact(xa)(f)).refineToOrDie[SQLException]
    )
}
