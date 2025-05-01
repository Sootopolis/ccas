package ccas.utils.sql

import zio.{IO, ZIO}

import java.sql.SQLException

object SqlZioTypes {
  type SqlRIO[-R, +A] = ZIO[R, SQLException, A]
  type SqlIO[+A] = IO[SQLException, A]
}