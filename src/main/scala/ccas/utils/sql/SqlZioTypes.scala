package ccas.utils.sql

import zio.IO

import java.sql.SQLException

object SqlZioTypes {
  type SqlTask[+A] = IO[SQLException, A]
}
