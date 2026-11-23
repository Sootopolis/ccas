package ccas.utils.sql

/** How a list's element goes into a PostgreSQL array: the array's element type, and the value JDBC is given for it.
  * Only a type stored as its raw value has one, since the array bypasses the element's `DbCodec`; a list of anything
  * else, such as an enum or a URL, does not compile (#538).
  */
trait ArrayElement[T] {
  def sqlTypeName: String
  def toJdbc(value: T): AnyRef
}

object ArrayElement {
  def text[T](raw: T => String): ArrayElement[T] = element("TEXT", raw)

  def bigint[T](raw: T => Long): ArrayElement[T] = element("BIGINT", value => Long.box(raw(value)))

  def integer[T](raw: T => Int): ArrayElement[T] = element("INTEGER", value => Int.box(raw(value)))

  given ArrayElement[String] = text(identity)
  given ArrayElement[Long]   = bigint(identity)
  given ArrayElement[Int]    = integer(identity)

  private def element[T](typeName: String, encode: T => AnyRef): ArrayElement[T] = new ArrayElement[T] {
    def sqlTypeName: String      = typeName
    def toJdbc(value: T): AnyRef = encode(value)
  }
}
