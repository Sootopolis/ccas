package ccas.utils.sql

import io.getquill.SnakeCase
import zio.{Chunk, ZIO}

import javax.sql.DataSource

trait CcasSql[T] { self =>
  val tableName: String = SnakeCase.table(self.getClass.getSimpleName)

  def insert(value: T): ZIO[DataSource, Throwable, Unit]

  def insert(values: IterableOnce[T]): ZIO[DataSource, Throwable, Unit]

  def update(value: T): ZIO[DataSource, Throwable, Unit]

  def update(values: IterableOnce[T]): ZIO[DataSource, Throwable, Unit]

  def selectAll: ZIO[DataSource, Throwable, Chunk[T]]

  def createTable: ZIO[DataSource, Throwable, Unit]

  def dropTable: ZIO[DataSource, Throwable, Unit]

  def clearTable: ZIO[DataSource, Throwable, Unit]
}
