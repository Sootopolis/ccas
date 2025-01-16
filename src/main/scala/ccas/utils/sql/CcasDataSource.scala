package ccas.utils.sql

import io.getquill.jdbczio.Quill.DataSource
import org.postgresql.ds.PGSimpleDataSource
import zio.TaskLayer

type CcasDataSource = javax.sql.DataSource

object CcasDataSource {
  val layer: TaskLayer[CcasDataSource] = DataSource.fromPrefix("database")
}
