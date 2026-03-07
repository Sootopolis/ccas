package ccas.utils.sql

import io.getquill.SnakeCase
import io.getquill.jdbczio.Quill
import zio.TaskLayer

object QuillWrapper {
  private val defaultDataSourcePrefix = "database"

  def liveFromPrefix(prefix: String = defaultDataSourcePrefix): TaskLayer[Quill.Postgres[SnakeCase]] =
    Quill.DataSource.fromPrefix(prefix) >>> Quill.Postgres.fromNamingStrategy(SnakeCase)
}
