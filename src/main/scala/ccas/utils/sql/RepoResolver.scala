package ccas.utils.sql

import io.getquill.SnakeCase
import io.getquill.jdbczio.Quill
import zio.{RLayer, Tag, ZIO, ZLayer}

case class RepoResolver[Repo: Tag](
  postgresSnake: Quill.Postgres[SnakeCase] => Repo = RepoResolver.defaultRepositoryResolver,
  sqliteSnake  : Quill.Sqlite[SnakeCase] => Repo = RepoResolver.defaultRepositoryResolver,
) {
  val live: RLayer[QuillWrapper, Repo] = ZLayer.fromZIO {
    ZIO.serviceWithZIO[QuillWrapper] {
      case QuillWrapper.Postgres(quill) => ZIO.attempt(postgresSnake(quill))
      case QuillWrapper.Sqlite(quill) => ZIO.attempt(sqliteSnake(quill))
    }
  }
}

object RepoResolver {
  /** Throws an error with information on the given quill and repo. */
  def defaultRepositoryResolver[Repo: Tag as tag, Q <: Quill[?, ?]](quill: Q): Nothing = {
    val repo = tag.tag.shortName
    val idiom = quill.idiom.getClass.getSimpleName
    val naming = quill.naming.getClass.getSimpleName
    throw new Exception(s"Repository `$repo` does not support idiom `$idiom` with naming strategy `$naming`.")
  }
}
