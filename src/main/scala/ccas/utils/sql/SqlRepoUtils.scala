package ccas.utils.sql

import ccas.utils.sql.SqlZioTypes.SqlTask
import io.getquill.SnakeCase
import io.getquill.jdbczio.Quill
import zio.{RIO, RLayer, Tag, ZIO, ZLayer}

trait SqlRepoUtils {
  protected type Repo: Tag

  protected def makeRepo(quill: Quill.Postgres[SnakeCase]): Repo

  final def live: RLayer[Quill.Postgres[SnakeCase], Repo] =
    ZLayer.fromFunction(makeRepo)

  final protected type RepoTask[+A] = RIO[Repo, A]

  final def repoService[A](f: Repo => SqlTask[A]): RepoTask[A] = ZIO.serviceWithZIO[Repo](f)
}
