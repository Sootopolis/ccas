package ccas.utils.sql

import ccas.utils.sql.SqlZioTypes.SqlTask
import zio.{RIO, RLayer, Tag, ZIO}

trait SqlRepoUtils {
  /** Type of the repository of the table, typically a `sealed trait`. */
  protected type Repo: Tag
  final protected type RepoTask[+A] = RIO[Repo, A]

  final def repoService[A](f: Repo => SqlTask[A]): RepoTask[A] = ZIO.serviceWithZIO[Repo](f)

  protected val repoResolver: RepoResolver[Repo]
  final lazy val live: RLayer[QuillWrapper, Repo] = repoResolver.live
}
