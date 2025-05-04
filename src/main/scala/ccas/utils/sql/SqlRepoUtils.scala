package ccas.utils.sql

import ccas.utils.sql.SqlZioTypes.SqlTask
import zio.{RIO, RLayer, Tag, ZIO}

trait SqlRepoUtils {
  protected type Repo: Tag

  protected val repoResolver: RepoResolver[Repo]

  final def live: RLayer[QuillWrapper, Repo] = repoResolver.live

  final protected type RepoTask[+A] = RIO[Repo, A]

  final def repoService[A](f: Repo => SqlTask[A]): RepoTask[A] = ZIO.serviceWithZIO[Repo](f)
}
