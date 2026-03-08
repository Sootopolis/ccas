package ccas.utils.sql

import ccas.utils.sql.SqlZioTypes.SqlTask
import com.augustnagro.magnum.Transactor
import zio.{RIO, RLayer, Tag, ZIO, ZLayer}

trait SqlRepoUtils {
  protected type Repo: Tag

  protected def makeRepo(xa: Transactor): Repo

  final def live: RLayer[Transactor, Repo] =
    ZLayer.fromFunction(makeRepo)

  final protected type RepoTask[+A] = RIO[Repo, A]

  final def repoService[A](f: Repo => SqlTask[A]): RepoTask[A] = ZIO.serviceWithZIO[Repo](f)
}
