package ccas.analysis.tables

import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}
import ccas.utils.sql.{RepoResolver, SqlRepoUtils}
import io.getquill.context.sql.idiom.SqlIdiom
import io.getquill.jdbczio.Quill
import io.getquill.{SnakeCase, UpdateMeta, updateMeta}

import java.time.Instant

final case class ClubMember(
  clubId  : ClubId,
  playerId: PlayerId,
  since   : Instant,
  username: Username,
  status  : PlayerStatusCategory,
  until   : Option[Instant]
) {
  def isCurrent: Boolean = until.isEmpty
}

object ClubMember extends SqlRepoUtils {
  inline given UpdateMeta[ClubMember] = updateMeta(_.clubId, _.playerId, _.since)

  override protected type Repo = ClubMemberRepo

  override protected val repoResolver: RepoResolver[Repo] = RepoResolver(
    postgresSnake = PostgresRepo.apply,
    sqliteSnake = SqliteRepo.apply,
  )

  sealed trait ClubMemberRepo {
    val quill: Quill[? <: SqlIdiom, SnakeCase]
  }

  private case class PostgresRepo(quill: Quill.Postgres[SnakeCase]) extends Repo

  private case class SqliteRepo(quill: Quill.Sqlite[SnakeCase]) extends Repo
}
