package ccas.analysis.tables.clubadmin

import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}
import io.getquill.jdbczio.Quill
import io.getquill.{InsertMeta, SnakeCase, UpdateMeta, insertMeta, updateMeta}

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

object ClubMember {
  inline given UpdateMeta[ClubMember] = updateMeta(_.clubId, _.playerId, _.since)

  protected class PostgresRepo(quill: Quill.Postgres[SnakeCase]) {
    
  }
}
