package ccas.analysis.tables.clubadmin

import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}

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
    
}
