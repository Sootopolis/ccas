package ccas.analysis.apps.membership

import ccas.analysis.tables.clubadmin.ClubMember
import ccas.analysis.tables.general.PlayerSnapshot
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{PlayerId, Username}
import zio.Chunk

import java.time.Instant

object MembershipChange {
  case class MemberChangeSummary(playerId: PlayerId, changes: Chunk[MemberChange])

  object MemberChangeSummary {
    def apply(playerId: PlayerId, changes: Chunk[MemberChange]) =
      new MemberChangeSummary(playerId, changes.sortBy(_.timestamp))
  }

  case class MemberState(player: PlayerSnapshot, member: ClubMember)

  sealed trait MemberChange {
    val timestamp: Instant
  }

  case class UsernameChange(
    timestamp  : Instant,
    oldUsername: Username,
  ) extends MemberChange

  case class StatusChange(
    timestamp : Instant,
    oldStatus : PlayerStatusCategory,
  ) extends MemberChange

  case class JoinedClub(timestamp: Instant) extends MemberChange
}
