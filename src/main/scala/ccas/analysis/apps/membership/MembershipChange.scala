package ccas.analysis.apps.membership

import ccas.analysis.tables.{ClubMember, Player, PlayerSnapshot}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{PlayerId, Username}
import zio.Chunk

import java.time.Instant

object MembershipChange {
  final case class MemberChangeSummary(playerId: PlayerId, changes: Chunk[MemberChange])

  object MemberChangeSummary {
    def apply(playerId: PlayerId, changes: Chunk[MemberChange]) =
      new MemberChangeSummary(playerId, changes.sortBy(_.timestamp))
  }

  final case class MemberState(player: PlayerSnapshot, member: ClubMember)

  sealed trait MemberChange {
    val timestamp: Instant
  }

  final case class NewMember(timestamp: Instant) extends MemberChange
  final case class JoinedClub(timestamp: Instant) extends MemberChange
  final case class LeftClub(timestamp: Instant) extends MemberChange
  final case class AccountClosed(timestamp: Instant, newStatus: PlayerStatusCategory) extends MemberChange
  final case class Rejoined(timestamp: Instant, previousUntil: Instant) extends MemberChange
  final case class Unresolvable(timestamp: Instant, oldUsername: Username) extends MemberChange
  final case class UsernameChange(timestamp: Instant, oldUsername: Username) extends MemberChange
  final case class StatusChange(timestamp: Instant, oldStatus: PlayerStatusCategory) extends MemberChange

  final case class DbState(
    membersByPlayerId: Map[PlayerId, MemberState],
    membersByUsername: Map[Username, MemberState],
  )

  final case class ReconciliationResult(
    changes: Chunk[MemberChangeSummary],
    newPlayers: Chunk[Player],
    newSnapshots: Chunk[PlayerSnapshot],
    newMemberships: Chunk[ClubMember],
    closedMemberships: Chunk[ClubMember],
  )
}
