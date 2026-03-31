package ccas.analysis.apps.membership

import java.time.Instant

import zio.Chunk

import ccas.analysis.tables.{ClubMember, Player, PlayerSnapshot}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{PlayerId, Username}

object MembershipChange {
  final case class MemberChangeSummary(playerId: PlayerId, username: Username, changes: Chunk[MemberChange])

  object MemberChangeSummary {
    def apply(playerId: PlayerId, username: Username, changes: Chunk[MemberChange]) =
      new MemberChangeSummary(playerId, username, changes.sortBy(_.timestamp))
  }

  final case class MemberState(player: Player, member: ClubMember)

  enum MemberChange(val timestamp: Instant) {
    case NewMember(ts: Instant)                                      extends MemberChange(ts)
    case JoinedClub(ts: Instant)                                     extends MemberChange(ts)
    case Rejoined(ts: Instant, previousUntil: Instant)               extends MemberChange(ts)
    case LeftClub(ts: Instant)                                       extends MemberChange(ts)
    case AccountClosed(ts: Instant, newStatus: PlayerStatusCategory) extends MemberChange(ts)
    case Unresolvable(ts: Instant, oldUsername: Username)             extends MemberChange(ts)
    case UsernameChange(ts: Instant, oldUsername: Username)           extends MemberChange(ts)
    case StatusChange(ts: Instant, oldStatus: PlayerStatusCategory)  extends MemberChange(ts)
  }

  final case class DbState(
    membersByPlayerId: Map[PlayerId, MemberState],
    membersByUsername: Map[Username, MemberState],
    knownPlayersByUsername: Map[Username, Player] = Map.empty
  )

  final case class ReconciliationResult(
    changes: Chunk[MemberChangeSummary],
    newPlayers: Chunk[Player],
    updatedPlayers: Chunk[Player],
    archivedSnapshots: Chunk[PlayerSnapshot],
    newMemberships: Chunk[ClubMember],
    closedMemberships: Chunk[ClubMember],
    currentMemberCount: Int,
    previousMemberCount: Int,
    startedAt: Instant,
    completedAt: Instant
  )
}
