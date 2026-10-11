package ccas.analysis.apps.membership

import java.time.Instant

import zio.Chunk

import ccas.analysis.tables.{ClubMember, Player, PlayerSnapshot}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}

object MembershipChange {
  final case class MemberChangeSummary(playerId: PlayerId, username: Username, changes: Chunk[MemberChange])

  object MemberChangeSummary {
    def apply(playerId: PlayerId, username: Username, changes: Chunk[MemberChange]) =
      new MemberChangeSummary(playerId, username, changes.sortBy(_.timestamp))
  }

  final case class MemberState(player: Player, member: ClubMember)

  enum MemberChange(val timestamp: Instant) {
    case NewMember(ts: Instant)                                                   extends MemberChange(ts)
    case JoinedClub(ts: Instant)                                                  extends MemberChange(ts)
    case Rejoined(ts: Instant, previousLastSeen: Instant, previousUntil: Instant) extends MemberChange(ts)
    case LeftClub(ts: Instant)                                                    extends MemberChange(ts)
    case AccountClosed(ts: Instant, newStatus: PlayerStatusCategory)              extends MemberChange(ts)
    case Unresolvable(ts: Instant, oldUsername: Username)                         extends MemberChange(ts)
    case UsernameChange(ts: Instant, oldUsername: Username)                       extends MemberChange(ts)
    case StatusChange(ts: Instant, oldStatus: PlayerStatusCategory)               extends MemberChange(ts)
  }

  /** What we held about a club's membership before a reconcile, which the fetched roster is compared against: its
    * current members, keyed by id and by the name each holds now, every player it has had, and when its roster was
    * last fetched.
    */
  final case class StoredRoster(
    membersByPlayerId: Map[PlayerId, MemberState],
    membersByUsername: Map[Username, MemberState],
    lastObservedAt: Option[Instant],
    knownPlayersByUsername: Map[Username, Player] = Map.empty
  ) {

    /** `member` ended at `at`, or at its start if `at` precedes it. A roster row was last known in at the club's
      * previous roster, which listed every open roster row, kept within the row, which can start after that roster or
      * end before it. No roster listed a guessed row, so it was known in only at its start (ADR 0021).
      */
    def close(member: ClubMember, at: Instant): ClubMember = {
      val until    = if (at.isBefore(member.since)) member.since else at
      val listedAt = lastObservedAt.filterNot(observed => member.sinceApproximate || observed.isBefore(member.since))
      val lastSeen = listedAt.fold(member.since)(observed => if (observed.isAfter(until)) until else observed)
      member.copy(lastSeen = Some(lastSeen), until = Some(until))
    }

    /** The name `state`'s player holds now. `membersByUsername` is keyed by exactly that, so a member absent from it
      * under the name its row stores holds none: the row keeps a name another player took as a display label.
      */
    def heldName(state: MemberState): Option[Username] =
      Option.when(membersByUsername.get(state.player.username).exists(_.player.playerId == state.player.playerId))(
        state.player.username
      )
  }

  final case class ReconciliationResult(
    clubId: ClubId,
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
