package ccas.analysis.apps.membership

import java.time.Instant

import zio.Chunk

import ccas.analysis.tables.{ClubMember, Player, PlayerSnapshot}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}

object MembershipChange {
  final case class MemberChangeSummary(playerId: PlayerId, username: Username, changes: Chunk[MemberChange])

  object MemberChangeSummary {
    def apply(playerId: PlayerId, username: Username, changes: Chunk[MemberChange]) =
      new MemberChangeSummary(playerId, username, changes.sortBy(_.timestamp))
  }

  final case class MemberState(player: Player, member: ClubMember)

  private[membership] def later(a: Instant, b: Instant): Instant   = if (a.isAfter(b)) a else b
  private[membership] def earlier(a: Instant, b: Instant): Instant = if (a.isBefore(b)) a else b

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

  /** What we held about a club's membership before a reconcile, which the fetched roster is compared against: the
    * members its roster should list, keyed by id and by the name each holds now, every other row by player, every
    * player it has had, every slug it has held, and when its roster was last fetched.
    */
  final case class StoredRoster(
    membersByPlayerId: Map[PlayerId, MemberState],
    membersByUsername: Map[Username, MemberState],
    unlistedByPlayerId: Map[PlayerId, List[ClubMember]],
    lastObservedAt: Option[Instant],
    slugs: Set[ClubSlug],
    knownPlayersByUsername: Map[Username, Player] = Map.empty
  ) {

    /** `member` ended at `at`, or at its start if `at` precedes it, and was last known in no later than that. */
    def close(member: ClubMember, at: Instant): ClubMember = {
      val until = later(member.since, at)
      member.copy(lastSeen = Some(earlier(lastKnownIn(member), until)), until = Some(until))
    }

    /** When `member` was last known in, its account having closed after `lastOnline` with the club on its list. */
    def hiddenAt(member: ClubMember, lastOnline: Instant): Instant = later(lastKnownIn(member), lastOnline)

    /** The last time `member` is known to have been in, before this roster. A listed row was last listed by the club's
      * previous roster, kept within the row, which can start after that roster. No roster listed a guessed row, so it
      * was known in only at its start (ADR 0021).
      */
    private def lastKnownIn(member: ClubMember): Instant =
      member.lastSeen.getOrElse {
        val listedAt = lastObservedAt.filterNot(observed => member.sinceApproximate || observed.isBefore(member.since))
        listedAt.getOrElse(member.since)
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
    hiddenMemberships: Chunk[ClubMember],
    reopenedMemberships: Chunk[ClubMember],
    unsettledLists: Chunk[ClosedAccountClubs.Unsettled],
    currentMemberCount: Int,
    previousMemberCount: Int,
    startedAt: Instant,
    completedAt: Instant
  )
}
