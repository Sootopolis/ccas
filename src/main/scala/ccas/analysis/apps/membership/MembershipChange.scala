package ccas.analysis.apps.membership

import java.time.Instant

import zio.Chunk

import ccas.analysis.tables.{ClubMember, MembershipStanding, Player, PlayerSnapshot}
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
    case NewMember(ts: Instant)                                      extends MemberChange(ts)
    case JoinedClub(ts: Instant)                                     extends MemberChange(ts)
    case Rejoined(ts: Instant, previous: PreviousMembership)         extends MemberChange(ts)
    case Reopened(ts: Instant, was: MembershipStanding.OffRoster)    extends MemberChange(ts)
    case LeftClub(ts: Instant)                                       extends MemberChange(ts)
    case AccountClosed(ts: Instant, newStatus: PlayerStatusCategory) extends MemberChange(ts)
    case Unresolvable(ts: Instant, oldUsername: Username)            extends MemberChange(ts)
    case UsernameChange(ts: Instant, oldUsername: Username)          extends MemberChange(ts)
    case StatusChange(ts: Instant, oldStatus: PlayerStatusCategory, newStatus: PlayerStatusCategory)
        extends MemberChange(ts)

    /** Whether this only happened if the player's rows changed, so a refused verdict takes it back (ADR 0027). */
    def changesRows: Boolean = this match {
      case _: NewMember | _: JoinedClub | _: Rejoined | _: Reopened | _: LeftClub | _: Unresolvable => true
      case _: AccountClosed | _: UsernameChange | _: StatusChange                                   => false
    }

    /** Whether this puts a player on the active roster. */
    def gains: Boolean = this match {
      case _: NewMember | _: JoinedClub | _: Rejoined | _: Reopened => true
      case StatusChange(_, _, newStatus)                            => newStatus == PlayerStatusCategory.Active
      case _: LeftClub | _: AccountClosed | _: Unresolvable | _: UsernameChange => false
    }

    /** Whether this takes a player off the active roster. */
    def loses: Boolean = this match {
      case _: LeftClub | _: AccountClosed | _: Unresolvable => true
      case StatusChange(_, oldStatus, _)                    => oldStatus == PlayerStatusCategory.Active
      case _: NewMember | _: JoinedClub | _: Rejoined | _: Reopened | _: UsernameChange => false
    }
  }

  object MemberChange {

    /** A join at `ts` following the row `previousOption`: a rejoin if that row has ended. */
    def join(ts: Instant, previousOption: Option[ClubMember]): MemberChange =
      previousOption.flatMap(PreviousMembership.of).fold[MemberChange](JoinedClub(ts))(Rejoined(ts, _))
  }

  /** The membership a rejoin follows. */
  enum PreviousMembership {

    /** One a roster recorded, which ended between `lastSeen` and `until`. */
    case Recorded(lastSeen: Instant, until: Instant)

    /** One inferred from the matches the player played for the club (ADR 0023). */
    case Inferred(firstMatch: Instant, lastMatch: Instant)
  }

  object PreviousMembership {

    /** The membership `row` recorded, once it has ended. */
    def of(row: ClubMember): Option[PreviousMembership] =
      row.standing match {
        case MembershipStanding.Ended(lastSeen, until)              => Some(Recorded(lastSeen, until))
        case MembershipStanding.Open | _: MembershipStanding.Hidden => None
      }
  }

  /** A club's active members at the previous report's end and after this reconcile's save, both read from the rows
    * (ADR 0026).
    */
  final case class RosterMovement(before: Set[PlayerId], after: Set[PlayerId]) {
    def gains: Set[PlayerId]  = after -- before
    def losses: Set[PlayerId] = before -- after
    def net: Int              = after.size - before.size
  }

  object RosterMovement {

    /** Those of `memberIds` whose account was active at `at`, by the states each has been in. */
    def activeAt(memberIds: Set[PlayerId], states: List[PlayerSnapshot], at: Instant): Set[PlayerId] = {
      val activePlayers = states
        .filterNot(_.since.isAfter(at))
        .groupBy(_.playerId)
        .flatMap { case (playerId, history) =>
          history.maxByOption(_.since).filter(_.status == PlayerStatusCategory.Active).map(_ => playerId)
        }
        .toSet
      memberIds.intersect(activePlayers)
    }
  }

  /** A join a player's rows cannot place, which changes none of them (ADR 0026). */
  final case class UnsettledJoin(playerId: PlayerId, username: Username, joinedAt: Instant, latest: ClubMember)

  /** A player whose rows, as this reconcile would leave them, the database would refuse (ADR 0027). */
  final case class RefusedMembershipVerdict(playerId: PlayerId, username: Username, conflict: MemberRowConflict)

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

    /** All `playerId`'s rows in the club, listed or not. */
    def rowsOf(playerId: PlayerId): MemberRows =
      MemberRows(membersByPlayerId.get(playerId).map(_.member).toList ++ unlistedByPlayerId.getOrElse(playerId, Nil))

    /** `member` ended at `at`, and was last known in no later than that. An `at` before its start is kept, so the row
      * fails [[MembershipVerdictCheck]] and is reported, rather than given an end nothing observed (ADR 0027).
      */
    def close(member: ClubMember, at: Instant): ClubMember =
      member.copy(lastSeen = Some(earlier(lastKnownIn(member), at)), until = Some(at))

    /** When `member` was last known in, its account having closed after `lastOnline` with the club on its list. */
    def hiddenAt(member: ClubMember, lastOnline: Instant): Instant = later(lastKnownIn(member), lastOnline)

    /** The last time `member` is known to have been in, before this roster: the club's previous roster, which listed
      * it, or its start if it began after that roster (ADR 0021).
      */
    private def lastKnownIn(member: ClubMember): Instant =
      member.lastSeen.getOrElse(lastObservedAt.filterNot(_.isBefore(member.since)).getOrElse(member.since))

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
    unsettledJoins: Chunk[UnsettledJoin],
    refused: Chunk[RefusedMembershipVerdict],
    settlement: ClosedAccountClubs.Settlement,
    rosterMovement: RosterMovement,
    startedAt: Instant,
    completedAt: Instant
  )
}
