package ccas.analysis.apps.membership

import java.time.Instant

import zio.Chunk

import ccas.analysis.apps.membership.MembershipChange.{MemberChangeSummary, RefusedMembershipVerdict, StoredRoster}
import ccas.analysis.apps.membership.MembershipClassify.{PhaseBResult, PhaseCResult}
import ccas.analysis.tables.{ClubMember, MembershipStanding}
import ccas.api.misc.subtypes.{PlayerId, Username}

/** Where a listed `joined` sits against a player's rows in a club (ADR 0026). */
enum JoinPlacement {

  /** They have no rows. */
  case First

  /** It is the start of `latest`, so the same membership. */
  case SameMembership(latest: ClubMember)

  /** It falls before `latest`, or inside it no later than when it was last seen. */
  case Contradicted(latest: ClubMember)

  /** `latest` was still running then, so they had left by then. */
  case EndsLatest(latest: ClubMember)

  /** `latest` had already ended. */
  case FollowsLatest(latest: ClubMember)
}

/** Why the database would refuse a player's rows (ADR 0027). */
enum MemberRowConflict {
  case OutOfOrder(row: ClubMember)
  case SameStart(row: ClubMember)
  case Overlap(earlier: ClubMember, later: ClubMember)
}

/** One player's rows in one club. */
final case class MemberRows(rows: List[ClubMember]) {

  def place(join: Instant): JoinPlacement =
    rows.maxByOption(_.since) match {
      case None => JoinPlacement.First
      case Some(latest) =>
        if (latest.since == join) { JoinPlacement.SameMembership(latest) }
        else if (latest.since.isAfter(join)) { JoinPlacement.Contradicted(latest) }
        else { placeAfterStart(latest, join) }
    }

  // An ended row's last sighting is normally a roster listing it under its old join, so a fresh join should not come
  // before then. A hidden row's can be only its account's last visit, which does not show membership.
  private def placeAfterStart(latest: ClubMember, join: Instant): JoinPlacement =
    latest.standing match {
      case MembershipStanding.Open | _: MembershipStanding.Hidden => JoinPlacement.EndsLatest(latest)
      case MembershipStanding.Ended(lastSeen, until) =>
        if (!until.isAfter(join)) { JoinPlacement.FollowsLatest(latest) }
        else if (!lastSeen.isBefore(join)) { JoinPlacement.Contradicted(latest) }
        else { JoinPlacement.EndsLatest(latest) }
    }

  /** The latest row started before `at`. */
  def latestBefore(at: Instant): Option[ClubMember] = rows.filter(_.since.isBefore(at)).maxByOption(_.since)

  /** The first `club_member` constraint writing `changes`, all this player's, would break, in the order `MembershipApp`
    * writes them. The database checks each write, so a row changed twice has to hold at each step, not only the last.
    */
  def conflictAfter(changes: MemberRowChanges): Option[MemberRowConflict] = {
    val updates = (changes.narrowed ++ changes.widened).scanLeft(rows) { (current, change) =>
      current.map(row => if (row.since == change.since) change else row)
    }
    (updates :+ (updates.last ++ changes.opened)).flatMap(MemberRows(_).conflict).headOption
  }

  /** The first `club_member` constraint these rows break. */
  def conflict: Option[MemberRowConflict] = {
    val pairs = rows.sortBy(_.since).combinations(2).toList
    rows.find(!_.windowHolds).map(MemberRowConflict.OutOfOrder(_))
      .orElse(pairs.collectFirst { case List(a, b) if a.since == b.since => MemberRowConflict.SameStart(a) })
      .orElse(pairs.collectFirst { case List(a, b) if a.overlaps(b) => MemberRowConflict.Overlap(a, b) })
  }
}

/** The rows a reconcile writes, by the order it writes them: closes first, as a rejoin opens a row the old one would
  * overlap while still open.
  */
final case class MemberRowChanges(narrowed: Chunk[ClubMember], widened: Chunk[ClubMember], opened: Chunk[ClubMember]) {

  def byPlayer: Map[PlayerId, MemberRowChanges] = {
    val narrowedByPlayer = narrowed.groupBy(_.playerId)
    val widenedByPlayer  = widened.groupBy(_.playerId)
    val openedByPlayer   = opened.groupBy(_.playerId)
    (narrowedByPlayer.keySet ++ widenedByPlayer.keySet ++ openedByPlayer.keySet).map { playerId =>
      playerId -> MemberRowChanges(
        narrowed = narrowedByPlayer.getOrElse(playerId, Chunk.empty),
        widened = widenedByPlayer.getOrElse(playerId, Chunk.empty),
        opened = openedByPlayer.getOrElse(playerId, Chunk.empty)
      )
    }.toMap
  }
}

private[membership] object MemberRowChanges {

  def of(b: PhaseBResult, c: PhaseCResult, settlement: ClosedAccountClubs.Settlement): MemberRowChanges =
    MemberRowChanges(
      narrowed = b.closedMemberships ++ c.closedMemberships ++ c.hiddenMemberships,
      widened = b.reopenedMemberships ++ settlement.hidden.map(_.row),
      opened = b.newMemberships ++ settlement.opened.map(_.row)
    )
}

/** Holds back any player whose rows the database would refuse, so one player cannot fail the club's save (ADR 0027). */
private[membership] object MembershipVerdictCheck {

  /** What is left to save, and the players held back. */
  final case class CheckedVerdicts(
    b: PhaseBResult,
    c: PhaseCResult,
    settlement: ClosedAccountClubs.Settlement,
    refused: Chunk[RefusedMembershipVerdict]
  )

  def apply(
    storedRoster: StoredRoster,
    b: PhaseBResult,
    c: PhaseCResult,
    settlement: ClosedAccountClubs.Settlement
  ): CheckedVerdicts = {
    val conflicts = MemberRowChanges.of(b, c, settlement).byPlayer.flatMap { (playerId, changes) =>
      storedRoster.rowsOf(playerId).conflictAfter(changes).map(playerId -> _)
    }
    val names = (b.changes ++ c.changes).map(summary => summary.playerId -> summary.username).toMap ++
      settlement.hidden.map(hidden => hidden.row.playerId -> hidden.username) ++
      settlement.opened.map(opened => opened.row.playerId -> opened.username)
    def kept(row: ClubMember) = !conflicts.contains(row.playerId)
    def withoutRowEvents(summaries: Chunk[MemberChangeSummary]) =
      summaries.map { summary =>
        if (conflicts.contains(summary.playerId)) summary.copy(changes = summary.changes.filterNot(_.changesRows))
        else summary
      }.filter(_.changes.nonEmpty)
    CheckedVerdicts(
      b = b.copy(
        changes = withoutRowEvents(b.changes),
        newMemberships = b.newMemberships.filter(kept),
        closedMemberships = b.closedMemberships.filter(kept),
        reopenedMemberships = b.reopenedMemberships.filter(kept)
      ),
      c = c.copy(
        changes = withoutRowEvents(c.changes),
        closedMemberships = c.closedMemberships.filter(kept),
        hiddenMemberships = c.hiddenMemberships.filter(kept)
      ),
      settlement = settlement.copy(
        hidden = settlement.hidden.filter(hidden => kept(hidden.row)),
        opened = settlement.opened.filter(opened => kept(opened.row))
      ),
      refused = Chunk.from(conflicts).map { (playerId, conflict) =>
        RefusedMembershipVerdict(
          playerId = playerId,
          username = names.getOrElse(playerId, Username.wrap("unknown")),
          conflict = conflict
        )
      }.sortBy(_.username.value)
    )
  }
}
