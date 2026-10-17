package ccas.analysis.apps.membership

import java.time.Instant

import ccas.analysis.tables.ClubMatchBoard.Sighting
import ccas.analysis.tables.ClubMember
import ccas.api.misc.subtypes.PlayerId

/** A membership no roster recorded, inferred from the matches a player played for the club: when the first and the last
  * of them started, and how many there were (ADR 0023).
  */
final case class InferredMembership(playerId: PlayerId, firstMatch: Instant, lastMatch: Instant, matches: Int)

object InferredMembership {

  /** The memberships of a club that `sightings` of its matches show and its `rows` do not record. A match on the club's
    * side shows the player a member when it started (docs/chess-com-api.md, "Registration follows membership"), and
    * those outside every row are one membership unless a match against the club, or the start of a row, comes between
    * two of them. From the club's first full roster on, each stands alone: the rosters between two are not kept.
    */
  def infer(
    rows: List[ClubMember],
    sightings: List[Sighting],
    firstRosterOption: Option[Instant]
  ): List[InferredMembership] = {
    val rowsByPlayer = rows.groupBy(_.playerId)
    sightings.groupBy(_.playerId).toList.flatMap { (playerId, playerSightings) =>
      inferForPlayer(playerId, rowsByPlayer.getOrElse(playerId, Nil), playerSightings, firstRosterOption)
    }
  }

  private def inferForPlayer(
    playerId: PlayerId,
    rows: List[ClubMember],
    sightings: List[Sighting],
    firstRosterOption: Option[Instant]
  ): List[InferredMembership] = {
    def recorded(at: Instant): Boolean = rows.exists(row => !row.since.isAfter(at) && row.until.forall(at.isBefore))
    val unrecorded   = sightings.collect { case s if s.forClub && !recorded(s.startTime) => s.startTime }.sorted
    val outOrJoining = sightings.collect { case s if !s.forClub => s.startTime } ++ rows.map(_.since)

    /** Neither match lies in a row, so any row between them starts between them. */
    def apart(earlier: Instant, later: Instant): Boolean =
      firstRosterOption.exists(roster => later.isAfter(earlier) && !later.isBefore(roster)) ||
        outOrJoining.exists(at => at.isAfter(earlier) && at.isBefore(later))

    unrecorded.foldLeft(List.empty[InferredMembership]) {
      case (current :: done, at) if !apart(current.lastMatch, at) =>
        current.copy(lastMatch = at, matches = current.matches + 1) :: done
      case (done, at) => InferredMembership(playerId = playerId, firstMatch = at, lastMatch = at, matches = 1) :: done
    }.reverse
  }
}
