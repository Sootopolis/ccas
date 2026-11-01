package ccas.analysis.apps.membership

import java.time.Instant

import zio.test.Gen

import ccas.analysis.tables.ClubMember
import ccas.api.misc.subtypes.{ClubId, PlayerId}

/** Generators of a club's membership rows, and the rules the database holds them to, for properties. Every instant is
  * a day on a grid of [[Days]], so that two often coincide: the boundaries these rules turn on, which instants drawn
  * from the whole timeline would all but never reach.
  */
object MembershipGen {

  val Days = 30

  def day(n: Int): Instant = Instant.parse("2020-01-01T00:00:00Z").plusSeconds(n * 86_400L)

  val instant: Gen[Any, Instant] = Gen.int(0, Days).map(day)

  /** A row a roster lists, or one a closure hides, last seen no earlier than its start. */
  def current(clubId: ClubId, playerId: PlayerId): Gen[Any, ClubMember] =
    for {
      since       <- Gen.int(0, Days)
      lastSeenDay <- Gen.option(Gen.int(since, Days))
    } yield ClubMember(
      clubId = clubId,
      playerId = playerId,
      since = day(since),
      lastSeen = lastSeenDay.map(day),
      until = None
    )

  /** From `minRows` to three of `playerId`'s rows in `clubId`, oldest first: each valid, and none overlapping another
    * or sharing its start. All have ended but the latest, which may instead be open or hidden.
    */
  def history(clubId: ClubId, playerId: PlayerId, minRows: Int): Gen[Any, List[ClubMember]] = {
    val rows = for {
      count  <- Gen.int(minRows, 3)
      bounds <- Gen.listOfN(3 * count)(Gen.int(0, Days))
      latest <- Gen.fromIterable(Latest.values.toList)
    } yield {
      val ended = bounds.sorted.grouped(3).toList.collect { case List(since, lastSeen, until) =>
        ClubMember(
          clubId = clubId,
          playerId = playerId,
          since = day(since),
          lastSeen = Some(day(lastSeen)),
          until = Some(day(until))
        )
      }
      ended.dropRight(1) ++ ended.lastOption.map(latest.standing)
    }
    rows.filter(rows => rows.map(_.since).distinct.size == rows.size)
  }

  private enum Latest {
    case Ended, Open, Hidden

    def standing(row: ClubMember): ClubMember = this match {
      case Ended  => row
      case Open   => row.copy(lastSeen = None, until = None)
      case Hidden => row.copy(until = None)
    }
  }

  /** Whether `row` passes `club_member_window` as SQL reads the `CHECK`, where a comparison with null passes. */
  def valid(row: ClubMember): Boolean =
    (row.until.isEmpty || row.lastSeen.nonEmpty) &&
      row.lastSeen.forall(lastSeen => !lastSeen.isBefore(row.since) && row.until.forall(!lastSeen.isAfter(_)))

  /** Whether `club_member_no_overlap` refuses `a` and `b` together. Each is `[since, until)`: unbounded without an
    * `until`, and empty, overlapping nothing, when its ends meet.
    */
  def overlap(a: ClubMember, b: ClubMember): Boolean = {
    def nonEmpty(row: ClubMember) = row.until.forall(row.since.isBefore)
    a.clubId == b.clubId && a.playerId == b.playerId && nonEmpty(a) && nonEmpty(b) &&
    a.until.forall(b.since.isBefore) && b.until.forall(a.since.isBefore)
  }

  /** Whether `row`'s `[since, until)` holds `at`. */
  def covers(row: ClubMember, at: Instant): Boolean = !row.since.isAfter(at) && row.until.forall(at.isBefore)
}
