package ccas.analysis.apps.membership

import java.time.Instant

import zio.test.Gen

import ccas.analysis.tables.ClubMember
import ccas.api.misc.subtypes.{ClubId, PlayerId}

/** Generators of a club's membership rows, for properties. Every instant is a day on a grid of [[Days]], so that two
  * often coincide: the boundaries the rules on these rows turn on, which instants drawn from the whole timeline would
  * all but never reach.
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

  /** Any one to three of `playerId`'s rows in `clubId`, whatever their dates, on a grid small enough that their ends
    * often meet: rows the database may or may not take.
    */
  def anyRows(clubId: ClubId, playerId: PlayerId): Gen[Any, List[ClubMember]] = {
    val nearby = Gen.int(0, 5).map(day)
    val row = for {
      since    <- nearby
      lastSeen <- Gen.option(nearby)
      until    <- Gen.option(nearby)
    } yield ClubMember(clubId = clubId, playerId = playerId, since = since, lastSeen = lastSeen, until = until)
    // Two empty rows with one start break only the key, which rows drawn at random would all but never show.
    val empty = nearby.map { at =>
      ClubMember(clubId = clubId, playerId = playerId, since = at, lastSeen = Some(at), until = Some(at))
    }
    Gen.listOfBounded(1, 3)(Gen.oneOf(row, empty))
  }

  /** Whether `row`'s `[since, until)` holds `at`. */
  def covers(row: ClubMember, at: Instant): Boolean = !row.since.isAfter(at) && row.until.forall(at.isBefore)
}
