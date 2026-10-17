package ccas.analysis.apps.membership

import java.time.Instant

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.tables.ClubMatchBoard.Sighting
import ccas.analysis.tables.ClubMember
import ccas.api.misc.subtypes.{ClubId, PlayerId}

object TestInferredMembership extends ZIOSpecDefault {

  override def spec: Spec[Any, Nothing] = suite("TestInferredMembership")(
    testMatchesOutsideEveryRowAreOneMembership,
    testAMatchAgainstTheClubSplitsThem,
    testARowBetweenThemSplitsThem,
    testOpenAndHiddenRowsRecordTheirMatches,
    testTheWorkedExampleFromIssue302,
    testEachMatchFromTheFirstRosterOnStandsAlone,
    testMatchesStartingAtOnceAreOneMembership,
    testOnlyAPlayersOwnRowsAndMatchesForTheClubCount
  )

  private val clubId = ClubId(1)
  private val alice  = PlayerId(10)
  private val bob    = PlayerId(11)

  private def day(n: Int): Instant = Instant.parse("2020-01-01T00:00:00Z").plusSeconds(n * 86_400L)

  private def forClub(playerId: PlayerId, at: Instant) = Sighting(playerId = playerId, startTime = at, forClub = true)
  private def against(playerId: PlayerId, at: Instant) = Sighting(playerId = playerId, startTime = at, forClub = false)

  private def ended(playerId: PlayerId, since: Instant, lastSeen: Instant, until: Instant) =
    ClubMember(clubId = clubId, playerId = playerId, since = since, lastSeen = Some(lastSeen), until = Some(until))

  private def membership(playerId: PlayerId, first: Instant, last: Instant, matches: Int) =
    InferredMembership(playerId = playerId, firstMatch = first, lastMatch = last, matches = matches)

  private def testMatchesOutsideEveryRowAreOneMembership =
    test("matches for the club outside every row are one membership, from the first to the last") {
      val sightings = List(forClub(alice, day(3)), forClub(alice, day(1)), forClub(alice, day(2)))
      assertTrue(
        InferredMembership.infer(rows = Nil, sightings = sightings, firstRosterOption = None) ==
          List(membership(alice, day(1), day(3), 3))
      )
    }

  /** A player can register only on the side of the club they belong to (docs/chess-com-api.md). */
  private def testAMatchAgainstTheClubSplitsThem =
    test("a match against the club between two matches for it splits them into two memberships") {
      val sightings = List(forClub(alice, day(1)), against(alice, day(2)), forClub(alice, day(3)))
      assertTrue(
        InferredMembership.infer(rows = Nil, sightings = sightings, firstRosterOption = None) ==
          List(membership(alice, day(1), day(1), 1), membership(alice, day(3), day(3), 1))
      )
    }

  private def testARowBetweenThemSplitsThem =
    test("a row between two matches for the club splits them, its join being a membership of its own") {
      val row       = ended(playerId = alice, since = day(2), lastSeen = day(3), until = day(4))
      val sightings = List(forClub(alice, day(1)), forClub(alice, day(5)))
      assertTrue(
        InferredMembership.infer(rows = List(row), sightings = sightings, firstRosterOption = None) ==
          List(membership(alice, day(1), day(1), 1), membership(alice, day(5), day(5), 1))
      )
    }

  /** A closure hides a membership without ending it (ADR 0022), so its row covers what follows its `lastSeen` too. */
  private def testOpenAndHiddenRowsRecordTheirMatches =
    test("matches inside an open row, or a hidden one after it was last seen, infer nothing") {
      val open      = ClubMember.joined(clubId = clubId, playerId = alice, since = day(1))
      val hidden    = ClubMember.joined(clubId = clubId, playerId = bob, since = day(1)).hidden(day(2))
      val sightings = List(forClub(alice, day(3)), forClub(bob, day(3)))
      assertTrue(
        InferredMembership.infer(rows = List(open, hidden), sightings = sightings, firstRosterOption = None).isEmpty
      )
    }

  /** The player's second row dates its join from Chess.com, and a match they played between the roster that last listed
    * them and the one that did not falls inside it.
    */
  private def testTheWorkedExampleFromIssue302 =
    test("matches years before an observed membership are one earlier membership, and those inside the row none") {
      val firstRoster = Instant.parse("2026-03-15T23:54:00Z")
      val row = ended(
        playerId = alice,
        since = Instant.parse("2026-07-06T17:23:44Z"),
        lastSeen = Instant.parse("2026-09-09T16:53:00Z"),
        until = Instant.parse("2026-09-13T20:11:38Z")
      )
      val early     = List("2014-12-06T12:00:00Z", "2017-08-07T08:06:58Z", "2020-08-16T09:00:00Z").map(Instant.parse)
      val late      = List("2026-07-11T10:00:00Z", "2026-09-10T14:48:00Z").map(Instant.parse)
      val sightings = (early ++ late).map(forClub(alice, _))
      assertTrue(
        InferredMembership.infer(rows = List(row), sightings = sightings, firstRosterOption = Some(firstRoster)) ==
          List(membership(alice, early.head, early.last, 3))
      )
    }

  /** Those rosters are not kept, so two such matches could have had any number of them in between. */
  private def testEachMatchFromTheFirstRosterOnStandsAlone =
    test("from the club's first full roster on, each match outside every row stands alone") {
      val sightings = List(forClub(alice, day(1)), forClub(alice, day(2)), forClub(alice, day(3)))
      assertTrue(
        InferredMembership.infer(rows = Nil, sightings = sightings, firstRosterOption = Some(day(2))) ==
          List(
            membership(alice, day(1), day(1), 1),
            membership(alice, day(2), day(2), 1),
            membership(alice, day(3), day(3), 1)
          )
      )
    }

  /** No roster can come between two matches that started together. */
  private def testMatchesStartingAtOnceAreOneMembership =
    test("matches outside every row that start at once are one membership, after the first roster too") {
      val sightings = List(forClub(alice, day(3)), forClub(alice, day(3)))
      assertTrue(
        InferredMembership.infer(rows = Nil, sightings = sightings, firstRosterOption = Some(day(2))) ==
          List(membership(alice, day(3), day(3), 2))
      )
    }

  private def testOnlyAPlayersOwnRowsAndMatchesForTheClubCount =
    test("matches against the club infer nothing, and one player's row neither covers nor splits another's matches") {
      val bobsRow   = ended(playerId = bob, since = day(2), lastSeen = day(2), until = day(3))
      val sightings = List(forClub(alice, day(1)), forClub(alice, day(2)), forClub(alice, day(4)), against(bob, day(1)))
      assertTrue(
        InferredMembership.infer(rows = List(bobsRow), sightings = sightings, firstRosterOption = None) ==
          List(membership(alice, day(1), day(4), 3))
      )
    }
}
