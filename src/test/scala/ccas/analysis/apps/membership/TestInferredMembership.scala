package ccas.analysis.apps.membership

import java.time.Instant

import zio.Random
import zio.test.{assertTrue, check, Gen, Spec, ZIOSpecDefault}

import ccas.analysis.apps.membership.MembershipGen.day
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
    testOnlyAPlayersOwnRowsAndMatchesForTheClubCount,
    suiteProperties
  )

  private val clubId = ClubId(1)
  private val alice  = PlayerId(10)
  private val bob    = PlayerId(11)

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

  // ==========================================================================
  // Properties: the rule, against generated rows and matches
  // ==========================================================================

  private def suiteProperties = suite("properties")(
    testNoInferredMembershipOverlapsARow,
    testEachMatchOutsideEveryRowIsInOneMembership,
    testNothingThatPartsMembershipsComesBetweenTheirMatches,
    testConsecutiveMembershipsArePartedForAReason,
    testFromTheFirstRosterOnAMembershipSpansOneInstant,
    testInferenceReadsEachPlayerAloneInAnyOrder
  )

  /** What [[InferredMembership.infer]] reads of one club. */
  private final case class Evidence(
    rows: List[ClubMember],
    sightings: List[Sighting],
    firstRosterOption: Option[Instant]
  ) {
    lazy val inferred: List[InferredMembership] =
      InferredMembership.infer(rows = rows, sightings = sightings, firstRosterOption = firstRosterOption)

    private lazy val rowsByPlayer = rows.groupBy(_.playerId)

    def rowsOf(playerId: PlayerId): List[ClubMember] = rowsByPlayer.getOrElse(playerId, Nil)

    /** The matches for the club that no row of the player's holds: those memberships are inferred from. */
    lazy val unrecorded: List[Sighting] =
      sightings.filter(s => s.forClub && !rowsOf(s.playerId).exists(MembershipGen.covers(_, s.startTime)))

    def matchesOf(membership: InferredMembership): List[Instant] =
      unrecorded.collect {
        case s if s.playerId == membership.playerId && holds(membership, s.startTime) => s.startTime
      }

    /** When something that parts two of `playerId`'s matches for the club happened: a match against it, a row's start. */
    def partings(playerId: PlayerId): List[Instant] = {
      val matchesAgainst = sightings.collect { case s if s.playerId == playerId && !s.forClub => s.startTime }
      matchesAgainst ++ rowsOf(playerId).map(_.since)
    }
  }

  private def holds(membership: InferredMembership, at: Instant): Boolean =
    !at.isBefore(membership.firstMatch) && !at.isAfter(membership.lastMatch)

  private val sighting = for {
    playerId <- Gen.elements(alice, bob)
    at       <- MembershipGen.instant
    forClub  <- Gen.boolean
  } yield Sighting(playerId = playerId, startTime = at, forClub = forClub)

  private val clubEvidence = for {
    alicesRows        <- MembershipGen.history(clubId = clubId, playerId = alice, minRows = 0)
    bobsRows          <- MembershipGen.history(clubId = clubId, playerId = bob, minRows = 0)
    sightings         <- Gen.listOfBounded(0, 12)(sighting)
    firstRosterOption <- Gen.option(MembershipGen.instant)
  } yield Evidence(rows = alicesRows ++ bobsRows, sightings = sightings, firstRosterOption = firstRosterOption)

  /** A membership spans `[firstMatch, lastMatch]`, and a row `[since, until)`. */
  private def testNoInferredMembershipOverlapsARow =
    test("an inferred membership never overlaps a row of its player") {
      check(clubEvidence) { evidence =>
        val overlapping = for {
          membership <- evidence.inferred
          row        <- evidence.rowsOf(membership.playerId)
          if row.until.forall(row.since.isBefore) && !row.since.isAfter(membership.lastMatch) &&
            row.until.forall(membership.firstMatch.isBefore)
        } yield (membership, row)
        assertTrue(overlapping.isEmpty)
      }
    }

  /** And its first and last match are among those it counts. */
  private def testEachMatchOutsideEveryRowIsInOneMembership =
    test("each match for the club outside every row is in exactly one inferred membership, which counts it") {
      check(clubEvidence) { evidence =>
        val inferred = evidence.inferred
        val homes = evidence.unrecorded.map(s => inferred.count(m => m.playerId == s.playerId && holds(m, s.startTime)))
        val miscounted = inferred.filterNot { membership =>
          val matches = evidence.matchesOf(membership)
          membership.matches == matches.size && matches.contains(membership.firstMatch) &&
          matches.contains(membership.lastMatch)
        }
        assertTrue(homes.forall(_ == 1), miscounted.isEmpty)
      }
    }

  /** Between two matches, not at one: what happens at the instant a membership's match started does not part it. */
  private def testNothingThatPartsMembershipsComesBetweenTheirMatches =
    test("no match against the club, nor a row's start, comes between two matches of one inferred membership") {
      check(clubEvidence) { evidence =>
        val inside = for {
          membership <- evidence.inferred
          at         <- evidence.partings(membership.playerId)
          if at.isAfter(membership.firstMatch) && at.isBefore(membership.lastMatch) &&
            !evidence.matchesOf(membership).contains(at)
        } yield (membership, at)
        assertTrue(inside.isEmpty)
      }
    }

  /** The converse of the test above. The others pass a rule that parts every two matches not started at once. */
  private def testConsecutiveMembershipsArePartedForAReason =
    test("consecutive inferred memberships are parted by a match against the club or a row's start, or a full roster") {
      check(clubEvidence) { evidence =>
        val unexplained = evidence.inferred.groupBy(_.playerId).toList.flatMap { (playerId, memberships) =>
          val inOrder = memberships.sortBy(_.firstMatch)
          inOrder.zip(inOrder.drop(1)).filterNot { (earlier, later) =>
            def between(at: Instant) = at.isAfter(earlier.lastMatch) && at.isBefore(later.firstMatch)
            later.firstMatch.isAfter(earlier.lastMatch) && (
              evidence.partings(playerId).exists(between) ||
                evidence.firstRosterOption.exists(roster => !later.firstMatch.isBefore(roster))
            )
          }
        }
        assertTrue(unexplained.isEmpty)
      }
    }

  private def testFromTheFirstRosterOnAMembershipSpansOneInstant =
    test("from the club's first full roster on, an inferred membership spans one instant") {
      check(clubEvidence) { evidence =>
        val spanning = evidence.inferred.filter { membership =>
          evidence.firstRosterOption.exists(roster => !membership.lastMatch.isBefore(roster)) &&
          membership.firstMatch != membership.lastMatch
        }
        assertTrue(spanning.isEmpty)
      }
    }

  private def testInferenceReadsEachPlayerAloneInAnyOrder = {
    val reordered = for {
      evidence  <- clubEvidence
      sightings <- Gen.fromZIO(Random.shuffle(evidence.sightings))
    } yield (evidence, evidence.copy(sightings = sightings))
    def inOrder(memberships: List[InferredMembership]) =
      memberships.sortBy(membership => (PlayerId.unwrap(membership.playerId), membership.firstMatch))

    test("inference reads each player alone, whatever order the sightings come in") {
      check(reordered) { (evidence, shuffled) =>
        val alone = List(alice, bob).flatMap { playerId =>
          Evidence(
            rows = evidence.rowsOf(playerId),
            sightings = evidence.sightings.filter(_.playerId == playerId),
            firstRosterOption = evidence.firstRosterOption
          ).inferred
        }
        assertTrue(
          inOrder(shuffled.inferred) == inOrder(evidence.inferred),
          inOrder(alone) == inOrder(evidence.inferred)
        )
      }
    }
  }
}
