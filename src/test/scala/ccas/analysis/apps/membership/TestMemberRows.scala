package ccas.analysis.apps.membership

import zio.Chunk
import zio.test.{assertTrue, check, Gen, Spec, ZIOSpecDefault}

import TestMembershipAppSupport.*

import ccas.analysis.apps.membership.ClosedAccountClubs.Outcome
import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.*
import ccas.analysis.apps.membership.MembershipClassify.PhaseBResult
import ccas.analysis.apps.membership.MembershipGen.day
import ccas.analysis.tables.{ClubMember, MembershipStanding}
import ccas.analysis.tables.PlayerClubList.Listing
import ccas.api.misc.enums.PlayerStatusCategory.{Active, Closed}
import ccas.api.misc.subtypes.Username

object TestMemberRows extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] =
    suite("TestMemberRows")(suitePlace, suiteStanding, suiteTheCheck, suiteAClosedAccountsList)

  /** The latest row, started on day 10: last seen on day 20 and ended on day 30 where it has either. */
  private val open   = ClubMember.joined(clubId = clubId, playerId = pid0, since = day(10))
  private val hidden = open.hidden(day(20))
  private val ended  = open.copy(lastSeen = Some(day(20)), until = Some(day(30)))

  private def after(latest: ClubMember) = {
    val older = ClubMember(
      clubId = clubId,
      playerId = pid0,
      since = day(0),
      lastSeen = Some(day(2)),
      until = Some(day(5))
    )
    MemberRows(List(older, latest))
  }

  private def suitePlace = suite("place")(
    test("a player with no rows is joining for the first time") {
      assertTrue(MemberRows(Nil).place(day(10)) == JoinPlacement.First)
    },
    test("a join at the latest row's start is the same membership, whether the row is open, hidden or ended") {
      assertTrue(
        after(open).place(day(10)) == JoinPlacement.SameMembership(open),
        after(hidden).place(day(10)) == JoinPlacement.SameMembership(hidden),
        after(ended).place(day(10)) == JoinPlacement.SameMembership(ended)
      )
    },
    test("a join before the latest row's start contradicts the rows") {
      assertTrue(
        after(open).place(day(0)) == JoinPlacement.Contradicted(open),
        after(hidden).place(day(7)) == JoinPlacement.Contradicted(hidden),
        after(ended).place(day(9)) == JoinPlacement.Contradicted(ended)
      )
    },
    test("a join inside an ended row, up to when it was last seen, contradicts the rows") {
      assertTrue(
        after(ended).place(day(15)) == JoinPlacement.Contradicted(ended),
        after(ended).place(day(20)) == JoinPlacement.Contradicted(ended)
      )
    },
    test("a join inside an ended row, after it was last seen, ends the row") {
      assertTrue(after(ended).place(day(25)) == JoinPlacement.EndsLatest(ended))
    },
    test("a join after an open or hidden row's start ends the row") {
      assertTrue(
        after(open).place(day(11)) == JoinPlacement.EndsLatest(open),
        after(hidden).place(day(15)) == JoinPlacement.EndsLatest(hidden),
        after(hidden).place(day(25)) == JoinPlacement.EndsLatest(hidden)
      )
    },
    test("a join at or after an ended row's end follows it") {
      assertTrue(
        after(ended).place(day(30)) == JoinPlacement.FollowsLatest(ended),
        after(ended).place(day(35)) == JoinPlacement.FollowsLatest(ended)
      )
    }
  )

  private def suiteStanding = suite("standing")(
    test("a row's standing is open, hidden or ended by its last sighting and end") {
      assertTrue(
        open.standing == MembershipStanding.Open,
        hidden.standing == MembershipStanding.Hidden(day(20)),
        ended.standing == MembershipStanding.Ended(day(20), day(30))
      )
    }
  )

  private def listed(
    changes: Chunk[MemberChangeSummary],
    newMemberships: Chunk[ClubMember],
    closedMemberships: Chunk[ClubMember]
  ) = nothingListed.copy(changes = changes, newMemberships = newMemberships, closedMemberships = closedMemberships)

  private def holding(rows: ClubMember*) = emptyStoredRoster.copy(unlistedByPlayerId = rows.toList.groupBy(_.playerId))

  private def openFrom(n: Int) = ClubMember.joined(clubId = clubId, playerId = pid0, since = day(n))

  private def refusalOf(storedRoster: StoredRoster, b: PhaseBResult) =
    MembershipVerdictCheck(storedRoster, b, nothingDisappeared, noSettlement).refused.map(_.conflict)

  private def suiteTheCheck = suite("the check")(
    test("changes that leave two rows overlapping are refused") {
      val b = listed(changes = Chunk.empty, newMemberships = Chunk(openFrom(14)), closedMemberships = Chunk.empty)
      assertTrue(refusalOf(holding(openFrom(10)), b) == Chunk(MemberRowConflict.Overlap(openFrom(10), openFrom(14))))
    },
    test("changes that give two rows one start are refused") {
      val endedAtThatStart = openFrom(10).copy(lastSeen = Some(day(10)), until = Some(day(10)))
      val b = listed(changes = Chunk.empty, newMemberships = Chunk(endedAtThatStart), closedMemberships = Chunk.empty)
      assertTrue(refusalOf(holding(endedAtThatStart), b) == Chunk(MemberRowConflict.SameStart(endedAtThatStart)))
    },
    test("a row whose dates are out of order is refused") {
      val endedBeforeItStarted = openFrom(10).copy(lastSeen = Some(day(9)), until = Some(day(9)))
      val b =
        listed(changes = Chunk.empty, newMemberships = Chunk.empty, closedMemberships = Chunk(endedBeforeItStarted))
      assertTrue(refusalOf(holding(openFrom(10)), b) == Chunk(MemberRowConflict.OutOfOrder(endedBeforeItStarted)))
    },
    test("a row changed twice is refused if either write would break a rule, though the last would not") {
      val endedBeforeItStarted = openFrom(10).copy(lastSeen = Some(day(9)), until = Some(day(9)))
      val b = nothingListed.copy(
        closedMemberships = Chunk(endedBeforeItStarted),
        reopenedMemberships = Chunk(openFrom(10))
      )
      assertTrue(refusalOf(holding(openFrom(10)), b) == Chunk(MemberRowConflict.OutOfOrder(endedBeforeItStarted)))
    },
    test("an open row with an empty row after it refuses a later join") {
      val empty = openFrom(12).copy(lastSeen = Some(day(12)), until = Some(day(12)))
      val b     = listed(changes = Chunk.empty, newMemberships = Chunk(openFrom(14)), closedMemberships = Chunk.empty)
      assertTrue(
        MemberRows(List(openFrom(10), empty)).place(day(14)) == JoinPlacement.FollowsLatest(empty),
        refusalOf(holding(openFrom(10), empty), b) == Chunk(MemberRowConflict.Overlap(openFrom(10), openFrom(14)))
      )
    },
    test("a refused player keeps their rename, status change and closure, and only they lose their row changes") {
      val bobsRow = ClubMember.joined(clubId = clubId, playerId = pid1, since = day(14))
      val kept = Chunk[MemberChange](
        AccountClosed(day(14), Closed),
        UsernameChange(day(14), Username("rita-old")),
        StatusChange(day(14), Active)
      )
      val ritas = MemberChangeSummary(pid0, Username("rita"), kept :+ JoinedClub(day(14)))
      val bobs  = MemberChangeSummary(pid1, Username("bob"), Chunk(NewMember(day(14))))
      val b = listed(
        changes = Chunk(ritas, bobs),
        newMemberships = Chunk(openFrom(14), bobsRow),
        closedMemberships = Chunk.empty
      )
      val checked = MembershipVerdictCheck(holding(openFrom(10)), b, nothingDisappeared, noSettlement)
      assertTrue(
        checked.b.newMemberships == Chunk(bobsRow),
        checked.b.changes.map(_.playerId) == Chunk(pid0, pid1),
        checked.b.changes.head.changes.toSet == kept.toSet,
        checked.b.changes.last == bobs,
        checked.refused.map(_.username) == Chunk(Username("rita"))
      )
    },
    test("a player refused over a club list is named as the list names them, and the row it would open stays out") {
      val opening = ClosedAccountClubs.Opened(username = Username("finn"), row = openFrom(14).hidden(day(15)))
      val lists   = noSettlement.copy(opened = Chunk(opening))
      val b       = listed(changes = Chunk.empty, newMemberships = Chunk.empty, closedMemberships = Chunk.empty)
      val checked = MembershipVerdictCheck(holding(openFrom(10)), b, nothingDisappeared, lists)
      assertTrue(checked.settlement.opened.isEmpty, checked.refused.map(_.username) == Chunk(Username("finn")))
    }
  )

  private val aListAgainstRows = for {
    rows       <- MembershipGen.history(clubId = clubId, playerId = pid0, minRows = 0)
    joined     <- Gen.oneOf(MembershipGen.instant, Gen.fromIterable(day(0) :: rows.map(_.since)))
    lastOnline <- MembershipGen.instant
  } yield (rows, Listing(playerId = pid0, lastOnline = lastOnline, joined = joined))

  private def suiteAClosedAccountsList = suite("a closed account's list")(
    test("whatever join a closed account's list shows, their rows stay valid and apart") {
      check(aListAgainstRows) { (rows, listing) =>
        val unchanged = MemberRowChanges(narrowed = Chunk.empty, widened = Chunk.empty, opened = Chunk.empty)
        val changes = ClosedAccountClubs.outcome(clubId, MemberRows(rows), listing) match {
          case Outcome.Hide(row, _)                 => unchanged.copy(widened = Chunk(row))
          case Outcome.Open(row)                    => unchanged.copy(opened = Chunk(row))
          case Outcome.Overlaps | Outcome.Unchanged => unchanged
        }
        assertTrue(MemberRows(rows).conflictAfter(changes).isEmpty)
      }
    }
  )
}
