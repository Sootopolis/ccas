package ccas.analysis.apps.membership

import zio.Chunk
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.*
import ccas.analysis.apps.membership.MembershipClassify.{PhaseBResult, PhaseCResult}
import ccas.analysis.tables.{ClubMember, Player, PlayerSnapshot}
import ccas.api.misc.enums.PlayerStatusCategory.{Active, Closed}
import ccas.api.misc.subtypes.Username

import TestMembershipAppSupport.*

object TestMembershipAppPure extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestMembershipAppPure")(
    suiteClassifyFromDb,
    suiteMergeResults,
    suiteFormatReport
  ) @@ TestAspect.sequential

  // ==========================================================================
  // Suite A: classifyFromDb (pure)
  // ==========================================================================

  private def suiteClassifyFromDb = suite("classifyFromDb")(
    testEmptyInputs,
    testMemberSinceInRangeNoSnapshots,
    testMemberSinceInRangeWithSnapshots,
    testMemberSinceInRangeWithClosedMembership,
    testMemberUntilInRangeActiveSnap,
    testMemberUntilInRangeClosedSnap,
    testMemberUntilInRangeNoSnapshot,
    testTwoSnapsWithDifferentUsernames,
    testTwoSnapsWithDifferentStatuses,
    testAllDatesOutsideRange
  )

  private def testEmptyInputs = test("empty inputs") {
    val result = MembershipReport.classifyFromDb(clubId, Nil, Nil, Times.t0, Times.t2)
    assertTrue(result.isEmpty)
  }

  private def testMemberSinceInRangeNoSnapshots = test("member since in range, no prior snaps → NewMember") {
    val member = ClubMember(clubId, pid0, Times.t1, None, sinceApproximate = false)
    val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.playerId == pid0,
      result.head.changes.exists(_.isInstanceOf[NewMember])
    )
  }

  private def testMemberSinceInRangeWithSnapshots = test("member since in range, prior snaps exist → JoinedClub") {
    val member = ClubMember(clubId, pid0, Times.t1, None, sinceApproximate = false)
    val snap   = PlayerSnapshot(pid0, Times.t0, Username("alice"), Active, None)
    val result = MembershipReport.classifyFromDb(clubId, List(member), List(snap), Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.changes.exists(_.isInstanceOf[JoinedClub])
    )
  }

  private def testMemberSinceInRangeWithClosedMembership = test("member since in range, prior closed membership → Rejoined") {
    val oldMember = ClubMember(clubId, pid0, Times.t0, Some(Times.t1), sinceApproximate = false)
    val newMember = ClubMember(clubId, pid0, Times.t2, None, sinceApproximate = false)
    val result    = MembershipReport.classifyFromDb(clubId, List(oldMember, newMember), Nil, Times.t1, Times.t3)
    assertTrue(
      result.size == 1,
      result.head.changes.exists(_.isInstanceOf[Rejoined])
    )
  }

  private def testMemberUntilInRangeActiveSnap = test("member until in range, latest snap Active → LeftClub") {
    val member = ClubMember(clubId, pid0, Times.t0, Some(Times.t1), sinceApproximate = false)
    val snap   = PlayerSnapshot(pid0, Times.t0, Username("alice"), Active, None)
    val result = MembershipReport.classifyFromDb(clubId, List(member), List(snap), Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.changes.exists(_.isInstanceOf[LeftClub])
    )
  }

  private def testMemberUntilInRangeClosedSnap = test("member until in range, latest snap Closed → AccountClosed") {
    val member = ClubMember(clubId, pid0, Times.t0, Some(Times.t1), sinceApproximate = false)
    val snap   = PlayerSnapshot(pid0, Times.t0, Username("alice"), Closed, None)
    val result = MembershipReport.classifyFromDb(clubId, List(member), List(snap), Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.changes.exists(_.isInstanceOf[AccountClosed])
    )
  }

  private def testMemberUntilInRangeNoSnapshot = test("member until in range, no snapshot → Unresolvable") {
    val member = ClubMember(clubId, pid0, Times.t0, Some(Times.t1), sinceApproximate = false)
    val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.changes.exists(_.isInstanceOf[Unresolvable])
    )
  }

  private def testTwoSnapsWithDifferentUsernames = test("two snaps in range, different usernames → UsernameChange") {
    val member = ClubMember(clubId, pid0, Times.t0, None, sinceApproximate = false)
    val snap1  = PlayerSnapshot(pid0, Times.t0, Username("alice-old"), Active, None)
    val snap2  = PlayerSnapshot(pid0, Times.t1, Username("alice-new"), Active, None)
    val result = MembershipReport.classifyFromDb(clubId, List(member), List(snap1, snap2), Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.changes.exists(_.isInstanceOf[UsernameChange])
    )
  }

  private def testTwoSnapsWithDifferentStatuses = test("two snaps in range, different statuses → StatusChange") {
    val member = ClubMember(clubId, pid0, Times.t0, None, sinceApproximate = false)
    val snap1  = PlayerSnapshot(pid0, Times.t0, Username("alice"), Active, None)
    val snap2  = PlayerSnapshot(pid0, Times.t1, Username("alice"), Closed, None)
    val result = MembershipReport.classifyFromDb(clubId, List(member), List(snap1, snap2), Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.changes.exists(_.isInstanceOf[StatusChange])
    )
  }

  private def testAllDatesOutsideRange = test("all dates outside range → empty list") {
    val member = ClubMember(clubId, pid0, Times.t0, None, sinceApproximate = false)
    val snap   = PlayerSnapshot(pid0, Times.t0, Username("alice"), Active, None)
    val result = MembershipReport.classifyFromDb(clubId, List(member), List(snap), Times.t2, Times.t3)
    assertTrue(result.isEmpty)
  }

  // ==========================================================================
  // Suite B: mergeResults (pure)
  // ==========================================================================

  private def suiteMergeResults = suite("mergeResults")(
    testConcatenatesPhaseBAndPhaseCFields
  )

  private def testConcatenatesPhaseBAndPhaseCFields = test("concatenates PhaseBResult and PhaseCResult fields") {
    val bChange   = MemberChangeSummary(pid0, Username("alice"), Chunk(NewMember(Times.t1)))
    val cChange   = MemberChangeSummary(pid1, Username("bob"), Chunk(LeftClub(Times.t1)))
    val bPlayer   = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
    val bUpdated  = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t1)
    val bArchived = PlayerSnapshot(pid0, Times.t0, Username("alice"), Active, None)
    val cUpdated  = Player(pid1, Times.t0, Username("bob"), Closed, None, Times.t1)
    val cArchived = PlayerSnapshot(pid1, Times.t0, Username("bob"), Active, None)
    val bMember   = ClubMember(clubId, pid0, Times.t1, None, sinceApproximate = false)
    val bClosed   = ClubMember(clubId, pid2, Times.t0, Some(Times.t1), sinceApproximate = false)
    val cClosed   = ClubMember(clubId, pid1, Times.t0, Some(Times.t1), sinceApproximate = false)

    val phaseB = PhaseBResult(
      Set(pid0),
      Chunk(bChange),
      Chunk(bPlayer),
      Chunk(bUpdated),
      Chunk(bArchived),
      Chunk(bMember),
      Chunk(bClosed)
    )
    val phaseC = PhaseCResult(Chunk(cChange), Chunk(cUpdated), Chunk(cArchived), Chunk(cClosed))
    val result = MembershipApp.mergeResults(clubId, phaseB, phaseC, 10, 8, Times.t0, Times.t1)

    assertTrue(
      result.changes == Chunk(bChange, cChange),
      result.newPlayers == Chunk(bPlayer),
      result.updatedPlayers == Chunk(bUpdated, cUpdated),
      result.archivedSnapshots == Chunk(bArchived, cArchived),
      result.newMemberships == Chunk(bMember),
      result.closedMemberships == Chunk(bClosed, cClosed)
    )
  }

  // ==========================================================================
  // Suite C: formatReport (pure)
  // ==========================================================================

  private def suiteFormatReport = suite("formatReport")(
    testEmptySummariesNoChanges,
    testGroupsChangesByCategory,
    testCategoriesInEnumOrdinalOrder,
    testEntriesSortedByTimestamp,
    testShowsMemberCountDelta,
    testShowsInvitationDateOnJoinChanges,
    testShowsInvitationDateOnRejoinedChanges
  )

  private def testEmptySummariesNoChanges = test("empty summaries → 'No changes'") {
    val rr     = MembershipReport.ReportResult(Nil, 10, 10, Map.empty)
    val output = MembershipReport.formatReport(rr)
    assertTrue(
      output.contains("Total members: 10 (+0)"),
      output.contains("No changes")
    )
  }

  private def testGroupsChangesByCategory = test("groups changes by category, not by player") {
    val summaries = List(
      MemberChangeSummary(
        pid0,
        Username("alice"),
        Chunk(NewMember(Times.t1), UsernameChange(Times.t2, Username("alice-old")))
      ),
      MemberChangeSummary(pid1, Username("bob"), Chunk(NewMember(Times.t1)))
    )
    val rr     = MembershipReport.ReportResult(summaries, 8, 10, Map.empty)
    val output = MembershipReport.formatReport(rr)
    assertTrue(
      output.contains("[JOINED]\n  alice"),
      output.contains("[JOINED]\n  alice") && output.contains("  bob"),
      output.contains("[USERNAME CHANGE]\n  alice")
    )
  }

  private def testCategoriesInEnumOrdinalOrder = test("categories appear in enum ordinal order") {
    val summaries = List(
      MemberChangeSummary(pid0, Username("alice"), Chunk(UsernameChange(Times.t2, Username("old")))),
      MemberChangeSummary(pid1, Username("bob"), Chunk(NewMember(Times.t1)))
    )
    val rr     = MembershipReport.ReportResult(summaries, 8, 9, Map.empty)
    val output = MembershipReport.formatReport(rr)
    val newIdx = output.indexOf("[JOINED]")
    val usrIdx = output.indexOf("[USERNAME CHANGE]")
    assertTrue(
      newIdx >= 0,
      usrIdx >= 0,
      newIdx < usrIdx
    )
  }

  private def testEntriesSortedByTimestamp = test("entries within a category are sorted by timestamp") {
    val summaries = List(
      MemberChangeSummary(pid0, Username("bob"), Chunk(NewMember(Times.t2))),
      MemberChangeSummary(pid1, Username("alice"), Chunk(NewMember(Times.t1)))
    )
    val rr       = MembershipReport.ReportResult(summaries, 8, 10, Map.empty)
    val output   = MembershipReport.formatReport(rr)
    val aliceIdx = output.indexOf("alice")
    val bobIdx   = output.indexOf("bob")
    assertTrue(aliceIdx < bobIdx)
  }

  private def testShowsMemberCountDelta = test("shows member count delta") {
    val rr     = MembershipReport.ReportResult(Nil, 12, 10, Map.empty)
    val output = MembershipReport.formatReport(rr)
    assertTrue(output.contains("Total members: 10 (-2)"))
  }

  private def testShowsInvitationDateOnJoinChanges = test("shows invitation date on join changes but not on other changes") {
    val invitedAt = Times.t0
    val summaries = List(
      MemberChangeSummary(
        pid0,
        Username("alice"),
        Chunk(NewMember(Times.t1), UsernameChange(Times.t2, Username("alice-old")))
      ),
      MemberChangeSummary(pid1, Username("bob"), Chunk(JoinedClub(Times.t1)))
    )
    val invitations = Map(pid0 -> invitedAt)
    val rr          = MembershipReport.ReportResult(summaries, 8, 10, invitations)
    val output      = MembershipReport.formatReport(rr)
    val joinedSection = output.substring(output.indexOf("[JOINED]"), output.indexOf("[USERNAME CHANGE]"))
    val usernameSection = output.substring(output.indexOf("[USERNAME CHANGE]"))
    assertTrue(
      joinedSection.contains(s"alice — at ${Times.t1} — invited at $invitedAt"),
      !joinedSection.contains("bob — at ${Times.t1} — invited"),
      !usernameSection.contains("invited")
    )
  }

  private def testShowsInvitationDateOnRejoinedChanges = test("shows invitation date on rejoined changes") {
    val invitedAt = Times.t1
    val summaries = List(
      MemberChangeSummary(pid0, Username("alice"), Chunk(Rejoined(Times.t2, Times.t0)))
    )
    val invitations = Map(pid0 -> invitedAt)
    val rr          = MembershipReport.ReportResult(summaries, 10, 10, invitations)
    val output      = MembershipReport.formatReport(rr)
    assertTrue(output.contains(s"at ${Times.t2} — previously left at ${Times.t0} — invited at $invitedAt"))
  }

}
