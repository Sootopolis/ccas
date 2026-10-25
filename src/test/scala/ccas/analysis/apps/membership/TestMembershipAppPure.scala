package ccas.analysis.apps.membership

import java.time.Instant

import zio.Chunk
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import TestMembershipAppSupport.*

import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.*
import ccas.analysis.apps.membership.MembershipClassify.{PhaseBResult, PhaseCResult}
import ccas.analysis.apps.membership.MembershipReport.InferredMember
import ccas.analysis.tables.{ClubMember, Player, PlayerName, PlayerSnapshot}
import ccas.api.misc.enums.PlayerStatusCategory.{Active, Closed}
import ccas.api.misc.subtypes.Username

object TestMembershipAppPure extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestMembershipAppPure")(
    suiteClassifyFromDb,
    suiteMergeResults,
    suiteFormatReport,
    suiteClose
  ) @@ TestAspect.sequential

  // ==========================================================================
  // Suite A: classifyFromDb (pure)
  // ==========================================================================

  private def suiteClassifyFromDb = suite("classifyFromDb")(
    testEmptyInputs,
    testMemberSinceInRangeNoSnapshots,
    testMemberSinceInRangeWithSnapshots,
    testMemberSinceInRangeWithClosedMembership,
    testLaterMembershipIsNotAPriorOne,
    testJoinAfterAnInferredMembershipIsARejoin,
    testRejoinNamesTheLatestPreviousMembership,
    testMemberUntilInRangeActiveSnap,
    testMemberUntilInRangeClosedSnap,
    testMemberUntilInRangeNoSnapshot,
    testHiddenRowIsAClosureWhenLastSeen,
    testHiddenRowOfAReopenedAccountIsStillAClosure,
    testTwoSnapsWithDifferentUsernames,
    testTwoSnapsWithDifferentStatuses,
    testStatusChangeThenRename,
    testAllDatesOutsideRange
  )

  private def testEmptyInputs = test("empty inputs") {
    val result = MembershipReport.classifyFromDb(clubId, Nil, Nil, Nil, Nil, Times.t0, Times.t2)
    assertTrue(result.isEmpty)
  }

  private def testMemberSinceInRangeNoSnapshots = test("member since in range, no prior snaps → NewMember") {
    val member = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t1)
    val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, Nil, Nil, Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.playerId == pid0,
      result.head.changes.exists(_.isInstanceOf[NewMember])
    )
  }

  private def testMemberSinceInRangeWithSnapshots = test("member since in range, prior snaps exist → JoinedClub") {
    val member = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t1)
    val snap   = PlayerSnapshot(pid0, Times.t0, Active, None)
    val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, List(snap), Nil, Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.changes.exists(_.isInstanceOf[JoinedClub])
    )
  }

  private def testMemberSinceInRangeWithClosedMembership =
    test("member since in range, prior closed membership → Rejoined, with when that membership ended") {
      val lastSeen = Times.t0.plusSeconds(3600)
      val oldMember = ClubMember(
        clubId = clubId,
        playerId = pid0,
        since = Times.t0,
        lastSeen = Some(lastSeen),
        until = Some(Times.t1)
      )
      val newMember = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t2)
      val members   = List(oldMember, newMember)
      val result    = MembershipReport.classifyFromDb(clubId, members, Nil, Nil, Nil, Times.t1, Times.t3)
      val previous  = PreviousMembership.Recorded(lastSeen = lastSeen, until = Times.t1)
      assertTrue(result.size == 1, result.head.changes.contains(Rejoined(ts = Times.t2, previous = previous)))
    }

  private def testLaterMembershipIsNotAPriorOne =
    test("a join is a rejoin only of a membership that started before it") {
      def closed(since: Instant, until: Instant) = ClubMember(
        clubId = clubId,
        playerId = pid0,
        since = since,
        lastSeen = Some(since),
        until = Some(until)
      )
      val members  = List(closed(since = Times.t0, until = Times.t1), closed(since = Times.t2, until = Times.t3))
      val result   = MembershipReport.classifyFromDb(clubId, members, Nil, Nil, Nil, Times.t0, Times.t3)
      val rejoins  = result.flatMap(_.changes.toList).collect { case r: Rejoined => r }
      val previous = PreviousMembership.Recorded(lastSeen = Times.t0, until = Times.t1)
      assertTrue(rejoins == List(Rejoined(ts = Times.t2, previous = previous)))
    }

  private def testJoinAfterAnInferredMembershipIsARejoin =
    test("a join after a membership inferred from matches → Rejoined, with when those matches were played") {
      val inferred = InferredMembership(playerId = pid0, firstMatch = Times.t0, lastMatch = Times.t1, matches = 3)
      val member   = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t2)
      val result   = MembershipReport.classifyFromDb(clubId, List(member), List(inferred), Nil, Nil, Times.t1, Times.t3)
      val previous = PreviousMembership.Inferred(firstMatch = Times.t0, lastMatch = Times.t1)
      assertTrue(result.flatMap(_.changes) == List(Rejoined(ts = Times.t2, previous = previous)))
    }

  private def testRejoinNamesTheLatestPreviousMembership =
    test("a rejoin names the latest membership before it, recorded or inferred, and none after it") {
      val recorded = ClubMember(
        clubId = clubId,
        playerId = pid0,
        since = Times.t0,
        lastSeen = Some(Times.t0),
        until = Some(Times.t0.plusSeconds(3600))
      )
      val earlier  = InferredMembership(playerId = pid0, firstMatch = Times.t1, lastMatch = Times.t1, matches = 1)
      val later    = InferredMembership(playerId = pid0, firstMatch = Times.t3, lastMatch = Times.t3, matches = 1)
      val member   = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t2)
      val members  = List(recorded, member)
      val inferred = List(earlier, later)
      val result   = MembershipReport.classifyFromDb(clubId, members, inferred, Nil, Nil, Times.t2, Times.t2)
      val previous = PreviousMembership.Inferred(firstMatch = Times.t1, lastMatch = Times.t1)
      assertTrue(result.flatMap(_.changes) == List(Rejoined(ts = Times.t2, previous = previous)))
    }

  private def testMemberUntilInRangeActiveSnap = test("member until in range, latest snap Active → LeftClub") {
    val member = ClubMember(
      clubId = clubId,
      playerId = pid0,
      since = Times.t0,
      lastSeen = Some(Times.t0),
      until = Some(Times.t1)
    )
    val snap   = PlayerSnapshot(pid0, Times.t0, Active, None)
    val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, List(snap), Nil, Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.changes.exists(_.isInstanceOf[LeftClub])
    )
  }

  private def testMemberUntilInRangeClosedSnap = test("member until in range, latest snap Closed → AccountClosed") {
    val member = ClubMember(
      clubId = clubId,
      playerId = pid0,
      since = Times.t0,
      lastSeen = Some(Times.t0),
      until = Some(Times.t1)
    )
    val snap   = PlayerSnapshot(pid0, Times.t0, Closed, None)
    val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, List(snap), Nil, Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.changes.exists(_.isInstanceOf[AccountClosed])
    )
  }

  private def testMemberUntilInRangeNoSnapshot = test("member until in range, no snapshot → Unresolvable") {
    val member = ClubMember(
      clubId = clubId,
      playerId = pid0,
      since = Times.t0,
      lastSeen = Some(Times.t0),
      until = Some(Times.t1)
    )
    val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, Nil, Nil, Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.changes.exists(_.isInstanceOf[Unresolvable])
    )
  }

  /** A closure hides the membership rather than ending it, dated when the player was last known in (ADR 0022). */
  private def testHiddenRowIsAClosureWhenLastSeen =
    test("a row a closed account hides, last seen in range → AccountClosed then, not LeftClub") {
      val member = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0).hidden(Times.t1)
      val states = List(PlayerSnapshot(pid0, Times.t0, Active, None), PlayerSnapshot(pid0, Times.t2, Closed, None))
      val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, states, Nil, Times.t1, Times.t3)
      val ends   = result.flatMap(_.changes.toList).collect { case end @ (_: AccountClosed | _: LeftClub) => end }
      assertTrue(ends == List(AccountClosed(Times.t1, Closed)))
    }

  /** The count drops at the row's `lastSeen` until a roster reopens it, so the closure stays reported until then. */
  private def testHiddenRowOfAReopenedAccountIsStillAClosure =
    test("a row still hidden though its account has reopened still reports the closure that hid it") {
      val member   = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0).hidden(Times.t1)
      val states   = List(PlayerSnapshot(pid0, Times.t2, Closed, None), PlayerSnapshot(pid0, Times.t3, Active, None))
      val result   = MembershipReport.classifyFromDb(clubId, List(member), Nil, states, Nil, Times.t1, Times.t3)
      val closures = result.flatMap(_.changes).collect { case closure: AccountClosed => closure }
      assertTrue(closures == List(AccountClosed(Times.t1, Closed)))
    }

  private def testTwoSnapsWithDifferentUsernames = test("a rename in range with no status change → UsernameChange") {
    val member = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
    val snap   = PlayerSnapshot(pid0, Times.t0, Active, None)
    val names = List(
      PlayerName(pid0, Username("alice-old"), Times.t0, Some(Times.t1)),
      PlayerName(pid0, Username("alice-new"), Times.t1, None)
    )
    val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, List(snap), names, Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.username == Username("alice-new"),
      result.head.changes == Chunk(NewMember(Times.t0), UsernameChange(Times.t1, Username("alice-old")))
    )
  }

  private def testTwoSnapsWithDifferentStatuses = test("two snaps in range, different statuses → StatusChange") {
    val member = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
    val snap1  = PlayerSnapshot(pid0, Times.t0, Active, None)
    val snap2  = PlayerSnapshot(pid0, Times.t1, Closed, None)
    val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, List(snap1, snap2), Nil, Times.t0, Times.t2)
    assertTrue(
      result.size == 1,
      result.head.changes.exists(_.isInstanceOf[StatusChange])
    )
  }

  // The rename archives the Closed state it interrupts (`PlayerUpdater`), so the closure keeps its own date rather than
  // being read as starting at the rename.
  private def testStatusChangeThenRename = test("a status change, then a rename → each reported when it happened") {
    val member = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
    val states = List(
      PlayerSnapshot(pid0, Times.t0, Active, None),
      PlayerSnapshot(pid0, Times.t1, Closed, None),
      PlayerSnapshot(pid0, Times.t2, Closed, None)
    )
    val names = List(
      PlayerName(pid0, Username("before"), Times.t0, Some(Times.t2)),
      PlayerName(pid0, Username("after"), Times.t2, None)
    )
    val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, states, names, Times.t0, Times.t3)
    assertTrue(
      result.map(_.changes) == List(
        Chunk(NewMember(Times.t0), StatusChange(Times.t1, Active), UsernameChange(Times.t2, Username("before")))
      )
    )
  }

  private def testAllDatesOutsideRange = test("all dates outside range → empty list") {
    val member = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t0)
    val snap   = PlayerSnapshot(pid0, Times.t0, Active, None)
    val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, List(snap), Nil, Times.t2, Times.t3)
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
    val bArchived = PlayerSnapshot(pid0, Times.t0, Active, None)
    val cUpdated  = Player(pid1, Times.t0, Username("bob"), Closed, None, Times.t1)
    val cArchived = PlayerSnapshot(pid1, Times.t0, Active, None)
    val bMember   = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t1)
    val bClosed = ClubMember(
      clubId = clubId,
      playerId = pid2,
      since = Times.t0,
      lastSeen = Some(Times.t0),
      until = Some(Times.t1)
    )
    val cClosed = ClubMember(
      clubId = clubId,
      playerId = pid1,
      since = Times.t0,
      lastSeen = Some(Times.t0),
      until = Some(Times.t1)
    )

    val bReopened = ClubMember.joined(clubId = clubId, playerId = pid3, since = Times.t0)
    val cHidden   = ClubMember.joined(clubId = clubId, playerId = pid4, since = Times.t0).hidden(Times.t1)
    val settled   = ClubMember.joined(clubId = clubId, playerId = pid5, since = Times.t0).hidden(Times.t1)
    val unsettled = ClosedAccountClubs.Unsettled(pid5, Username("eve"), "a reason")

    val phaseB = PhaseBResult(
      resolvedIds = Set(pid0),
      changes = Chunk(bChange),
      newPlayers = Chunk(bPlayer),
      updatedPlayers = Chunk(bUpdated),
      archivedSnapshots = Chunk(bArchived),
      newMemberships = Chunk(bMember),
      closedMemberships = Chunk(bClosed),
      reopenedMemberships = Chunk(bReopened)
    )
    val phaseC = PhaseCResult(Chunk(cChange), Chunk(cUpdated), Chunk(cArchived), Chunk(cClosed), Chunk(cHidden))
    val result = MembershipApp.mergeResults(
      clubId = clubId,
      b = phaseB,
      c = phaseC,
      settlement = ClosedAccountClubs.Settlement(
        hidden = Chunk.empty,
        opened = Chunk(settled),
        unsettled = Chunk(unsettled)
      ),
      currentMemberCount = 10,
      previousMemberCount = 8,
      startedAt = Times.t0,
      completedAt = Times.t1
    )

    assertTrue(
      result.changes == Chunk(bChange, cChange),
      result.newPlayers == Chunk(bPlayer),
      result.updatedPlayers == Chunk(bUpdated, cUpdated),
      result.archivedSnapshots == Chunk(bArchived, cArchived),
      result.newMemberships == Chunk(bMember),
      result.closedMemberships == Chunk(bClosed, cClosed),
      result.hiddenMemberships == Chunk(cHidden, settled),
      result.reopenedMemberships == Chunk(bReopened),
      result.unsettledLists == Chunk(unsettled)
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
    testShowsInvitationDateOnRejoinedChanges,
    testRejoinAfterEndsThatMeet,
    testRejoinAfterAnInferredMembership,
    testListsInferredMemberships,
    testInferredMembershipsAloneAreNotNoChanges
  )

  private def testEmptySummariesNoChanges = test("empty summaries → 'No changes'") {
    val rr     = MembershipReport.ReportResult(Nil, Nil, 10, 10, Map.empty)
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
    val rr     = MembershipReport.ReportResult(summaries, Nil, 8, 10, Map.empty)
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
    val rr     = MembershipReport.ReportResult(summaries, Nil, 8, 9, Map.empty)
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
    val rr       = MembershipReport.ReportResult(summaries, Nil, 8, 10, Map.empty)
    val output   = MembershipReport.formatReport(rr)
    val aliceIdx = output.indexOf("alice")
    val bobIdx   = output.indexOf("bob")
    assertTrue(aliceIdx < bobIdx)
  }

  private def testShowsMemberCountDelta = test("shows member count delta") {
    val rr     = MembershipReport.ReportResult(Nil, Nil, 12, 10, Map.empty)
    val output = MembershipReport.formatReport(rr)
    assertTrue(output.contains("Total members: 10 (-2)"))
  }

  private def testShowsInvitationDateOnJoinChanges =
    test("shows invitation date on join changes but not on other changes") {
      val invitedAt = Times.t0
      val summaries = List(
        MemberChangeSummary(
          pid0,
          Username("alice"),
          Chunk(NewMember(Times.t1), UsernameChange(Times.t2, Username("alice-old")))
        ),
        MemberChangeSummary(pid1, Username("bob"), Chunk(JoinedClub(Times.t1)))
      )
      val invitations     = Map(pid0 -> invitedAt)
      val rr              = MembershipReport.ReportResult(summaries, Nil, 8, 10, invitations)
      val output          = MembershipReport.formatReport(rr)
      val joinedSection   = output.substring(output.indexOf("[JOINED]"), output.indexOf("[USERNAME CHANGE]"))
      val usernameSection = output.substring(output.indexOf("[USERNAME CHANGE]"))
      assertTrue(
        joinedSection.contains(s"alice — at ${Times.t1} — invited at $invitedAt"),
        !joinedSection.contains("bob — at ${Times.t1} — invited"),
        !usernameSection.contains("invited")
      )
    }

  private def testShowsInvitationDateOnRejoinedChanges = test("shows invitation date on rejoined changes") {
    val invitedAt   = Times.t1
    val previous    = PreviousMembership.Recorded(lastSeen = Times.t0, until = Times.t1)
    val rejoined    = Rejoined(ts = Times.t2, previous = previous)
    val summaries   = List(MemberChangeSummary(pid0, Username("alice"), Chunk(rejoined)))
    val invitations = Map(pid0 -> invitedAt)
    val rr          = MembershipReport.ReportResult(summaries, Nil, 10, 10, invitations)
    val output      = MembershipReport.formatReport(rr)
    assertTrue(
      output.contains(
        s"at ${Times.t2} — previous membership ended between ${Times.t0} and ${Times.t1} — invited at $invitedAt"
      )
    )
  }

  private def testRejoinAfterEndsThatMeet =
    test("a previous membership whose ends meet gives only when the player was last seen") {
      val previous  = PreviousMembership.Recorded(lastSeen = Times.t1, until = Times.t1)
      val rejoined  = Rejoined(ts = Times.t2, previous = previous)
      val summaries = List(MemberChangeSummary(pid0, Username("alice"), Chunk(rejoined)))
      val output    = MembershipReport.formatReport(MembershipReport.ReportResult(summaries, Nil, 10, 10, Map.empty))
      assertTrue(output.contains(s"alice — at ${Times.t2} — previous membership last seen at ${Times.t1}\n"))
    }

  private def testRejoinAfterAnInferredMembership =
    test("a rejoin after an inferred membership gives when the player played for the club") {
      val previous  = PreviousMembership.Inferred(firstMatch = Times.t0, lastMatch = Times.t1)
      val rejoined  = Rejoined(ts = Times.t2, previous = previous)
      val summaries = List(MemberChangeSummary(pid0, Username("alice"), Chunk(rejoined)))
      val output    = MembershipReport.formatReport(MembershipReport.ReportResult(summaries, Nil, 10, 10, Map.empty))
      val played    = s"previously played for the club from ${Times.t0} to ${Times.t1}"
      assertTrue(output.contains(s"alice — at ${Times.t2} — $played\n"))
    }

  private def testListsInferredMemberships =
    test("inferred memberships are listed in a section of their own, after the changes") {
      val summaries = List(MemberChangeSummary(pid1, Username("bob"), Chunk(NewMember(Times.t2))))
      val alice     = InferredMembership(playerId = pid0, firstMatch = Times.t0, lastMatch = Times.t1, matches = 2)
      val carol     = InferredMembership(playerId = pid2, firstMatch = Times.t1, lastMatch = Times.t1, matches = 1)
      val inferred  = List(InferredMember(Username("alice"), alice), InferredMember(Username("carol"), carol))
      val output = MembershipReport.formatReport(MembershipReport.ReportResult(summaries, inferred, 9, 10, Map.empty))
      assertTrue(
        output.endsWith(
          s"""[INFERRED FROM MATCHES]
             |  alice — played for the club from ${Times.t0} to ${Times.t1}, 2 matches
             |  carol — played for the club from ${Times.t1} to ${Times.t1}, 1 match
             |""".stripMargin
        ),
        output.indexOf("[JOINED]") < output.indexOf("[INFERRED FROM MATCHES]")
      )
    }

  private def testInferredMembershipsAloneAreNotNoChanges =
    test("a range with inferred memberships and no changes lists them rather than 'No changes'") {
      val alice    = InferredMembership(playerId = pid0, firstMatch = Times.t0, lastMatch = Times.t1, matches = 2)
      val inferred = List(InferredMember(Username("alice"), alice))
      val output   = MembershipReport.formatReport(MembershipReport.ReportResult(Nil, inferred, 10, 10, Map.empty))
      assertTrue(!output.contains("No changes"), output.contains("[INFERRED FROM MATCHES]\n  alice"))
    }

  // ==========================================================================
  // StoredRoster.close (pure)
  // ==========================================================================

  private def suiteClose = {
    val row = ClubMember.joined(clubId = clubId, playerId = pid0, since = Times.t1)
    def observedAt(lastObservedAt: Option[Instant]) =
      StoredRoster(
        membersByPlayerId = Map.empty,
        membersByUsername = Map.empty,
        unlistedByPlayerId = Map.empty,
        lastObservedAt = lastObservedAt,
        slugs = Set.empty
      )
    def ends(closed: ClubMember) = (closed.lastSeen, closed.until)

    suite("close")(
      test("last seen at the previous roster, which listed the row") {
        assertTrue(ends(observedAt(Some(Times.t2)).close(row, Times.t3)) == (Some(Times.t2), Some(Times.t3)))
      },
      test("last seen at its start when the previous roster came before it") {
        assertTrue(ends(observedAt(Some(Times.t0)).close(row, Times.t3)) == (Some(Times.t1), Some(Times.t3)))
      },
      test("last seen at its end when it ends before the previous roster") {
        assertTrue(ends(observedAt(Some(Times.t3)).close(row, Times.t2)) == (Some(Times.t2), Some(Times.t2)))
      },
      test("last seen at its start when no roster was seen before") {
        assertTrue(ends(observedAt(None).close(row, Times.t3)) == (Some(Times.t1), Some(Times.t3)))
      },
      test("an end before the start becomes the start") {
        assertTrue(ends(observedAt(Some(Times.t2)).close(row, Times.t0)) == (Some(Times.t1), Some(Times.t1)))
      },
      test("a hidden row, which no roster listed since, keeps when it was last seen") {
        assertTrue(
          ends(observedAt(Some(Times.t3)).close(row.hidden(Times.t2), Times.t3)) == (Some(Times.t2), Some(Times.t3))
        )
      },
      test("a closure hides a row from the later of its last listing and the account's last visit") {
        val roster = observedAt(Some(Times.t2))
        assertTrue(roster.hiddenAt(row, Times.t3) == Times.t3, roster.hiddenAt(row, Times.t0) == Times.t2)
      },
      test("a membership is hidden at its last sighting only while no until has ended it") {
        val closed = observedAt(Some(Times.t2)).close(row, Times.t3)
        assertTrue(row.hiddenAt.isEmpty, row.hidden(Times.t2).hiddenAt.contains(Times.t2), closed.hiddenAt.isEmpty)
      }
    )
  }
}
