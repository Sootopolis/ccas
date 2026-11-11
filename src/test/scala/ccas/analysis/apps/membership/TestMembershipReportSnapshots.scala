package ccas.analysis.apps.membership

import snapshot4s.generated.snapshotConfig
import zio.{Chunk, Runtime, ZLayer}
import zio.test.{Spec, ZIOSpecDefault, ZTestLogger}

import TestMembershipAppSupport.*

import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.*
import ccas.analysis.apps.membership.MembershipReport.{InferredMember, ReportResult}
import ccas.analysis.tables.{ClubMember, MembershipStanding}
import ccas.api.misc.enums.PlayerStatusCategory.{Abuse, Active, Closed, Fairplay}
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.{ProgressDisplay, ZioSnapshots}

/** Each membership report whole, as the operator reads it, against a checked-in file under
  * `src/test/resources/snapshot/membership/`. A deliberate change to the wording fails here: `sbt snapshot4sPromote`
  * rewrites the files, and their diff is the change as the operator will see it (#455).
  */
object TestMembershipReportSnapshots extends ZIOSpecDefault with ZioSnapshots {

  override def spec: Spec[Any, Throwable] = suite("TestMembershipReportSnapshots")(
    test("the reconcile report file, with every section filled, reads as checked in") {
      assertFileSnapshot(
        MembershipReport.formatReconciliation(everySection, invitations),
        "membership/reconcile-report.txt"
      )
    },
    test("the reconcile job log, with every section filled, reads as checked in") {
      for {
        _    <- MembershipReport.reportReconciliation(everySection, invitations)
        logs <- ZTestLogger.logOutput
      } yield assertFileSnapshot(logs.map(_.message() + "\n").mkString, "membership/reconcile-log.txt")
    }.provide(capturedLog, ZLayer.succeed(ProgressDisplay.make(enabled = false))),
    test("the warnings a reconcile logs for what it did not record read as checked in") {
      for {
        _    <- MembershipReport.warnOfWhatWasNotRecorded(everySection)
        logs <- ZTestLogger.logOutput
        lines = logs.map(entry => s"${entry.logLevel.label} ${entry.message()}\n")
      } yield assertFileSnapshot(lines.mkString, "membership/reconcile-warnings.txt")
    }.provide(capturedLog),
    test("the report file of a reconcile with nothing to say reads as checked in") {
      assertFileSnapshot(
        MembershipReport.formatReconciliation(nothingToSay, Map.empty),
        "membership/reconcile-report-empty.txt"
      )
    },
    test("the job log of a reconcile with nothing to say reads as checked in") {
      for {
        _    <- MembershipReport.reportReconciliation(nothingToSay, Map.empty)
        logs <- ZTestLogger.logOutput
      } yield assertFileSnapshot(logs.map(_.message() + "\n").mkString, "membership/reconcile-log-empty.txt")
    }.provide(capturedLog, ZLayer.succeed(ProgressDisplay.make(enabled = false))),
    test("the date-range report, with every section and inferred memberships, reads as checked in") {
      val report = ReportResult(
        summaries = memberChanges.toList,
        inferred = inferredMembers,
        memberCountAtStart = 40,
        memberCountAtEnd = 42,
        invitations = invitations
      )
      assertFileSnapshot(MembershipReport.formatReport(report), "membership/date-range-report.txt")
    },
    test("a date-range report with nothing to say reads as checked in") {
      val report = ReportResult(Nil, Nil, memberCountAtStart = 40, memberCountAtEnd = 40, invitations = Map.empty)
      assertFileSnapshot(MembershipReport.formatReport(report), "membership/date-range-report-empty.txt")
    }
  )

  /** The log a test reads back, kept off the console, where it would read as the run's own warnings. */
  private val capturedLog = Runtime.removeDefaultLoggers >>> ZTestLogger.default

  private def player(n: Int): PlayerId = PlayerId(200 + n)

  private def changed(n: Int, name: String, change: MemberChange): MemberChangeSummary =
    MemberChangeSummary(player(n), Username(name), Chunk(change))

  /** One member, or two where a section reads differently with each, for every section of member changes. */
  private val memberChanges = Chunk(
    changed(1, "alice", NewMember(Times.t2)),
    changed(2, "bob", JoinedClub(Times.t2)),
    changed(3, "carol", Rejoined(Times.t2, PreviousMembership.Recorded(lastSeen = Times.t0, until = Times.t1))),
    changed(4, "dave", Rejoined(Times.t3, PreviousMembership.Inferred(firstMatch = Times.t0, lastMatch = Times.t1))),
    changed(5, "erin", Reopened(Times.t0, MembershipStanding.Ended(lastSeen = Times.t1, until = Times.t2))),
    changed(6, "frank", LeftClub(Times.t2)),
    changed(7, "gina", AccountClosed(Times.t1, Fairplay)),
    changed(8, "hugo", StatusChange(Times.t2, Active, Abuse)),
    changed(9, "ivy", Reopened(Times.t0, MembershipStanding.Hidden(Times.t1))),
    changed(10, "jack", StatusChange(Times.t3, Closed, Active)),
    changed(11, "kate", UsernameChange(Times.t2, Username("kate-old"))),
    changed(12, "liam", Unresolvable(Times.t2, Username("liam-old"))),
    changed(13, "mia", StatusChange(Times.t2, Closed, Fairplay))
  )

  /** alice and carol were invited; bob joined uninvited. */
  private val invitations = Map(player(1) -> Times.t1, player(3) -> Times.t0)

  private val settlement = ClosedAccountClubs.Settlement(
    hidden = Chunk(
      ClosedAccountClubs.Hidden(
        username = Username("noah"),
        row = ClubMember.joined(clubId = clubId, playerId = player(14), since = Times.t0).hidden(Times.t2),
        status = Closed
      )
    ),
    opened = Chunk(
      ClosedAccountClubs.Opened(
        username = Username("olga"),
        row = ClubMember.joined(clubId = clubId, playerId = player(15), since = Times.t0).hidden(Times.t1),
        status = Closed
      )
    ),
    unsettled = Chunk(
      ClosedAccountClubs.Unsettled(
        player(16),
        Username("pete"),
        "the closed account's club list can't be read under the name we hold"
      )
    )
  )

  /** One join date conflict for each standing of the membership we hold. */
  private val unsettledJoins = {
    val open = ClubMember.joined(clubId = clubId, playerId = player(17), since = Times.t2)
    val ended =
      open.copy(playerId = player(18), since = Times.t2, lastSeen = Some(Times.t2), until = Some(Times.t3))
    val hidden = ClubMember.joined(clubId = clubId, playerId = player(19), since = Times.t3).hidden(Times.t3)
    Chunk(
      UnsettledJoin(playerId = player(17), username = Username("quinn"), joinedAt = Times.t0, latest = open),
      UnsettledJoin(playerId = player(18), username = Username("rosa"), joinedAt = Times.t1, latest = ended),
      UnsettledJoin(playerId = player(19), username = Username("sam"), joinedAt = Times.t2, latest = hidden)
    )
  }

  /** One refusal for each conflict, the first two with events the run saw. */
  private val refused = {
    val first  = ClubMember.joined(clubId = clubId, playerId = player(20), since = Times.t0)
    val second = ClubMember.joined(clubId = clubId, playerId = player(20), since = Times.t2)
    val outside = ClubMember(
      clubId = clubId,
      playerId = player(22),
      since = Times.t1,
      lastSeen = Some(Times.t0),
      until = Some(Times.t0)
    )
    Chunk(
      RefusedMembershipVerdict(
        playerId = player(20),
        username = Username("tom"),
        conflict = MemberRowConflict.Overlap(first, second),
        unreported = Chunk(JoinedClub(Times.t2), AccountClosed(Times.t2, Closed))
      ),
      RefusedMembershipVerdict(
        playerId = player(21),
        username = Username("uma"),
        conflict = MemberRowConflict.SameStart(second),
        unreported = Chunk(LeftClub(Times.t2))
      ),
      RefusedMembershipVerdict(
        playerId = player(22),
        username = Username("vic"),
        conflict = MemberRowConflict.OutOfOrder(outside),
        unreported = Chunk.empty
      )
    )
  }

  /** Members on neither side of the changes above, so the roster's movement doesn't contradict them. */
  private def onRoster(range: Range): Set[PlayerId] = range.map(n => PlayerId(1000 + n)).toSet

  private val everySection = ReconciliationResult(
    clubId = clubId,
    changes = memberChanges,
    newPlayers = Chunk.empty,
    updatedPlayers = Chunk.empty,
    unsettledJoins = unsettledJoins,
    refused = refused,
    settlement = settlement,
    rosterMovement = RosterMovement(before = onRoster(1 to 40), after = onRoster(3 to 44)),
    startedAt = Times.t3,
    completedAt = Times.t3.plusSeconds(754)
  )

  private val nothingToSay = ReconciliationResult(
    clubId = clubId,
    changes = Chunk.empty,
    newPlayers = Chunk.empty,
    updatedPlayers = Chunk.empty,
    unsettledJoins = Chunk.empty,
    refused = Chunk.empty,
    settlement = noSettlement,
    rosterMovement = RosterMovement(before = onRoster(1 to 40), after = onRoster(1 to 40)),
    startedAt = Times.t3,
    completedAt = Times.t3.plusSeconds(754)
  )

  private val inferredMembers = List(
    InferredMember(Username("wes"), InferredMembership(player(23), Times.t0, Times.t2, matches = 3)),
    InferredMember(Username("xena"), InferredMembership(player(24), Times.t1, Times.t1, matches = 1))
  )
}
