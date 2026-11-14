package ccas.analysis.apps.recruitment

import java.time.Duration

import snapshot4s.generated.snapshotConfig
import zio.test.{Spec, ZIOSpecDefault}

import ccas.analysis.apps.TestTimes
import ccas.analysis.apps.recruitment.CandidateOutcome.{Rejected, RejectedFromCache}
import ccas.analysis.apps.recruitment.RejectionReason.*
import ccas.analysis.tables.RejectionCount
import ccas.api.misc.subtypes.Username
import ccas.server.jobs.JobLogFile
import ccas.utils.ZioSnapshots

/** A recruit run's report file and the rejection block of its job log, as the operator reads them, against checked-in
  * files under `src/test/resources/snapshot/recruitment/`. `sbt snapshot4sPromote` rewrites them after a deliberate
  * change, and their diff is the change as the operator will see it (#478).
  */
object TestRecruitmentReportSnapshots extends ZIOSpecDefault with ZioSnapshots {

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentReportSnapshots")(
    test("the recruit report file, with rejections live, cached and unrecorded, reads as checked in") {
      assertFileSnapshot(report(rejections), "recruitment/report.txt")
    },
    test("the recruit job log's rejection block reads as checked in") {
      JobLogFile
        .of(RecruitmentApp.logRejections(rejections))
        .map(assertFileSnapshot(_, "recruitment/rejections-log.txt"))
    },
    test("the recruit report file of a run that rejected no one reads as checked in") {
      assertFileSnapshot(report(Nil), "recruitment/report-no-rejections.txt")
    }
  )

  /** Two invited, and every other candidate rejected, so the figures add up as a real run's would. */
  private def report(rejections: List[RejectionCount]): String =
    RecruitmentApp.formatRecruitmentOutput(
      usernames = List(Username("alice"), Username("bob")),
      evaluatedCount = 2 + rejections.map(_.count).sum,
      rejections = rejections,
      startedAt = TestTimes.t0,
      completedAt = TestTimes.t0.plus(Duration.ofMinutes(12))
    )

  /** maxClubs both live and from cache, so the two add up; a tie at 4 between two checks; a row from before #430. */
  private val rejections = List(
    RejectionCount(Some(MaxClubs), Rejected, 17),
    RejectionCount(Some(MaxClubs), RejectedFromCache, 9),
    RejectionCount(Some(DailyMinElo), Rejected, 12),
    RejectionCount(Some(DailyMinGamesFinished), RejectedFromCache, 6),
    RejectionCount(Some(InClubMatch), Rejected, 4),
    RejectionCount(Some(AccountNotActive), Rejected, 4),
    RejectionCount(Some(RecentTmTimeout), RejectedFromCache, 2),
    RejectionCount(None, Rejected, 1)
  )
}
