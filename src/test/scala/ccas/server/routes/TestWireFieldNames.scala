package ccas.server.routes

import zio.json.{DecoderOps, EncoderOps}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.apps.{ClubRef, ClubResolution}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.server.routes.JobRoutes.*
import ccas.server.routes.ScheduleRoutes.{CreateScheduleRequest, ScheduleResponse}

/** Pins the JSON key of every wire field whose Scala name carries the `Option` suffix. The keys are the contract with
  * clients this repo does not compile — a curl caller, and any CLI older than the running server — so a Scala-side
  * rename must stay invisible on the wire (`@jsonField`). Both ends of the CLI/server pair share these case classes, so
  * nothing else in the suite would notice a dropped annotation.
  */
object TestWireFieldNames extends ZIOSpecDefault {

  private val club = ClubRef(ClubId(42L), ClubSlug("team-alpha"))

  override def spec: Spec[Any, Nothing] = suite("TestWireFieldNames")(
    test("a club-scoped submit request reads its club id from `clubId`") {
      val body = """{"clubSlug":"team-alpha","clubSlugs":["team-alpha"],"clubId":42}"""
      assertTrue(
        body.fromJson[RecruitmentRequest].map(_.clubIdOption) == Right(Some(ClubId(42L))),
        body.fromJson[MembershipRequest].map(_.clubIdOption) == Right(Some(ClubId(42L))),
        body.fromJson[HistoryRequest].map(_.clubIdOption) == Right(Some(ClubId(42L))),
        body.fromJson[StatsRequest].map(_.clubIdOption) == Right(Some(ClubId(42L)))
      )
    },
    test("a submit request re-encodes `clubId`, the form stored in job_run.params") {
      val request = RecruitmentRequest(
        clubSlug = ClubSlug("team-alpha"),
        alias = None,
        target = None,
        cumulative = None,
        sourceClubs = None,
        timeLimitMinutes = None,
        explore = None,
        autoConfirm = None,
        clubIdOption = Some(ClubId(42L))
      )
      assertTrue(request.toJson.contains("\"clubId\":42"))
    },
    test("a submit response answers with `jobId`") {
      val single = JobResult(jobIdOption = Some("job-1"), error = None)
      val clubScoped = ClubJobResult(
        clubSlug = "team-alpha",
        jobIdOption = Some("job-1"),
        error = None,
        resolution = ClubResolution.Known(club)
      )
      assertTrue(single.toJson.contains("\"jobId\":\"job-1\""), clubScoped.toJson.contains("\"jobId\":\"job-1\""))
    },
    test("job status and schedule listings answer with `clubId`") {
      val status = JobStatusResponse(
        id = "job-1",
        kind = "Membership",
        status = "Running",
        clubIdOption = Some(42L),
        startedAt = "2026-01-01T00:00:00Z",
        completedAt = None,
        error = None,
        trigger = "Api"
      )
      val schedule = ScheduleResponse(
        id = 1L,
        kind = "Membership",
        clubIdOption = Some(42L),
        params = None,
        triggerType = "interval",
        intervalHours = Some(24),
        cron = None,
        timezone = None,
        misfire = None,
        enabled = true,
        lastRunAt = None
      )
      assertTrue(status.toJson.contains("\"clubId\":42"), schedule.toJson.contains("\"clubId\":42"))
    },
    test("a schedule create request reads its club from `clubSlug`") {
      val body = """{"kind":"Membership","clubSlug":"team-alpha","intervalHours":24}"""
      assertTrue(body.fromJson[CreateScheduleRequest].map(_.clubSlugOption) == Right(Some("team-alpha")))
    }
  )
}
