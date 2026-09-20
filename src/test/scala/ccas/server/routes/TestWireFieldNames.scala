package ccas.server.routes

import zio.json.{DecoderOps, EncoderOps}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.server.routes.JobRoutes.*
import ccas.server.routes.ScheduleRoutes.{CreateScheduleRequest, CreateScheduleResponse, ScheduleResponse}

/** Pins the shape of the submit wire: how a request names its club, and the JSON key of every field whose Scala name
  * carries the `Option` suffix. These are the contract with clients this repo does not compile — a curl caller, and
  * any CLI older than the running server — so a Scala-side rename must stay invisible on the wire (`@jsonField`). Both
  * ends of the CLI/server pair share these case classes, so nothing else in the suite would notice a dropped
  * annotation.
  */
object TestWireFieldNames extends ZIOSpecDefault {

  private val club = ClubRef(ClubId(42L), ClubSlug("team-alpha"))

  override def spec: Spec[Any, Nothing] = suite("TestWireFieldNames")(
    test("a club-scoped submit request names its club by id or by slug, and by nothing else") {
      val byId   = """{"club":{"kind":"by_id","clubId":42},"clubs":[{"kind":"by_id","clubId":42}]}"""
      val named  = """{"kind":"by_slug","slug":"team-alpha"}"""
      val bySlug = s"""{"club":$named,"clubs":[$named]}"""
      assertTrue(
        byId.fromJson[RecruitmentRequest].map(_.club) == Right(ClubQuery.ById(ClubId(42L))),
        byId.fromJson[StatsRequest].map(_.club) == Right(ClubQuery.ById(ClubId(42L))),
        byId.fromJson[MembershipRequest].map(_.clubs.head) == Right(ClubQuery.ById(ClubId(42L))),
        byId.fromJson[HistoryRequest].map(_.clubs.head) == Right(ClubQuery.ById(ClubId(42L))),
        bySlug.fromJson[RecruitmentRequest].map(_.club) == Right(ClubQuery.BySlug(ClubSlug("team-alpha"))),
        bySlug.fromJson[MembershipRequest].map(_.clubs.head) == Right(ClubQuery.BySlug(ClubSlug("team-alpha"))),
        // One field, and it is a sum: a caller cannot name a club twice over. A stray slug alongside an id is not a
        // second answer the server has to weigh, it is a key zio-json drops; the old flat pair no longer decodes.
        """{"club":{"kind":"by_id","clubId":42,"slug":"other"}}""".fromJson[StatsRequest].map(_.club) ==
          Right(ClubQuery.ById(ClubId(42L))),
        """{"clubSlug":"team-alpha","clubId":42}""".fromJson[StatsRequest].isLeft
      )
    },
    test("a submit request re-encodes its club the way it arrived, the form stored in job_run.params") {
      val request = RecruitmentRequest(
        club = ClubQuery.ById(ClubId(42L)),
        alias = None,
        target = None,
        cumulative = None,
        sourceClubs = None,
        timeLimitMinutes = None,
        explore = None,
        autoConfirm = None
      )
      assertTrue(request.toJson.contains("\"kind\":\"by_id\""), request.toJson.contains("\"clubId\":42"))
    },
    test("a submit response answers with `jobId`") {
      val single = JobResult(jobIdOption = Some("job-1"), error = None)
      val clubScoped = ClubJobResult(
        club = "team-alpha",
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
    test("a schedule create request names its club under `club`, and answers with `schedule` and `resolution`") {
      val body    = """{"kind":"Membership","club":{"kind":"by_slug","slug":"team-alpha"},"intervalHours":24}"""
      val created = CreateScheduleResponse(scheduleOption = None, resolutionOption = Some(ClubResolution.Known(club)))
      assertTrue(
        body.fromJson[CreateScheduleRequest].map(_.clubOption) == Right(Some(ClubQuery.BySlug(ClubSlug("team-alpha")))),
        created.toJson.contains("\"resolution\":{"),
        CreateScheduleResponse(scheduleOption = None, resolutionOption = None).toJson == "{}"
      )
    },
    test("a synchronous club request answers with `club`, `resolution` and `result`") {
      val answered = ClubResult(club = "team-alpha", resolution = ClubResolution.Known(club), resultOption = Some(true))
      assertTrue(
        answered.toJson.contains("\"result\":true"),
        answered.toJson.fromJson[ClubResult[Boolean]] == Right(answered)
      )
    }
  )
}
