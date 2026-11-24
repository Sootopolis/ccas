package ccas.server.routes

import zio.json.{DecoderOps, EncoderOps}
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubResolution, NamedClub}
import ccas.analysis.apps.recruitment.CriteriaSpec
import ccas.analysis.tables.RecruitmentCriteria
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.server.jobs.JobOptions.RecruitmentOptions
import ccas.server.routes.JobRoutes.*
import ccas.server.routes.ManagedClubRoutes.ManagedClubResponse
import ccas.server.routes.RecruitmentCriteriaRoutes.CriteriaView
import ccas.server.routes.ScheduleRoutes.{CreateScheduleRequest, CreateScheduleResponse, ScheduleResponse}

/** Pins the shape of the submit wire: how a request names its club, and the JSON key of every field whose Scala name
  * carries the `Option` suffix. These are the contract with clients this repo does not compile — a curl caller, and
  * any CLI older than the running server — so a Scala-side rename must stay invisible on the wire (`@jsonField`). Both
  * ends of the CLI/server pair share these case classes, so nothing else in the suite would notice a dropped
  * annotation.
  */
object TestWireFieldNames extends ZIOSpecDefault {

  private val club = NamedClub(ClubId(42L), ClubSlug("team-alpha"))

  override def spec: Spec[Any, Nothing] = suite("TestWireFieldNames")(
    test("a club-scoped submit request names its club by id or by slug, and by nothing else") {
      val byId                = """{"kind":"by_id","clubId":42}"""
      val bySlug              = """{"kind":"by_slug","slug":"team-alpha"}"""
      def one(club: String)   = s"""{"club":$club,"options":{}}"""
      def batch(club: String) = s"""{"clubs":[$club],"options":{}}"""
      assertTrue(
        one(byId).fromJson[RecruitmentRequest].map(_.club) == Right(ClubQuery.ById(ClubId(42L))),
        one(byId).fromJson[StatsRequest].map(_.club) == Right(ClubQuery.ById(ClubId(42L))),
        batch(byId).fromJson[MembershipRequest].map(_.clubs.head) == Right(ClubQuery.ById(ClubId(42L))),
        batch(byId).fromJson[HistoryRequest].map(_.clubs.head) == Right(ClubQuery.ById(ClubId(42L))),
        one(bySlug).fromJson[RecruitmentRequest].map(_.club) == Right(ClubQuery.BySlug(ClubSlug("team-alpha"))),
        batch(bySlug).fromJson[MembershipRequest].map(_.clubs.head) == Right(ClubQuery.BySlug(ClubSlug("team-alpha"))),
        // One field, and it is a sum: a caller cannot name a club twice over. A stray slug alongside an id is not a
        // second answer the server has to weigh, it is a key zio-json drops; the old flat pair no longer decodes.
        one("""{"kind":"by_id","clubId":42,"slug":"other"}""").fromJson[StatsRequest].map(_.club) ==
          Right(ClubQuery.ById(ClubId(42L))),
        """{"clubSlug":"team-alpha","clubId":42,"options":{}}""".fromJson[StatsRequest].isLeft
      )
    },
    test("a job request carries its options under `options`, in the same JSON a schedule's params take") {
      val params = """{"target":20,"timeLimitMinutes":10,"explore":false}"""
      val body   = s"""{"club":{"kind":"by_id","clubId":42},"options":$params,"autoConfirm":false}"""
      assertTrue(
        body.fromJson[RecruitmentRequest].map(_.options) == params.fromJson[RecruitmentOptions],
        body.fromJson[RecruitmentRequest].map(_.autoConfirm) == Right(Some(false))
      )
    },
    test("a flat job request from before #265 is refused rather than run on defaults") {
      val club  = """"club":{"kind":"by_id","clubId":42}"""
      val clubs = """"clubs":[{"kind":"by_id","clubId":42}]"""
      assertTrue(
        // A flat body has no `options`, which every request requires.
        s"""{$club,"target":20}""".fromJson[RecruitmentRequest].isLeft,
        s"""{$club,"since":"2026-01-01","until":"2026-02-01"}""".fromJson[StatsRequest].isLeft,
        s"""{$clubs,"full":true}""".fromJson[HistoryRequest].isLeft,
        s"""{$clubs}""".fromJson[MembershipRequest].isLeft,
        // A body half moved over keeps a flat field beside `options`: refused, not run without it.
        s"""{$club,"options":{},"target":20}""".fromJson[RecruitmentRequest].isLeft,
        s"""{$club,"options":{},"since":"2026-01-01"}""".fromJson[StatsRequest].isLeft,
        s"""{$clubs,"options":{},"full":true}""".fromJson[HistoryRequest].isLeft,
        s"""{$clubs,"options":{},"trustUsernames":false}""".fromJson[MembershipRequest].isLeft
      )
    },
    test("a mistyped key inside a job request's options is refused rather than dropped") {
      val body = """{"club":{"kind":"by_id","clubId":42},"options":{"targett":3}}"""
      assertTrue(body.fromJson[RecruitmentRequest].isLeft)
    },
    test("an HTTP stats request carries minGames") {
      val body =
        """{"club":{"kind":"by_id","clubId":42},"options":{"since":"2026-01-01","until":"2026-02-01","minGames":3}}"""
      assertTrue(body.fromJson[StatsRequest].map(_.options.minGames) == Right(Some(3)))
    },
    test("a submit request re-encodes its club the way it arrived, the form stored in job_run.params") {
      val request = RecruitmentRequest(
        club = ClubQuery.ById(ClubId(42L)),
        options = RecruitmentOptions.Default,
        autoConfirm = None,
        confirmedCriteriaId = None
      )
      assertTrue(request.toJson.contains("\"kind\":\"by_id\""), request.toJson.contains("\"clubId\":42"))
    },
    test("a criteria view answers its version as `criteriaId`, and one from an older server without it still reads") {
      val view   = CriteriaView(Some(7L), CriteriaSpec.fromCriteria(RecruitmentCriteria.defaultDaily), Nil)
      val withNo = view.copy(criteriaIdOption = None).toJson.replace("\"criteriaId\":null,", "")
      assertTrue(
        view.toJson.contains("\"criteriaId\":7"),
        !withNo.contains("criteriaId"),
        withNo.fromJson[CriteriaView].map(_.criteriaIdOption) == Right(None)
      )
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
    },
    test("a managed-club listing answers with `slug`, and a club holding no name with `clubId` and no `slug`") {
      val named    = ManagedClubResponse(clubId = 42L, slugOption = Some("team-alpha"), name = "A", markedAt = "t")
      val nameless = named.copy(clubId = 43L, slugOption = None)
      assertTrue(
        named.toJson.contains("\"slug\":\"team-alpha\""),
        nameless.toJson == """{"clubId":43,"name":"A","markedAt":"t"}""",
        nameless.toJson.fromJson[ManagedClubResponse] == Right(nameless)
      )
    }
  )
}
