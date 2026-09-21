package ccas.server.routes

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.{Clock, RIO, Scope, Task, ZIO, ZLayer}
import zio.http.*
import zio.json.{DecoderOps, JsonCodec}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.{ClubQuery, ClubRef, ClubResolution}
import ccas.analysis.apps.recruitment.RecruitmentTestSupport
import ccas.analysis.apps.recruitment.RecruitmentTestSupport.apiPlayerJson
import ccas.analysis.tables.{Club, Player, RecruitmentBlacklist, Tables}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.server.routes.BlacklistRoutes.BlacklistEntryResponse
import ccas.utils.client.{BodyStore, ChessComClient}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient, TestDbCleanup}
import ccas.utils.ProgressDisplay

object TestBlacklistRoutes extends ZIOSpecDefault {

  // FK-aware cleanup: blacklist → player_snapshot → player → club. Run before each test
  // because the suite shares one schema (FreshSchemaLayer.provideShared).
  private val resetTables =
    TestDbCleanup.clearRecruitmentBlacklist *> TestDbCleanup.clearPlayer *> TestDbCleanup.clearClub

  override def spec: Spec[Any, Throwable] = (suite("TestBlacklistRoutes")(
    suiteGet,
    suitePost,
    suiteDelete
  ) @@ TestAspect.before(resetTables)).provideShared(
    FreshSchemaLayer("test_blacklist_routes", onInit = Tables.ensureTables),
    ZLayer.succeed[ProgressDisplay](ProgressDisplay.make(enabled = false)),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // --- Fixtures ---

  private val testClubId    = ClubId(8001)
  private val testClubSlug  = ClubSlug("blacklist-test-club")
  private val renamedSlug   = ClubSlug("blacklist-test-club-renamed")
  private val otherClubSlug = ClubSlug("nonexistent-blacklist-club")
  private val pidA          = PlayerId(8101)
  private val pidB          = PlayerId(8102)
  private val pidC          = PlayerId(8103)
  private val pidD          = PlayerId(8104)
  private val pidE          = PlayerId(8105)
  private val seedAt        = Instant.parse("2026-04-01T00:00:00Z")

  private val ensureClub = Club.upsert(
    Club(testClubId, seedAt, testClubSlug, "Blacklist Test Club", None, None, None)
  )

  // Leaves `testClubSlug` a former name of the club, which only `club_name` still knows.
  private val renameClub = Club.upsert(
    Club(testClubId, seedAt, renamedSlug, "Blacklist Test Club", None, None, None)
  )

  private def ensurePlayer(playerId: PlayerId, username: String) =
    Player.insertIfNew(
      Player(playerId, seedAt, Username(username), PlayerStatusCategory.Active, None, seedAt)
    )

  private def jsonRequest(method: Method, path: String, body: String): Request = {
    val url = URL.decode(path).toOption.get
    Request(
      method = method,
      url = url,
      body = if (body.isEmpty) Body.empty else Body.fromString(body)
    ).addHeader(Header.ContentType(MediaType.application.json))
  }

  /** Run a route request with a fake ChessComClient backed by the given response map. An empty map yields 404
    * for any unmocked URL.
    */
  private def runReq(
    method: Method,
    path: String,
    body: String,
    responses: Map[String, String]
  ): RIO[Scope & ProgressDisplay & PostgresClient & BodyStore, Response] =
    RecruitmentTestSupport.fakeChessComClient(responses).flatMap { client =>
      BlacklistRoutes.routes
        .runZIO(jsonRequest(method, path, body))
        .provideSomeLayer[Scope & ProgressDisplay & PostgresClient](ZLayer.succeed[ChessComClient](client))
    }

  /** Convenience for GET / DELETE routes that take no body and don't trigger any HTTP fetch. */
  private def runNoBody(method: Method, path: String): RIO[Scope & ProgressDisplay & PostgresClient & BodyStore, Response] =
    runReq(method, path, body = "", responses = Map.empty)

  /** Decode a 200 response as the [[ClubResult]] every blacklist route answers with. Fails loudly on any other status
    * so a 5xx with a JSON error envelope surfaces the status rather than a cryptic decode error.
    */
  private def parseResult[A: JsonCodec](response: Response): Task[ClubResult[A]] =
    if (response.status != Status.Ok) {
      ZIO.fail(new IllegalStateException(s"unexpected status ${response.status}"))
    } else {
      for {
        body <- response.body.asString
        result <- ZIO
          .fromEither(body.fromJson[ClubResult[A]])
          .mapError(msg => new IllegalStateException(s"JSON decode failed: $msg"))
      } yield result
    }

  // ==========================================================================
  // Suite: GET /api/blacklist
  // ==========================================================================

  private def suiteGet = suite("GET /api/blacklist")(
    testGetUnnamedClub,
    testGetUnknownClub,
    testGetEmpty,
    testGetReturnsEntries,
    testGetByFormerName
  )

  private def testGetUnnamedClub = test("GET that names no club, or names it twice, returns 400") {
    for {
      none <- runNoBody(Method.GET, "/api/blacklist")
      both <- runNoBody(Method.GET, s"/api/blacklist?slug=$testClubSlug&clubId=$testClubId")
    } yield assertTrue(none.status == Status.BadRequest, both.status == Status.BadRequest)
  }

  private def testGetUnknownClub = test("GET for a club never ingested answers not_local, with no entries") {
    for {
      response <- runNoBody(Method.GET, s"/api/blacklist?slug=$otherClubSlug")
      result   <- parseResult[List[BlacklistEntryResponse]](response)
    } yield assertTrue(
      result.resolution == ClubResolution.NotLocal(ClubQuery.BySlug(otherClubSlug)),
      result.resultOption.isEmpty
    )
  }

  private def testGetEmpty = test("GET known club with no entries returns []") {
    for {
      _        <- ensureClub
      response <- runNoBody(Method.GET, s"/api/blacklist?slug=$testClubSlug")
      result   <- parseResult[List[BlacklistEntryResponse]](response)
    } yield assertTrue(result.resultOption.contains(Nil))
  }

  private def testGetReturnsEntries = test("GET by id returns active blacklist entries with username") {
    val addedAt = Instant.parse("2026-04-10T00:00:00Z").truncatedTo(ChronoUnit.MICROS)
    for {
      _ <- ensureClub
      _ <- ensurePlayer(pidA, "blacklisted-user")
      _ <- RecruitmentBlacklist.upsert(
        RecruitmentBlacklist(testClubId, pidA, addedAt, expiresAt = None, reason = Some("spam"))
      )
      response <- runNoBody(Method.GET, s"/api/blacklist?clubId=$testClubId")
      result   <- parseResult[List[BlacklistEntryResponse]](response)
      entries = result.resultOption.getOrElse(Nil)
    } yield assertTrue(
      entries.size == 1,
      entries.head.username.contains("blacklisted-user"),
      entries.head.reason.contains("spam"),
      entries.head.playerId == pidA.value
    )
  }

  // The regression #254 step 3a exists for: before it, a former name that ran a job still missed here.
  private def testGetByFormerName = test("GET by a former name reaches the renamed club, and says so") {
    for {
      _ <- ensureClub
      _ <- renameClub
      _ <- ensurePlayer(pidA, "blacklisted-user")
      _ <- RecruitmentBlacklist.upsert(RecruitmentBlacklist(testClubId, pidA, seedAt, expiresAt = None, reason = None))
      response <- runNoBody(Method.GET, s"/api/blacklist?slug=$testClubSlug")
      result   <- parseResult[List[BlacklistEntryResponse]](response)
    } yield assertTrue(
      result.club == renamedSlug.value,
      result.resolution == ClubResolution.Renamed(ClubRef(testClubId, renamedSlug), testClubSlug),
      result.resultOption.exists(_.map(_.playerId) == List(pidA.value))
    )
  }

  // ==========================================================================
  // Suite: POST /api/blacklist
  // ==========================================================================

  private def suitePost = suite("POST /api/blacklist")(
    testPostBadJson,
    testPostUnknownClub,
    testPostAddPersistsEntry,
    testPostAddMultipleUsernames,
    testPostAddWithMonthsSetsExpiresAt
  )

  private def bySlug(slug: ClubSlug): String = s"""{"kind":"by_slug","slug":"$slug"}"""

  private def testPostBadJson = test("POST with malformed JSON returns 400") {
    for {
      response <- runReq(Method.POST, "/api/blacklist", "not json", Map.empty)
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testPostUnknownClub = test("POST for a club never ingested blacklists nobody") {
    val responses = Map("player/new-blacklist" -> apiPlayerJson(pidB.value, "new-blacklist"))
    val body      = s"""{"club":${bySlug(otherClubSlug)},"usernames":["new-blacklist"]}"""
    for {
      response <- runReq(Method.POST, "/api/blacklist", body, responses)
      result   <- parseResult[List[String]](response)
      player   <- Player.selectId(pidB)
    } yield assertTrue(result.resultOption.isEmpty, player.isEmpty)
  }

  private def testPostAddPersistsEntry = test("POST adds a single player and answers with the name blacklisted") {
    val responses = Map("player/new-blacklist" -> apiPlayerJson(pidB.value, "new-blacklist"))
    val body      = s"""{"club":${bySlug(testClubSlug)},"usernames":["new-blacklist"],"reason":"test reason"}"""
    for {
      _        <- ensureClub
      response <- runReq(Method.POST, "/api/blacklist", body, responses)
      result   <- parseResult[List[String]](response)
      entries  <- RecruitmentBlacklist.selectByClub(testClubId)
    } yield assertTrue(
      result.resultOption.contains(List("new-blacklist")),
      entries.exists(_.playerId == pidB),
      entries.find(_.playerId == pidB).flatMap(_.reason).contains("test reason"),
      entries.find(_.playerId == pidB).flatMap(_.expiresAt).isEmpty
    )
  }

  private def testPostAddMultipleUsernames = test("POST adds multiple players in one request") {
    val responses = Map(
      "player/multi-a" -> apiPlayerJson(pidC.value, "multi-a"),
      "player/multi-b" -> apiPlayerJson(pidD.value, "multi-b")
    )
    val body = s"""{"club":${bySlug(testClubSlug)},"usernames":["multi-a","multi-b"]}"""
    for {
      _        <- ensureClub
      response <- runReq(Method.POST, "/api/blacklist", body, responses)
      entries  <- RecruitmentBlacklist.selectByClub(testClubId)
      entryC = entries.find(_.playerId == pidC)
      entryD = entries.find(_.playerId == pidD)
    } yield assertTrue(
      response.status == Status.Ok,
      entryC.exists(e => e.reason.isEmpty && e.expiresAt.isEmpty),
      entryD.exists(e => e.reason.isEmpty && e.expiresAt.isEmpty)
    )
  }

  private def testPostAddWithMonthsSetsExpiresAt = test("POST with months sets expiresAt ~3 months out") {
    val responses = Map("player/temp-ban" -> apiPlayerJson(pidE.value, "temp-ban"))
    val body      = s"""{"club":${bySlug(testClubSlug)},"usernames":["temp-ban"],"months":3}"""
    for {
      _        <- ensureClub
      now      <- Clock.instant
      response <- runReq(Method.POST, "/api/blacklist", body, responses)
      entries  <- RecruitmentBlacklist.selectByClub(testClubId)
      entry = entries.find(_.playerId == pidE)
      // Window brackets ~3 months. Wide because (a) the SUT's `Clock.instant` runs a few
      // millis after the test's `now`, and (b) `plusMonths(3)` yields a calendar-arithmetic
      // value (~90-92 days depending on the month). 85d / 95d absorbs both. Don't tighten
      // without addressing both sources of skew first.
      lower = now.plus(Duration.ofDays(85))
      upper = now.plus(Duration.ofDays(95))
    } yield assertTrue(
      response.status == Status.Ok,
      entry.flatMap(_.expiresAt).exists(t => t.isAfter(lower) && t.isBefore(upper))
    )
  }

  // ==========================================================================
  // Suite: DELETE /api/blacklist/:username
  // ==========================================================================

  private def suiteDelete = suite("DELETE /api/blacklist/:username")(
    testDeleteUnknownClub,
    testDeleteUnknownPlayer,
    testDeleteRemovesEntry
  )

  private def testDeleteUnknownClub = test("DELETE for a club never ingested answers not_local and removes nothing") {
    for {
      response <- runNoBody(Method.DELETE, s"/api/blacklist/anyone?slug=$otherClubSlug")
      result   <- parseResult[Boolean](response)
    } yield assertTrue(result.resultOption.isEmpty)
  }

  private def testDeleteUnknownPlayer = test("DELETE on unknown username returns 404") {
    for {
      _        <- ensureClub
      response <- runNoBody(Method.DELETE, s"/api/blacklist/this-user-does-not-exist?slug=$testClubSlug")
    } yield assertTrue(response.status == Status.NotFound)
  }

  private def testDeleteRemovesEntry = test("DELETE removes the blacklist row, then says there was none to remove") {
    val addedAt = Instant.parse("2026-04-12T00:00:00Z").truncatedTo(ChronoUnit.MICROS)
    val path    = s"/api/blacklist/removable-user?clubId=$testClubId"
    val remove  = runNoBody(Method.DELETE, path).flatMap(parseResult[Boolean])
    for {
      _ <- ensureClub
      _ <- ensurePlayer(pidB, "removable-user")
      _ <- RecruitmentBlacklist.upsert(
        RecruitmentBlacklist(testClubId, pidB, addedAt, expiresAt = None, reason = None)
      )
      before <- RecruitmentBlacklist.selectByClub(testClubId)
      first  <- remove
      after  <- RecruitmentBlacklist.selectByClub(testClubId)
      again  <- remove
    } yield assertTrue(
      before.exists(_.playerId == pidB),
      first.resultOption.contains(true),
      after.forall(_.playerId != pidB),
      again.resultOption.contains(false)
    )
  }

}
