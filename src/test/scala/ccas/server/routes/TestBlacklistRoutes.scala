package ccas.server.routes

import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.{Clock, RIO, Scope, Task, ZLayer}
import zio.http.*
import zio.json.DecoderOps
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport
import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{apiClubJson, apiPlayerJson}
import ccas.analysis.tables.{Club, Player, RecruitmentBlacklist, Tables}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.server.routes.BlacklistRoutes.BlacklistEntryResponse
import ccas.utils.client.ChessComClient
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
  ): RIO[Scope & ProgressDisplay & PostgresClient, Response] =
    RecruitmentTestSupport.fakeChessComClient(responses).flatMap { client =>
      BlacklistRoutes.routes
        .runZIO(jsonRequest(method, path, body))
        .provideSomeLayer[Scope & ProgressDisplay & PostgresClient](ZLayer.succeed[ChessComClient](client))
    }

  /** Convenience for GET / DELETE routes that take no body and don't trigger any HTTP fetch. */
  private def runNoBody(method: Method, path: String): RIO[Scope & ProgressDisplay & PostgresClient, Response] =
    runReq(method, path, body = "", responses = Map.empty)

  /** Decode the response body as a JSON list of [[BlacklistEntryResponse]]. Fails loudly on a non-OK status
    * so a 5xx with a JSON error envelope surfaces the status rather than a cryptic decode error.
    */
  private def parseEntries(response: Response): Task[List[BlacklistEntryResponse]] =
    if (response.status != Status.Ok) {
      zio.ZIO.fail(new IllegalStateException(s"unexpected status ${response.status}"))
    } else {
      for {
        body <- response.body.asString
        list <- zio.ZIO
          .fromEither(body.fromJson[List[BlacklistEntryResponse]])
          .mapError(msg => new IllegalStateException(s"JSON decode failed: $msg"))
      } yield list
    }

  // ==========================================================================
  // Suite: GET /api/blacklist/:clubSlug
  // ==========================================================================

  private def suiteGet = suite("GET /api/blacklist/:slug")(
    testGetUnknownClub,
    testGetEmpty,
    testGetReturnsEntries
  )

  private def testGetUnknownClub = test("GET unknown club returns 404") {
    for {
      response <- runNoBody(Method.GET, s"/api/blacklist/$otherClubSlug")
    } yield assertTrue(response.status == Status.NotFound)
  }

  private def testGetEmpty = test("GET known club with no entries returns []") {
    for {
      _        <- ensureClub
      response <- runNoBody(Method.GET, s"/api/blacklist/$testClubSlug")
      entries  <- parseEntries(response)
    } yield assertTrue(
      response.status == Status.Ok,
      entries.isEmpty
    )
  }

  private def testGetReturnsEntries = test("GET returns active blacklist entries with username") {
    val addedAt = Instant.parse("2026-04-10T00:00:00Z").truncatedTo(ChronoUnit.MICROS)
    for {
      _ <- ensureClub
      _ <- ensurePlayer(pidA, "blacklisted-user")
      _ <- RecruitmentBlacklist.upsert(
        RecruitmentBlacklist(testClubId, pidA, addedAt, expiresAt = None, reason = Some("spam"))
      )
      response <- runNoBody(Method.GET, s"/api/blacklist/$testClubSlug")
      entries  <- parseEntries(response)
    } yield assertTrue(
      response.status == Status.Ok,
      entries.size == 1,
      entries.head.username.contains("blacklisted-user"),
      entries.head.reason.contains("spam"),
      entries.head.playerId == pidA.value
    )
  }

  // ==========================================================================
  // Suite: POST /api/blacklist
  // ==========================================================================

  private def suitePost = suite("POST /api/blacklist")(
    testPostBadJson,
    testPostAddPersistsEntry,
    testPostAddMultipleUsernames,
    testPostAddWithMonthsSetsExpiresAt
  )

  private def testPostBadJson = test("POST with malformed JSON returns 400") {
    for {
      response <- runReq(Method.POST, "/api/blacklist", "not json", Map.empty)
    } yield assertTrue(response.status == Status.BadRequest)
  }

  private def testPostAddPersistsEntry = test("POST adds a single player to the blacklist") {
    val responses = Map(
      s"club/$testClubSlug"  -> apiClubJson(testClubId.value, testClubSlug.value),
      "player/new-blacklist" -> apiPlayerJson(pidB.value, "new-blacklist")
    )
    val body = s"""{"clubSlug":"$testClubSlug","usernames":["new-blacklist"],"reason":"test reason"}"""
    for {
      response <- runReq(Method.POST, "/api/blacklist", body, responses)
      entries  <- RecruitmentBlacklist.selectByClub(testClubId)
    } yield assertTrue(
      response.status == Status.Ok,
      entries.exists(_.playerId == pidB),
      entries.find(_.playerId == pidB).flatMap(_.reason).contains("test reason"),
      entries.find(_.playerId == pidB).flatMap(_.expiresAt).isEmpty
    )
  }

  private def testPostAddMultipleUsernames = test("POST adds multiple players in one request") {
    val responses = Map(
      s"club/$testClubSlug" -> apiClubJson(testClubId.value, testClubSlug.value),
      "player/multi-a"      -> apiPlayerJson(pidC.value, "multi-a"),
      "player/multi-b"      -> apiPlayerJson(pidD.value, "multi-b")
    )
    val body = s"""{"clubSlug":"$testClubSlug","usernames":["multi-a","multi-b"]}"""
    for {
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
    val responses = Map(
      s"club/$testClubSlug" -> apiClubJson(testClubId.value, testClubSlug.value),
      "player/temp-ban"     -> apiPlayerJson(pidE.value, "temp-ban")
    )
    val body = s"""{"clubSlug":"$testClubSlug","usernames":["temp-ban"],"months":3}"""
    for {
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
  // Suite: DELETE /api/blacklist/:clubSlug/:username
  // ==========================================================================

  private def suiteDelete = suite("DELETE /api/blacklist/:slug/:username")(
    testDeleteUnknownClub,
    testDeleteUnknownPlayer,
    testDeleteRemovesEntry
  )

  private def testDeleteUnknownClub = test("DELETE on unknown club returns 404") {
    for {
      response <- runNoBody(Method.DELETE, s"/api/blacklist/$otherClubSlug/anyone")
    } yield assertTrue(response.status == Status.NotFound)
  }

  private def testDeleteUnknownPlayer = test("DELETE on unknown username returns 404") {
    for {
      _        <- ensureClub
      response <- runNoBody(Method.DELETE, s"/api/blacklist/$testClubSlug/this-user-does-not-exist")
    } yield assertTrue(response.status == Status.NotFound)
  }

  private def testDeleteRemovesEntry = test("DELETE removes blacklist row and returns 204") {
    val addedAt = Instant.parse("2026-04-12T00:00:00Z").truncatedTo(ChronoUnit.MICROS)
    for {
      _ <- ensureClub
      _ <- ensurePlayer(pidB, "removable-user")
      _ <- RecruitmentBlacklist.upsert(
        RecruitmentBlacklist(testClubId, pidB, addedAt, expiresAt = None, reason = None)
      )
      before   <- RecruitmentBlacklist.selectByClub(testClubId)
      response <- runNoBody(Method.DELETE, s"/api/blacklist/$testClubSlug/removable-user")
      after    <- RecruitmentBlacklist.selectByClub(testClubId)
    } yield assertTrue(
      before.exists(_.playerId == pidB),
      response.status == Status.NoContent,
      after.forall(_.playerId != pidB)
    )
  }

}
