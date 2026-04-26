package ccas.server.routes

import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.{Scope, ZLayer}
import zio.http.*
import zio.json.DecoderOps
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, Player, RecruitmentBlacklist, Tables}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.server.routes.BlacklistRoutes.BlacklistEntryResponse
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.{CcasLogger, TestCcasLogger}

object TestBlacklistRoutes extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestBlacklistRoutes")(
    suiteGet,
    suitePost,
    suiteDelete
  ).provideShared(
    FreshSchemaLayer("test_blacklist_routes", onInit = Tables.ensureTables),
    TestChessComClientSupport.dummyLayer,
    ZLayer.succeed[CcasLogger](TestCcasLogger.noop),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // --- Fixtures ---

  private val testClubId    = ClubId(8001)
  private val testClubSlug  = ClubSlug("blacklist-test-club")
  private val otherClubSlug = ClubSlug("nonexistent-blacklist-club")
  private val pidA          = PlayerId(8101)
  private val pidB          = PlayerId(8102)
  private val seedAt        = Instant.parse("2026-04-01T00:00:00Z")

  private val ensureClub = Club.upsert(
    Club(testClubId, seedAt, testClubSlug, "Blacklist Test Club", None, None, None)
  )

  private def ensurePlayer(playerId: PlayerId, username: String) =
    Player.insertIfNew(
      Player(playerId, seedAt, Username(username), PlayerStatusCategory.Active, None, seedAt)
    )

  private def jsonRequest(method: Method, path: String, body: String = ""): Request = {
    val url = URL.decode(path).toOption.get
    Request(
      method = method,
      url = url,
      body = if (body.isEmpty) Body.empty else Body.fromString(body)
    ).addHeader(Header.ContentType(MediaType.application.json))
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
      response <- BlacklistRoutes.routes.runZIO(
        jsonRequest(Method.GET, s"/api/blacklist/$otherClubSlug")
      )
    } yield assertTrue(response.status == Status.NotFound)
  }

  private def testGetEmpty = test("GET known club with no entries returns []") {
    for {
      _        <- ensureClub
      response <- BlacklistRoutes.routes.runZIO(
        jsonRequest(Method.GET, s"/api/blacklist/$testClubSlug")
      )
      body   <- response.body.asString
      parsed = body.fromJson[List[BlacklistEntryResponse]]
    } yield assertTrue(
      response.status == Status.Ok,
      parsed == Right(List.empty)
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
      response <- BlacklistRoutes.routes.runZIO(
        jsonRequest(Method.GET, s"/api/blacklist/$testClubSlug")
      )
      body    <- response.body.asString
      entries <- zioFromEither(body.fromJson[List[BlacklistEntryResponse]])
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
    testPostBadJson
  )

  private def testPostBadJson = test("POST with malformed JSON returns 400") {
    for {
      response <- BlacklistRoutes.routes.runZIO(
        jsonRequest(Method.POST, "/api/blacklist", "not json")
      )
    } yield assertTrue(response.status == Status.BadRequest)
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
      response <- BlacklistRoutes.routes.runZIO(
        jsonRequest(Method.DELETE, s"/api/blacklist/$otherClubSlug/anyone")
      )
    } yield assertTrue(response.status == Status.NotFound)
  }

  private def testDeleteUnknownPlayer = test("DELETE on unknown username returns 404") {
    for {
      _ <- ensureClub
      response <- BlacklistRoutes.routes.runZIO(
        jsonRequest(Method.DELETE, s"/api/blacklist/$testClubSlug/this-user-does-not-exist")
      )
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
      response <- BlacklistRoutes.routes.runZIO(
        jsonRequest(Method.DELETE, s"/api/blacklist/$testClubSlug/removable-user")
      )
      after <- RecruitmentBlacklist.selectByClub(testClubId)
    } yield assertTrue(
      before.exists(_.playerId == pidB),
      response.status == Status.NoContent,
      after.forall(_.playerId != pidB)
    )
  }

  // --- Helpers ---

  private def zioFromEither[A](e: Either[String, A]): zio.Task[A] =
    zio.ZIO.fromEither(e).mapError(msg => new RuntimeException(s"JSON decode failed: $msg"))
}
