package ccas.analysis.apps.clubdata

import java.time.Instant

import com.augustnagro.magnum.sql
import zio.{Chunk, RIO, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{apiClubJson, apiDailyMatchJson, apiPlayerJson}
import ccas.analysis.tables.{Club, ClubAdmin, ClubMatch, ClubMatchRef, Player, PlayerSnapshot, Tables}
import ccas.api.misc.enums.{ClubMatchStatus, PlayerStatusCategory, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.ProgressDisplay
import ccas.utils.client.{ChessComClient, TestChessComClientSupport}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.connectZIO

object TestClubDataApp extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestClubDataApp")(
    suiteParseArgs,
    suiteRefreshClub,
    suiteResolveAndPersistAdmins
  ).provideShared(
    FreshSchemaLayer("test_club_data_app", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false))
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite: parseArgs (pure)
  // ==========================================================================

  private def suiteParseArgs = suite("parseArgs")(
    test("no --min-age flag returns None and parsed slugs") {
      val result = ClubDataApp.parseArgs(Chunk("club-a", "club-b"))
      assertTrue(result == Right(ClubDataApp.ClubDataAppArgs(None, List(ClubSlug("club-a"), ClubSlug("club-b")))))
    },
    test("--min-age with hours returns Some(hours) and strips both") {
      val result = ClubDataApp.parseArgs(Chunk("club-a", "--min-age", "24"))
      assertTrue(result == Right(ClubDataApp.ClubDataAppArgs(Some(24), List(ClubSlug("club-a")))))
    },
    test("--min-age at end of args is an error") {
      val result = ClubDataApp.parseArgs(Chunk("club-a", "--min-age"))
      assertTrue(result.isLeft, result.left.exists(_.contains("--min-age requires")))
    },
    test("--min-age followed by non-integer is an error") {
      val result = ClubDataApp.parseArgs(Chunk("--min-age", "club-a"))
      assertTrue(result.isLeft, result.left.exists(_.contains("--min-age requires")))
    },
    test("--min-age with zero hours is allowed") {
      val result = ClubDataApp.parseArgs(Chunk("--min-age", "0"))
      assertTrue(result == Right(ClubDataApp.ClubDataAppArgs(Some(0), Nil)))
    },
    test("--min-age in middle of slug list strips cleanly") {
      val result = ClubDataApp.parseArgs(Chunk("club-a", "--min-age", "12", "club-b"))
      assertTrue(result == Right(ClubDataApp.ClubDataAppArgs(Some(12), List(ClubSlug("club-a"), ClubSlug("club-b")))))
    },
    test("unknown -- flags are dropped") {
      val result = ClubDataApp.parseArgs(Chunk("club-a", "--unknown", "club-b"))
      assertTrue(result == Right(ClubDataApp.ClubDataAppArgs(None, List(ClubSlug("club-a"), ClubSlug("club-b")))))
    }
  )

  // ==========================================================================
  // Suite: refreshClub rename-404 recovery
  // ==========================================================================

  private val stuckClubId = ClubId(9000)
  private val oldSlug     = ClubSlug("old-slug")
  private val newSlug     = ClubSlug("new-slug")
  private val refMatchId  = ClubMatchId(9_999_001)

  /** Wipes the tables touched by the refreshClub and resolveAndPersistAdmins tests so each case starts from a clean
    * slate.
    */
  private val clearTables: ZIO[PostgresClient, Throwable, Unit] =
    for {
      _ <- connectZIO(sql"DELETE FROM club_admin".update.run())
      _ <- connectZIO(sql"DELETE FROM club_match_ref".update.run())
      _ <- connectZIO(sql"DELETE FROM club_match".update.run())
      _ <- connectZIO(sql"DELETE FROM club".update.run())
      _ <- connectZIO(sql"DELETE FROM player_snapshot".update.run())
      _ <- connectZIO(sql"DELETE FROM player".update.run())
    } yield ()

  private val seedCreated  = Instant.parse("2024-01-01T00:00:00Z")
  private val seedMatchStart = Instant.parse("2024-06-01T00:00:00Z")
  private val seedMatchEnd   = Instant.parse("2024-06-30T00:00:00Z")
  private val seedMatchFetched = Instant.parse("2024-07-01T00:00:00Z")

  /** Seeds a stale club. `withInferredRef` seeds a `club_match` row (tier-2 synthesis source for `findOrInfer`);
    * `withExplicitRef` seeds a `club_match_ref` row directly (tier-1 source). Independent flags — either, both, or
    * neither can be set.
    */
  private def seedStaleClub(
    clubId: ClubId,
    slug: ClubSlug,
    withInferredRef: Boolean,
    withExplicitRef: Boolean
  ): ZIO[PostgresClient, Throwable, Unit] =
    for {
      _ <- Club.upsert(Club(clubId, seedCreated, slug, "Stale Club", None, None, None))
      _ <- ZIO.whenDiscard(withInferredRef) {
        ClubMatch.upsert(
          ClubMatch(
            refMatchId,
            s"Match ${ClubMatchId.unwrap(refMatchId)}",
            ClubMatchStatus.Finished,
            TimeClass.Daily,
            Some(seedMatchStart),
            Some(seedMatchEnd),
            1,
            Some(clubId),
            20,
            None,
            10,
            seedMatchFetched
          )
        )
      }
      _ <- ZIO.whenDiscard(withExplicitRef) {
        ClubMatchRef.upsert(ClubMatchRef(clubId, refMatchId, isLive = false, isTeam1 = true))
      }
    } yield ()

  private def fakeClient(
    responses: Map[String, String],
    profileFailureStatus: Status = Status.NotFound
  ): RIO[PostgresClient, ChessComClient] = {
    val emptyClubMatches = """{"finished": [], "in_progress": [], "registered": []}"""
    val routes: Routes[Any, Response] = Routes(
      Method.GET / "pub" / "club" / string("slug") / "matches" -> handler {
        (_: String, _: Request) => Response.json(emptyClubMatches)
      },
      Method.GET / "pub" / "club" / string("slug") -> handler { (slug: String, _: Request) =>
        responses.get(s"club/$slug") match {
          case Some(json) => Response.json(json)
          case None       => Response(status = profileFailureStatus)
        }
      },
      Method.GET / "pub" / "match" / long("matchId") -> handler { (matchId: Long, _: Request) =>
        responses.get(s"match/$matchId") match {
          case Some(json) => Response.json(json)
          case None       => Response(status = Status.NotFound)
        }
      },
      Method.GET / "pub" / "player" / string("username") -> handler { (username: String, _: Request) =>
        responses.get(s"player/$username") match {
          case Some(json) => Response.json(json)
          case None       => Response(status = Status.NotFound)
        }
      }
    )
    TestChessComClientSupport.fakeClient(routes)
  }

  private def runRefresh(client: ChessComClient): RIO[ProgressDisplay & PostgresClient, ClubDataApp.RefreshResult] =
    for {
      xa     <- ZIO.service[PostgresClient]
      logger <- ZIO.service[ProgressDisplay]
      result <- ClubDataApp.refresh(None).provideEnvironment(zio.ZEnvironment(client, xa, logger))
    } yield result

  private def suiteRefreshClub = suite("refreshClub rename-404 recovery")(
    test("404 + match ref with new slug → rediscovers, retries, and persists new slug") {
      val matchJson = apiDailyMatchJson(
        matchId = ClubMatchId.unwrap(refMatchId),
        team1Club = newSlug.value,
        team2Club = "opponent-club",
        team1Players = List(("alice", 1)),
        team2Players = List(("bob", 1))
      )
      val responses = Map(
        s"club/${newSlug.value}"                -> apiClubJson(ClubId.unwrap(stuckClubId), newSlug.value),
        s"match/${ClubMatchId.unwrap(refMatchId)}" -> matchJson
      )
      for {
        _       <- clearTables
        _       <- seedStaleClub(stuckClubId, oldSlug, withInferredRef = true, withExplicitRef = false)
        client  <- fakeClient(responses)
        result  <- runRefresh(client)
        updated <- Club.selectId(stuckClubId)
        // Verify the tier-2 synthesis was promoted to an explicit `club_match_ref` row by `findOrInfer`.
        promoted <- ClubMatchRef.selectId(stuckClubId)
      } yield assertTrue(
        result.clubsProcessed == 1,
        result.clubsFailed == 0,
        updated.exists(_.slug == newSlug),
        updated.exists(_.fetchedAt.isDefined),
        promoted.exists(_.matchId == refMatchId)
      )
    },
    // Regression: production `mkr-community` had a `club_match_ref` row but no `club_match` row; before the
    // `ClubMatchRef.findOrInfer` fix, `Club.slugFromMatchRef` consulted only `club_match` (via
    // `ClubMatch.inferClubMatchRef`) and the rename recovery never fired.
    test("404 + explicit club_match_ref only (no club_match row) → rediscovers via tier 1, retries, persists new slug") {
      val matchJson = apiDailyMatchJson(
        matchId = ClubMatchId.unwrap(refMatchId),
        team1Club = newSlug.value,
        team2Club = "opponent-club",
        team1Players = List(("alice", 1)),
        team2Players = List(("bob", 1))
      )
      val responses = Map(
        s"club/${newSlug.value}"                   -> apiClubJson(ClubId.unwrap(stuckClubId), newSlug.value),
        s"match/${ClubMatchId.unwrap(refMatchId)}" -> matchJson
      )
      for {
        _       <- clearTables
        _       <- seedStaleClub(stuckClubId, oldSlug, withInferredRef = false, withExplicitRef = true)
        client  <- fakeClient(responses)
        result  <- runRefresh(client)
        updated <- Club.selectId(stuckClubId)
      } yield assertTrue(
        result.clubsProcessed == 1,
        result.clubsFailed == 0,
        updated.exists(_.slug == newSlug),
        updated.exists(_.fetchedAt.isDefined)
      )
    },
    test("404 + no match ref → still fails, fetched_at untouched") {
      for {
        _       <- clearTables
        _       <- seedStaleClub(stuckClubId, oldSlug, withInferredRef = false, withExplicitRef = false)
        client  <- fakeClient(Map.empty) // old-slug 404s, no match ref to recover
        result  <- runRefresh(client)
        unchanged <- Club.selectId(stuckClubId)
      } yield assertTrue(
        result.clubsProcessed == 1,
        result.clubsFailed == 1,
        unchanged.exists(_.slug == oldSlug),
        unchanged.exists(_.fetchedAt.isEmpty)
      )
    },
    test("404 + match ref returns same slug → no retry, still fails") {
      val matchJson = apiDailyMatchJson(
        matchId = ClubMatchId.unwrap(refMatchId),
        team1Club = oldSlug.value, // ref points back at the same stale slug
        team2Club = "opponent-club",
        team1Players = List(("alice", 1)),
        team2Players = List(("bob", 1))
      )
      val responses = Map(
        s"match/${ClubMatchId.unwrap(refMatchId)}" -> matchJson
      )
      for {
        _         <- clearTables
        _         <- seedStaleClub(stuckClubId, oldSlug, withInferredRef = true, withExplicitRef = false)
        client    <- fakeClient(responses)
        result    <- runRefresh(client)
        unchanged <- Club.selectId(stuckClubId)
      } yield assertTrue(
        result.clubsProcessed == 1,
        result.clubsFailed == 1,
        unchanged.exists(_.slug == oldSlug),
        unchanged.exists(_.fetchedAt.isEmpty)
      )
    },
    test("non-404 profile error → no rediscover attempt") {
      val matchJson = apiDailyMatchJson(
        matchId = ClubMatchId.unwrap(refMatchId),
        team1Club = newSlug.value,
        team2Club = "opponent-club",
        team1Players = List(("alice", 1)),
        team2Players = List(("bob", 1))
      )
      // Even though a match ref exists pointing at newSlug, a 500 on the profile must NOT trigger the 404 recovery
      // path. The club should fail, and the slug should stay as-is (since no ApiClub upsert happens).
      val responses = Map(
        s"club/${newSlug.value}"                   -> apiClubJson(ClubId.unwrap(stuckClubId), newSlug.value),
        s"match/${ClubMatchId.unwrap(refMatchId)}" -> matchJson
      )
      for {
        _         <- clearTables
        _         <- seedStaleClub(stuckClubId, oldSlug, withInferredRef = true, withExplicitRef = false)
        client    <- fakeClient(responses, profileFailureStatus = Status.InternalServerError)
        result    <- runRefresh(client)
        unchanged <- Club.selectId(stuckClubId)
      } yield assertTrue(
        result.clubsProcessed == 1,
        result.clubsFailed == 1,
        unchanged.exists(_.slug == oldSlug),
        unchanged.exists(_.fetchedAt.isEmpty)
      )
    }
  )

  // ==========================================================================
  // Suite: ClubAdminResolver.resolveAndPersistAdmins
  // ==========================================================================

  private val adminClubId = ClubId(9500)
  private val adminClubSlug = ClubSlug("admin-club")

  private def seedAdminClub: RIO[PostgresClient, Unit] =
    Club.upsert(Club(adminClubId, seedCreated, adminClubSlug, "Admin Club", None, None, None)).unit

  private def seedPlayer(
    playerId: PlayerId,
    username: Username,
    status: PlayerStatusCategory = PlayerStatusCategory.Active
  ): RIO[PostgresClient, Unit] =
    Player.insert(Player(playerId, seedCreated, username, status, None, seedCreated))

  private def snapshotCount(playerId: PlayerId): RIO[PostgresClient, Int] =
    PlayerSnapshot.selectId(playerId).map(_.size)

  private def suiteResolveAndPersistAdmins = suite("resolveAndPersistAdmins")(
    test("fresh admin not in DB → Player inserted, no snapshot, club_admin row written") {
      val adminUsername = Username.wrap("fresh-admin")
      val adminPlayerId = PlayerId(9100)
      val responses = Map(
        s"player/${adminUsername.value}" -> apiPlayerJson(PlayerId.unwrap(adminPlayerId), adminUsername.value)
      )
      for {
        _      <- clearTables
        _      <- seedAdminClub
        client <- fakeClient(responses)
        result <- ClubAdminResolver.resolveAndPersistAdmins(client, adminClubId, Set(adminUsername), Set.empty)
        row    <- Player.selectId(adminPlayerId)
        snaps  <- snapshotCount(adminPlayerId)
        admins <- ClubAdmin.selectPlayerIdsByClub(adminClubId)
      } yield assertTrue(
        result == Set(adminPlayerId),
        row.exists(_.username == adminUsername),
        snaps == 0,
        admins == Set(adminPlayerId)
      )
    },
    test("admin already in DB under current username → no fetch needed, no snapshot, club_admin row points to existing player") {
      val adminUsername = Username.wrap("current-admin")
      val adminPlayerId = PlayerId(9150)
      // Intentionally provide NO player/ response: if the resolver tries to fetch the player endpoint for an
      // already-current username, the fake client will 404 and the test will fail.
      for {
        _      <- clearTables
        _      <- seedAdminClub
        _      <- seedPlayer(adminPlayerId, adminUsername)
        client <- fakeClient(Map.empty)
        result <- ClubAdminResolver.resolveAndPersistAdmins(client, adminClubId, Set(adminUsername), Set.empty)
        row    <- Player.selectId(adminPlayerId)
        snaps  <- snapshotCount(adminPlayerId)
        admins <- ClubAdmin.selectPlayerIdsByClub(adminClubId)
      } yield assertTrue(
        result == Set(adminPlayerId),
        row.exists(_.username == adminUsername),
        snaps == 0,
        admins == Set(adminPlayerId)
      )
    },
    test("renamed admin — existing Player row under old username → snapshot archives prior state, Player updated") {
      val oldUsername = Username.wrap("old-admin")
      val newUsername = Username.wrap("new-admin")
      val adminPlayerId = PlayerId(9200)
      val responses = Map(
        s"player/${newUsername.value}" -> apiPlayerJson(PlayerId.unwrap(adminPlayerId), newUsername.value)
      )
      for {
        _      <- clearTables
        _      <- seedAdminClub
        _      <- seedPlayer(adminPlayerId, oldUsername)
        client <- fakeClient(responses)
        result <- ClubAdminResolver.resolveAndPersistAdmins(client, adminClubId, Set(newUsername), Set.empty)
        row    <- Player.selectId(adminPlayerId)
        snaps  <- PlayerSnapshot.selectId(adminPlayerId)
        admins <- ClubAdmin.selectPlayerIdsByClub(adminClubId)
      } yield assertTrue(
        result == Set(adminPlayerId),
        row.exists(_.username == newUsername),
        snaps.size == 1,
        snaps.exists(_.username == oldUsername),
        admins == Set(adminPlayerId)
      )
    },
    test("renamed admin — re-running does not write duplicate snapshot") {
      val oldUsername = Username.wrap("rerun-old")
      val newUsername = Username.wrap("rerun-new")
      val adminPlayerId = PlayerId(9250)
      val responses = Map(
        s"player/${newUsername.value}" -> apiPlayerJson(PlayerId.unwrap(adminPlayerId), newUsername.value)
      )
      for {
        _       <- clearTables
        _       <- seedAdminClub
        _       <- seedPlayer(adminPlayerId, oldUsername)
        client  <- fakeClient(responses)
        _       <- ClubAdminResolver.resolveAndPersistAdmins(client, adminClubId, Set(newUsername), Set.empty)
        _       <- ClubAdminResolver.resolveAndPersistAdmins(client, adminClubId, Set(newUsername), Set(adminPlayerId))
        row     <- Player.selectId(adminPlayerId)
        snaps   <- snapshotCount(adminPlayerId)
      } yield assertTrue(
        row.exists(_.username == newUsername),
        snaps == 1
      )
    }
  )
}
