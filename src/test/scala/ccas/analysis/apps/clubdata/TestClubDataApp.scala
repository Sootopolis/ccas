package ccas.analysis.apps.clubdata

import java.time.Instant

import com.augustnagro.magnum.sql
import zio.{Chunk, RIO, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{apiClubJson, apiDailyMatchJson}
import ccas.analysis.tables.{Club, ClubMatch, Tables}
import ccas.api.misc.enums.{ClubMatchStatus, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug}
import ccas.utils.{CcasLogger, TestCcasLogger}
import ccas.utils.client.{ChessComClient, TestChessComClientSupport}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.connectZIO

object TestClubDataApp extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestClubDataApp")(
    suiteParseMinAgeArg,
    suiteRefreshClub
  ).provideShared(
    FreshSchemaLayer("test_club_data_app", onInit = Tables.ensureTables),
    ZLayer.succeed(TestCcasLogger.noop)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite: parseMinAgeArg (pure)
  // ==========================================================================

  private def suiteParseMinAgeArg = suite("parseMinAgeArg")(
    test("no --min-age flag returns None and unchanged args") {
      val args   = Chunk("club-a", "club-b")
      val result = ClubDataApp.parseMinAgeArg(args)
      assertTrue(result == Right((None, args)))
    },
    test("--min-age with hours returns Some(hours) and strips both") {
      val result = ClubDataApp.parseMinAgeArg(Chunk("club-a", "--min-age", "24"))
      assertTrue(result == Right((Some(24), Chunk("club-a"))))
    },
    test("--min-age at end of args is an error") {
      val result = ClubDataApp.parseMinAgeArg(Chunk("club-a", "--min-age"))
      assertTrue(result.isLeft, result.left.exists(_.contains("--min-age requires")))
    },
    test("--min-age followed by non-integer is an error") {
      val result = ClubDataApp.parseMinAgeArg(Chunk("--min-age", "club-a"))
      assertTrue(result.isLeft, result.left.exists(_.contains("--min-age requires")))
    },
    test("--min-age with zero hours is allowed") {
      val result = ClubDataApp.parseMinAgeArg(Chunk("--min-age", "0"))
      assertTrue(result == Right((Some(0), Chunk.empty)))
    },
    test("--min-age in middle of slug list strips cleanly") {
      val result = ClubDataApp.parseMinAgeArg(Chunk("club-a", "--min-age", "12", "club-b"))
      assertTrue(result == Right((Some(12), Chunk("club-a", "club-b"))))
    }
  )

  // ==========================================================================
  // Suite: refreshClub rename-404 recovery
  // ==========================================================================

  private val stuckClubId = ClubId(9000)
  private val oldSlug     = ClubSlug("old-slug")
  private val newSlug     = ClubSlug("new-slug")
  private val refMatchId  = ClubMatchId(9_999_001)

  /** Wipes the tables touched by the refreshClub tests so each case starts from a clean slate. */
  private val clearTables: ZIO[PostgresClient, Throwable, Unit] =
    for {
      _ <- connectZIO(sql"DELETE FROM club_admin".update.run())
      _ <- connectZIO(sql"DELETE FROM club_match".update.run())
      _ <- connectZIO(sql"DELETE FROM club".update.run())
    } yield ()

  private val seedCreated  = Instant.parse("2024-01-01T00:00:00Z")
  private val seedMatchStart = Instant.parse("2024-06-01T00:00:00Z")
  private val seedMatchEnd   = Instant.parse("2024-06-30T00:00:00Z")
  private val seedMatchFetched = Instant.parse("2024-07-01T00:00:00Z")

  private def seedStaleClub(
    clubId: ClubId,
    slug: ClubSlug,
    withMatchRef: Boolean
  ): ZIO[PostgresClient, Throwable, Unit] =
    for {
      _ <- Club.upsert(Club(clubId, seedCreated, slug, "Stale Club", None, None, None))
      _ <- ZIO.whenDiscard(withMatchRef) {
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
      }
    )
    TestChessComClientSupport.fakeClient(routes)
  }

  private def runRefresh(client: ChessComClient): RIO[PostgresClient & CcasLogger, ClubDataApp.RefreshResult] =
    for {
      xa     <- ZIO.service[PostgresClient]
      logger <- ZIO.service[CcasLogger]
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
        _       <- seedStaleClub(stuckClubId, oldSlug, withMatchRef = true)
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
        _       <- seedStaleClub(stuckClubId, oldSlug, withMatchRef = false)
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
        _         <- seedStaleClub(stuckClubId, oldSlug, withMatchRef = true)
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
        _         <- seedStaleClub(stuckClubId, oldSlug, withMatchRef = true)
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
}
