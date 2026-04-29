package ccas.analysis.apps.history

import java.time.Instant

import com.augustnagro.magnum.sql
import zio.{RIO, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{
  apiDailyMatchJson,
  apiMatchBoardJson,
  apiPlayerJson
}
import ccas.analysis.tables.*
import ccas.api.misc.enums.{ClubMatchStatus, PlayerStatusCategory, TimeClass, Title}
import ccas.api.misc.subtypes.{ClubMatchId, PlayerId, Username}
import ccas.utils.ProgressDisplay
import ccas.utils.client.{ChessComClient, TestChessComClientSupport}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.connectZIO

object TestHistorySeeding extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestHistorySeeding")(
    suiteRetryUnresolvedPlayers
  ).provideShared(
    FreshSchemaLayer("test_history_seeding", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false))
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Fixtures
  // ==========================================================================

  private val matchId      = ClubMatchId(777_001)
  private val boardNum: Short = 1
  private val ourPlayerId  = PlayerId(50_001)
  private val oppPlayerId  = PlayerId(50_002)
  private val oldUsername  = Username.wrap("alice-old")
  private val newUsername  = Username.wrap("alice-new")
  private val oppUsername  = Username.wrap("bob")
  private val t0           = Instant.parse("2025-01-01T00:00:00Z")
  private val t1           = Instant.parse("2025-06-01T00:00:00Z")

  private val clearTables: ZIO[PostgresClient, Throwable, Unit] =
    for {
      _ <- connectZIO(sql"DELETE FROM unresolved_board_player".update.run())
      _ <- connectZIO(sql"DELETE FROM club_match_board".update.run())
      _ <- connectZIO(sql"DELETE FROM club_match".update.run())
      _ <- connectZIO(sql"DELETE FROM player_snapshot".update.run())
      _ <- connectZIO(sql"DELETE FROM player".update.run())
    } yield ()

  private def seedClubMatch: RIO[PostgresClient, Unit] =
    ClubMatch.upsert(
      ClubMatch(
        matchId,
        s"Match ${ClubMatchId.unwrap(matchId)}",
        ClubMatchStatus.Finished,
        TimeClass.Daily,
        Some(t0),
        Some(t1),
        1,
        None,
        10,
        None,
        10,
        t1
      )
    ).unit

  private def seedBoardRow(
    team1Pid: Option[PlayerId],
    team2Pid: Option[PlayerId]
  ): RIO[PostgresClient, Unit] =
    ClubMatchBoard.insert(
      ClubMatchBoard(
        matchId = matchId,
        board = boardNum,
        team1PlayerId = team1Pid,
        team1FairPlay = false,
        team2PlayerId = team2Pid,
        team2FairPlay = false,
        team1ScoreX2 = 1,
        team2ScoreX2 = 1
      )
    ).unit

  private def seedUnresolved(username: Username): RIO[PostgresClient, Unit] =
    UnresolvedBoardPlayer.insert(matchId, boardNum, isTeam1 = true, username).unit

  private def seedPlayer(
    playerId: PlayerId,
    username: Username,
    title: Option[Title] = None,
    since: Instant = t0
  ): RIO[PostgresClient, Unit] =
    Player.insertIfNew(
      Player(playerId, t0, username, PlayerStatusCategory.Active, title, since)
    ).unit

  private def playerRow(playerId: PlayerId): RIO[PostgresClient, Option[Player]] =
    Player.selectId(playerId)

  private def snapshotCount(playerId: PlayerId): RIO[PostgresClient, Int] =
    PlayerSnapshot.selectId(playerId).map(_.size)

  private def unresolvedCount: RIO[PostgresClient, Int] =
    UnresolvedBoardPlayer.selectAll.map(_.size)

  private def boardTeam1Pid: RIO[PostgresClient, Option[PlayerId]] =
    ClubMatchBoard.selectMatch(matchId).map(_.find(_.board == boardNum).flatMap(_.team1PlayerId))

  // ==========================================================================
  // Fake HTTP client
  // ==========================================================================

  private def fakeClient(
    responses: Map[String, String],
    playerErrors: Map[String, Status] = Map.empty
  ): RIO[PostgresClient, ChessComClient] = {
    val routes: Routes[Any, Response] = Routes(
      Method.GET / "pub" / "player" / string("username") -> handler { (username: String, _: Request) =>
        playerErrors.get(username) match {
          case Some(status) => Response(status = status)
          case None =>
            responses.get(s"player/$username") match {
              case Some(json) => Response.json(json)
              case None       => Response(status = Status.NotFound)
            }
        }
      },
      Method.GET / "pub" / "match" / long("id") / int("board") -> handler {
        (id: Long, board: Int, _: Request) =>
          responses.get(s"match/$id/$board") match {
            case Some(json) => Response.json(json)
            case None       => Response(status = Status.NotFound)
          }
      },
      Method.GET / "pub" / "match" / long("id") -> handler { (id: Long, _: Request) =>
        responses.get(s"match/$id") match {
          case Some(json) => Response.json(json)
          case None       => Response(status = Status.NotFound)
        }
      }
    )
    TestChessComClientSupport.fakeClient(routes)
  }

  private def runRetry(
    client: ChessComClient
  ): RIO[ProgressDisplay & PostgresClient, Int] =
    HistorySeeding.retryUnresolvedPlayers(client)

  private val matchIdL = ClubMatchId.unwrap(matchId)

  private val newPlayerJson =
    apiPlayerJson(PlayerId.unwrap(ourPlayerId), newUsername.value)

  private val boardJsonRenamed =
    apiMatchBoardJson(matchIdL, boardNum.toInt, newUsername.value, oppUsername.value)

  private val boardJsonStale =
    apiMatchBoardJson(matchIdL, boardNum.toInt, oldUsername.value, oppUsername.value)

  private val matchJsonForRecovery =
    apiDailyMatchJson(
      matchId = matchIdL,
      team1Club = "our-club",
      team2Club = "their-club",
      team1Players = List((oldUsername.value, boardNum.toInt)),
      team2Players = List((oppUsername.value, boardNum.toInt))
    )

  // ==========================================================================
  // Tests
  // ==========================================================================

  private def suiteRetryUnresolvedPlayers = suite("retryUnresolvedPlayers")(
    test("404 + opposing side resolved in DB + board shows new username → DB-first recovery resolves the row") {
      val responses = Map(
        s"player/${newUsername.value}" -> newPlayerJson,
        s"match/$matchIdL/${boardNum.toInt}" -> boardJsonRenamed
      )
      for {
        _      <- clearTables
        _      <- seedClubMatch
        _      <- seedPlayer(oppPlayerId, oppUsername)
        _      <- seedBoardRow(team1Pid = None, team2Pid = Some(oppPlayerId))
        _      <- seedUnresolved(oldUsername)
        client <- fakeClient(responses, playerErrors = Map(oldUsername.value -> Status.NotFound))
        resolved <- runRetry(client)
        unresolved <- unresolvedCount
        team1Pid   <- boardTeam1Pid
        snaps      <- snapshotCount(ourPlayerId)
      } yield assertTrue(
        resolved == 1,
        unresolved == 0,
        team1Pid.contains(ourPlayerId),
        snaps == 0 // fresh player discovered via recovery → no fabricated snapshot
      )
    },
    test("404 + player already in DB under stale username → archive-and-update writes exactly one snapshot") {
      val responses = Map(
        s"player/${newUsername.value}" -> newPlayerJson,
        s"match/$matchIdL/${boardNum.toInt}" -> boardJsonRenamed
      )
      for {
        _ <- clearTables
        _ <- seedClubMatch
        _ <- seedPlayer(oppPlayerId, oppUsername)
        _ <- seedPlayer(ourPlayerId, oldUsername) // known with old username
        _ <- seedBoardRow(team1Pid = None, team2Pid = Some(oppPlayerId))
        _ <- seedUnresolved(oldUsername)
        client <- fakeClient(responses, playerErrors = Map(oldUsername.value -> Status.NotFound))
        // First retry: does the recovery + archive-and-update
        _          <- runRetry(client)
        snapsAfter1 <- snapshotCount(ourPlayerId)
        playerAfter <- playerRow(ourPlayerId)
        // Reinsert unresolved row and retry again to verify PK-dedup (snapshot count stays at 1)
        _          <- seedUnresolved(newUsername) // now the username on the row is current
        _          <- runRetry(client)
        snapsAfter2 <- snapshotCount(ourPlayerId)
      } yield assertTrue(
        snapsAfter1 == 1,
        snapsAfter2 == 1,
        playerAfter.exists(_.username == newUsername)
      )
    },
    test("404 + both sides unresolved, match endpoint identifies opposing → recovery via match-endpoint fallback") {
      val responses = Map(
        s"player/${newUsername.value}" -> newPlayerJson,
        s"match/$matchIdL/${boardNum.toInt}" -> boardJsonRenamed,
        s"match/$matchIdL" -> matchJsonForRecovery
      )
      for {
        _      <- clearTables
        _      <- seedClubMatch
        _      <- seedBoardRow(team1Pid = None, team2Pid = None) // both sides unresolved
        _      <- seedUnresolved(oldUsername)
        client <- fakeClient(responses, playerErrors = Map(oldUsername.value -> Status.NotFound))
        resolved <- runRetry(client)
        unresolved <- unresolvedCount
        team1Pid   <- boardTeam1Pid
      } yield assertTrue(
        resolved == 1,
        unresolved == 0,
        team1Pid.contains(ourPlayerId)
      )
    },
    test("404 + board endpoint cached with stale username → row persists, no loop") {
      val responses = Map(
        s"match/$matchIdL/${boardNum.toInt}" -> boardJsonStale,
        s"match/$matchIdL" -> matchJsonForRecovery
      )
      for {
        _ <- clearTables
        _ <- seedClubMatch
        _ <- seedPlayer(oppPlayerId, oppUsername)
        _ <- seedBoardRow(team1Pid = None, team2Pid = Some(oppPlayerId))
        _ <- seedUnresolved(oldUsername)
        client <- fakeClient(responses, playerErrors = Map(oldUsername.value -> Status.NotFound))
        resolved <- runRetry(client)
        unresolved <- unresolvedCount
        team1Pid   <- boardTeam1Pid
      } yield assertTrue(
        resolved == 0,
        unresolved == 1,
        team1Pid.isEmpty
      )
    },
    test("Non-404 error on happy-path fetch → no recovery attempted, row persists") {
      for {
        _ <- clearTables
        _ <- seedClubMatch
        _ <- seedBoardRow(team1Pid = None, team2Pid = None)
        _ <- seedUnresolved(oldUsername)
        client <- fakeClient(
          responses = Map.empty,
          playerErrors = Map(oldUsername.value -> Status.InternalServerError)
        )
        resolved <- runRetry(client)
        unresolved <- unresolvedCount
      } yield assertTrue(
        resolved == 0,
        unresolved == 1
      )
    },
    test("Happy path: stale username still valid on API + player row has different title → snapshot of prior state") {
      // Player in DB had title FM, API now returns no title. Happy-path fetch succeeds (no 404).
      val happyPlayerJson =
        apiPlayerJson(PlayerId.unwrap(ourPlayerId), oldUsername.value) // status=basic, no title
      val responses = Map(s"player/${oldUsername.value}" -> happyPlayerJson)
      for {
        _ <- clearTables
        _ <- seedClubMatch
        _ <- seedPlayer(ourPlayerId, oldUsername, title = Some(Title.FM))
        _ <- seedBoardRow(team1Pid = None, team2Pid = None)
        _ <- seedUnresolved(oldUsername)
        client <- fakeClient(responses)
        _       <- runRetry(client)
        snaps   <- snapshotCount(ourPlayerId)
        player  <- playerRow(ourPlayerId)
      } yield assertTrue(
        snaps == 1,
        player.exists(_.title.isEmpty)
      )
    }
  )
}
