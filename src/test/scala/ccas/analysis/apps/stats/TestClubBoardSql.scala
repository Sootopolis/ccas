package ccas.analysis.apps.stats

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import com.augustnagro.magnum.sql

import ccas.analysis.tables.{Club, ClubMatch, ClubMatchBoard, ClubMatchGame, Player, Tables}
import ccas.api.misc.enums.*
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.connectZIO

object TestClubBoardSql extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestClubBoardSql")(
    testSelectClubBoardsTeam1,
    testSelectClubBoardsTeam2Flipped,
    testSelectClubBoardsTeam2DrawPreserved,
    testSelectClubBoardsTeam2NullPreserved,
    testSelectClubBoardsExcludesInProgress,
    testSelectClubBoardsExcludesNullPlayer,
    testSelectClubBoardsInPeriodFilters,
    testSelectClubBoardsBothSides
  ).provideShared(
    FreshSchemaLayer("test_club_board_sql", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(30))
    val t2: Instant = t0.plus(Duration.ofDays(60))
    val t3: Instant = t0.plus(Duration.ofDays(90))
  }

  private val ourClubId  = ClubId(100)
  private val oppClubId  = ClubId(200)
  private val player1    = PlayerId.wrap(1L)
  private val player2    = PlayerId.wrap(2L)
  private val matchId1   = ClubMatchId(1001L)
  private val matchId2   = ClubMatchId(1002L)
  private val matchId3   = ClubMatchId(1003L)

  private val seedClubs = for {
    _ <- Club.upsert(Club(ourClubId, Times.t0, ClubSlug("our-club"), "Our Club", None, None, None))
    _ <- Club.upsert(Club(oppClubId, Times.t0, ClubSlug("opp-club"), "Opponent Club", None, None, None))
  } yield ()

  private val seedPlayers = for {
    _ <- Player.insertIfNew(Player(player1, Times.t0, Username.wrap("alice"), PlayerStatusCategory.Active, None, Times.t0))
    _ <- Player.insertIfNew(Player(player2, Times.t0, Username.wrap("bob"), PlayerStatusCategory.Active, None, Times.t0))
  } yield ()

  private def matchRow(
    matchId: ClubMatchId,
    team1Club: ClubId,
    team2Club: ClubId,
    status: ClubMatchStatus = ClubMatchStatus.Finished,
    endTime: Option[Instant] = Some(Times.t1)
  ): ClubMatch =
    ClubMatch(matchId, s"Match ${ClubMatchId.unwrap(matchId)}", status, TimeClass.Daily,
      Some(Times.t0), endTime, 10, Some(team1Club), 10, Some(team2Club), 10, Times.t1)

  private def boardRow(
    matchId: ClubMatchId,
    board: Short,
    team1Player: Option[PlayerId],
    team2Player: Option[PlayerId],
    team1FP: Boolean = false,
    team2FP: Boolean = false
  ): ClubMatchBoard =
    ClubMatchBoard(matchId, board, team1Player, team1FP, team2Player, team2FP, 1, 1)

  private def gameRow(
    matchId: ClubMatchId,
    board: Short,
    team1IsWhite: Boolean,
    winner: Option[BoardGameWinner]
  ): ClubMatchGame =
    ClubMatchGame(matchId, board, team1IsWhite, None, None, None, winner, None, None, None)

  private val clearMatches = connectZIO {
    sql"DELETE FROM club_match_game".update.run()
    sql"DELETE FROM club_match_board".update.run()
    sql"DELETE FROM club_match".update.run()
  }

  private val seedAll = clearMatches *> seedClubs *> seedPlayers

  private def insertBoardWithGames(
    matchId: ClubMatchId,
    board: Short,
    team1Player: Option[PlayerId],
    team2Player: Option[PlayerId],
    g1: Option[BoardGameWinner] = Some(BoardGameWinner.Team1),
    g2: Option[BoardGameWinner] = Some(BoardGameWinner.Team2),
    team1FP: Boolean = false,
    team2FP: Boolean = false
  ) = for {
    _ <- ClubMatchBoard.insert(boardRow(matchId, board, team1Player, team2Player, team1FP, team2FP))
    _ <- ClubMatchGame.insertBatch(
      List(g1.map(w => gameRow(matchId, board, team1IsWhite = true, Some(w))),
           g2.map(w => gameRow(matchId, board, team1IsWhite = false, Some(w)))).flatten
    )
  } yield ()

  // --- Tests ---

  private def testSelectClubBoardsTeam1 = test("returns boards when our club is team1") {
    for {
      _ <- seedAll
      _ <- ClubMatch.upsert(matchRow(matchId1, ourClubId, oppClubId))
      _ <- insertBoardWithGames(matchId1, 1, Some(player1), Some(player2))
      boards <- ClubBoard.selectClubBoards(ourClubId)
    } yield assertTrue(
      boards.size == 1,
      boards.head.playerId == player1,
      boards.head.game1Winner.contains(BoardGameWinner.Team1),
      boards.head.game2Winner.contains(BoardGameWinner.Team2),
      boards.head.ourFairPlay == false,
      boards.head.oppFairPlay == false
    )
  }

  private def testSelectClubBoardsTeam2Flipped = test("flips winners when our club is team2") {
    for {
      _ <- seedAll
      _ <- ClubMatch.upsert(matchRow(matchId1, oppClubId, ourClubId))
      _ <- insertBoardWithGames(matchId1, 1, Some(player2), Some(player1),
        g1 = Some(BoardGameWinner.Team1), g2 = Some(BoardGameWinner.Team2),
        team1FP = false, team2FP = true)
      boards <- ClubBoard.selectClubBoards(ourClubId)
    } yield assertTrue(
      boards.size == 1,
      boards.head.playerId == player1,
      boards.head.game1Winner.contains(BoardGameWinner.Team2),
      boards.head.game2Winner.contains(BoardGameWinner.Team1),
      boards.head.ourFairPlay == true,
      boards.head.oppFairPlay == false
    )
  }

  private def testSelectClubBoardsTeam2DrawPreserved = test("Draw stays Draw when flipped for team2") {
    for {
      _ <- seedAll
      _ <- ClubMatch.upsert(matchRow(matchId1, oppClubId, ourClubId))
      _ <- insertBoardWithGames(matchId1, 1, Some(player2), Some(player1),
        g1 = Some(BoardGameWinner.Draw), g2 = Some(BoardGameWinner.Draw))
      boards <- ClubBoard.selectClubBoards(ourClubId)
    } yield assertTrue(
      boards.size == 1,
      boards.head.game1Winner.contains(BoardGameWinner.Draw),
      boards.head.game2Winner.contains(BoardGameWinner.Draw)
    )
  }

  private def testSelectClubBoardsTeam2NullPreserved = test("null winner stays null when flipped for team2") {
    for {
      _ <- seedAll
      _ <- ClubMatch.upsert(matchRow(matchId1, oppClubId, ourClubId))
      // board with no games at all (both winners null)
      _ <- ClubMatchBoard.insert(boardRow(matchId1, 1, Some(player2), Some(player1)))
      boards <- ClubBoard.selectClubBoards(ourClubId)
    } yield assertTrue(
      boards.size == 1,
      boards.head.game1Winner.isEmpty,
      boards.head.game2Winner.isEmpty
    )
  }

  private def testSelectClubBoardsExcludesInProgress = test("excludes in-progress matches") {
    for {
      _ <- seedAll
      _ <- ClubMatch.upsert(matchRow(matchId1, ourClubId, oppClubId, status = ClubMatchStatus.InProgress, endTime = None))
      _ <- insertBoardWithGames(matchId1, 1, Some(player1), Some(player2))
      boards <- ClubBoard.selectClubBoards(ourClubId)
    } yield assertTrue(boards.isEmpty)
  }

  private def testSelectClubBoardsExcludesNullPlayer = test("excludes boards with null player") {
    for {
      _ <- seedAll
      _ <- ClubMatch.upsert(matchRow(matchId1, ourClubId, oppClubId))
      _ <- insertBoardWithGames(matchId1, 1, None, Some(player2))
      boards <- ClubBoard.selectClubBoards(ourClubId)
    } yield assertTrue(boards.isEmpty)
  }

  private def testSelectClubBoardsInPeriodFilters = test("period query filters by end_time range") {
    for {
      _ <- seedAll
      _ <- ClubMatch.upsert(matchRow(matchId1, ourClubId, oppClubId, endTime = Some(Times.t1)))
      _ <- ClubMatch.upsert(matchRow(matchId2, ourClubId, oppClubId, endTime = Some(Times.t2)))
      _ <- ClubMatch.upsert(matchRow(matchId3, ourClubId, oppClubId, endTime = Some(Times.t3)))
      _ <- insertBoardWithGames(matchId1, 1, Some(player1), Some(player2))
      _ <- insertBoardWithGames(matchId2, 1, Some(player1), Some(player2))
      _ <- insertBoardWithGames(matchId3, 1, Some(player1), Some(player2))
      // only matchId2 falls in [t1+1day, t3)
      since = Times.t1.plus(Duration.ofDays(1))
      boards <- ClubBoard.selectClubBoardsInPeriod(ourClubId, since, Times.t3)
    } yield assertTrue(
      boards.size == 1,
      boards.head.matchId == matchId2
    )
  }

  private def testSelectClubBoardsBothSides = test("returns boards from both team1 and team2 matches") {
    for {
      _ <- seedAll
      _ <- ClubMatch.upsert(matchRow(matchId1, ourClubId, oppClubId))
      _ <- ClubMatch.upsert(matchRow(matchId2, oppClubId, ourClubId))
      _ <- insertBoardWithGames(matchId1, 1, Some(player1), Some(player2))
      _ <- insertBoardWithGames(matchId2, 1, Some(player2), Some(player1))
      boards <- ClubBoard.selectClubBoards(ourClubId)
    } yield assertTrue(boards.size == 2)
  }
}
