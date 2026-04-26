package ccas.analysis.apps.history

import zio.http.URL
import zio.json.readJsonLinesAs
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.tables.ClubMatchBoard
import ccas.api.clubmatch.ApiDailyMatch
import ccas.api.clubmatch.ApiMatchBoard.{ApiBoardGame, ApiBoardPlayer}
import ccas.api.misc.enums.{BoardGameWinner, GameResultDetail, GameRule, TimeClass}
import ccas.api.misc.subtypes.{ClubMatchId, Elo, PlayerId, Username}

object TestHistoryBoardBuilder extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestHistoryBoardBuilder")(
    suiteFinishedRating,
    suiteBuildGameRow,
    suiteScoresMatch,
    suiteComputeExpectedScores
  )

  private def url(s: String): URL = URL.decode(s).toOption.get

  private def elo(n: Int): Elo = Elo(n.toShort)

  private def boardPlayer(username: String, rating: Int, result: Option[GameResultDetail]): ApiBoardPlayer =
    ApiBoardPlayer(
      Username(username),
      elo(rating),
      result,
      url(s"https://api.chess.com/pub/player/$username")
    )

  private def boardGame(
    white: Either[URL, ApiBoardPlayer],
    black: Either[URL, ApiBoardPlayer],
    startTime: Option[Long] = Some(1000L),
    endTime: Option[Long] = Some(2000L)
  ): ApiBoardGame = ApiBoardGame(
    white = white,
    black = black,
    accuracies = None,
    url = url("https://www.chess.com/game/daily/100"),
    fen = "",
    pgn = None,
    startTime = startTime,
    endTime = endTime,
    timeControl = "1/259200",
    timeClass = TimeClass.Daily,
    rules = GameRule.Chess,
    rated = true,
    eco = None,
    `match` = None
  )

  // ==========================================================================
  // Suite: finishedRating
  // ==========================================================================

  private def suiteFinishedRating = suite("finishedRating")(
    test("returns Some(rating) for a finished player with a result") {
      val player = boardPlayer("alice", 1500, Some(GameResultDetail.Win))
      assertTrue(HistoryBoardBuilder.finishedRating(Right(player)).contains(elo(1500)))
    },
    test("returns None when player has no result (game not finished)") {
      val player = boardPlayer("alice", 1500, None)
      assertTrue(HistoryBoardBuilder.finishedRating(Right(player)).isEmpty)
    },
    test("returns None for a closed account (URL-only side)") {
      val left: Either[URL, ApiBoardPlayer] = Left(url("https://api.chess.com/pub/player/closed"))
      assertTrue(HistoryBoardBuilder.finishedRating(left).isEmpty)
    }
  )

  // ==========================================================================
  // Suite: buildGameRow
  // ==========================================================================

  private val matchId = ClubMatchId(1234567L)

  private def suiteBuildGameRow = suite("buildGameRow")(
    test("returns None when winner and boardGame are both absent") {
      val row = HistoryBoardBuilder.buildGameRow(matchId, board = 1, team1IsWhite = true, None, None, None)
      assertTrue(row.isEmpty)
    },
    test("returns row with no ratings when boardGame is absent") {
      val row = HistoryBoardBuilder.buildGameRow(
        matchId,
        board = 1,
        team1IsWhite = true,
        winner = Some(BoardGameWinner.Team1),
        detail = Some(GameResultDetail.Checkmated),
        boardGame = None
      )
      val r = row.get
      assertTrue(
        r.matchId == matchId,
        r.board == 1.toShort,
        r.team1IsWhite,
        r.winner.contains(BoardGameWinner.Team1),
        r.detail.contains(GameResultDetail.Checkmated),
        r.team1Rating.isEmpty,
        r.team2Rating.isEmpty,
        r.startTime.isEmpty,
        r.endTime.isEmpty,
        r.gameId.isEmpty
      )
    },
    test("when team1IsWhite=true ratings come from white→team1, black→team2") {
      val game = boardGame(
        white = Right(boardPlayer("alice", 1500, Some(GameResultDetail.Win))),
        black = Right(boardPlayer("bob", 1300, Some(GameResultDetail.Checkmated)))
      )
      val row = HistoryBoardBuilder.buildGameRow(
        matchId,
        board = 1,
        team1IsWhite = true,
        winner = Some(BoardGameWinner.Team1),
        detail = Some(GameResultDetail.Checkmated),
        boardGame = Some(game)
      ).get
      assertTrue(
        row.team1Rating.contains(elo(1500)),
        row.team2Rating.contains(elo(1300)),
        row.gameId.contains(100L),
        row.startTime.contains(1000L),
        row.endTime.contains(2000L)
      )
    },
    test("when team1IsWhite=false ratings flip: white→team2, black→team1") {
      val game = boardGame(
        white = Right(boardPlayer("carol", 1700, Some(GameResultDetail.Win))),
        black = Right(boardPlayer("dave", 1400, Some(GameResultDetail.Resigned)))
      )
      val row = HistoryBoardBuilder.buildGameRow(
        matchId,
        board = 2,
        team1IsWhite = false,
        winner = Some(BoardGameWinner.Team2),
        detail = Some(GameResultDetail.Resigned),
        boardGame = Some(game)
      ).get
      assertTrue(
        row.team1Rating.contains(elo(1400)),
        row.team2Rating.contains(elo(1700))
      )
    }
  )

  // ==========================================================================
  // Suite: scoresMatch
  // ==========================================================================

  private def boardRow(boardNum: Short, t1: Short, t2: Short): ClubMatchBoard =
    ClubMatchBoard(
      matchId = matchId,
      board = boardNum,
      team1PlayerId = Some(PlayerId(1)),
      team1FairPlay = false,
      team2PlayerId = Some(PlayerId(2)),
      team2FairPlay = false,
      team1ScoreX2 = t1,
      team2ScoreX2 = t2
    )

  private def suiteScoresMatch = suite("scoresMatch")(
    test("returns true for empty expected and empty existing") {
      assertTrue(HistoryBoardBuilder.scoresMatch(Map.empty, Nil))
    },
    test("returns true when expected and existing align board-by-board") {
      val expected = Map[Short, (Short, Short)](1.toShort -> (4.toShort, 0.toShort), 2.toShort -> (2.toShort, 2.toShort))
      val existing = List(boardRow(1, 4, 0), boardRow(2, 2, 2))
      assertTrue(HistoryBoardBuilder.scoresMatch(expected, existing))
    },
    test("returns false when sizes differ") {
      val expected = Map[Short, (Short, Short)](1.toShort -> (4.toShort, 0.toShort))
      val existing = List(boardRow(1, 4, 0), boardRow(2, 2, 2))
      assertTrue(!HistoryBoardBuilder.scoresMatch(expected, existing))
    },
    test("returns false when a board's score differs") {
      val expected = Map[Short, (Short, Short)](1.toShort -> (4.toShort, 0.toShort))
      val existing = List(boardRow(1, 3, 1))
      assertTrue(!HistoryBoardBuilder.scoresMatch(expected, existing))
    },
    test("returns false when a board number is missing from expected") {
      val expected = Map[Short, (Short, Short)](1.toShort -> (4.toShort, 0.toShort))
      val existing = List(boardRow(2, 4, 0))
      assertTrue(!HistoryBoardBuilder.scoresMatch(expected, existing))
    }
  )

  // ==========================================================================
  // Suite: computeExpectedScores (against the matchFinished.json fixture)
  // ==========================================================================

  private val matchFinishedFixture =
    readJsonLinesAs[ApiDailyMatch]("data/test/api/matchFinished.json").runHead.someOrFailException

  private def suiteComputeExpectedScores = suite("computeExpectedScores")(
    test("Registered match returns empty map") {
      readJsonLinesAs[ApiDailyMatch]("data/test/api/matchRegistered.json").runHead.someOrFailException.map { m =>
        assertTrue(HistoryBoardBuilder.computeExpectedScores(m).isEmpty)
      }
    },
    test("Finished match returns one entry per board with totals summing to 4") {
      matchFinishedFixture.map { m =>
        val scores = HistoryBoardBuilder.computeExpectedScores(m)
        val allTotalsAreFour = scores.values.forall { case (t1, t2) => (t1 + t2) == 4 }
        // matchFinished.json has 13 boards
        assertTrue(
          scores.size == 13,
          allTotalsAreFour
        )
      }
    }
  )
}
