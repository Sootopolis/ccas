package ccas.analysis.apps.stats

import java.time.Instant

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.apps.stats.StatsUtils.PlayerBoardStats
import ccas.api.misc.enums.BoardGameWinner
import ccas.api.misc.subtypes.{ClubMatchId, PlayerId, Username}

object TestStatsAggregation extends ZIOSpecDefault {
  override def spec: Spec[Any, Nothing] = suite("StatsAggregation")(
    suitePlayerBoardStats,
    suiteAggregate
  )

  // ==========================================================================
  // Suite: PlayerBoardStats
  // ==========================================================================

  private def suitePlayerBoardStats = suite("PlayerBoardStats")(
    testPointsDividesPointsX2ByTwo,
    testScoreRateReturnsZeroWhenGamesIsZero,
    testScoreRateComputesPointsOverGames,
    testPlusCombinesTwoInstances,
    testEmptyIsIdentityForPlus
  )

  private def testPointsDividesPointsX2ByTwo = test("points divides pointsX2 by 2") {
    val even = PlayerBoardStats(1, 2, 1, 0, 1, 4)
    val odd  = PlayerBoardStats(1, 2, 1, 1, 0, 3)
    assertTrue(
      even.points == 2.0,
      odd.points == 1.5
    )
  }

  private def testScoreRateReturnsZeroWhenGamesIsZero = test("scoreRate returns 0 when games is 0") {
    assertTrue(PlayerBoardStats.empty.scoreRate == 0.0)
  }

  private def testScoreRateComputesPointsOverGames = test("scoreRate computes points / games") {
    val stats = PlayerBoardStats(2, 4, 2, 1, 1, 5)
    assertTrue(stats.scoreRate == 2.5 / 4)
  }

  private def testPlusCombinesTwoInstances = test("+ combines two instances") {
    val a = PlayerBoardStats(1, 2, 1, 0, 1, 2)
    val b = PlayerBoardStats(2, 3, 2, 1, 0, 5)
    val c = a + b
    assertTrue(
      c.boards == 3,
      c.games == 5,
      c.wins == 3,
      c.draws == 1,
      c.losses == 1,
      c.pointsX2 == 7
    )
  }

  private def testEmptyIsIdentityForPlus = test("empty is identity for +") {
    val a = PlayerBoardStats(1, 2, 1, 0, 1, 2)
    val c = a + PlayerBoardStats.empty
    assertTrue(c == a)
  }

  // ==========================================================================
  // Suite: aggregate
  // ==========================================================================

  private val player1 = PlayerId.wrap(1L)
  private val player2 = PlayerId.wrap(2L)
  private val match1  = ClubMatchId.wrap(100L)
  private val match2  = ClubMatchId.wrap(200L)
  private val now     = Some(Instant.parse("2026-01-15T12:00:00Z"))

  private val usernames = Map(
    player1 -> Username.wrap("alice"),
    player2 -> Username.wrap("bob")
  )

  private def board(
    playerId: PlayerId,
    matchId: ClubMatchId = match1,
    g1: Option[BoardGameWinner] = Some(BoardGameWinner.Team1),
    g2: Option[BoardGameWinner] = Some(BoardGameWinner.Team2),
    ourFP: Boolean = false,
    oppFP: Boolean = false
  ): ClubBoard =
    ClubBoard(matchId, now, playerId, ourFP, oppFP, g1, g2)

  private def suiteAggregate = suite("aggregate")(
    testEmptyInput,
    testSingleBoardBothGamesNoFairplay,
    testRawAndFairplayAgreeWhenNoFairplayFlags,
    testOpponentFairplayRawShowsLossesFairplayShowsWins,
    testBothFairplayFlagsDrawsInFairplayView,
    testSingleGameBoard,
    testNoGamesPlayedOnBoard,
    testMultiplePlayersAggregatedSeparately,
    testMultipleBoardsForSamePlayerSummed,
    testUnknownPlayerGetsFallbackUsername,
    testSortedByUsernameCaseInsensitively
  )

  private def testEmptyInput = test("empty input") {
    val result = StatsUtils.aggregate(Nil, usernames)
    assertTrue(result.isEmpty)
  }

  private def testSingleBoardBothGamesNoFairplay = test("single board, both games played, no fairplay") {
    // player1 wins game1 (Team1), loses game2 (Team2)
    val rows   = List(board(player1))
    val result = StatsUtils.aggregate(rows, usernames)
    assertTrue(
      result.size == 1,
      result.head.username == Username.wrap("alice"),
      result.head.raw.boards == 1,
      result.head.raw.games == 2,
      result.head.raw.wins == 1,
      result.head.raw.losses == 1,
      result.head.raw.draws == 0,
      result.head.raw.pointsX2 == 2,
      result.head.fairPlay.pointsX2 == 2
    )
  }

  private def testRawAndFairplayAgreeWhenNoFairplayFlags =
    test("raw and fairplay agree when no fairplay flags are set") {
      val rows   = List(board(player1, g1 = Some(BoardGameWinner.Team1), g2 = Some(BoardGameWinner.Team1)))
      val result = StatsUtils.aggregate(rows, usernames)
      val mc     = result.head
      assertTrue(
        mc.raw.pointsX2 == mc.fairPlay.pointsX2,
        mc.raw.wins == mc.fairPlay.wins,
        mc.raw.wins == 2
      )
    }

  private def testOpponentFairplayRawShowsLossesFairplayShowsWins =
    test("opponent fairplay: raw shows losses, fairplay shows wins") {
      // opponent has fairplay flag, both games recorded as Team2 wins
      // raw: 2 losses. fairplay-adjusted: 2 wins (opponent forfeits).
      val rows = List(board(player1, g1 = Some(BoardGameWinner.Team2), g2 = Some(BoardGameWinner.Team2), oppFP = true))
      val result = StatsUtils.aggregate(rows, usernames)
      assertTrue(
        result.head.fairPlay.wins == 2,
        result.head.fairPlay.losses == 0,
        result.head.fairPlay.pointsX2 == 4,
        result.head.raw.wins == 0,
        result.head.raw.losses == 2,
        result.head.raw.pointsX2 == 0
      )
    }

  private def testBothFairplayFlagsDrawsInFairplayView = test("both fairplay flags → draws in fairplay view") {
    val rows = List(board(player1, g1 = Some(BoardGameWinner.Team1), g2 = Some(BoardGameWinner.Team1),
      ourFP = true, oppFP = true))
    val result = StatsUtils.aggregate(rows, usernames)
    assertTrue(
      result.head.fairPlay.draws == 2,
      result.head.fairPlay.wins == 0,
      result.head.fairPlay.pointsX2 == 2,
      result.head.raw.wins == 2,
      result.head.raw.pointsX2 == 4
    )
  }

  private def testSingleGameBoard = test("single game board (game2 not played)") {
    val rows   = List(board(player1, g2 = None))
    val result = StatsUtils.aggregate(rows, usernames)
    assertTrue(
      result.head.raw.boards == 1,
      result.head.raw.games == 1,
      result.head.raw.wins == 1,
      result.head.raw.pointsX2 == 2
    )
  }

  private def testNoGamesPlayedOnBoard = test("no games played on board") {
    val rows   = List(board(player1, g1 = None, g2 = None))
    val result = StatsUtils.aggregate(rows, usernames)
    assertTrue(
      result.head.raw.boards == 1,
      result.head.raw.games == 0,
      result.head.raw.pointsX2 == 0
    )
  }

  private def testMultiplePlayersAggregatedSeparately = test("multiple players aggregated separately") {
    val rows = List(
      board(player1, match1, g1 = Some(BoardGameWinner.Team1), g2 = Some(BoardGameWinner.Team1)),
      board(player2, match1, g1 = Some(BoardGameWinner.Team2), g2 = Some(BoardGameWinner.Draw))
    )
    val result = StatsUtils.aggregate(rows, usernames)
    val alice  = result.find(_.username == Username.wrap("alice")).get
    val bob    = result.find(_.username == Username.wrap("bob")).get
    assertTrue(
      alice.raw.wins == 2,
      alice.raw.losses == 0,
      bob.raw.wins == 0,
      bob.raw.losses == 1,
      bob.raw.draws == 1
    )
  }

  private def testMultipleBoardsForSamePlayerSummed = test("multiple boards for same player are summed") {
    val rows = List(
      board(player1, match1, g1 = Some(BoardGameWinner.Team1), g2 = Some(BoardGameWinner.Team1)),
      board(player1, match2, g1 = Some(BoardGameWinner.Draw), g2 = Some(BoardGameWinner.Team2))
    )
    val result = StatsUtils.aggregate(rows, usernames)
    assertTrue(
      result.head.raw.boards == 2,
      result.head.raw.games == 4,
      result.head.raw.wins == 2,
      result.head.raw.draws == 1,
      result.head.raw.losses == 1,
      result.head.raw.pointsX2 == 5
    )
  }

  private def testUnknownPlayerGetsFallbackUsername = test("unknown player gets fallback username") {
    val unknownPlayer = PlayerId.wrap(999L)
    val rows          = List(board(unknownPlayer))
    val result        = StatsUtils.aggregate(rows, Map.empty)
    assertTrue(result.head.username == Username.wrap("unknown"))
  }

  private def testSortedByUsernameCaseInsensitively = test("sorted by username case-insensitively") {
    val rows = List(board(player2), board(player1))
    val result = StatsUtils.aggregate(rows, usernames)
    assertTrue(
      result.head.username == Username.wrap("alice"),
      result.last.username == Username.wrap("bob")
    )
  }
}
