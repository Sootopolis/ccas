package ccas.analysis.apps.stats

import java.time.Instant

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.misc.enums.BoardGameWinner
import ccas.api.misc.subtypes.{ClubMatchId, PlayerId, Username}

object TestStatsAggregation extends ZIOSpecDefault {
  override def spec: Spec[Any, Nothing] = suite("StatsAggregation")(
    suiteAggregate
  )

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
    test("empty input") {
      val result = StatsUtils.aggregate(Nil, usernames)
      assertTrue(result.isEmpty)
    },
    test("single board, both games played, no fairplay") {
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
    },
    test("raw and fairplay agree when no fairplay flags are set") {
      val rows   = List(board(player1, g1 = Some(BoardGameWinner.Team1), g2 = Some(BoardGameWinner.Team1)))
      val result = StatsUtils.aggregate(rows, usernames)
      val mc     = result.head
      assertTrue(
        mc.raw.pointsX2 == mc.fairPlay.pointsX2,
        mc.raw.wins == mc.fairPlay.wins,
        mc.raw.wins == 2
      )
    },
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
    },
    test("both fairplay flags → draws in fairplay view") {
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
    },
    test("single game board (game2 not played)") {
      val rows   = List(board(player1, g2 = None))
      val result = StatsUtils.aggregate(rows, usernames)
      assertTrue(
        result.head.raw.boards == 1,
        result.head.raw.games == 1,
        result.head.raw.wins == 1,
        result.head.raw.pointsX2 == 2
      )
    },
    test("no games played on board") {
      val rows   = List(board(player1, g1 = None, g2 = None))
      val result = StatsUtils.aggregate(rows, usernames)
      assertTrue(
        result.head.raw.boards == 1,
        result.head.raw.games == 0,
        result.head.raw.pointsX2 == 0
      )
    },
    test("multiple players aggregated separately") {
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
    },
    test("multiple boards for same player are summed") {
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
    },
    test("unknown player gets fallback username") {
      val unknownPlayer = PlayerId.wrap(999L)
      val rows          = List(board(unknownPlayer))
      val result        = StatsUtils.aggregate(rows, Map.empty)
      assertTrue(result.head.username == Username.wrap("unknown"))
    },
    test("sorted by username case-insensitively") {
      val rows = List(board(player2), board(player1))
      val result = StatsUtils.aggregate(rows, usernames)
      assertTrue(
        result.head.username == Username.wrap("alice"),
        result.last.username == Username.wrap("bob")
      )
    }
  )
}
