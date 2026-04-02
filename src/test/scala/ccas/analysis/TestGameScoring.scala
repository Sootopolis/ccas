package ccas.analysis

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.misc.enums.{BoardGameWinner, GameResult}

object TestGameScoring extends ZIOSpecDefault {
  override def spec: Spec[Any, Nothing] = suite("GameScoring")(
    suiteClassifyGameRaw,
    suiteClassifyGame,
    suiteScoreX2
  )

  // --- classifyGameRaw ---

  private def suiteClassifyGameRaw = suite("classifyGameRaw")(
    test("Team1 → Win") {
      assertTrue(GameScoring.classifyGameRaw(Some(BoardGameWinner.Team1)).contains(GameResult.Win))
    },
    test("Team2 → Loss") {
      assertTrue(GameScoring.classifyGameRaw(Some(BoardGameWinner.Team2)).contains(GameResult.Loss))
    },
    test("Draw → Draw") {
      assertTrue(GameScoring.classifyGameRaw(Some(BoardGameWinner.Draw)).contains(GameResult.Draw))
    },
    test("None → None") {
      assertTrue(GameScoring.classifyGameRaw(None).isEmpty)
    }
  )

  // --- classifyGame (fairplay) ---

  private def suiteClassifyGame = suite("classifyGame")(
    test("no fairplay, Team1 → Win") {
      assertTrue(
        GameScoring.classifyGame(Some(BoardGameWinner.Team1), ourFairPlay = false, oppFairPlay = false)
          .contains(GameResult.Win)
      )
    },
    test("no fairplay, Team2 → Loss") {
      assertTrue(
        GameScoring.classifyGame(Some(BoardGameWinner.Team2), ourFairPlay = false, oppFairPlay = false)
          .contains(GameResult.Loss)
      )
    },
    test("no fairplay, Draw → Draw") {
      assertTrue(
        GameScoring.classifyGame(Some(BoardGameWinner.Draw), ourFairPlay = false, oppFairPlay = false)
          .contains(GameResult.Draw)
      )
    },
    test("our fairplay → Loss regardless of winner") {
      assertTrue(
        GameScoring.classifyGame(Some(BoardGameWinner.Team1), ourFairPlay = true, oppFairPlay = false)
          .contains(GameResult.Loss),
        GameScoring.classifyGame(Some(BoardGameWinner.Draw), ourFairPlay = true, oppFairPlay = false)
          .contains(GameResult.Loss)
      )
    },
    test("opponent fairplay → Win regardless of winner") {
      assertTrue(
        GameScoring.classifyGame(Some(BoardGameWinner.Team2), ourFairPlay = false, oppFairPlay = true)
          .contains(GameResult.Win),
        GameScoring.classifyGame(Some(BoardGameWinner.Draw), ourFairPlay = false, oppFairPlay = true)
          .contains(GameResult.Win)
      )
    },
    test("both fairplay → Draw regardless of winner") {
      assertTrue(
        GameScoring.classifyGame(Some(BoardGameWinner.Team1), ourFairPlay = true, oppFairPlay = true)
          .contains(GameResult.Draw),
        GameScoring.classifyGame(Some(BoardGameWinner.Team2), ourFairPlay = true, oppFairPlay = true)
          .contains(GameResult.Draw)
      )
    },
    test("None → None even with fairplay flags") {
      assertTrue(
        GameScoring.classifyGame(None, ourFairPlay = true, oppFairPlay = true).isEmpty
      )
    }
  )

  // --- scoreX2 ---

  private def suiteScoreX2 = suite("scoreX2")(
    test("Win → 2") {
      assertTrue(GameScoring.scoreX2(GameResult.Win) == 2)
    },
    test("Draw → 1") {
      assertTrue(GameScoring.scoreX2(GameResult.Draw) == 1)
    },
    test("Loss → 0") {
      assertTrue(GameScoring.scoreX2(GameResult.Loss) == 0)
    }
  )
}
