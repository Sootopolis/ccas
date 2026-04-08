package ccas.analysis.apps.stats

import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.analysis.apps.stats.StatsUtils.{MemberContribution, PlayerBoardStats}
import ccas.api.misc.subtypes.{PlayerId, Username}

object TestStatsReport extends ZIOSpecDefault {
  override def spec: Spec[Any, Nothing] = suite("StatsReport")(
    suiteContribution,
    suitePlayerOfPeriod
  )

  private def mc(name: String, rawPtsX2: Int, fpPtsX2: Int, games: Int = 4): MemberContribution = {
    val wins  = rawPtsX2 / 2
    val draws = rawPtsX2 % 2
    val losses = games - wins - draws
    val fpWins  = fpPtsX2 / 2
    val fpDraws = fpPtsX2 % 2
    val fpLosses = games - fpWins - fpDraws
    MemberContribution(
      PlayerId.wrap(name.hashCode.toLong.abs),
      Username.wrap(name),
      PlayerBoardStats(games, games, wins, draws, losses, rawPtsX2),
      PlayerBoardStats(games, games, fpWins, fpDraws, fpLosses, fpPtsX2)
    )
  }

  private val alice = mc("alice", rawPtsX2 = 6, fpPtsX2 = 4)
  private val bob   = mc("bob", rawPtsX2 = 8, fpPtsX2 = 8)
  private val carol = mc("carol", rawPtsX2 = 6, fpPtsX2 = 6)

  private def lines(csv: String): Array[String] = csv.split("\n")

  // ==========================================================================
  // Suite: formatContribution
  // ==========================================================================

  private def suiteContribution = suite("formatContribution")(
    testEmptyInputProducesHeaderOnly,
    testHeaderRowHasExpectedColumns,
    testRowsOrderedByRawPointsDescending,
    testTiesBrokenByUsername,
    testRankColumnIsSequential,
    testTotalRowPresent,
    testPointsFormattedWithoutDecimalWhenWhole,
    testPointsFormattedWithOneDecimalWhenHalf,
    testTotalRowSumsValuesCorrectly,
    testScorePercentShowsZeroForAllLosses,
    testScorePercentShowsHundredForAllWins,
    testScorePercentShowsFiftyForHalfWins
  )

  private def testEmptyInputProducesHeaderOnly = test("empty input produces header only") {
    val csv = StatsReport.formatContribution(Nil)
    assertTrue(lines(csv).length == 1, csv.startsWith("#,Username,"))
  }

  private def testHeaderRowHasExpectedColumns = test("header row has expected columns") {
    val csv = StatsReport.formatContribution(List(alice))
    val header = lines(csv).head
    assertTrue(header == "#,Username,Boards,Games,W,D,L,Points,Score%,FP_W,FP_D,FP_L,FP_Points,FP_Score%")
  }

  private def testRowsOrderedByRawPointsDescending = test("rows ordered by raw points descending") {
    val csv = StatsReport.formatContribution(List(alice, bob, carol))
    val dataLines = lines(csv).drop(1).dropRight(1) // skip header and TOTAL
    val names = dataLines.map(_.split(",").apply(1))
    assertTrue(names.toList == List("bob", "alice", "carol"))
  }

  private def testTiesBrokenByUsername = test("ties broken by username") {
    // alice and carol both have rawPtsX2=6; alice should come first
    val csv = StatsReport.formatContribution(List(carol, alice))
    val dataLines = lines(csv).drop(1).dropRight(1)
    val names = dataLines.map(_.split(",").apply(1))
    assertTrue(names.toList == List("alice", "carol"))
  }

  private def testRankColumnIsSequential = test("rank column is sequential") {
    val csv = StatsReport.formatContribution(List(alice, bob, carol))
    val dataLines = lines(csv).drop(1).dropRight(1)
    val ranks = dataLines.map(_.split(",").apply(0))
    assertTrue(ranks.toList == List("1", "2", "3"))
  }

  private def testTotalRowPresent = test("TOTAL row present with empty rank") {
    val csv = StatsReport.formatContribution(List(alice, bob))
    val totalLine = lines(csv).last
    val fields = totalLine.split(",", -1)
    assertTrue(fields(0) == "", fields(1) == "TOTAL")
  }

  private def testPointsFormattedWithoutDecimalWhenWhole = test("points formatted without decimal when whole") {
    val whole = mc("dave", rawPtsX2 = 4, fpPtsX2 = 4, games = 2) // 2.0 points
    val csv = StatsReport.formatContribution(List(whole))
    val dataLine = lines(csv)(1)
    val points = dataLine.split(",").apply(7)
    assertTrue(points == "2")
  }

  private def testPointsFormattedWithOneDecimalWhenHalf = test("points formatted with one decimal when half") {
    val half = mc("eve", rawPtsX2 = 5, fpPtsX2 = 5, games = 4) // 2.5 points
    val csv = StatsReport.formatContribution(List(half))
    val dataLine = lines(csv)(1)
    val points = dataLine.split(",").apply(7)
    assertTrue(points == "2.5")
  }

  private def testTotalRowSumsValuesCorrectly = test("TOTAL row sums values correctly") {
    val csv = StatsReport.formatContribution(List(alice, bob, carol))
    val totalLine = lines(csv).last
    val fields = totalLine.split(",", -1)
    // alice: 4 games, bob: 4 games, carol: 4 games = 12 total games
    // alice raw: 6 pts, bob raw: 8 pts, carol raw: 6 pts = 20 ptsx2 = 10 points
    assertTrue(
      fields(3) == "12",                // total games
      fields(7) == "10"                 // total raw points (20 / 2 = 10)
    )
  }

  private def testScorePercentShowsZeroForAllLosses = test("score% shows 0.0% for all losses") {
    val allLosses = mc("loser", rawPtsX2 = 0, fpPtsX2 = 0, games = 4)
    val csv = StatsReport.formatContribution(List(allLosses))
    val dataLine = lines(csv)(1)
    val scoreRate = dataLine.split(",").apply(8)
    assertTrue(scoreRate == "0.0%")
  }

  private def testScorePercentShowsHundredForAllWins = test("score% shows 100.0% for all wins") {
    val allWins = mc("winner", rawPtsX2 = 8, fpPtsX2 = 8, games = 4) // 4/4 = 100%
    val csv = StatsReport.formatContribution(List(allWins))
    val dataLine = lines(csv)(1)
    val scoreRate = dataLine.split(",").apply(8)
    assertTrue(scoreRate == "100.0%")
  }

  private def testScorePercentShowsFiftyForHalfWins = test("score% shows 50.0% for half wins") {
    val half = mc("half", rawPtsX2 = 4, fpPtsX2 = 4, games = 4) // 2/4 = 50%
    val csv = StatsReport.formatContribution(List(half))
    val dataLine = lines(csv)(1)
    val scoreRate = dataLine.split(",").apply(8)
    assertTrue(scoreRate == "50.0%")
  }

  // ==========================================================================
  // Suite: formatPlayerOfPeriod
  // ==========================================================================

  private def suitePlayerOfPeriod = suite("formatPlayerOfPeriod")(
    testFiltersByMinGames,
    testOrderedByRawPointsDescending,
    testPlayerOfPeriodHeaderRow,
    testNoTotalRow,
    testEmptyWhenNoPlayersMeetThreshold
  )

  private def testFiltersByMinGames = test("filters by minGames") {
    val few = mc("fay", rawPtsX2 = 4, fpPtsX2 = 4, games = 2)
    val csv = StatsReport.formatPlayerOfPeriod(List(alice, few), minGames = 3)
    assertTrue(lines(csv).length == 2) // header + alice only
  }

  private def testOrderedByRawPointsDescending = test("ordered by raw points descending") {
    val csv = StatsReport.formatPlayerOfPeriod(List(alice, bob, carol), minGames = 1)
    val dataLines = lines(csv).drop(1)
    val names = dataLines.map(_.split(",").apply(1))
    assertTrue(names.toList == List("bob", "alice", "carol"))
  }

  private def testPlayerOfPeriodHeaderRow = test("header row has expected columns") {
    val csv = StatsReport.formatPlayerOfPeriod(List(alice), minGames = 1)
    val header = lines(csv).head
    assertTrue(header == "#,Username,Games,W,D,L,Points,Score%")
  }

  private def testNoTotalRow = test("no TOTAL row") {
    val csv = StatsReport.formatPlayerOfPeriod(List(alice, bob), minGames = 1)
    val lastLine = lines(csv).last
    assertTrue(!lastLine.contains("TOTAL"))
  }

  private def testEmptyWhenNoPlayersMeetThreshold = test("empty when no players meet threshold") {
    val csv = StatsReport.formatPlayerOfPeriod(List(alice), minGames = 100)
    assertTrue(lines(csv).length == 1) // header only
  }
}
