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

  private def suiteContribution = suite("formatContribution")(
    test("empty input produces header only") {
      val csv = StatsReport.formatContribution(Nil)
      assertTrue(lines(csv).length == 1, csv.startsWith("#,Username,"))
    },
    test("header row has expected columns") {
      val csv = StatsReport.formatContribution(List(alice))
      val header = lines(csv).head
      assertTrue(header == "#,Username,Boards,Games,W,D,L,Points,Score%,FP_W,FP_D,FP_L,FP_Points,FP_Score%")
    },
    test("rows ordered by raw points descending") {
      val csv = StatsReport.formatContribution(List(alice, bob, carol))
      val dataLines = lines(csv).drop(1).dropRight(1) // skip header and TOTAL
      val names = dataLines.map(_.split(",").apply(1))
      assertTrue(names.toList == List("bob", "alice", "carol"))
    },
    test("ties broken by username") {
      // alice and carol both have rawPtsX2=6; alice should come first
      val csv = StatsReport.formatContribution(List(carol, alice))
      val dataLines = lines(csv).drop(1).dropRight(1)
      val names = dataLines.map(_.split(",").apply(1))
      assertTrue(names.toList == List("alice", "carol"))
    },
    test("rank column is sequential") {
      val csv = StatsReport.formatContribution(List(alice, bob, carol))
      val dataLines = lines(csv).drop(1).dropRight(1)
      val ranks = dataLines.map(_.split(",").apply(0))
      assertTrue(ranks.toList == List("1", "2", "3"))
    },
    test("TOTAL row present with empty rank") {
      val csv = StatsReport.formatContribution(List(alice, bob))
      val totalLine = lines(csv).last
      val fields = totalLine.split(",", -1)
      assertTrue(fields(0) == "", fields(1) == "TOTAL")
    },
    test("points formatted without decimal when whole") {
      val whole = mc("dave", rawPtsX2 = 4, fpPtsX2 = 4, games = 2) // 2.0 points
      val csv = StatsReport.formatContribution(List(whole))
      val dataLine = lines(csv)(1)
      val points = dataLine.split(",").apply(7)
      assertTrue(points == "2")
    },
    test("points formatted with one decimal when half") {
      val half = mc("eve", rawPtsX2 = 5, fpPtsX2 = 5, games = 4) // 2.5 points
      val csv = StatsReport.formatContribution(List(half))
      val dataLine = lines(csv)(1)
      val points = dataLine.split(",").apply(7)
      assertTrue(points == "2.5")
    }
  )

  private def suitePlayerOfPeriod = suite("formatPlayerOfPeriod")(
    test("filters by minGames") {
      val few = mc("fay", rawPtsX2 = 4, fpPtsX2 = 4, games = 2)
      val csv = StatsReport.formatPlayerOfPeriod(List(alice, few), minGames = 3)
      assertTrue(lines(csv).length == 2) // header + alice only
    },
    test("ordered by raw points descending") {
      val csv = StatsReport.formatPlayerOfPeriod(List(alice, bob, carol), minGames = 1)
      val dataLines = lines(csv).drop(1)
      val names = dataLines.map(_.split(",").apply(1))
      assertTrue(names.toList == List("bob", "alice", "carol"))
    },
    test("header row has expected columns") {
      val csv = StatsReport.formatPlayerOfPeriod(List(alice), minGames = 1)
      val header = lines(csv).head
      assertTrue(header == "#,Username,Games,W,D,L,Points,Score%")
    },
    test("no TOTAL row") {
      val csv = StatsReport.formatPlayerOfPeriod(List(alice, bob), minGames = 1)
      val lastLine = lines(csv).last
      assertTrue(!lastLine.contains("TOTAL"))
    },
    test("empty when no players meet threshold") {
      val csv = StatsReport.formatPlayerOfPeriod(List(alice), minGames = 100)
      assertTrue(lines(csv).length == 1) // header only
    }
  )
}
