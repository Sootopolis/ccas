package ccas.analysis.apps.stats

import ccas.analysis.apps.stats.StatsUtils.{MemberContribution, PlayerBoardStats}
import ccas.analysis.tables.Player

/** Formats stats results as CSV for output files. */
object StatsReport {

  /** All-time contribution CSV with both raw and fairplay-adjusted stats, ranked by raw points descending. */
  def formatContribution(contributions: List[MemberContribution]): String = {
    val sb = new StringBuilder
    sb.append("#,Username,Boards,Games,W,D,L,Points,Score%,FP_W,FP_D,FP_L,FP_Points,FP_Score%\n")

    val ranked = contributions.sortBy(mc => (-mc.raw.pointsX2, mc.username.value.toLowerCase))
    ranked.zipWithIndex.foreach { case (mc, idx) =>
      val r  = mc.raw
      val fp = mc.fairPlay
      sb.append(csvRow(
        (idx + 1).toString, Player.displayUsername(mc.username, mc.playerId), r.boards.toString, r.games.toString,
        r.wins.toString, r.draws.toString, r.losses.toString,
        formatPoints(r.points), formatPercent(r.scoreRate),
        fp.wins.toString, fp.draws.toString, fp.losses.toString,
        formatPoints(fp.points), formatPercent(fp.scoreRate)
      ))
    }

    if (contributions.nonEmpty) {
      val rawTotal = contributions.map(_.raw).foldLeft(PlayerBoardStats.empty)(_ + _)
      val fpTotal  = contributions.map(_.fairPlay).foldLeft(PlayerBoardStats.empty)(_ + _)
      sb.append(csvRow(
        "", "TOTAL", rawTotal.boards.toString, rawTotal.games.toString,
        rawTotal.wins.toString, rawTotal.draws.toString, rawTotal.losses.toString,
        formatPoints(rawTotal.points), formatPercent(rawTotal.scoreRate),
        fpTotal.wins.toString, fpTotal.draws.toString, fpTotal.losses.toString,
        formatPoints(fpTotal.points), formatPercent(fpTotal.scoreRate)
      ))
    }

    sb.toString
  }

  /** Period-filtered CSV ranked by raw points descending, showing only players meeting the minimum games threshold. */
  def formatPlayerOfPeriod(contributions: List[MemberContribution], minGames: Int): String = {
    val sb = new StringBuilder
    sb.append("#,Username,Games,W,D,L,Points,Score%\n")

    val eligible = contributions.filter(_.raw.games >= minGames)
      .sortBy(mc => (-mc.raw.pointsX2, mc.username.value.toLowerCase))

    eligible.zipWithIndex.foreach { case (mc, idx) =>
      val s = mc.raw
      sb.append(csvRow(
        (idx + 1).toString, Player.displayUsername(mc.username, mc.playerId), s.games.toString,
        s.wins.toString, s.draws.toString, s.losses.toString,
        formatPoints(s.points), formatPercent(s.scoreRate)
      ))
    }

    sb.toString
  }

  private def csvRow(fields: String*): String = fields.mkString(",") + "\n"

  private def formatPoints(points: Double): String =
    if (points == points.toLong.toDouble) f"$points%.0f"
    else f"$points%.1f"

  private def formatPercent(rate: Double): String = f"${rate * 100}%.1f%%"
}
