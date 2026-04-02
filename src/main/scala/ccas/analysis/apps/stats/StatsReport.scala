package ccas.analysis.apps.stats

import ccas.analysis.apps.stats.StatsUtils.{MemberContribution, PlayerBoardStats}

import java.time.Instant
import ccas.api.misc.subtypes.ClubSlug

/** Formats stats results as aligned text tables for output files. */
object StatsReport {

  def formatContribution(
    clubSlug: ClubSlug,
    contributions: List[MemberContribution],
    matchCount: Long,
    generatedAt: Instant
  ): String = {
    val sb = new StringBuilder
    sb.append(s"=== Member Contribution: $clubSlug ===\n")
    sb.append(s"Generated: $generatedAt\n")
    sb.append(s"Finished matches: $matchCount\n\n")

    sb.append("--- Raw ---\n")
    appendTable(sb, contributions, _.raw)

    sb.append("\n--- Fairplay-adjusted ---\n")
    appendTable(sb, contributions, _.fairPlay)

    sb.toString
  }

  def formatPlayerOfPeriod(
    clubSlug: ClubSlug,
    contributions: List[MemberContribution],
    since: Instant,
    until: Instant,
    minGames: Int,
    generatedAt: Instant
  ): String = {
    val sb = new StringBuilder
    sb.append(s"=== Player of the Period: $clubSlug ===\n")
    sb.append(s"Period: $since to $until\n")
    sb.append(s"Minimum games: $minGames\n")
    sb.append(s"Generated: $generatedAt\n\n")

    val eligible = contributions.filter(_.raw.games >= minGames).sortBy(-_.raw.scoreRate)

    sb.append("--- Ranking (raw) ---\n")
    appendRankedTable(sb, eligible)

    sb.toString
  }

  private def appendTable(
    sb: StringBuilder,
    contributions: List[MemberContribution],
    selectStats: MemberContribution => PlayerBoardStats
  ): Unit =
    if (contributions.isEmpty) {
      sb.append("No data\n")
    } else {
      val nameWidth = contributions.map(_.username.value.length).max max 8
      val fmt       = s"%-${nameWidth}s  %6d  %5d  %3d  %3d  %3d  %6s  %6s\n"
      val header    = s"%-${nameWidth}s  %6s  %5s  %3s  %3s  %3s  %6s  %6s\n"
      val sep       = "-" * nameWidth + "  " + "------  -----  ---  ---  ---  ------  ------\n"

      sb.append(header.format("Username", "Boards", "Games", "W", "D", "L", "Points", "Score%"))
      sb.append(sep)

      contributions.foreach { mc =>
        val s = selectStats(mc)
        sb.append(fmt.format(
          mc.username.value, s.boards, s.games, s.wins, s.draws, s.losses,
          formatPoints(s.points), formatPercent(s.scoreRate)
        ))
      }

      val total = contributions.map(selectStats).foldLeft(PlayerBoardStats.empty)(_ + _)
      sb.append(sep)
      sb.append(fmt.format(
        "TOTAL", total.boards, total.games, total.wins, total.draws, total.losses,
        formatPoints(total.points), formatPercent(total.scoreRate)
      ))
    }

  private def appendRankedTable(sb: StringBuilder, ranked: List[MemberContribution]): Unit =
    if (ranked.isEmpty) {
      sb.append("No eligible players\n")
    } else {
      val nameWidth = ranked.map(_.username.value.length).max max 8
      val fmt       = s"%3d  %-${nameWidth}s  %5d  %3d  %3d  %3d  %6s  %6s\n"
      val header    = s"%3s  %-${nameWidth}s  %5s  %3s  %3s  %3s  %6s  %6s\n"
      val sep       = "---  " + "-" * nameWidth + "  " + "-----  ---  ---  ---  ------  ------\n"

      sb.append(header.format("#", "Username", "Games", "W", "D", "L", "Points", "Score%"))
      sb.append(sep)

      ranked.zipWithIndex.foreach { case (mc, idx) =>
        val s = mc.raw
        sb.append(fmt.format(
          idx + 1, mc.username.value, s.games, s.wins, s.draws, s.losses,
          formatPoints(s.points), formatPercent(s.scoreRate)
        ))
      }
    }

  private def formatPoints(points: Double): String =
    if (points == points.toLong.toDouble) f"$points%.0f"
    else f"$points%.1f"

  private def formatPercent(rate: Double): String = f"${rate * 100}%.1f%%"
}
