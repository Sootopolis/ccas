package ccas.analysis.apps.recruitment

import java.time.{Instant, LocalDate, YearMonth, ZoneOffset}
import java.time.temporal.ChronoUnit

import ccas.utils.sql.PostgresClient
import zio.{RIO, ZIO}

import ccas.analysis.apps.withPlayerRenameRecovery
import ccas.analysis.tables.{Player, RecruitmentCriteria}
import ccas.api.misc.enums.GameResultDetail
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.api.player.ApiPlayerArchive
import ccas.utils.client.ChessComClient

private[recruitment] object RecruitmentStatsHelpers {

  def fetchTmStats(
    client: ChessComClient,
    username: Username,
    playerIdHint: PlayerId,
    criteria: RecruitmentCriteria,
    overallTimeoutPct: Double,
    now: Instant,
    recentArchives: Option[List[ApiPlayerArchive]]
  ): RIO[PostgresClient, TmStatsResult] = {
    val needsTmStats = criteria.dailyMinTmGamesFinished.isDefined || criteria.dailyMaxTmTimeoutPercent.isDefined
    if (!needsTmStats) ZIO.succeed(TmStatsResult(0, None, None, Set.empty))
    else {
      // Fetch last ~90 days of archives
      val cutoff = now.minus(90, ChronoUnit.DAYS)
      val months = recentArchiveMonths(now, 90)

      def fetchMonth(uname: Username, ym: YearMonth): RIO[PostgresClient, ApiPlayerArchive] =
        client.get[ApiPlayerArchive](ApiPlayerArchive.getUrl(uname, ym.getYear, ym.getMonthValue))

      val fetchArchives: RIO[PostgresClient, List[ApiPlayerArchive]] = recentArchives match {
        case Some(cached) => ZIO.succeed(cached)
        case None =>
          ZIO.foreachPar(months) { ym =>
            fetchMonth(username, ym)
              .withPlayerRenameRecovery(client, username, Some(playerIdHint))(uname => fetchMonth(uname, ym))
          }
      }

      for {
        archives <- fetchArchives
        // Always derive the canonical handle from `Player` regardless of which path produced `archives`. The cached
        // path is reached when an upstream filter (CheckDailyStats) already fetched the archives — possibly under a
        // post-recovery handle — but `username` may still be the pre-recovery value because `env.candidate.username`
        // isn't refreshed mid-pipeline. Reading it off the Player row is the only reliable source of the canonical
        // name. Tombstoned rows are unlikely to surface here in practice (FetchAndCheckPlayer runs first), but the
        // `.filterNot(_.isTombstoned)` guard exists in case a parallel job tombstones the row mid-evaluation —
        // without it a `_stale_<id>` placeholder would leak into the per-game predicates and miscount.
        effectiveUname <- Player.selectId(playerIdHint)
          .map(_.filterNot(_.isTombstoned).fold(username)(_.username))
      } yield {
        val tmGames = archives.flatMap(
          _.games.filter(g => g.timeClass == "daily" && g.`match`.isDefined && g.endTime >= cutoff.getEpochSecond)
        )
        val tmGamesFinished = tmGames.size
        val tmTimeoutPct =
          if (tmGamesFinished == 0) None
          else if (criteria.dailyMaxTmTimeoutPercent.isDefined && overallTimeoutPct == 0.0) Some(0.0)
          else {
            val timeouts = tmGames.count(g => playerResult(g, effectiveUname) == GameResultDetail.Timeout)
            Some(timeouts.toDouble / tmGamesFinished * 100.0)
          }
        val lastTmTimeoutAt = tmGames
          .filter(g => playerResult(g, effectiveUname) == GameResultDetail.Timeout)
          .sortBy(_.endTime)(using Ordering[Long].reverse)
          .headOption
          .map(g => Instant.ofEpochSecond(g.endTime))
        val opponentUsernames = tmGames.flatMap(nonTimeoutOpponent(_, effectiveUname)).toSet
        TmStatsResult(tmGamesFinished, tmTimeoutPct, lastTmTimeoutAt, opponentUsernames)
      }
    }
  }

  def extractLastDailyTimeout(archives: List[ApiPlayerArchive], username: Username): Option[Instant] =
    archives.flatMap(_.games)
      .filter(g => g.timeClass == "daily" && g.`match`.isEmpty) // non-match daily games
      .filter(g => playerResult(g, username) == GameResultDetail.Timeout)
      .sortBy(_.endTime)(using Ordering[Long].reverse)
      .headOption
      .map(g => Instant.ofEpochSecond(g.endTime))

  def mergeOptionalInstants(a: Option[Instant], b: Option[Instant]): Option[Instant] =
    (a, b) match {
      case (Some(x), Some(y)) => Some(if (x.isAfter(y)) x else y)
      case (Some(x), None)    => Some(x)
      case (None, Some(y))    => Some(y)
      case (None, None)       => None
    }

  def recentArchiveMonths(now: Instant, days: Int): List[YearMonth] = {
    val today      = LocalDate.ofInstant(now, ZoneOffset.UTC)
    val cutoff     = today.minusDays(days)
    val startMonth = YearMonth.from(cutoff)
    val endMonth   = YearMonth.from(today)
    Iterator.iterate(startMonth)(_.plusMonths(1)).takeWhile(!_.isAfter(endMonth)).toList
  }
}
