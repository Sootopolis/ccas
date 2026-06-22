package ccas.analysis.tables

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.typesafe.config.ConfigFactory
import zio.{RIO, Scope, ZIO, ZIOAppDefault}

import ccas.utils.ProgressDisplay
import ccas.utils.sql.PostgresClient

object Tables extends ZIOAppDefault {

  override def run: RIO[Scope, Unit] =
    for {
      _ <- ProgressDisplay.live(showProgress = false).build
      _ <- ensureTables.provide(PostgresClient.live())
      _ <- ZIO.logInfo("All tables ensured")
    } yield ()

  def ensureTables: RIO[PostgresClient, Unit] =
    for {
      _ <- AppSetting.createTable
      _ <- Player.createTable
      _ <- PlayerMatchRef.createTable
      _ <- PlayerTournamentRef.createTable
      _ <- PlayerSnapshot.createTable
      _ <- Club.createTable
      _ <- ClubAdmin.createTable
      _ <- ClubMatchRef.createTable
      _ <- PlayerRefSkip.createTable
      _ <- ClubRefSkip.createTable
      _ <- ClubMember.createTable
      _ <- MembershipRun.createTable
      _ <- RecruitmentCriteria.createTable
      _ <- RecruitmentAlias.createTable
      _ <- RecruitmentBlacklist.createTable
      _ <- ManagedClub.createTable
      _ <- RecruitmentRun.createTable
      _ <- RecruitmentCandidate.createTable
      _ <- PlayerRecruitmentCache.createTable
      _ <- ApiResponseBody.createTable
      _ <- ApiResponseCache.createTable
      _ <- ApiFetchFailure.createTable
      _ <- ApiResponseBody.normalizeCfBodies
      days <- cacheRetentionDays
      _ <- ApiResponseCache.deleteBefore(Instant.now().minus(days.toLong, ChronoUnit.DAYS))
      _ <- ClubMatch.createTable
      _ <- ClubMatchBoard.createTable
      _ <- ClubMatchGame.createTable
      _ <- HistoryMemberQuery.createTable
      _ <- HistoryPendingMatch.createTable
      _ <- HistoryRun.createTable
      _ <- UnresolvedBoardPlayer.createTable
      _ <- UnresolvedMatchClub.createTable
      _ <- ClientConfig.createTable
      _ <- ClientStats.createTable
    } yield ()

  /** Resolve the cache-retention window (days), DB-first. The authoritative value lives in `app_setting` so multiple
    * consumers sharing one DB agree on the window. The stored DB value wins; only when the row is absent (fresh DB) is
    * it seeded from the HOCON default (`chess-com-client.cache.retention-days`, env override
    * `CHESS_COM_API_CACHE_RETENTION_DAYS` — both apply at seed time only), after which HOCON/env are ignored. Reading
    * the DB first keeps the common path to a single SELECT (no needless HOCON load or write on every startup). A
    * non-numeric stored value falls back to the HOCON default defensively.
    */
  private def cacheRetentionDays: RIO[PostgresClient, Int] =
    AppSetting.select(AppSetting.CacheRetentionDays).flatMap {
      case Some(stored) => stored.toIntOption.fold(seedRetentionDays)(ZIO.succeed)
      case None         => seedRetentionDays
    }

  /** Seed `cache_retention_days` from the HOCON default and return it. HOCON is read directly via
    * `ConfigFactory.load()` (same pattern `PostgresClient.live` uses) since `ensureTables` runs before
    * `ChessComClient.live`, so the zio-config provider used there isn't available yet here. `insertIfAbsent` keeps two
    * consumers racing a fresh DB safe — both seed, both then see the same value.
    */
  private def seedRetentionDays: RIO[PostgresClient, Int] =
    for {
      days <- ZIO.attempt(ConfigFactory.load().getInt("chess-com-client.cache.retention-days"))
      _ <- AppSetting.insertIfAbsent(AppSetting.CacheRetentionDays, days.toString)
    } yield days
}
