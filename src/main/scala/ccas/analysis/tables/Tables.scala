package ccas.analysis.tables

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.typesafe.config.ConfigFactory
import zio.{RIO, Scope, ZIO, ZIOAppDefault}

import ccas.utils.sql.PostgresClient
import ccas.utils.CcasLogger

object Tables extends ZIOAppDefault {

  override def run: RIO[Scope, Unit] =
    (ensureTables.provide(PostgresClient.live()) <* CcasLogger.info("All tables ensured"))
      .provideSome[Scope](CcasLogger.live())

  def ensureTables: RIO[PostgresClient, Unit] =
    for {
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
      _ <- RecruitmentRun.createTable
      _ <- RecruitmentCandidate.createTable
      _ <- PlayerRecruitmentCache.createTable
      _ <- ApiResponseBody.createTable
      _ <- ApiResponseCache.createTable
      _ <- ApiFetchFailure.createTable
      _ <- ApiResponseBody.normalizeCfBodies
      retentionCutoff <- cacheRetentionCutoff
      _ <- ApiResponseCache.deleteBefore(retentionCutoff)
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

  /** Read `chess-com-client.cache.retention-days` from config and compute the cutoff Instant. Read directly via
    * `ConfigFactory.load()` (same pattern `PostgresClient.live` uses) rather than threading a CacheConfig layer
    * through startup — `ensureTables` runs before `ChessComClient.live`, so the zio-config provider used there
    * isn't available yet here.
    */
  private def cacheRetentionCutoff: ZIO[Any, Throwable, Instant] =
    ZIO.attempt {
      val days = ConfigFactory.load().getInt("chess-com-client.cache.retention-days")
      Instant.now().minus(days.toLong, ChronoUnit.DAYS)
    }
}
