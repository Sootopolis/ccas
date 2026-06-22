package ccas.analysis.tables

import java.time.Instant
import java.time.temporal.ChronoUnit

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
      days <- AppSetting.get(AppSettings.CacheRetentionDays)
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
}
