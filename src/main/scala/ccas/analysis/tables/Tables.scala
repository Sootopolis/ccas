package ccas.analysis.tables

import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.{RIO, Scope, ZIO, ZIOAppDefault}

import ccas.utils.ProgressDisplay
import ccas.utils.client.BodyStore
import ccas.utils.sql.PostgresClient

object Tables extends ZIOAppDefault {

  override def run: RIO[Scope, Unit] =
    for {
      _ <- ProgressDisplay.live(showProgress = false).build
      _ <- ensureTables.provide(PostgresClient.live(), BodyStore.live)
      _ <- ZIO.logInfo("All tables ensured")
    } yield ()

  /** [[ensureTables]] with its [[BodyStore]] self-provided, so it fits the `PostgresClient.live(onInit = ...)` hook —
    * which runs the init effect with only `PostgresClient` in scope. The store instance is scoped to the init run
    * (a fresh, cheap client, distinct from the long-lived one the app wires into `ChessComClient`).
    */
  val ensureTablesOnInit: RIO[PostgresClient, Unit] =
    ensureTables.provideSomeLayer[PostgresClient](BodyStore.live)

  def ensureTables: RIO[PostgresClient & BodyStore, Unit] =
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
      days <- AppSetting.get(AppSetting.CacheRetentionDays)
      _ <- ApiResponseCache.deleteBefore(Instant.now().minus(days.toLong, ChronoUnit.DAYS))
      // Sweep the failure audit trail on the same startup pass. `deleteBefore` also runs `ApiResponseBody.deleteOrphans`
      // internally, catching bodies freed once their last fetch-failure reference is gone (cache sweep already ran, so a
      // body pinned only by a just-deleted failure row is now collectable).
      failureDays <- AppSetting.get(AppSetting.FetchFailureRetentionDays)
      _ <- ApiFetchFailure.deleteBefore(Instant.now().minus(failureDays.toLong, ChronoUnit.DAYS))
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
