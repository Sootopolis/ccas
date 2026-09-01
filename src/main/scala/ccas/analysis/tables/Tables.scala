package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.{duration2DurationOps, Clock, RIO, Scope, ZIO, ZIOAppDefault}

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
    *
    * Health tracking is per-instance, so a store outage spanning startup logs its one WARN here and another from the
    * long-lived instance later. Two lines for one outage is the accepted cost of not threading a shared store
    * through the pool's init hook.
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

  /** Retention sweep for the two API-diagnostics tables, deliberately NOT part of [[ensureTables]]: it frees an
    * unbounded number of body objects at one round trip each, so on the boot path it held the HTTP port closed for as
    * long as the backlog took. `CcasServer` forks it alongside `Server.serve` instead.
    * See docs/adr/0007-response-caching-in-postgres.md.
    */
  private[ccas] def retentionSweep: RIO[PostgresClient & BodyStore, Unit] =
    ProgressDisplay.sourced("retention") {
      for {
        days        <- AppSetting.get(AppSetting.CacheRetentionDays)
        failureDays <- AppSetting.get(AppSetting.FetchFailureRetentionDays)
        now         <- Clock.instant
        cacheCutoff   = now.minus(days.toLong, ChronoUnit.DAYS)
        failureCutoff = now.minus(failureDays.toLong, ChronoUnit.DAYS)
        timed <- sweep(cacheCutoff, failureCutoff).timed
        (elapsed, (cacheRows, failureRows)) = timed
        _ <- ZIO.logInfo(
               s"Retention sweep: $cacheRows cache rows (>${days}d), $failureRows fetch-failure rows " +
                 s"(>${failureDays}d) in ${elapsed.render}"
             )
      } yield ()
    }

  // `ApiFetchFailure.deleteBefore` re-runs `ApiResponseBody.deleteOrphans` internally, so ordering it second catches
  // bodies pinned only by a just-deleted failure row.
  private def sweep(
    cacheCutoff: Instant,
    failureCutoff: Instant
  ): ZIO[PostgresClient & BodyStore, SQLException, (Int, Int)] =
    for {
      cacheRows   <- ApiResponseCache.deleteBefore(cacheCutoff)
      failureRows <- ApiFetchFailure.deleteBefore(failureCutoff)
    } yield (cacheRows, failureRows)
}
