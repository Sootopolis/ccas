package ccas.analysis.tables

import zio.{RIO, Scope, ZIOAppDefault}

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
      _ <- ApiFetchFailure.createTable
      _ <- ClubMatch.createTable
      _ <- ClubMatchBoard.createTable
      _ <- HistoryMemberQuery.createTable
      _ <- HistoryPendingMatch.createTable
      _ <- HistoryRun.createTable
      _ <- UnresolvedBoardPlayer.createTable
      _ <- UnresolvedMatchClub.createTable
      _ <- ClientStats.createTable
    } yield ()
}
