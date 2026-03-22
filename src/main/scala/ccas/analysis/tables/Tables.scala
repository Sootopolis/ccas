package ccas.analysis.tables

import com.augustnagro.magnum.Transactor
import zio.{RIO, Scope, ZIO, ZIOAppArgs, ZIOAppDefault}

import ccas.utils.sql.DataSourceLayer

object Tables extends ZIOAppDefault {
  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    ensureTables.provide(DataSourceLayer.liveFromPrefix()) <* ZIO.logInfo("All tables ensured")

  def ensureTables: RIO[Transactor, Unit] =
    for {
      _ <- Player.createTable
      _ <- PlayerMatchRef.createTable
      _ <- PlayerSnapshot.createTable
      _ <- Club.createTable
      _ <- ClubMatchRef.createTable
      _ <- ClubMember.createTable
      _ <- MembershipRun.createTable
      _ <- RecruitmentCriteria.createTable
      _ <- RecruitmentAlias.createTable
      _ <- RecruitmentBlacklist.createTable
      _ <- RecruitmentRun.createTable
      _ <- RecruitmentCandidate.createTable
      _ <- PlayerRecruitmentCache.createTable
      _ <- ApiFetchFailure.createTable
      _ <- ClubMatch.createTable
      _ <- ClubMatchBoard.createTable
      _ <- HistoryMemberQuery.createTable
      _ <- HistoryPendingMatch.createTable
      _ <- HistoryRun.createTable
    } yield ()
}
