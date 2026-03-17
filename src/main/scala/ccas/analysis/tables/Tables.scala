package ccas.analysis.tables

import com.augustnagro.magnum.Transactor
import zio.RIO

object Tables {
  def ensureTables: RIO[Transactor, Unit] =
    for {
      _ <- Player.createTable
      _ <- PlayerMatchRef.createTable
      _ <- PlayerSnapshot.createTable
      _ <- Club.createTable
      _ <- ClubMatchRef.createTable
      _ <- ClubMember.createTable
      _ <- MembershipRun.createTable
      _ <- RecruitmentConfig.createTable
      _ <- RecruitmentBlacklist.createTable
      _ <- RecruitmentRun.createTable
      _ <- RecruitmentCandidate.createTable
      _ <- PlayerRecruitmentCache.createTable
      _ <- ApiFetchFailure.createTable
    } yield ()
}
