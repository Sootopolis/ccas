package ccas.analysis.tables

import com.augustnagro.magnum.Transactor
import zio.ZIO

object Tables {
  def ensureTables: ZIO[Transactor, Throwable, Unit] =
    for {
      _ <- Player.createTable
      _ <- PlayerSnapshot.createTable
      _ <- Club.createTable
      _ <- ClubMember.createTable
      _ <- MembershipRun.createTable
      _ <- RecruitmentConfig.createTable
      _ <- RecruitmentRun.createTable
      _ <- RecruitmentCandidate.createTable
    } yield ()
}
