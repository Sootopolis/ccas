package ccas.analysis.tables

import ccas.api.misc.subtypes.ClubId
import ccas.utils.sql.SqlZioTypes.connectZIO
import ccas.utils.sql.DbCodecs.given
import com.augustnagro.magnum.*
import zio.ZIO

import java.sql.SQLException
import java.time.Instant

final case class RecruitmentRun(
  runId          : Long,
  clubId         : ClubId,
  configName     : String,
  startedAt      : Instant,
  completedAt    : Option[Instant],
  candidatesFound: Int,
) derives DbCodec

object RecruitmentRun {
  private val selectCols = SqlLiteral("run_id, club_id, config_name, started_at, completed_at, candidates_found")

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS recruitment_run (
              run_id            BIGSERIAL PRIMARY KEY,
              club_id           BIGINT NOT NULL,
              config_name       VARCHAR NOT NULL,
              started_at        TIMESTAMPTZ NOT NULL,
              completed_at      TIMESTAMPTZ,
              candidates_found  INT NOT NULL DEFAULT 0
            )""".update.run()
    }

  def insert(clubId: ClubId, configName: String, startedAt: Instant): ZIO[Transactor, SQLException, Long] =
    connectZIO {
      sql"""INSERT INTO recruitment_run (club_id, config_name, started_at, candidates_found)
            VALUES ($clubId, $configName, $startedAt, 0)
            RETURNING run_id""".query[Long].run().head
    }

  def update(item: RecruitmentRun): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""UPDATE recruitment_run SET completed_at = ${item.completedAt}, candidates_found = ${item.candidatesFound}
            WHERE run_id = ${item.runId}""".update.run()
    }

  def selectLatest(clubId: ClubId): ZIO[Transactor, SQLException, Option[RecruitmentRun]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_run WHERE club_id = $clubId ORDER BY started_at DESC"
        .query[RecruitmentRun].run().headOption
    }

  def selectId(runId: Long): ZIO[Transactor, SQLException, Option[RecruitmentRun]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_run WHERE run_id = $runId"
        .query[RecruitmentRun].run().headOption
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM recruitment_run".update.run()
    }
}
