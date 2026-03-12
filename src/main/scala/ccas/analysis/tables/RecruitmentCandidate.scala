package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.apps.recruitment.CandidateOutcome
import ccas.analysis.apps.recruitment.CandidateOutcome.given
import ccas.api.misc.subtypes.Username
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

final case class RecruitmentCandidate(
    runId: Long,
    username: Username,
    evaluatedAt: Instant,
    outcome: CandidateOutcome,
    rejectionReason: Option[String])
    derives DbCodec

object RecruitmentCandidate {
  private val selectCols = SqlLiteral("run_id, username, evaluated_at, outcome, rejection_reason")

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS recruitment_candidate (
              run_id            BIGINT NOT NULL,
              username          VARCHAR NOT NULL,
              evaluated_at      TIMESTAMPTZ NOT NULL,
              outcome           VARCHAR NOT NULL,
              rejection_reason  VARCHAR,
              PRIMARY KEY (run_id, username)
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_rc_username ON recruitment_candidate (username)""".update.run()
    }

  def insert(item: RecruitmentCandidate): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO recruitment_candidate (run_id, username, evaluated_at, outcome, rejection_reason)
            VALUES (${item.runId}, ${item.username}, ${item.evaluatedAt}, ${item.outcome.toString}, ${item
          .rejectionReason})""".update.run()
    }

  def insertBatch(items: Iterable[RecruitmentCandidate]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO recruitment_candidate (run_id, username, evaluated_at, outcome, rejection_reason)
              VALUES (${item.runId}, ${item.username}, ${item.evaluatedAt}, ${item.outcome.toString}, ${item
            .rejectionReason})""".update
      }
    }

  def selectByRun(runId: Long): ZIO[Transactor, SQLException, List[RecruitmentCandidate]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_candidate WHERE run_id = $runId".query[RecruitmentCandidate].run().toList
    }

  def selectInvitedByRun(runId: Long): ZIO[Transactor, SQLException, List[RecruitmentCandidate]] =
    connectZIO {
      val invited = CandidateOutcome.Invited.toString
      sql"SELECT $selectCols FROM recruitment_candidate WHERE run_id = $runId AND outcome = $invited"
        .query[RecruitmentCandidate].run().toList
    }

  def selectLatestInvited(username: Username): ZIO[Transactor, SQLException, Option[RecruitmentCandidate]] =
    connectZIO {
      val invited = CandidateOutcome.Invited.toString
      sql"""SELECT $selectCols FROM recruitment_candidate
            WHERE username = $username AND outcome = $invited
            ORDER BY evaluated_at DESC""".query[RecruitmentCandidate].run().headOption
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM recruitment_candidate".update.run()
    }
}
