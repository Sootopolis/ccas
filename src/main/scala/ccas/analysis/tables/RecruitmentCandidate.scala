package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.apps.recruitment.CandidateOutcome
import ccas.analysis.apps.recruitment.CandidateOutcome.given
import ccas.api.misc.subtypes.{ClubId, PlayerId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

final case class RecruitmentCandidate(
  runId: Long,
  playerId: PlayerId,
  evaluatedAt: Instant,
  outcome: CandidateOutcome,
  rejectionReason: Option[String])
    derives DbCodec

object RecruitmentCandidate {
  private val selectCols = SqlLiteral("run_id, player_id, evaluated_at, outcome, rejection_reason")

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS recruitment_candidate (
              run_id            BIGINT NOT NULL,
              player_id         BIGINT NOT NULL,
              evaluated_at      TIMESTAMPTZ NOT NULL,
              outcome           VARCHAR NOT NULL CHECK (outcome IN ('Invited', 'Rejected', 'AlreadyMember', 'Error')),
              rejection_reason  VARCHAR,
              PRIMARY KEY (run_id, player_id),
              FOREIGN KEY (run_id) REFERENCES recruitment_run (run_id) ON DELETE RESTRICT,
              FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_rc_player_id ON recruitment_candidate (player_id)""".update.run()
    }

  def insert(item: RecruitmentCandidate): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO recruitment_candidate (run_id, player_id, evaluated_at, outcome, rejection_reason)
            VALUES (${item.runId}, ${item.playerId}, ${item.evaluatedAt}, ${item.outcome.toString}, ${item
          .rejectionReason})""".update.run()
    }

  def insertBatch(items: Iterable[RecruitmentCandidate]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO recruitment_candidate (run_id, player_id, evaluated_at, outcome, rejection_reason)
              VALUES (${item.runId}, ${item.playerId}, ${item.evaluatedAt}, ${item.outcome.toString}, ${item
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

  def selectLatestInvited(playerId: PlayerId): ZIO[Transactor, SQLException, Option[RecruitmentCandidate]] =
    connectZIO {
      val invited = CandidateOutcome.Invited.toString
      sql"""SELECT $selectCols FROM recruitment_candidate
            WHERE player_id = $playerId AND outcome = $invited
            ORDER BY evaluated_at DESC""".query[RecruitmentCandidate].run().headOption
    }

  def selectLatestRejectedByAlias(
    playerId: PlayerId,
    clubId: ClubId,
    alias: String
  ): ZIO[Transactor, SQLException, Option[RecruitmentCandidate]] =
    connectZIO {
      val rejected = CandidateOutcome.Rejected.toString
      val selectColsQualified = SqlLiteral(
        "rc.run_id, rc.player_id, rc.evaluated_at, rc.outcome, rc.rejection_reason"
      )
      sql"""SELECT $selectColsQualified FROM recruitment_candidate rc
            JOIN recruitment_run rr ON rc.run_id = rr.run_id
            WHERE rc.player_id = $playerId AND rr.club_id = $clubId
              AND rr.criteria_id IN (
                SELECT criteria_id FROM recruitment_alias WHERE club_id = $clubId AND alias = $alias
              )
              AND rc.outcome = $rejected
            ORDER BY rc.evaluated_at DESC LIMIT 1""".query[RecruitmentCandidate].run().headOption
    }

  def selectInvitedToday(clubId: ClubId, alias: String): ZIO[Transactor, SQLException, List[RecruitmentCandidate]] =
    connectZIO {
      val invited = CandidateOutcome.Invited.toString
      sql"""SELECT rc.run_id, rc.player_id, rc.evaluated_at, rc.outcome, rc.rejection_reason
            FROM recruitment_candidate rc
            JOIN recruitment_run rr ON rc.run_id = rr.run_id
            WHERE rr.club_id = $clubId AND rr.criteria_id IN (
              SELECT criteria_id FROM recruitment_alias WHERE club_id = $clubId AND alias = $alias
            )
              AND rr.completed_at IS NOT NULL
              AND (rr.started_at AT TIME ZONE 'UTC')::date = (NOW() AT TIME ZONE 'UTC')::date
              AND rc.outcome = $invited"""
        .query[RecruitmentCandidate].run().toList
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM recruitment_candidate".update.run()
    }
}
