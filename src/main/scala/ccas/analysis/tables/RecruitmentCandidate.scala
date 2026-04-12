package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.apps.recruitment.CandidateOutcome
import ccas.analysis.apps.recruitment.CandidateOutcome.given
import ccas.api.misc.subtypes.{ClubId, PlayerId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

final case class RecruitmentCandidate(
  runId: Long,
  playerId: PlayerId,
  evaluatedAt: Instant,
  outcome: CandidateOutcome,
  rejectionReason: Option[String]
) derives DbCodec

object RecruitmentCandidate {
  private val selectCols = SqlLiteral("run_id, player_id, evaluated_at, outcome, rejection_reason")

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS recruitment_candidate (
              run_id            BIGINT NOT NULL,
              player_id         BIGINT NOT NULL,
              evaluated_at      TIMESTAMPTZ NOT NULL,
              outcome           TEXT NOT NULL CHECK (outcome IN ('Invited', 'Rejected', 'AlreadyMember', 'Error', 'Deferred')),
              rejection_reason  TEXT,
              PRIMARY KEY (run_id, player_id),
              FOREIGN KEY (run_id) REFERENCES recruitment_run (run_id) ON DELETE RESTRICT,
              FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_rc_player_outcome_eval
            ON recruitment_candidate (player_id, outcome, evaluated_at DESC)""".update.run()
    }

  def selectByRun(runId: Long): ZIO[PostgresClient, SQLException, List[RecruitmentCandidate]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_candidate WHERE run_id = $runId".query[RecruitmentCandidate].run().toList
    }

  def selectInvitedByRun(runId: Long): ZIO[PostgresClient, SQLException, List[RecruitmentCandidate]] =
    connectZIO {
      val invited = CandidateOutcome.Invited.toString
      sql"SELECT $selectCols FROM recruitment_candidate WHERE run_id = $runId AND outcome = $invited"
        .query[RecruitmentCandidate].run().toList
    }

  def selectLatestInvitedByClub(
    playerId: PlayerId,
    clubId: ClubId
  ): ZIO[PostgresClient, SQLException, Option[RecruitmentCandidate]] =
    connectZIO {
      val invited = CandidateOutcome.Invited.toString
      val selectColsQualified = SqlLiteral(
        "rc.run_id, rc.player_id, rc.evaluated_at, rc.outcome, rc.rejection_reason"
      )
      sql"""SELECT $selectColsQualified FROM recruitment_candidate rc
            JOIN recruitment_run rr ON rc.run_id = rr.run_id
            WHERE rc.player_id = $playerId AND rr.club_id = $clubId AND rc.outcome = $invited
            ORDER BY rc.evaluated_at DESC LIMIT 1""".query[RecruitmentCandidate].run().headOption
    }

  def selectLatestRejectedByAlias(
    playerId: PlayerId,
    clubId: ClubId,
    alias: String
  ): ZIO[PostgresClient, SQLException, Option[RecruitmentCandidate]] =
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

  def selectInvitedToday(clubId: ClubId, alias: String): ZIO[PostgresClient, SQLException, List[RecruitmentCandidate]] =
    connectZIO {
      val invited = CandidateOutcome.Invited.toString
      sql"""SELECT rc.run_id, rc.player_id, rc.evaluated_at, rc.outcome, rc.rejection_reason
            FROM recruitment_candidate rc
            JOIN recruitment_run rr ON rc.run_id = rr.run_id
            WHERE rr.club_id = $clubId AND rr.criteria_id IN (
              SELECT criteria_id FROM recruitment_alias WHERE club_id = $clubId AND alias = $alias
            )
              AND rr.completed_at IS NOT NULL
              AND rr.started_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
              AND rr.started_at < date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + INTERVAL '1 day'
              AND rc.outcome = $invited"""
        .query[RecruitmentCandidate].run().toList
    }

  def selectCountByRun(runId: Long): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"SELECT COUNT(*) FROM recruitment_candidate WHERE run_id = $runId"
        .query[Int].run().headOption
    }.someOrFail(new SQLException("COUNT query produced no rows"))

  def selectDeferredCountByRun(runId: Long): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      val deferred = CandidateOutcome.Deferred.toString
      sql"SELECT COUNT(*) FROM recruitment_candidate WHERE run_id = $runId AND outcome = $deferred"
        .query[Int].run().headOption
    }.someOrFail(new SQLException("COUNT query produced no rows"))

  /** Returns deferred candidates for a club that have not been resolved (Invited/Rejected) in a later run. */
  def selectDeferredByClub(clubId: ClubId): ZIO[PostgresClient, SQLException, List[RecruitmentCandidate]] =
    connectZIO {
      val deferred = CandidateOutcome.Deferred.toString
      val invited  = CandidateOutcome.Invited.toString
      val rejected = CandidateOutcome.Rejected.toString
      val selectColsQualified = SqlLiteral(
        "rc.run_id, rc.player_id, rc.evaluated_at, rc.outcome, rc.rejection_reason"
      )
      sql"""SELECT $selectColsQualified FROM recruitment_candidate rc
            JOIN recruitment_run rr ON rc.run_id = rr.run_id
            WHERE rr.club_id = $clubId AND rc.outcome = $deferred
              AND NOT EXISTS (
                SELECT 1 FROM recruitment_candidate rc2
                JOIN recruitment_run rr2 ON rc2.run_id = rr2.run_id
                WHERE rr2.club_id = $clubId AND rc2.player_id = rc.player_id
                  AND rc2.outcome IN ($invited, $rejected)
                  AND rc2.evaluated_at > rc.evaluated_at
              )
            ORDER BY rc.evaluated_at DESC"""
        .query[RecruitmentCandidate].run().toList
    }

  def insert(item: RecruitmentCandidate): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO recruitment_candidate (run_id, player_id, evaluated_at, outcome, rejection_reason)
            VALUES (${item.runId}, ${item.playerId}, ${item.evaluatedAt}, ${item.outcome}, ${item
          .rejectionReason})""".update.run()
    }

  def insertBatch(items: Iterable[RecruitmentCandidate]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO recruitment_candidate (run_id, player_id, evaluated_at, outcome, rejection_reason)
              VALUES (${item.runId}, ${item.playerId}, ${item.evaluatedAt}, ${item.outcome}, ${item
            .rejectionReason})""".update
      }
    }

  def updateOutcome(runId: Long, playerId: PlayerId, outcome: CandidateOutcome): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""UPDATE recruitment_candidate SET outcome = ${outcome}
            WHERE run_id = $runId AND player_id = $playerId""".update.run()
    }
}
