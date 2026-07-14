package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.apps.recruitment.CandidateOutcome
import ccas.analysis.apps.recruitment.CandidateOutcome.given
import ccas.analysis.tables.subtypes.RecruitmentRunId
import ccas.api.misc.subtypes.{ClubId, PlayerId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

final case class RecruitmentCandidate(
  runId: RecruitmentRunId,
  playerId: PlayerId,
  evaluatedAt: Instant,
  outcome: CandidateOutcome,
  rejectionReason: Option[String]
) derives DbCodec

object RecruitmentCandidate {
  private val selectCols = SqlLiteral("run_id, player_id, evaluated_at, outcome, rejection_reason")

  private val selectColsRc = SqlLiteral(
    "rc.run_id, rc.player_id, rc.evaluated_at, rc.outcome, rc.rejection_reason"
  )

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

  def selectByRun(runId: RecruitmentRunId): ZIO[PostgresClient, SQLException, List[RecruitmentCandidate]] =
    connectZIO {
      sql"SELECT $selectCols FROM recruitment_candidate WHERE run_id = $runId".query[RecruitmentCandidate].run().toList
    }

  def selectInvitedByRun(runId: RecruitmentRunId): ZIO[PostgresClient, SQLException, List[RecruitmentCandidate]] =
    connectZIO {
      val invited: CandidateOutcome = CandidateOutcome.Invited
      sql"""SELECT $selectCols FROM recruitment_candidate WHERE run_id = $runId AND outcome = $invited
            ORDER BY player_id"""
        .query[RecruitmentCandidate].run().toList
    }

  def selectLatestInvitedByClub(
    playerId: PlayerId,
    clubId: ClubId
  ): ZIO[PostgresClient, SQLException, Option[RecruitmentCandidate]] =
    connectZIO {
      val invited: CandidateOutcome = CandidateOutcome.Invited
      sql"""SELECT $selectColsRc FROM recruitment_candidate rc
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
      val rejected: CandidateOutcome = CandidateOutcome.Rejected
      sql"""SELECT $selectColsRc FROM recruitment_candidate rc
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
      val invited: CandidateOutcome = CandidateOutcome.Invited
      // Spans every run today, so a player invited in more than one run has multiple rows. DISTINCT ON keeps one
      // per player — the latest invite wins — and leads ORDER BY with player_id so the output stays ascending.
      sql"""SELECT DISTINCT ON (rc.player_id) $selectColsRc FROM recruitment_candidate rc
            JOIN recruitment_run rr ON rc.run_id = rr.run_id
            WHERE rr.club_id = $clubId AND rr.criteria_id IN (
              SELECT criteria_id FROM recruitment_alias WHERE club_id = $clubId AND alias = $alias
            )
              AND rr.completed_at IS NOT NULL
              AND rr.started_at >= date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
              AND rr.started_at < date_trunc('day', NOW() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + INTERVAL '1 day'
              AND rc.outcome = $invited
            ORDER BY rc.player_id, rc.evaluated_at DESC, rc.run_id DESC"""
        .query[RecruitmentCandidate].run().toList
    }

  def selectCountByRun(runId: RecruitmentRunId): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"SELECT COUNT(*) FROM recruitment_candidate WHERE run_id = $runId"
        .query[Int].run().headOption
    }.someOrFail(new SQLException("COUNT query produced no rows"))

  def selectDeferredCountByRun(runId: RecruitmentRunId): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      val deferred: CandidateOutcome = CandidateOutcome.Deferred
      sql"SELECT COUNT(*) FROM recruitment_candidate WHERE run_id = $runId AND outcome = $deferred"
        .query[Int].run().headOption
    }.someOrFail(new SQLException("COUNT query produced no rows"))

  // The run's still-deferred candidates the operator will confirm, for the interactive `ccas recruit` confirm prompt (a
  // deferred-confirm run leaves everything Deferred; the operator reviews these before any are marked Invited). Capped
  // at the run's remaining budget — `target` minus those already Invited this run — so a chunk overshoot isn't
  // shown/confirmed above target and a re-fetch after a partial confirm shows only what's still confirmable. The excess
  // stays Deferred and carries to the next run. Legacy runs (NULL target) fall through to `LIMIT NULL` (no cap). Ordered
  // by player_id to match `selectInvitedByRun` and `confirmDeferredByRun`.
  def selectDeferredByRun(runId: RecruitmentRunId): ZIO[PostgresClient, SQLException, List[RecruitmentCandidate]] =
    connectZIO {
      val deferred: CandidateOutcome = CandidateOutcome.Deferred
      val invited: CandidateOutcome  = CandidateOutcome.Invited
      sql"""SELECT $selectCols FROM recruitment_candidate WHERE run_id = $runId AND outcome = $deferred
            ORDER BY player_id
            LIMIT (SELECT CASE WHEN rr.target IS NULL THEN NULL
                               ELSE GREATEST(rr.target - (SELECT COUNT(*) FROM recruitment_candidate
                                                          WHERE run_id = $runId AND outcome = $invited), 0)
                          END
                   FROM recruitment_run rr WHERE rr.run_id = $runId)"""
        .query[RecruitmentCandidate].run().toList
    }

  // Confirm a deferred-confirm run: flip its remaining-budget Deferred candidates (lowest player_ids first) to Invited.
  // Matches `selectDeferredByRun`'s cap+order so the operator invites exactly what the prompt showed. The cap is
  // `target - already-invited-this-run`, so a re-POST after a full confirm flips 0 (idempotent, per the endpoint's
  // contract) rather than sweeping up the still-Deferred overshoot; that excess carries to the next run. Legacy runs
  // (NULL target) flip all Deferred. Returns rows affected (0 if already confirmed or nothing found), so the caller can
  // update `recruitment_run.candidates_found`.
  def confirmDeferredByRun(runId: RecruitmentRunId): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      val invited: CandidateOutcome  = CandidateOutcome.Invited
      val deferred: CandidateOutcome = CandidateOutcome.Deferred
      sql"""UPDATE recruitment_candidate SET outcome = $invited
            WHERE run_id = $runId AND player_id IN (
              SELECT player_id FROM recruitment_candidate
              WHERE run_id = $runId AND outcome = $deferred
              ORDER BY player_id
              LIMIT (SELECT CASE WHEN rr.target IS NULL THEN NULL
                                 ELSE GREATEST(rr.target - (SELECT COUNT(*) FROM recruitment_candidate
                                                            WHERE run_id = $runId AND outcome = $invited), 0)
                            END
                     FROM recruitment_run rr WHERE rr.run_id = $runId)
            ) AND outcome = $deferred""".update.run()
    }

  /** Returns deferred candidates for a club that have not been resolved (Invited/Rejected) in a later run. */
  def selectDeferredByClub(clubId: ClubId): ZIO[PostgresClient, SQLException, List[RecruitmentCandidate]] =
    connectZIO {
      val deferred: CandidateOutcome = CandidateOutcome.Deferred
      val invited: CandidateOutcome  = CandidateOutcome.Invited
      val rejected: CandidateOutcome = CandidateOutcome.Rejected
      sql"""SELECT $selectColsRc FROM recruitment_candidate rc
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

  def updateOutcome(runId: RecruitmentRunId, playerId: PlayerId, outcome: CandidateOutcome): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""UPDATE recruitment_candidate SET outcome = ${outcome}
            WHERE run_id = $runId AND player_id = $playerId""".update.run()
    }
}
