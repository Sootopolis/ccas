package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.tables.RunTrigger.given
import ccas.analysis.tables.subtypes.HistoryRunId
import ccas.api.misc.subtypes.{ClubId, JobRunId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

final case class HistoryRun(
  runId: HistoryRunId,
  clubId: ClubId,
  trigger: RunTrigger,
  startedAt: Instant,
  completedAt: Option[Instant],
  matchesProcessed: Option[Int],
  playersDiscovered: Option[Int],
  jobRunId: Option[JobRunId],
  refreshMatchUnchanged: Int,
  seedClubMatchesUnchanged: Int,
  seedPlayerMatchesUnchanged: Int,
  abortedMatches: Int
) derives DbCodec

object HistoryRun {

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS history_run (
              run_id       BIGSERIAL PRIMARY KEY,
              club_id      BIGINT NOT NULL REFERENCES club (club_id) ON DELETE RESTRICT,
              trigger      TEXT NOT NULL,
              started_at   TIMESTAMPTZ NOT NULL,
              completed_at TIMESTAMPTZ,
              matches_processed  INT,
              players_discovered INT,
              job_run_id   TEXT,
              refresh_match_unchanged        INT NOT NULL DEFAULT 0,
              seed_club_matches_unchanged    INT NOT NULL DEFAULT 0,
              seed_player_matches_unchanged  INT NOT NULL DEFAULT 0,
              aborted_matches                INT NOT NULL DEFAULT 0
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_history_run_club_started
            ON history_run (club_id, started_at DESC)""".update.run()
    }

  def insert(
    clubId: ClubId,
    trigger: RunTrigger,
    startedAt: Instant,
    jobRunId: Option[JobRunId]
  ): ZIO[PostgresClient, SQLException, HistoryRunId] =
    connectZIO {
      sql"""INSERT INTO history_run (club_id, trigger, started_at, job_run_id)
            VALUES ($clubId, $trigger, $startedAt, $jobRunId)
            RETURNING run_id""".query[HistoryRunId].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))

  def complete(
    runId: HistoryRunId,
    completedAt: Instant,
    matchesProcessed: Int,
    playersDiscovered: Int,
    refreshMatchUnchanged: Int,
    seedClubMatchesUnchanged: Int,
    seedPlayerMatchesUnchanged: Int,
    abortedMatches: Int
  ): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""UPDATE history_run
            SET completed_at = $completedAt,
                matches_processed = $matchesProcessed,
                players_discovered = $playersDiscovered,
                refresh_match_unchanged = $refreshMatchUnchanged,
                seed_club_matches_unchanged = $seedClubMatchesUnchanged,
                seed_player_matches_unchanged = $seedPlayerMatchesUnchanged,
                aborted_matches = $abortedMatches
            WHERE run_id = $runId""".update.run()
    }
}
