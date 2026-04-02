package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.tables.RunTrigger.given
import ccas.api.misc.subtypes.{ClubId, JobRunId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

final case class HistoryRun(
  runId: Long,
  clubId: ClubId,
  trigger: RunTrigger,
  startedAt: Instant,
  completedAt: Option[Instant],
  matchesProcessed: Option[Int],
  playersDiscovered: Option[Int],
  jobRunId: Option[JobRunId]
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
              job_run_id   TEXT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_history_run_club_started
            ON history_run (club_id, started_at DESC)""".update.run()
    }

  def insert(
    clubId: ClubId,
    trigger: RunTrigger,
    startedAt: Instant,
    jobRunId: Option[JobRunId] = None
  ): ZIO[PostgresClient, SQLException, Long] =
    connectZIO {
      sql"""INSERT INTO history_run (club_id, trigger, started_at, job_run_id)
            VALUES ($clubId, $trigger, $startedAt, $jobRunId)
            RETURNING run_id""".query[Long].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))

  def complete(
    runId: Long,
    completedAt: Instant,
    matchesProcessed: Int,
    playersDiscovered: Int
  ): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""UPDATE history_run
            SET completed_at = $completedAt, matches_processed = $matchesProcessed, players_discovered = $playersDiscovered
            WHERE run_id = $runId""".update.run()
    }
}
