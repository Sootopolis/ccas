package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.analysis.tables.RunTrigger.given
import ccas.api.misc.subtypes.ClubId
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

final case class HistoryRun(
  runId: Long,
  clubId: ClubId,
  trigger: RunTrigger,
  startedAt: Instant,
  completedAt: Option[Instant],
  matchesProcessed: Option[Int],
  playersDiscovered: Option[Int]
) derives DbCodec

object HistoryRun {

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS history_run (
              run_id       BIGSERIAL PRIMARY KEY,
              club_id      BIGINT NOT NULL REFERENCES club (club_id),
              trigger      TEXT NOT NULL,
              started_at   TIMESTAMPTZ NOT NULL,
              completed_at TIMESTAMPTZ,
              matches_processed  INT,
              players_discovered INT
            )""".update.run()
    }

  def insert(clubId: ClubId, trigger: RunTrigger, startedAt: Instant): ZIO[Transactor, SQLException, Long] =
    connectZIO {
      sql"""INSERT INTO history_run (club_id, trigger, started_at)
            VALUES ($clubId, $trigger, $startedAt)
            RETURNING run_id""".query[Long].run().headOption
    }.someOrFail(new SQLException("INSERT RETURNING produced no rows"))

  def complete(
    runId: Long,
    completedAt: Instant,
    matchesProcessed: Int,
    playersDiscovered: Int
  ): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""UPDATE history_run
            SET completed_at = $completedAt, matches_processed = $matchesProcessed, players_discovered = $playersDiscovered
            WHERE run_id = $runId""".update.run()
    }
}
