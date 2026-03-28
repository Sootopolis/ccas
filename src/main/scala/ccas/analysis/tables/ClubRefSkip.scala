package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.ClubId
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

import RefSkipReason.given

final case class ClubRefSkip(
  clubId: ClubId,
  reason: RefSkipReason,
  detail: Option[String],
  lastAttempted: Instant
) derives DbCodec

object ClubRefSkip {

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club_ref_skip (
              club_id        BIGINT PRIMARY KEY REFERENCES club (club_id),
              reason         VARCHAR NOT NULL,
              detail         VARCHAR,
              last_attempted TIMESTAMPTZ NOT NULL
            )""".update.run()
    }

  def upsert(item: ClubRefSkip): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club_ref_skip (club_id, reason, detail, last_attempted)
            VALUES (${item.clubId}, ${item.reason}, ${item.detail}, ${item.lastAttempted})
            ON CONFLICT (club_id) DO UPDATE SET
              reason = EXCLUDED.reason,
              detail = EXCLUDED.detail,
              last_attempted = EXCLUDED.last_attempted""".update.run()
    }

  def selectId(clubId: ClubId): ZIO[Transactor, SQLException, Option[ClubRefSkip]] =
    connectZIO {
      sql"SELECT club_id, reason, detail, last_attempted FROM club_ref_skip WHERE club_id = $clubId"
        .query[ClubRefSkip].run().headOption
    }

  def countByReason: ZIO[Transactor, SQLException, List[(RefSkipReason, Long)]] =
    connectZIO {
      sql"SELECT reason, COUNT(*) FROM club_ref_skip GROUP BY reason"
        .query[(RefSkipReason, Long)].run().toList
    }

  def deleteId(clubId: ClubId): ZIO[Transactor, SQLException, Int] =
    connectZIO(sql"DELETE FROM club_ref_skip WHERE club_id = $clubId".update.run())

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO(sql"DELETE FROM club_ref_skip".update.run())
}
