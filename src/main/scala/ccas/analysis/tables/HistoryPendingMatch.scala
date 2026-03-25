package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubId, ClubMatchId}
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

final case class HistoryPendingMatch(clubId: ClubId, matchId: ClubMatchId, isLive: Boolean) derives DbCodec

object HistoryPendingMatch {

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      // Transient work queue — PK includes is_live to distinguish daily/live match IDs.
      sql"""CREATE TABLE IF NOT EXISTS history_pending_match (
              club_id    BIGINT NOT NULL REFERENCES club (club_id),
              match_id   BIGINT NOT NULL,
              is_live    BOOLEAN NOT NULL DEFAULT false,
              PRIMARY KEY (club_id, match_id, is_live)
            )""".update.run()
      sql"""ALTER TABLE history_pending_match ADD COLUMN IF NOT EXISTS is_live BOOLEAN NOT NULL DEFAULT false""".update.run()
    }

  def selectClub(clubId: ClubId): ZIO[Transactor, SQLException, List[HistoryPendingMatch]] =
    connectZIO {
      sql"SELECT club_id, match_id, is_live FROM history_pending_match WHERE club_id = $clubId"
        .query[HistoryPendingMatch].run().toList
    }

  def selectClubBatch(clubId: ClubId, limit: Int): ZIO[Transactor, SQLException, List[HistoryPendingMatch]] =
    connectZIO {
      sql"SELECT club_id, match_id, is_live FROM history_pending_match WHERE club_id = $clubId LIMIT $limit"
        .query[HistoryPendingMatch].run().toList
    }

  def insert(item: HistoryPendingMatch): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO history_pending_match (club_id, match_id, is_live)
            VALUES (${item.clubId}, ${item.matchId}, ${item.isLive})
            ON CONFLICT DO NOTHING""".update.run()
    }

  def insertBatch(items: Iterable[HistoryPendingMatch]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO history_pending_match (club_id, match_id, is_live)
              VALUES (${item.clubId}, ${item.matchId}, ${item.isLive})
              ON CONFLICT DO NOTHING""".update
      }
    }

  def delete(clubId: ClubId, matchId: ClubMatchId, isLive: Boolean): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM history_pending_match WHERE club_id = $clubId AND match_id = $matchId AND is_live = $isLive"
        .update.run()
    }

  def count(clubId: ClubId): ZIO[Transactor, SQLException, Long] =
    connectZIO {
      sql"SELECT COUNT(*) FROM history_pending_match WHERE club_id = $clubId"
        .query[Long].run().head
    }
}
