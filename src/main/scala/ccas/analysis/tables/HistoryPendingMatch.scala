package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubId, ClubMatchId}
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

final case class HistoryPendingMatch(clubId: ClubId, matchId: ClubMatchId) derives DbCodec

object HistoryPendingMatch {

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS history_pending_match (
              club_id    BIGINT NOT NULL REFERENCES club (club_id),
              match_id   BIGINT NOT NULL,
              PRIMARY KEY (club_id, match_id)
            )""".update.run()
    }

  def selectClub(clubId: ClubId): ZIO[Transactor, SQLException, List[ClubMatchId]] =
    connectZIO {
      sql"SELECT match_id FROM history_pending_match WHERE club_id = $clubId"
        .query[ClubMatchId].run().toList
    }

  def selectClubBatch(clubId: ClubId, limit: Int): ZIO[Transactor, SQLException, List[ClubMatchId]] =
    connectZIO {
      sql"SELECT match_id FROM history_pending_match WHERE club_id = $clubId LIMIT $limit"
        .query[ClubMatchId].run().toList
    }

  def insert(item: HistoryPendingMatch): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO history_pending_match (club_id, match_id)
            VALUES (${item.clubId}, ${item.matchId})
            ON CONFLICT DO NOTHING""".update.run()
    }

  def insertBatch(items: Iterable[HistoryPendingMatch]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO history_pending_match (club_id, match_id)
              VALUES (${item.clubId}, ${item.matchId})
              ON CONFLICT DO NOTHING""".update
      }
    }

  def delete(clubId: ClubId, matchId: ClubMatchId): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM history_pending_match WHERE club_id = $clubId AND match_id = $matchId".update.run()
    }

  def count(clubId: ClubId): ZIO[Transactor, SQLException, Long] =
    connectZIO {
      sql"SELECT COUNT(*) FROM history_pending_match WHERE club_id = $clubId"
        .query[Long].run().head
    }
}
