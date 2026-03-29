package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubId, ClubMatchId}
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

import PendingMatchStatus.given

final case class HistoryPendingMatch(
  clubId: ClubId,
  matchId: ClubMatchId,
  isLive: Boolean,
  status: PendingMatchStatus = PendingMatchStatus.New
) derives DbCodec

object HistoryPendingMatch {

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      // Transient work queue — PK includes is_live to distinguish daily/live match IDs.
      sql"""CREATE TABLE IF NOT EXISTS history_pending_match (
              club_id    BIGINT NOT NULL REFERENCES club (club_id),
              match_id   BIGINT NOT NULL,
              is_live    BOOLEAN NOT NULL,
              status     VARCHAR NOT NULL,
              PRIMARY KEY (club_id, match_id, is_live)
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_history_pending_new
            ON history_pending_match (club_id) WHERE status = 'New'""".update.run()
    }

  def selectClub(clubId: ClubId): ZIO[Transactor, SQLException, List[HistoryPendingMatch]] =
    connectZIO {
      sql"SELECT club_id, match_id, is_live, status FROM history_pending_match WHERE club_id = $clubId"
        .query[HistoryPendingMatch].run().toList
    }

  def selectClubBatch(clubId: ClubId, limit: Int): ZIO[Transactor, SQLException, List[HistoryPendingMatch]] =
    connectZIO {
      val status = PendingMatchStatus.New
      sql"""SELECT club_id, match_id, is_live, status FROM history_pending_match
            WHERE club_id = $clubId AND status = $status LIMIT $limit"""
        .query[HistoryPendingMatch].run().toList
    }

  def insert(item: HistoryPendingMatch): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO history_pending_match (club_id, match_id, is_live, status)
            VALUES (${item.clubId}, ${item.matchId}, ${item.isLive}, ${item.status})
            ON CONFLICT DO NOTHING""".update.run()
    }

  def insertBatch(items: Iterable[HistoryPendingMatch]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO history_pending_match (club_id, match_id, is_live, status)
              VALUES (${item.clubId}, ${item.matchId}, ${item.isLive}, ${item.status})
              ON CONFLICT DO NOTHING""".update
      }
    }

  def delete(clubId: ClubId, matchId: ClubMatchId, isLive: Boolean): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM history_pending_match WHERE club_id = $clubId AND match_id = $matchId AND is_live = $isLive"
        .update.run()
    }

  def updateStatus(
    clubId: ClubId,
    matchId: ClubMatchId,
    isLive: Boolean,
    status: PendingMatchStatus
  ): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""UPDATE history_pending_match SET status = $status
            WHERE club_id = $clubId AND match_id = $matchId AND is_live = $isLive""".update.run()
    }

  def resetStatuses(clubId: ClubId): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      val target = PendingMatchStatus.New
      sql"""UPDATE history_pending_match SET status = $target
            WHERE club_id = $clubId AND status != $target""".update.run()
    }

  def count(clubId: ClubId): ZIO[Transactor, SQLException, Long] =
    connectZIO {
      sql"SELECT COUNT(*) FROM history_pending_match WHERE club_id = $clubId"
        .query[Long].run().head
    }

  def countNew(clubId: ClubId): ZIO[Transactor, SQLException, Long] =
    connectZIO {
      val status = PendingMatchStatus.New
      sql"SELECT COUNT(*) FROM history_pending_match WHERE club_id = $clubId AND status = $status"
        .query[Long].run().head
    }
}
