package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubId, PlayerId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

final case class HistoryMemberQuery(clubId: ClubId, playerId: PlayerId, queriedAt: Instant) derives DbCodec

object HistoryMemberQuery {

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS history_member_query (
              club_id    BIGINT NOT NULL REFERENCES club (club_id) ON DELETE RESTRICT,
              player_id  BIGINT NOT NULL REFERENCES player (player_id) ON DELETE RESTRICT,
              queried_at TIMESTAMPTZ NOT NULL,
              PRIMARY KEY (club_id, player_id)
            )""".update.run()
    }

  def selectClubPlayerIds(clubId: ClubId): ZIO[Transactor, SQLException, Set[PlayerId]] =
    connectZIO {
      sql"SELECT player_id FROM history_member_query WHERE club_id = $clubId"
        .query[PlayerId].run().toSet
    }

  def upsert(item: HistoryMemberQuery): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO history_member_query (club_id, player_id, queried_at)
            VALUES (${item.clubId}, ${item.playerId}, ${item.queriedAt})
            ON CONFLICT (club_id, player_id) DO UPDATE SET queried_at = EXCLUDED.queried_at""".update.run()
    }

  def deleteClub(clubId: ClubId): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM history_member_query WHERE club_id = $clubId".update.run()
    }
}
