package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubMatchId, Username}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

final case class UnresolvedBoardPlayer(
  matchId: ClubMatchId,
  board: Short,
  isTeam1: Boolean,
  username: Username
) derives DbCodec

object UnresolvedBoardPlayer {
  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS unresolved_board_player (
              match_id   BIGINT NOT NULL,
              board      SMALLINT NOT NULL,
              is_team1   BOOLEAN NOT NULL,
              username   TEXT NOT NULL,
              first_seen TIMESTAMPTZ NOT NULL,
              PRIMARY KEY (match_id, board, is_team1)
            )""".update.run()
    }

  def selectAll: ZIO[PostgresClient, SQLException, List[UnresolvedBoardPlayer]] =
    connectZIO {
      sql"SELECT match_id, board, is_team1, username FROM unresolved_board_player"
        .query[UnresolvedBoardPlayer].run().toList
    }

  def insert(
    matchId: ClubMatchId,
    board: Short,
    isTeam1: Boolean,
    username: Username
  ): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO unresolved_board_player (match_id, board, is_team1, username, first_seen)
            VALUES ($matchId, $board, $isTeam1, $username, ${Instant.now()})
            ON CONFLICT (match_id, board, is_team1) DO NOTHING""".update.run()
    }

  def delete(matchId: ClubMatchId, board: Short, isTeam1: Boolean): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM unresolved_board_player WHERE match_id = $matchId AND board = $board AND is_team1 = $isTeam1"
        .update.run()
    }
}
