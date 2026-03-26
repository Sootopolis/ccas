package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubMatchId, Username}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

object UnresolvedBoardPlayer {
  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS unresolved_board_player (
              match_id   BIGINT NOT NULL,
              board      INT NOT NULL,
              is_team1   BOOLEAN NOT NULL,
              username   VARCHAR NOT NULL,
              first_seen TIMESTAMPTZ NOT NULL,
              PRIMARY KEY (match_id, board, is_team1)
            )""".update.run()
    }

  def insert(matchId: ClubMatchId, board: Int, isTeam1: Boolean, username: Username): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO unresolved_board_player (match_id, board, is_team1, username, first_seen)
            VALUES ($matchId, $board, $isTeam1, $username, ${Instant.now()})
            ON CONFLICT (match_id, board, is_team1) DO NOTHING""".update.run()
    }

  def selectAll: ZIO[Transactor, SQLException, List[(ClubMatchId, Int, Boolean, Username)]] =
    connectZIO {
      sql"SELECT match_id, board, is_team1, username FROM unresolved_board_player"
        .query[(ClubMatchId, Int, Boolean, Username)].run().toList
    }

  def delete(matchId: ClubMatchId, board: Int, isTeam1: Boolean): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM unresolved_board_player WHERE match_id = $matchId AND board = $board AND is_team1 = $isTeam1"
        .update.run()
    }
}
