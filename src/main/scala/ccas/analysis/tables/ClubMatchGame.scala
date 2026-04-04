package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.{BoardGameWinner, GameResultDetail}
import ccas.api.misc.subtypes.{ClubMatchId, Elo}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

final case class ClubMatchGame(
  matchId: ClubMatchId,
  board: Short,
  team1IsWhite: Boolean,
  gameId: Option[Long],
  startTime: Option[Long],
  endTime: Option[Long],
  winner: Option[BoardGameWinner],
  detail: Option[GameResultDetail],
  team1Rating: Option[Elo],
  team2Rating: Option[Elo]
) derives DbCodec

object ClubMatchGame {

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club_match_game (
              match_id         BIGINT NOT NULL,
              board            SMALLINT NOT NULL,
              team1_is_white   BOOLEAN NOT NULL,
              game_id          BIGINT,
              start_time       BIGINT,
              end_time         BIGINT,
              winner           TEXT,
              detail           TEXT,
              team1_rating     SMALLINT,
              team2_rating     SMALLINT,
              PRIMARY KEY (match_id, board, team1_is_white),
              FOREIGN KEY (match_id, board) REFERENCES club_match_board (match_id, board) ON DELETE CASCADE
            )""".update.run()
      sql"""CREATE UNIQUE INDEX IF NOT EXISTS idx_club_match_game_game_id
            ON club_match_game (game_id) WHERE game_id IS NOT NULL""".update.run()
    }

  def insertBatch(items: Iterable[ClubMatchGame]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO club_match_game (match_id, board, team1_is_white, game_id,
                start_time, end_time, winner, detail, team1_rating, team2_rating)
              VALUES (${item.matchId}, ${item.board}, ${item.team1IsWhite}, ${item.gameId},
                ${item.startTime}, ${item.endTime},
                ${item.winner}, ${item.detail},
                ${item.team1Rating}, ${item.team2Rating})""".update
      }
    }

  def selectMatch(matchId: ClubMatchId): ZIO[PostgresClient, SQLException, List[ClubMatchGame]] =
    connectZIO {
      sql"""SELECT match_id, board, team1_is_white, game_id, start_time, end_time,
                   winner, detail, team1_rating, team2_rating
            FROM club_match_game WHERE match_id = $matchId""".query[ClubMatchGame].run().toList
    }

  def deleteMatch(matchId: ClubMatchId): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM club_match_game WHERE match_id = $matchId".update.run()
    }
}
