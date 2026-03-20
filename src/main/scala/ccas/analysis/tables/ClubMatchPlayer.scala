package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.GameResultDetail
import ccas.api.misc.subtypes.{ClubMatchId, PlayerId, Username}
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

final case class ClubMatchPlayer(
  matchId: ClubMatchId,
  playerId: PlayerId,
  teamIdx: Short,
  username: Username,
  board: Option[Int],
  playedAsWhite: Option[GameResultDetail],
  playedAsBlack: Option[GameResultDetail],
  scoreX2: Short,
  fairPlayRemoval: Boolean
) derives DbCodec

object ClubMatchPlayer {
  private val selectCols =
    SqlLiteral("match_id, player_id, team_idx, username, board, played_as_white, played_as_black, score_x2, fair_play_removal")

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club_match_player (
              match_id          BIGINT NOT NULL REFERENCES club_match (match_id) ON DELETE CASCADE,
              player_id         BIGINT NOT NULL REFERENCES player (player_id) ON DELETE RESTRICT,
              team_idx          SMALLINT NOT NULL,
              username          VARCHAR NOT NULL,
              board             INT,
              played_as_white   VARCHAR,
              played_as_black   VARCHAR,
              score_x2          SMALLINT NOT NULL,
              fair_play_removal BOOLEAN NOT NULL,
              PRIMARY KEY (match_id, player_id)
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_club_match_player_player ON club_match_player (player_id)""".update.run()
    }

  def selectMatch(matchId: ClubMatchId): ZIO[Transactor, SQLException, List[ClubMatchPlayer]] =
    connectZIO {
      sql"SELECT $selectCols FROM club_match_player WHERE match_id = $matchId"
        .query[ClubMatchPlayer].run().toList
    }

  def deleteMatch(matchId: ClubMatchId): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM club_match_player WHERE match_id = $matchId".update.run()
    }

  def insert(item: ClubMatchPlayer): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club_match_player (match_id, player_id, team_idx, username, board,
              played_as_white, played_as_black, score_x2, fair_play_removal)
            VALUES (${item.matchId}, ${item.playerId}, ${item.teamIdx}, ${item.username},
              ${item.board}, ${item.playedAsWhite.map(_.toString)}, ${item.playedAsBlack.map(_.toString)},
              ${item.scoreX2}, ${item.fairPlayRemoval})""".update.run()
    }

  def insertBatch(items: Iterable[ClubMatchPlayer]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO club_match_player (match_id, player_id, team_idx, username, board,
                played_as_white, played_as_black, score_x2, fair_play_removal)
              VALUES (${item.matchId}, ${item.playerId}, ${item.teamIdx}, ${item.username},
                ${item.board}, ${item.playedAsWhite.map(_.toString)}, ${item.playedAsBlack.map(_.toString)},
                ${item.scoreX2}, ${item.fairPlayRemoval})""".update
      }
    }
}
