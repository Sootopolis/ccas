package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.{BoardGameWinner, GameResultDetail}
import ccas.api.misc.subtypes.{ClubMatchId, PlayerId}
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

final case class ClubMatchBoard(
  matchId: ClubMatchId,
  board: Short,
  team1PlayerId: Option[PlayerId],
  team1FairPlay: Boolean,
  team2PlayerId: Option[PlayerId],
  team2FairPlay: Boolean,
  game1Winner: Option[BoardGameWinner],
  game1Detail: Option[GameResultDetail],
  game2Winner: Option[BoardGameWinner],
  game2Detail: Option[GameResultDetail],
  team1ScoreX2: Short,
  team2ScoreX2: Short
) derives DbCodec

object ClubMatchBoard {
  private val selectCols = SqlLiteral(
    "match_id, board, team1_player_id, team1_fair_play, " +
      "team2_player_id, team2_fair_play, " +
      "game1_winner, game1_detail, game2_winner, game2_detail, team1_score_x2, team2_score_x2"
  )

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club_match_board (
              match_id         BIGINT NOT NULL REFERENCES club_match (match_id) ON DELETE CASCADE,
              board            SMALLINT NOT NULL,
              team1_player_id  BIGINT REFERENCES player (player_id) ON DELETE RESTRICT,
              team1_fair_play  BOOLEAN NOT NULL,
              team2_player_id  BIGINT REFERENCES player (player_id) ON DELETE RESTRICT,
              team2_fair_play  BOOLEAN NOT NULL,
              game1_winner     TEXT,
              game1_detail     TEXT,
              game2_winner     TEXT,
              game2_detail     TEXT,
              team1_score_x2   SMALLINT NOT NULL,
              team2_score_x2   SMALLINT NOT NULL,
              PRIMARY KEY (match_id, board)
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_club_match_board_team1_player ON club_match_board (team1_player_id)""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_club_match_board_team2_player ON club_match_board (team2_player_id)""".update.run()
    }

  def selectMatch(matchId: ClubMatchId): ZIO[Transactor, SQLException, List[ClubMatchBoard]] =
    connectZIO {
      sql"SELECT $selectCols FROM club_match_board WHERE match_id = $matchId"
        .query[ClubMatchBoard].run().toList
    }

  def selectPlayerMatchRef(playerId: PlayerId): ZIO[Transactor, SQLException, Option[PlayerMatchRef]] =
    connectZIO {
      sql"""SELECT match_id, board AS board_idx, (team1_player_id = $playerId) AS is_team1
            FROM club_match_board
            WHERE team1_player_id = $playerId OR team2_player_id = $playerId
            LIMIT 1""".query[(ClubMatchId, Short, Boolean)].run().headOption.map { case (matchId, boardIdx, isTeam1) =>
        PlayerMatchRef(playerId, matchId, isLive = false, isTeam1, boardIdx)
      }
    }

  def insert(item: ClubMatchBoard): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club_match_board (match_id, board, team1_player_id, team1_fair_play,
              team2_player_id, team2_fair_play,
              game1_winner, game1_detail, game2_winner, game2_detail,
              team1_score_x2, team2_score_x2)
            VALUES (${item.matchId}, ${item.board}, ${item.team1PlayerId},
              ${item.team1FairPlay}, ${item.team2PlayerId}, ${item.team2FairPlay},
              ${item.game1Winner.map(_.toString)}, ${item.game1Detail.map(_.toString)},
              ${item.game2Winner.map(_.toString)}, ${item.game2Detail.map(_.toString)},
              ${item.team1ScoreX2}, ${item.team2ScoreX2})""".update.run()
    }

  def insertBatch(items: Iterable[ClubMatchBoard]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO club_match_board (match_id, board, team1_player_id, team1_fair_play,
                team2_player_id, team2_fair_play,
                game1_winner, game1_detail, game2_winner, game2_detail,
                team1_score_x2, team2_score_x2)
              VALUES (${item.matchId}, ${item.board}, ${item.team1PlayerId},
                ${item.team1FairPlay}, ${item.team2PlayerId}, ${item.team2FairPlay},
                ${item.game1Winner.map(_.toString)}, ${item.game1Detail.map(_.toString)},
                ${item.game2Winner.map(_.toString)}, ${item.game2Detail.map(_.toString)},
                ${item.team1ScoreX2}, ${item.team2ScoreX2})""".update
      }
    }

  def updatePlayerId(
    matchId: ClubMatchId,
    board: Short,
    isTeam1: Boolean,
    playerId: PlayerId
  ): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      if (isTeam1) {
        sql"UPDATE club_match_board SET team1_player_id = $playerId WHERE match_id = $matchId AND board = $board"
          .update.run()
      } else {
        sql"UPDATE club_match_board SET team2_player_id = $playerId WHERE match_id = $matchId AND board = $board"
          .update.run()
      }
    }

  def deleteMatch(matchId: ClubMatchId): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM club_match_board WHERE match_id = $matchId".update.run()
    }
}
