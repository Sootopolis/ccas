package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.TimeClass
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, PlayerId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

final case class ClubMatchBoard(
  matchId: ClubMatchId,
  board: Short,
  @SqlName("team1_player_id") team1PlayerIdOption: Option[PlayerId],
  team1FairPlay: Boolean,
  @SqlName("team2_player_id") team2PlayerIdOption: Option[PlayerId],
  team2FairPlay: Boolean,
  team1ScoreX2: Short,
  team2ScoreX2: Short
) derives DbCodec

object ClubMatchBoard {

  /** A player on a board of one of a club's matches when it started, on the club's side or against it. */
  final case class Sighting(playerId: PlayerId, startTime: Instant, forClub: Boolean) derives DbCodec

  private val selectCols = SqlLiteral(
    "match_id, board, team1_player_id, team1_fair_play, " +
      "team2_player_id, team2_fair_play, team1_score_x2, team2_score_x2"
  )

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      sql"""CREATE TABLE IF NOT EXISTS club_match_board (
              match_id         BIGINT NOT NULL REFERENCES club_match (match_id) ON DELETE CASCADE,
              board            SMALLINT NOT NULL,
              team1_player_id  BIGINT REFERENCES player (player_id) ON DELETE RESTRICT,
              team1_fair_play  BOOLEAN NOT NULL,
              team2_player_id  BIGINT REFERENCES player (player_id) ON DELETE RESTRICT,
              team2_fair_play  BOOLEAN NOT NULL,
              team1_score_x2   SMALLINT NOT NULL,
              team2_score_x2   SMALLINT NOT NULL,
              PRIMARY KEY (match_id, board)
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_club_match_board_team1_player ON club_match_board (team1_player_id)""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_club_match_board_team2_player ON club_match_board (team2_player_id)""".update.run()
    }

  def selectMatch(matchId: ClubMatchId): ZIO[PostgresClient, SQLException, List[ClubMatchBoard]] =
    connectZIO {
      sql"SELECT $selectCols FROM club_match_board WHERE match_id = $matchId"
        .query[ClubMatchBoard].run().toList
    }

  /** Infers a [[PlayerMatchRef]] value by joining `club_match_board` with `club_match` (to read the match's
    * `time_class` for the `isLive` field). Does **not** query the `player_match_ref` table. Used as the synthesis tier
    * beneath `PlayerMatchRef.findOrInfer`; prefer that helper unless you explicitly need to skip the explicit ref table
    * (e.g., when operating on `UnresolvedPlayer` inputs where tier 1 is empty by definition).
    */
  def inferPlayerMatchRef(playerId: PlayerId): ZIO[PostgresClient, SQLException, Option[PlayerMatchRef]] =
    connectZIO {
      val daily: TimeClass = TimeClass.Daily
      sql"""SELECT cmb.match_id, cmb.board AS board_idx, (cmb.team1_player_id = $playerId) AS is_team1,
                   (cm.time_class != $daily) AS is_live
            FROM club_match_board cmb
            JOIN club_match cm ON cm.match_id = cmb.match_id
            WHERE cmb.team1_player_id = $playerId OR cmb.team2_player_id = $playerId
            LIMIT 1""".query[(ClubMatchId, Short, Boolean, Boolean)].run().headOption.map {
        case (matchId, boardIdx, isTeam1, isLive) =>
          PlayerMatchRef(playerId, matchId, isLive, isTeam1, boardIdx)
      }
    }

  /** Every player who sat on `clubId`'s side of a board. */
  def selectPlayerIdsForClub(clubId: ClubId): ZIO[PostgresClient, SQLException, Set[PlayerId]] =
    connectZIO {
      sql"""SELECT b.team1_player_id FROM club_match_board b JOIN club_match m ON m.match_id = b.match_id
            WHERE m.team1_club_id = $clubId AND b.team1_player_id IS NOT NULL
            UNION
            SELECT b.team2_player_id FROM club_match_board b JOIN club_match m ON m.match_id = b.match_id
            WHERE m.team2_club_id = $clubId AND b.team2_player_id IS NOT NULL""".query[PlayerId].run().toSet
    }

  /** Both players of every board of `clubId`'s started matches, for each player who sat on its side of one: no other
    * player's matches can show a membership. The other team's club can be unresolved.
    */
  def selectSightings(clubId: ClubId): ZIO[PostgresClient, SQLException, List[Sighting]] =
    connectZIO {
      sql"""WITH sighting AS (
              SELECT b.team1_player_id AS player_id, m.start_time,
                m.team1_club_id IS NOT DISTINCT FROM $clubId AS for_club
              FROM club_match_board b JOIN club_match m ON m.match_id = b.match_id
              WHERE (m.team1_club_id = $clubId OR m.team2_club_id = $clubId)
                AND m.start_time IS NOT NULL AND b.team1_player_id IS NOT NULL
              UNION ALL
              SELECT b.team2_player_id, m.start_time, m.team2_club_id IS NOT DISTINCT FROM $clubId
              FROM club_match_board b JOIN club_match m ON m.match_id = b.match_id
              WHERE (m.team1_club_id = $clubId OR m.team2_club_id = $clubId)
                AND m.start_time IS NOT NULL AND b.team2_player_id IS NOT NULL
            )
            SELECT player_id, start_time, for_club FROM sighting
            WHERE player_id IN (SELECT player_id FROM sighting WHERE for_club)""".query[Sighting].run().toList
    }

  def insert(item: ClubMatchBoard): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club_match_board (match_id, board, team1_player_id, team1_fair_play,
              team2_player_id, team2_fair_play, team1_score_x2, team2_score_x2)
            VALUES (${item.matchId}, ${item.board}, ${item.team1PlayerIdOption},
              ${item.team1FairPlay}, ${item.team2PlayerIdOption}, ${item.team2FairPlay},
              ${item.team1ScoreX2}, ${item.team2ScoreX2})""".update.run()
    }

  def insertBatch(items: Iterable[ClubMatchBoard]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO club_match_board (match_id, board, team1_player_id, team1_fair_play,
                team2_player_id, team2_fair_play, team1_score_x2, team2_score_x2)
              VALUES (${item.matchId}, ${item.board}, ${item.team1PlayerIdOption},
                ${item.team1FairPlay}, ${item.team2PlayerIdOption}, ${item.team2FairPlay},
                ${item.team1ScoreX2}, ${item.team2ScoreX2})""".update
      }
    }

  def updatePlayerId(
    matchId: ClubMatchId,
    board: Short,
    isTeam1: Boolean,
    playerId: PlayerId
  ): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      if (isTeam1) {
        sql"UPDATE club_match_board SET team1_player_id = $playerId WHERE match_id = $matchId AND board = $board"
          .update.run()
      } else {
        sql"UPDATE club_match_board SET team2_player_id = $playerId WHERE match_id = $matchId AND board = $board"
          .update.run()
      }
    }

  def deleteMatch(matchId: ClubMatchId): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM club_match_board WHERE match_id = $matchId".update.run()
    }
}
