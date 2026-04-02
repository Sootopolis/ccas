package ccas.analysis.apps.stats

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.BoardGameWinner
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, PlayerId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

/** A single board from our club's perspective, with the team1/team2 orientation normalized so that "team1" = our player.
  *
  * For matches where the club was team2, game winners are flipped (`Team1<->Team2`) and score/fairplay columns are
  * swapped, so consumers can always treat `Team1` as "us" regardless of the original match orientation.
  */
final case class ClubBoard(
  matchId: ClubMatchId,
  endTime: Option[Instant],
  playerId: PlayerId,
  ourFairPlay: Boolean,
  oppFairPlay: Boolean,
  game1Winner: Option[BoardGameWinner],
  game2Winner: Option[BoardGameWinner]
) derives DbCodec

object ClubBoard {

  private val Finished = SqlLiteral("Finished")

  /** All finished boards for a club, normalized to our-team perspective. */
  def selectClubBoards(clubId: ClubId): ZIO[PostgresClient, SQLException, List[ClubBoard]] =
    connectZIO {
      sql"""SELECT b.match_id, cm.end_time, b.team1_player_id, b.team1_fair_play, b.team2_fair_play,
                   b.game1_winner, b.game2_winner
            FROM club_match_board b
            JOIN club_match cm ON cm.match_id = b.match_id
            WHERE cm.team1_club_id = $clubId AND cm.status = $Finished AND b.team1_player_id IS NOT NULL

            UNION ALL

            SELECT b.match_id, cm.end_time, b.team2_player_id, b.team2_fair_play, b.team1_fair_play,
                   CASE b.game1_winner WHEN 'Team1' THEN 'Team2' WHEN 'Team2' THEN 'Team1' ELSE b.game1_winner END,
                   CASE b.game2_winner WHEN 'Team1' THEN 'Team2' WHEN 'Team2' THEN 'Team1' ELSE b.game2_winner END
            FROM club_match_board b
            JOIN club_match cm ON cm.match_id = b.match_id
            WHERE cm.team2_club_id = $clubId AND cm.status = $Finished AND b.team2_player_id IS NOT NULL"""
        .query[ClubBoard].run().toList
    }

  /** Finished boards for a club within a date range, normalized to our-team perspective. */
  def selectClubBoardsInPeriod(
    clubId: ClubId,
    since: Instant,
    until: Instant
  ): ZIO[PostgresClient, SQLException, List[ClubBoard]] =
    connectZIO {
      sql"""SELECT b.match_id, cm.end_time, b.team1_player_id, b.team1_fair_play, b.team2_fair_play,
                   b.game1_winner, b.game2_winner
            FROM club_match_board b
            JOIN club_match cm ON cm.match_id = b.match_id
            WHERE cm.team1_club_id = $clubId AND cm.status = $Finished AND b.team1_player_id IS NOT NULL
              AND cm.end_time >= $since AND cm.end_time < $until

            UNION ALL

            SELECT b.match_id, cm.end_time, b.team2_player_id, b.team2_fair_play, b.team1_fair_play,
                   CASE b.game1_winner WHEN 'Team1' THEN 'Team2' WHEN 'Team2' THEN 'Team1' ELSE b.game1_winner END,
                   CASE b.game2_winner WHEN 'Team1' THEN 'Team2' WHEN 'Team2' THEN 'Team1' ELSE b.game2_winner END
            FROM club_match_board b
            JOIN club_match cm ON cm.match_id = b.match_id
            WHERE cm.team2_club_id = $clubId AND cm.status = $Finished AND b.team2_player_id IS NOT NULL
              AND cm.end_time >= $since AND cm.end_time < $until"""
        .query[ClubBoard].run().toList
    }
}
