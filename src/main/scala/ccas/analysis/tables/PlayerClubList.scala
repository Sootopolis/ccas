package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

/** A player's club list as fetched at `fetchedAt`: when they joined each club, by the slug the club had then. The
  * profile's last visit comes with it, and is empty when no list answered for the player under the name we held, which
  * leaves the list empty too. Kept for closed accounts, whose list still shows what a roster drops (ADR 0022).
  */
final case class PlayerClubList(
  playerId: PlayerId,
  fetchedAt: Instant,
  lastOnlineOption: Option[Instant],
  clubs: Map[ClubSlug, Instant]
)

object PlayerClubList {

  /** What a closed account's list says about one club: it was a member at closure, since `joined`. */
  final case class Listing(playerId: PlayerId, lastOnline: Instant, joined: Instant) derives DbCodec

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      sql"""CREATE TABLE IF NOT EXISTS player_club_list (
              player_id   BIGINT PRIMARY KEY REFERENCES player (player_id) ON DELETE RESTRICT,
              fetched_at  TIMESTAMPTZ NOT NULL,
              last_online TIMESTAMPTZ
            )""".update.run()
      sql"""CREATE TABLE IF NOT EXISTS player_club_list_entry (
              player_id BIGINT NOT NULL REFERENCES player_club_list (player_id) ON DELETE CASCADE,
              club_slug TEXT NOT NULL,
              joined    TIMESTAMPTZ NOT NULL,
              PRIMARY KEY (player_id, club_slug)
            )""".update.run()
    }

  /** Replaces whatever list was kept for the player. */
  def save(list: PlayerClubList): ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      val saved =
        sql"""INSERT INTO player_club_list (player_id, fetched_at, last_online)
              VALUES (${list.playerId}, ${list.fetchedAt}, ${list.lastOnlineOption})
              ON CONFLICT (player_id) DO UPDATE SET
                fetched_at = EXCLUDED.fetched_at, last_online = EXCLUDED.last_online""".update.run()
      sql"DELETE FROM player_club_list_entry WHERE player_id = ${list.playerId}".update.run()
      val entries = batchUpdate(list.clubs) { (slug, joined) =>
        sql"""INSERT INTO player_club_list_entry (player_id, club_slug, joined)
              VALUES (${list.playerId}, $slug, $joined)""".update
      }
      entries match {
        case BatchUpdateResult.Success(count) => saved + count.toInt
        case BatchUpdateResult.SuccessNoInfo  => saved
      }
    }

  /** Up to `limit` closed accounts that played for `clubId` or have a row in it, with no list kept since their state
    * last changed, latest change first: the sooner after closure, the likelier the name we hold still answers.
    */
  def selectToFetch(clubId: ClubId, limit: Int): ZIO[PostgresClient, SQLException, List[(PlayerId, Username)]] =
    connectZIO {
      sql"""SELECT p.player_id, pn.username FROM player p
            JOIN player_name pn ON pn.player_id = p.player_id AND pn.until IS NULL
            LEFT JOIN player_club_list l ON l.player_id = p.player_id
            WHERE p.status <> ${Active: PlayerStatusCategory} AND (l.player_id IS NULL OR l.fetched_at < p.since)
              AND (EXISTS (SELECT 1 FROM club_member cm WHERE cm.club_id = $clubId AND cm.player_id = p.player_id)
                OR EXISTS (SELECT 1 FROM club_match_board b JOIN club_match m ON m.match_id = b.match_id
                           WHERE (m.team1_club_id = $clubId AND b.team1_player_id = p.player_id)
                              OR (m.team2_club_id = $clubId AND b.team2_player_id = p.player_id)))
            ORDER BY p.since DESC, p.player_id LIMIT $limit""".query[(PlayerId, Username)].run().toList
    }

  /** What the lists kept for closed accounts say about `clubId`, matched on every slug it has held. A list older than
    * its player's latest state is left out: it may predate a reopening.
    */
  def selectListings(clubId: ClubId): ZIO[PostgresClient, SQLException, List[Listing]] =
    connectZIO {
      sql"""SELECT l.player_id, l.last_online, e.joined FROM player_club_list l
            JOIN player p ON p.player_id = l.player_id
            JOIN player_club_list_entry e ON e.player_id = l.player_id
            WHERE p.status <> ${Active: PlayerStatusCategory} AND l.fetched_at >= p.since
              AND e.club_slug IN (SELECT slug FROM club_name WHERE club_id = $clubId)""".query[Listing].run().toList
    }
}
