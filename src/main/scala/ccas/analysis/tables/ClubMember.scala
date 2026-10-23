package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{ClubId, PlayerId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

/** One period a player was a member of a club. The player was known to be in at `lastSeen` and known to be out at
  * `until` (ADR 0021). Both are empty while the club's roster lists them, and only `until` while a closed account hides
  * them from it (ADR 0022).
  */
final case class ClubMember(
  clubId: ClubId,
  playerId: PlayerId,
  since: Instant,
  lastSeen: Option[Instant],
  until: Option[Instant]
) derives DbCodec {

  /** This membership hidden by a closed account, last known in at `lastSeen`. */
  def hidden(lastSeen: Instant): ClubMember = copy(lastSeen = Some(lastSeen), until = None)
}

object ClubMember {
  private val selectCols = SqlLiteral("club_id, player_id, since, last_seen, until")

  /** The row of a member who joined at `since`, by Chess.com's account. */
  def joined(clubId: ClubId, playerId: PlayerId, since: Instant): ClubMember =
    ClubMember(
      clubId = clubId,
      playerId = playerId,
      since = since,
      lastSeen = None,
      until = None
    )

  /** Fails with a pointer to the runbook when `btree_gist` is missing: see [[BtreeGist]]. */
  def createTable: ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      BtreeGist.require("club_member")
      sql"""CREATE TABLE IF NOT EXISTS club_member (
              club_id           BIGINT NOT NULL,
              player_id         BIGINT NOT NULL,
              since             TIMESTAMPTZ NOT NULL,
              last_seen         TIMESTAMPTZ,
              until             TIMESTAMPTZ,
              PRIMARY KEY (club_id, player_id, since),
              FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT,
              FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT,
              CONSTRAINT club_member_window
                CHECK ((until IS NULL OR last_seen IS NOT NULL) AND since <= last_seen AND last_seen <= until),
              CONSTRAINT club_member_no_overlap
                EXCLUDE USING gist (club_id WITH =, player_id WITH =, tstzrange(since, until) WITH &&)
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_club_member_player_id
            ON club_member (player_id)""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_club_member_current
            ON club_member (club_id) WHERE until IS NULL""".update.run()
    }

  def selectAll: ZIO[PostgresClient, SQLException, List[ClubMember]] =
    connectZIO(sql"SELECT $selectCols FROM club_member".query[ClubMember].run().toList)

  def selectClub(clubId: ClubId): ZIO[PostgresClient, SQLException, List[ClubMember]] =
    connectZIO(sql"SELECT $selectCols FROM club_member WHERE club_id = $clubId".query[ClubMember].run().toList)

  /** The rows the club's roster should list: every open one but those a closure hid while the account stays closed. */
  def selectClubListed(clubId: ClubId): ZIO[PostgresClient, SQLException, List[ClubMember]] =
    connectZIO(
      sql"""SELECT cm.club_id, cm.player_id, cm.since, cm.last_seen, cm.until FROM club_member cm
            JOIN player p ON p.player_id = cm.player_id
            WHERE cm.club_id = $clubId AND cm.until IS NULL
              AND (cm.last_seen IS NULL OR p.status = ${Active: PlayerStatusCategory})""".query[ClubMember].run().toList
    )

  def selectClubActive(clubId: ClubId): ZIO[PostgresClient, SQLException, List[ClubMember]] =
    connectZIO(
      sql"""SELECT cm.club_id, cm.player_id, cm.since, cm.last_seen, cm.until FROM club_member cm
            JOIN player p ON p.player_id = cm.player_id AND p.status = ${Active: PlayerStatusCategory}
            WHERE cm.club_id = $clubId AND cm.until IS NULL""".query[ClubMember]
        .run().toList
    )

  def insert(item: ClubMember): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club_member (club_id, player_id, since, last_seen, until)
            VALUES (${item.clubId}, ${item.playerId}, ${item.since}, ${item.lastSeen}, ${item.until})""".update.run()
    }

  def insertBatch(items: Iterable[ClubMember]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO club_member (club_id, player_id, since, last_seen, until)
              VALUES (${item.clubId}, ${item.playerId}, ${item.since}, ${item.lastSeen}, ${item.until})""".update
      }
    }

  def update(item: ClubMember): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""UPDATE club_member
            SET last_seen = ${item.lastSeen}, until = ${item.until}
            WHERE club_id = ${item.clubId} AND player_id = ${item.playerId} AND since = ${item.since}""".update.run()
    }

  def updateBatch(items: Iterable[ClubMember]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""UPDATE club_member
              SET last_seen = ${item.lastSeen}, until = ${item.until}
              WHERE club_id = ${item.clubId} AND player_id = ${item.playerId} AND since = ${item.since}""".update
      }
    }

  def selectPlayerIdsCurrentAt(clubId: ClubId, at: Instant): ZIO[PostgresClient, SQLException, Set[PlayerId]] =
    connectZIO {
      sql"""SELECT player_id FROM club_member
            WHERE club_id = $clubId AND since <= $at AND (until IS NULL OR until > $at)"""
        .query[PlayerId].run().toSet
    }

  def countActiveCurrentAt(clubId: ClubId, at: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""SELECT COUNT(*)::INT FROM club_member cm
            JOIN player p ON p.player_id = cm.player_id AND p.status = ${Active: PlayerStatusCategory}
            WHERE cm.club_id = $clubId AND cm.since <= $at AND (cm.until IS NULL OR cm.until > $at)"""
        .query[Int].run().head
    }

  /** The members at `at`, by the rows alone: a hidden row counts until its `lastSeen`, when its closure is dated, so a
    * count changes only where a row records why.
    */
  def countAt(clubId: ClubId, at: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""SELECT COUNT(*)::INT FROM club_member
            WHERE club_id = $clubId AND since <= $at
              AND (COALESCE(until, last_seen) IS NULL OR COALESCE(until, last_seen) > $at)""".query[Int].run().head
    }
}
