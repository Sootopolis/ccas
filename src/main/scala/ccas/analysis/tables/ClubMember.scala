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

final case class ClubMember(
  clubId: ClubId,
  playerId: PlayerId,
  since: Instant,
  until: Option[Instant],
  sinceApproximate: Boolean
) derives DbCodec {
  def isCurrent: Boolean = until.isEmpty
}

object ClubMember {
  private val selectCols = SqlLiteral("club_id, player_id, since, until, since_approximate")

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club_member (
              club_id           BIGINT NOT NULL,
              player_id         BIGINT NOT NULL,
              since             TIMESTAMPTZ NOT NULL,
              until             TIMESTAMPTZ,
              since_approximate BOOLEAN NOT NULL,
              PRIMARY KEY (club_id, player_id, since),
              FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT,
              FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT
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

  def selectClubCurrent(clubId: ClubId): ZIO[PostgresClient, SQLException, List[ClubMember]] =
    connectZIO(
      sql"SELECT $selectCols FROM club_member WHERE club_id = $clubId AND until IS NULL".query[ClubMember].run().toList
    )

  def selectClubActive(clubId: ClubId): ZIO[PostgresClient, SQLException, List[ClubMember]] =
    connectZIO(
      sql"""SELECT cm.club_id, cm.player_id, cm.since, cm.until, cm.since_approximate FROM club_member cm
            JOIN player p ON p.player_id = cm.player_id AND p.status = ${Active: PlayerStatusCategory}
            WHERE cm.club_id = $clubId AND cm.until IS NULL""".query[ClubMember]
        .run().toList
    )

  def selectClubFormer(clubId: ClubId): ZIO[PostgresClient, SQLException, List[ClubMember]] =
    connectZIO(
      sql"SELECT $selectCols FROM club_member WHERE club_id = $clubId AND until IS NOT NULL".query[ClubMember].run()
        .toList
    )

  def insert(item: ClubMember): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club_member (club_id, player_id, since, until, since_approximate)
            VALUES (${item.clubId}, ${item.playerId}, ${item.since}, ${item.until},
              ${item.sinceApproximate})""".update.run()
    }

  def insertBatch(items: Iterable[ClubMember]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO club_member (club_id, player_id, since, until, since_approximate)
              VALUES (${item.clubId}, ${item.playerId}, ${item.since}, ${item.until},
                ${item.sinceApproximate})""".update
      }
    }

  def update(item: ClubMember): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""UPDATE club_member SET until = ${item.until}, since_approximate = ${item.sinceApproximate}
            WHERE club_id = ${item.clubId} AND player_id = ${item.playerId} AND since = ${item.since}""".update.run()
    }

  def updateBatch(items: Iterable[ClubMember]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""UPDATE club_member SET until = ${item.until}, since_approximate = ${item.sinceApproximate}
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

  def exists(clubId: ClubId, playerId: PlayerId): ZIO[PostgresClient, SQLException, Boolean] =
    connectZIO {
      sql"SELECT 1 FROM club_member WHERE club_id = $clubId AND player_id = $playerId LIMIT 1"
        .query[Int].run().nonEmpty
    }

  def replaceSince(
    clubId: ClubId,
    playerId: PlayerId,
    oldSince: Instant,
    newSince: Instant
  ): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""UPDATE club_member SET since = $newSince, since_approximate = false
            WHERE club_id = $clubId AND player_id = $playerId AND since = $oldSince
              AND since_approximate = true""".update.run()
    }
}
