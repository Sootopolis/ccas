package ccas.analysis.tables

import java.sql.SQLException

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO, withTransaction}

final case class ClubAdmin(clubId: ClubId, playerId: PlayerId) derives DbCodec

object ClubAdmin {

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club_admin (
              club_id    BIGINT NOT NULL REFERENCES club (club_id) ON DELETE RESTRICT,
              player_id  BIGINT NOT NULL REFERENCES player (player_id) ON DELETE RESTRICT,
              PRIMARY KEY (club_id, player_id)
            )""".update.run()
    }

  def selectByClub(clubId: ClubId): ZIO[PostgresClient, SQLException, List[ClubAdmin]] =
    connectZIO {
      sql"SELECT club_id, player_id FROM club_admin WHERE club_id = $clubId"
        .query[ClubAdmin].run().toList
    }

  def selectPlayerIdsByClub(clubId: ClubId): ZIO[PostgresClient, SQLException, Set[PlayerId]] =
    connectZIO {
      sql"SELECT player_id FROM club_admin WHERE club_id = $clubId"
        .query[PlayerId].run().toSet
    }

  def deleteByClub(clubId: ClubId): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM club_admin WHERE club_id = $clubId".update.run()
    }

  def insertBatch(items: Iterable[ClubAdmin]): ZIO[PostgresClient, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO club_admin (club_id, player_id) VALUES (${item.clubId}, ${item.playerId})""".update
      }
    }

  /** Atomically replaces all admins for a club: deletes existing rows and inserts the new set. */
  def replaceForClub(clubId: ClubId, playerIds: Set[PlayerId]): ZIO[PostgresClient, Throwable, Unit] =
    withTransaction {
      deleteByClub(clubId) *>
        ZIO.whenDiscard(playerIds.nonEmpty) {
          connectZIO {
            batchUpdate(playerIds.map(pid => ClubAdmin(clubId, pid))) { item =>
              sql"INSERT INTO club_admin (club_id, player_id) VALUES (${item.clubId}, ${item.playerId})".update
            }
          }
        }
    }

  def selectPlayerIdsForSizableClubs(minMembersCount: Int): ZIO[PostgresClient, SQLException, Set[PlayerId]] =
    connectZIO {
      sql"""SELECT DISTINCT ca.player_id
            FROM club_admin ca
            JOIN club c ON ca.club_id = c.club_id
            WHERE c.members_count >= $minMembersCount"""
        .query[PlayerId].run().toSet
    }

  /** Extracts admin usernames from the admin profile URLs in an [[ApiClub]] response. */
  def extractAdminUsernames(apiClub: ApiClub): Set[Username] =
    apiClub.admin.map(url => Username.wrap(url.path.segments.last)).toSet
}
