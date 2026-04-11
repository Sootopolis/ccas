package ccas.analysis.tables

import java.sql.SQLException
import java.time.{Duration, Instant}

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO, withTransaction}

final case class ClubAdmin(clubId: ClubId, playerId: PlayerId) derives DbCodec

object ClubAdmin {

  /** A club is considered inactive if its `latest_match_at` is older than this. Admins of inactive clubs are not
    * excluded from recruitment because they don't reflect meaningful ownership. NULL `latest_match_at` is treated as
    * active (we have no signal yet).
    */
  val MaxInactivity: Duration = Duration.ofDays(365)

  /** Single-run / cross-run optimisation in [[ccas.analysis.apps.clubdata.ClubDataApp]]: if a club's cached
    * `latest_match_at` (or fresh DB scan) is more recent than this, we trust it and skip the API fallback fetch.
    * Smaller than [[MaxInactivity]] so we still re-check before the cache value crosses the inactivity threshold.
    */
  val ApiSkipThreshold: Duration = Duration.ofDays(180)

  /** A club is considered over-administered if more than this fraction of its members are admins. Admins of such clubs
    * are not excluded from recruitment — the admin role there carries little ownership.
    */
  val MaxAdminRatio: Double = 0.20

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

  /** Atomically replaces all admins for a club: deletes existing rows and inserts the new set.
    *
    * The INSERT uses `ON CONFLICT DO NOTHING` to tolerate concurrent writers (e.g. two recruitment fibers late-checking
    * the same unknown club): under READ COMMITTED, two transactions can both see an empty admin set, both DELETE 0
    * rows, and then race on the INSERT — without the conflict guard, the second writer hits a duplicate-key violation.
    * In our use case all racing writers derive their admin set from the same `ApiClub` fetch, so silently merging is
    * safe; in pathological cases (different sets) the result is the union of both, which is acceptable since
    * `ClubDataApp` will reconcile on its next refresh.
    */
  def replaceForClub(clubId: ClubId, playerIds: Set[PlayerId]): ZIO[PostgresClient, Throwable, Unit] =
    withTransaction {
      deleteByClub(clubId) *>
        ZIO.whenDiscard(playerIds.nonEmpty) {
          connectZIO {
            batchUpdate(playerIds.map(pid => ClubAdmin(clubId, pid))) { item =>
              sql"""INSERT INTO club_admin (club_id, player_id) VALUES (${item.clubId}, ${item.playerId})
                    ON CONFLICT (club_id, player_id) DO NOTHING""".update
            }
          }
        }
    }

  def selectPlayerIdsForSizableClubs(minMembersCount: Int): ZIO[PostgresClient, SQLException, Set[PlayerId]] = {
    val cutoff = Instant.now().minus(MaxInactivity)
    connectZIO {
      sql"""WITH admin_counts AS (
              SELECT club_id, COUNT(*) AS n FROM club_admin GROUP BY club_id
            )
            SELECT DISTINCT ca.player_id
            FROM club_admin ca
            JOIN club c ON ca.club_id = c.club_id
            JOIN admin_counts ac ON ac.club_id = c.club_id
            WHERE c.members_count >= $minMembersCount
              AND (c.latest_match_at IS NULL OR c.latest_match_at > $cutoff)
              AND ac.n::float / c.members_count < $MaxAdminRatio"""
        .query[PlayerId].run().toSet
    }
  }

  /** Extracts admin usernames from the admin profile URLs in an [[ApiClub]] response. */
  def extractAdminUsernames(apiClub: ApiClub): Set[Username] =
    apiClub.admin.map(url => Username.wrap(url.path.segments.last)).toSet
}
