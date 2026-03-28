package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

final case class BlacklistEntry(
  clubId: ClubId,
  playerId: PlayerId,
  username: Option[Username],
  addedAt: Instant,
  expiresAt: Option[Instant],
  reason: Option[String]
) derives DbCodec

final case class RecruitmentBlacklist(
  clubId: ClubId,
  playerId: PlayerId,
  addedAt: Instant,
  expiresAt: Option[Instant],
  reason: Option[String]
) derives DbCodec

object RecruitmentBlacklist {

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS recruitment_blacklist (
              club_id    BIGINT NOT NULL,
              player_id  BIGINT NOT NULL,
              added_at   TIMESTAMPTZ NOT NULL,
              expires_at TIMESTAMPTZ,
              reason     VARCHAR,
              PRIMARY KEY (club_id, player_id),
              FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT
            )""".update.run()
    }

  def insert(item: RecruitmentBlacklist): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO recruitment_blacklist (club_id, player_id, added_at, expires_at, reason)
            VALUES (${item.clubId}, ${item.playerId}, ${item.addedAt}, ${item.expiresAt}, ${item.reason})""".update.run()
    }

  def upsert(item: RecruitmentBlacklist): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO recruitment_blacklist (club_id, player_id, added_at, expires_at, reason)
            VALUES (${item.clubId}, ${item.playerId}, ${item.addedAt}, ${item.expiresAt}, ${item.reason})
            ON CONFLICT (club_id, player_id) DO UPDATE SET
              added_at = EXCLUDED.added_at,
              expires_at = EXCLUDED.expires_at,
              reason = EXCLUDED.reason""".update.run()
    }

  def delete(clubId: ClubId, playerId: PlayerId): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM recruitment_blacklist WHERE club_id = $clubId AND player_id = $playerId".update.run()
    }

  def selectByClub(clubId: ClubId): ZIO[Transactor, SQLException, List[RecruitmentBlacklist]] =
    connectZIO {
      sql"""SELECT club_id, player_id, added_at, expires_at, reason
            FROM recruitment_blacklist WHERE club_id = $clubId"""
        .query[RecruitmentBlacklist].run().toList
    }

  def selectActiveByClub(clubId: ClubId, now: Instant): ZIO[Transactor, SQLException, List[BlacklistEntry]] =
    connectZIO {
      sql"""SELECT rb.club_id, rb.player_id, ps.username, rb.added_at, rb.expires_at, rb.reason
            FROM recruitment_blacklist rb
            LEFT JOIN LATERAL (
              SELECT username FROM player_snapshot WHERE player_id = rb.player_id ORDER BY since DESC LIMIT 1
            ) ps ON true
            WHERE rb.club_id = $clubId
              AND (rb.expires_at IS NULL OR rb.expires_at > $now)
            ORDER BY rb.added_at DESC"""
        .query[BlacklistEntry].run().toList
    }

  def isBlacklisted(clubId: ClubId, playerId: PlayerId, now: Instant): ZIO[Transactor, SQLException, Boolean] =
    connectZIO {
      sql"""SELECT 1 FROM recruitment_blacklist
            WHERE club_id = $clubId AND player_id = $playerId
              AND (expires_at IS NULL OR expires_at > $now)"""
        .query[Int].run().nonEmpty
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM recruitment_blacklist".update.run()
    }
}
