package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubId, PlayerId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

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
