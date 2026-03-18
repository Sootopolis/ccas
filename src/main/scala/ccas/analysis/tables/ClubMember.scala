package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{ClubId, PlayerId}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

final case class ClubMember(
  clubId: ClubId,
  playerId: PlayerId,
  since: Instant,
  until: Option[Instant])
    derives DbCodec {
  def isCurrent: Boolean = until.isEmpty
}

object ClubMember {
  private val selectCols = SqlLiteral("club_id, player_id, since, until")

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club_member (
              club_id   BIGINT NOT NULL,
              player_id BIGINT NOT NULL,
              since     TIMESTAMPTZ NOT NULL,
              until     TIMESTAMPTZ,
              PRIMARY KEY (club_id, player_id, since),
              FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT,
              FOREIGN KEY (player_id) REFERENCES player (player_id) ON DELETE RESTRICT
            )""".update.run()
      sql"""CREATE INDEX IF NOT EXISTS idx_club_member_player_id
            ON club_member (player_id)""".update.run()
    }

  def selectAll: ZIO[Transactor, SQLException, List[ClubMember]] =
    connectZIO(sql"SELECT $selectCols FROM club_member".query[ClubMember].run().toList)

  def selectClub(clubId: ClubId): ZIO[Transactor, SQLException, List[ClubMember]] =
    connectZIO(sql"SELECT $selectCols FROM club_member WHERE club_id = $clubId".query[ClubMember].run().toList)

  def selectClubCurrent(clubId: ClubId): ZIO[Transactor, SQLException, List[ClubMember]] =
    connectZIO(
      sql"SELECT $selectCols FROM club_member WHERE club_id = $clubId AND until IS NULL".query[ClubMember].run().toList
    )

  def selectClubActive(clubId: ClubId): ZIO[Transactor, SQLException, List[ClubMember]] =
    connectZIO(
      sql"""SELECT cm.club_id, cm.player_id, cm.since, cm.until FROM club_member cm
            JOIN player_snapshot ps ON cm.player_id = ps.player_id
            JOIN (SELECT player_id, MAX(since) AS since FROM player_snapshot GROUP BY player_id) latest
            ON ps.player_id = latest.player_id AND ps.since = latest.since
            WHERE cm.club_id = $clubId AND cm.until IS NULL AND ps.status = ${Active.toString}""".query[ClubMember]
        .run().toList
    )

  def selectClubFormer(clubId: ClubId): ZIO[Transactor, SQLException, List[ClubMember]] =
    connectZIO(
      sql"SELECT $selectCols FROM club_member WHERE club_id = $clubId AND until IS NOT NULL".query[ClubMember].run()
        .toList
    )

  def insert(item: ClubMember): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club_member (club_id, player_id, since, until)
            VALUES (${item.clubId}, ${item.playerId}, ${item.since}, ${item.until})""".update.run()
    }

  def insertBatch(items: Iterable[ClubMember]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""INSERT INTO club_member (club_id, player_id, since, until)
              VALUES (${item.clubId}, ${item.playerId}, ${item.since}, ${item.until})""".update
      }
    }

  def update(item: ClubMember): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""UPDATE club_member SET until = ${item.until}
            WHERE club_id = ${item.clubId} AND player_id = ${item.playerId} AND since = ${item.since}""".update.run()
    }

  def updateBatch(items: Iterable[ClubMember]): ZIO[Transactor, SQLException, BatchUpdateResult] =
    transactZIO {
      batchUpdate(items) { item =>
        sql"""UPDATE club_member SET until = ${item.until}
              WHERE club_id = ${item.clubId} AND player_id = ${item.playerId} AND since = ${item.since}""".update
      }
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM club_member".update.run()
    }
}
