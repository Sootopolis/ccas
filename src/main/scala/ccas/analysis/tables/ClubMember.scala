package ccas.analysis.tables

import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{ClubId, PlayerId}
import ccas.utils.sql.SqlZioTypes.SqlTask
import ccas.utils.sql.SqlRepoUtils
import ccas.utils.sql.DbCodecs.given
import com.augustnagro.magnum.*
import zio.ZIO

import java.sql.SQLException
import java.time.Instant

final case class ClubMember(
  clubId  : ClubId,
  playerId: PlayerId,
  since   : Instant,
  until   : Option[Instant],
) derives DbCodec {
  def isCurrent: Boolean = until.isEmpty
}

object ClubMember extends SqlRepoUtils {
  override protected type Repo = ClubMemberRepository

  override protected def makeRepo(xa: Transactor): Repo = ClubMemberRepository(xa)

  def selectAll: RepoTask[List[ClubMember]] = repoService(_.selectAll)
  def selectClub(clubId: ClubId): RepoTask[List[ClubMember]] = repoService(_.selectClub(clubId))
  def selectClubCurrent(clubId: ClubId): RepoTask[List[ClubMember]] = repoService(_.selectClubCurrent(clubId))
  def selectClubActive(clubId: ClubId): RepoTask[List[ClubMember]] = repoService(_.selectClubActive(clubId))
  def selectClubFormer(clubId: ClubId): RepoTask[List[ClubMember]] = repoService(_.selectClubFormer(clubId))
  def insert(item: ClubMember): RepoTask[Unit] = repoService(_.insert(item))
  def insertBatch(items: Iterable[ClubMember]): RepoTask[Unit] = repoService(_.insertBatch(items))
  def update(item: ClubMember): RepoTask[Unit] = repoService(_.update(item))
  def updateBatch(items: Iterable[ClubMember]): RepoTask[Unit] = repoService(_.updateBatch(items))
  def deleteAll: RepoTask[Unit] = repoService(_.deleteAll)

  private val selectCols = "club_id, player_id, since, until"

  case class ClubMemberRepository(xa: Transactor) {
    def selectAll: SqlTask[List[ClubMember]] =
      ZIO.attempt { connect(xa)(sql"SELECT #$selectCols FROM club_member".query[ClubMember].run().toList) }
        .refineToOrDie[SQLException]

    def selectClub(clubId: ClubId): SqlTask[List[ClubMember]] =
      ZIO.attempt { connect(xa)(sql"SELECT #$selectCols FROM club_member WHERE club_id = $clubId".query[ClubMember].run().toList) }
        .refineToOrDie[SQLException]

    def selectClubCurrent(clubId: ClubId): SqlTask[List[ClubMember]] =
      ZIO.attempt { connect(xa)(sql"SELECT #$selectCols FROM club_member WHERE club_id = $clubId AND until IS NULL".query[ClubMember].run().toList) }
        .refineToOrDie[SQLException]

    def selectClubActive(clubId: ClubId): SqlTask[List[ClubMember]] =
      ZIO.attempt {
        connect(xa) {
          sql"""SELECT cm.club_id, cm.player_id, cm.since, cm.until FROM club_member cm
                JOIN player_snapshot ps ON cm.player_id = ps.player_id
                JOIN (SELECT player_id, MAX(since) AS since FROM player_snapshot GROUP BY player_id) latest
                ON ps.player_id = latest.player_id AND ps.since = latest.since
                WHERE cm.club_id = $clubId AND cm.until IS NULL AND ps.status = ${Active.toString}""".query[ClubMember].run().toList
        }
      }.refineToOrDie[SQLException]

    def selectClubFormer(clubId: ClubId): SqlTask[List[ClubMember]] =
      ZIO.attempt { connect(xa)(sql"SELECT #$selectCols FROM club_member WHERE club_id = $clubId AND until IS NOT NULL".query[ClubMember].run().toList) }
        .refineToOrDie[SQLException]

    def insert(item: ClubMember): SqlTask[Unit] =
      ZIO.attempt {
        connect(xa)(sql"""INSERT INTO club_member (club_id, player_id, since, until)
              VALUES (${item.clubId}, ${item.playerId}, ${item.since}, ${item.until})""".update.run())
      }.refineToOrDie[SQLException].unit

    def insertBatch(items: Iterable[ClubMember]): SqlTask[Unit] =
      ZIO.attempt {
        transact(xa) {
          batchUpdate(items) { item =>
            sql"""INSERT INTO club_member (club_id, player_id, since, until)
                  VALUES (${item.clubId}, ${item.playerId}, ${item.since}, ${item.until})""".update
          }
        }
      }.refineToOrDie[SQLException].unit

    def update(item: ClubMember): SqlTask[Unit] =
      ZIO.attempt {
        connect(xa)(sql"""UPDATE club_member SET until = ${item.until}
              WHERE club_id = ${item.clubId} AND player_id = ${item.playerId} AND since = ${item.since}""".update.run())
      }.refineToOrDie[SQLException].unit

    def updateBatch(items: Iterable[ClubMember]): SqlTask[Unit] =
      ZIO.attempt {
        transact(xa) {
          batchUpdate(items) { item =>
            sql"""UPDATE club_member SET until = ${item.until}
                  WHERE club_id = ${item.clubId} AND player_id = ${item.playerId} AND since = ${item.since}""".update
          }
        }
      }.refineToOrDie[SQLException].unit

    def deleteAll: SqlTask[Unit] =
      ZIO.attempt { connect(xa)(sql"DELETE FROM club_member".update.run()) }
        .refineToOrDie[SQLException].unit
  }
}
