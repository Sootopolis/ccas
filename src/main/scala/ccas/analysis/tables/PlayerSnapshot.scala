package ccas.analysis.tables

import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.enums.Title
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.SqlZioTypes.SqlTask
import ccas.utils.sql.SqlRepoUtils
import ccas.utils.sql.DbCodecs.given
import com.augustnagro.magnum.*
import zio.ZIO

import java.sql.SQLException
import java.time.Instant

case class PlayerSnapshot(
  playerId: PlayerId,
  since   : Instant,
  username: Username,
  status  : PlayerStatusCategory,
  title   : Option[Title]
) derives DbCodec

object PlayerSnapshot extends SqlRepoUtils {
  override protected type Repo = PlayerSnapshotRepository

  override protected def makeRepo(xa: Transactor): Repo = PlayerSnapshotRepository(xa)

  def selectAll: RepoTask[List[PlayerSnapshot]] = repoService(_.selectAll)
  def selectLatest: RepoTask[List[PlayerSnapshot]] = repoService(_.selectLatest)
  def selectActive: RepoTask[List[PlayerSnapshot]] = repoService(_.selectActive)
  def selectId(playerId: PlayerId): RepoTask[List[PlayerSnapshot]] = repoService(_.selectId(playerId))
  def selectName(username: Username): RepoTask[List[PlayerSnapshot]] = repoService(_.selectName(username))
  def selectIdLatest(playerId: PlayerId): RepoTask[Option[PlayerSnapshot]] = repoService(_.selectIdLatest(playerId))
  def selectNameLatest(username: Username): RepoTask[Option[PlayerSnapshot]] = repoService(_.selectNameLatest(username))
  def selectSince(since: Instant): RepoTask[List[PlayerSnapshot]] = repoService(_.selectSince(since))
  def insert(item: PlayerSnapshot): RepoTask[Unit] = repoService(_.insert(item))
  def insertBatch(items: Iterable[PlayerSnapshot]): RepoTask[Unit] = repoService(_.insertBatch(items))
  def update(item: PlayerSnapshot): RepoTask[Unit] = repoService(_.update(item))
  def updateBatch(items: Iterable[PlayerSnapshot]): RepoTask[Unit] = repoService(_.updateBatch(items))
  def deleteAll: RepoTask[Unit] = repoService(_.deleteAll)

  private val selectCols = "player_id, since, username, status, title"

  case class PlayerSnapshotRepository(xa: Transactor) {
    def selectAll: SqlTask[List[PlayerSnapshot]] =
      ZIO.attempt { connect(xa)(sql"SELECT #$selectCols FROM player_snapshot".query[PlayerSnapshot].run().toList) }
        .refineToOrDie[SQLException]

    def selectLatest: SqlTask[List[PlayerSnapshot]] =
      ZIO.attempt {
        connect(xa) {
          sql"""SELECT #$selectCols FROM player_snapshot ps
                INNER JOIN (SELECT player_id, MAX(since) AS since FROM player_snapshot GROUP BY player_id) latest
                ON ps.player_id = latest.player_id AND ps.since = latest.since""".query[PlayerSnapshot].run().toList
        }
      }.refineToOrDie[SQLException]

    def selectActive: SqlTask[List[PlayerSnapshot]] =
      ZIO.attempt {
        connect(xa) {
          sql"""SELECT #$selectCols FROM player_snapshot ps
                INNER JOIN (SELECT player_id, MAX(since) AS since FROM player_snapshot GROUP BY player_id) latest
                ON ps.player_id = latest.player_id AND ps.since = latest.since
                WHERE ps.status = ${Active.toString}""".query[PlayerSnapshot].run().toList
        }
      }.refineToOrDie[SQLException]

    def selectId(playerId: PlayerId): SqlTask[List[PlayerSnapshot]] =
      ZIO.attempt { connect(xa)(sql"SELECT #$selectCols FROM player_snapshot WHERE player_id = $playerId".query[PlayerSnapshot].run().toList) }
        .refineToOrDie[SQLException]

    def selectName(username: Username): SqlTask[List[PlayerSnapshot]] =
      ZIO.attempt { connect(xa)(sql"SELECT #$selectCols FROM player_snapshot WHERE username = $username".query[PlayerSnapshot].run().toList) }
        .refineToOrDie[SQLException]

    def selectIdLatest(playerId: PlayerId): SqlTask[Option[PlayerSnapshot]] =
      ZIO.attempt { connect(xa)(sql"SELECT #$selectCols FROM player_snapshot WHERE player_id = $playerId ORDER BY since DESC".query[PlayerSnapshot].run().headOption) }
        .refineToOrDie[SQLException]

    def selectNameLatest(username: Username): SqlTask[Option[PlayerSnapshot]] =
      ZIO.attempt { connect(xa)(sql"SELECT #$selectCols FROM player_snapshot WHERE username = $username ORDER BY since DESC".query[PlayerSnapshot].run().headOption) }
        .refineToOrDie[SQLException]

    def selectSince(since: Instant): SqlTask[List[PlayerSnapshot]] =
      ZIO.attempt {
        connect(xa) {
          sql"""SELECT #$selectCols FROM player_snapshot ps
                INNER JOIN (SELECT player_id, MAX(since) AS since FROM player_snapshot WHERE since <= $since GROUP BY player_id) jb
                ON ps.player_id = jb.player_id AND ps.since = jb.since
                UNION
                SELECT #$selectCols FROM player_snapshot WHERE since > $since""".query[PlayerSnapshot].run().toList
        }
      }.refineToOrDie[SQLException]

    def insert(item: PlayerSnapshot): SqlTask[Unit] =
      ZIO.attempt {
        connect(xa)(sql"""INSERT INTO player_snapshot (player_id, since, username, status, title)
              VALUES (${item.playerId}, ${item.since}, ${item.username}, ${item.status.toString}, ${item.title.map(_.toString)})""".update.run())
      }.refineToOrDie[SQLException].unit

    def insertBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] =
      ZIO.attempt {
        transact(xa) {
          batchUpdate(items) { item =>
            sql"""INSERT INTO player_snapshot (player_id, since, username, status, title)
                  VALUES (${item.playerId}, ${item.since}, ${item.username}, ${item.status.toString}, ${item.title.map(_.toString)})""".update
          }
        }
      }.refineToOrDie[SQLException].unit

    def update(item: PlayerSnapshot): SqlTask[Unit] =
      ZIO.attempt {
        connect(xa)(sql"""UPDATE player_snapshot SET username = ${item.username}, status = ${item.status.toString}, title = ${item.title.map(_.toString)}
              WHERE player_id = ${item.playerId} AND since = ${item.since}""".update.run())
      }.refineToOrDie[SQLException].unit

    def updateBatch(items: Iterable[PlayerSnapshot]): SqlTask[Unit] =
      ZIO.attempt {
        transact(xa) {
          batchUpdate(items) { item =>
            sql"""UPDATE player_snapshot SET username = ${item.username}, status = ${item.status.toString}, title = ${item.title.map(_.toString)}
                  WHERE player_id = ${item.playerId} AND since = ${item.since}""".update
          }
        }
      }.refineToOrDie[SQLException].unit

    def deleteAll: SqlTask[Unit] =
      ZIO.attempt { connect(xa)(sql"DELETE FROM player_snapshot".update.run()) }
        .refineToOrDie[SQLException].unit
  }
}
