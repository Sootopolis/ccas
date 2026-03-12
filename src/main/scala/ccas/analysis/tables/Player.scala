package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.http.URL
import zio.ZIO

import ccas.api.misc.subtypes.PlayerId
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class Player(@Id playerId: PlayerId, joined: Instant, boardUrl: Option[URL]) derives DbCodec

object Player {
  private val repo = Repo[Player, Player, PlayerId]

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player (
              player_id BIGINT PRIMARY KEY,
              joined    TIMESTAMPTZ NOT NULL,
              board_url VARCHAR
            )""".update.run()
    }

  def selectAll: ZIO[Transactor, SQLException, List[Player]] =
    connectZIO(repo.findAll.toList)

  def selectId(playerId: PlayerId): ZIO[Transactor, SQLException, Option[Player]] =
    connectZIO(repo.findById(playerId))

  def insert(player: Player): ZIO[Transactor, SQLException, Unit] =
    connectZIO(repo.insert(player))

  def insertBatch(players: Iterable[Player]): ZIO[Transactor, SQLException, Unit] =
    transactZIO(repo.insertAll(players))

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO { sql"DELETE FROM player".update.run() }

  def deleteId(playerId: PlayerId): ZIO[Transactor, SQLException, Unit] =
    connectZIO(repo.deleteById(playerId))
}
