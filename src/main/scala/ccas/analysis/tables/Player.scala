package ccas.analysis.tables

import ccas.api.misc.subtypes.PlayerId
import ccas.utils.sql.SqlZioTypes.{connectZIO, transactZIO}
import ccas.utils.sql.DbCodecs.given
import com.augustnagro.magnum.*
import zio.ZIO

import java.sql.SQLException
import java.time.Instant

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
case class Player(@Id playerId: PlayerId, joined: Instant) derives DbCodec

object Player {
  private val repo = Repo[Player, Player, PlayerId]

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS player (
              player_id BIGINT PRIMARY KEY,
              joined    TIMESTAMPTZ NOT NULL
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

  def deleteAll: ZIO[Transactor, SQLException, Unit] =
    connectZIO(repo.truncate())

  def deleteId(playerId: PlayerId): ZIO[Transactor, SQLException, Unit] =
    connectZIO(repo.deleteById(playerId))
}
