package ccas.analysis.tables.general

import ccas.api.misc.enums.{PlayerStatusCategory, Title}
import ccas.api.misc.subtypes.{PlayerId, Username}
import zio.{Chunk, RIO}

import java.time.Instant
import javax.sql.DataSource

case class Player(
  playerId: PlayerId,
  username: Username,
  status  : PlayerStatusCategory,
  title   : Option[Title],
  until   : Option[Instant]
)

object Player {
  def load(playerIds: Iterable[PlayerId]): RIO[DataSource, Chunk[Player]] = ???

  def update(players: Iterable[Player]): RIO[DataSource, Unit] = ???
}
