package ccas.analysis.tables.clubadmin

import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.PlayerId
import zio.{Chunk, RIO}

import java.time.Instant
import javax.sql.DataSource

case class ClubMember(
  playerId: PlayerId,
  since   : Instant,
  status  : PlayerStatusCategory,
  isMember: Boolean,
  until   : Option[Instant]
)

object ClubMember {

  def write(clubMembers: Iterable[ClubMember]): RIO[DataSource, Unit] = ???
}
