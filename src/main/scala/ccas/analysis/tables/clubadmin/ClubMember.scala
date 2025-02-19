package ccas.analysis.tables.clubadmin

import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.PlayerId
import zio.{Chunk, RIO}

import java.time.Instant
import javax.sql.DataSource

sealed trait ClubMember {
  val playerId: PlayerId
  val since: Instant
  val status: PlayerStatusCategory
  val isMember: Boolean
}

object ClubMember {
  sealed trait LatestMemberEntry extends ClubMember

  case class CurrentMember(playerId: PlayerId, since: Instant) extends LatestMemberEntry {
    override val status: PlayerStatusCategory = PlayerStatusCategory.Active
    override val isMember: Boolean = true
  }

  case class LatestFormerMember(playerId: PlayerId, since: Instant, status: PlayerStatusCategory)
    extends LatestMemberEntry {
    override val isMember: Boolean = false
  }

  private case class MemberEntry(
    playerId: PlayerId,
    since   : Instant,
    status  : PlayerStatusCategory,
    isMember: Boolean,
    until   : Option[Instant]
  ) extends ClubMember

  def loadCurrent: RIO[DataSource, Chunk[CurrentMember]] = ???

  def loadLatestFormer: RIO[DataSource, Chunk[LatestFormerMember]] = ???

  def loadLatest: RIO[DataSource, (Chunk[CurrentMember], Chunk[LatestFormerMember])] = ???

  def write(clubMembers: Iterable[ClubMember]): RIO[DataSource, Unit] = ???
}
