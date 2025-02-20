package ccas.analysis.tables.clubadmin

import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}
import zio.{Chunk, RIO}

import java.time.Instant
import javax.sql.DataSource

case class ClubMember(
  playerId: PlayerId,
  since   : Instant,
  username: Username,
  status  : PlayerStatusCategory,
  until   : Option[Instant]
) {
  def isCurrent: Boolean = until.isEmpty
}

object ClubMember {
  case class LatestMembershipRecords(current: Chunk[ClubMember], former: Chunk[ClubMember]) {
    lazy val currentById: Map[PlayerId, ClubMember] = current.map(member => member.playerId -> member).toMap
    lazy val formerById: Map[PlayerId, ClubMember] = former.map(member => member.playerId -> member).toMap
    lazy val currentByUsername: Map[Username, ClubMember] = current.map(member => member.username -> member).toMap
    lazy val formerByUsername: Map[Username, ClubMember] = former.map(member => member.username -> member).toMap
  }

  def loadForClub(clubId: ClubId): RIO[DataSource, Chunk[ClubMember]] = ???

  def loadLatestForClub(clubId: ClubId): RIO[DataSource, LatestMembershipRecords] = ???

  def write(clubMembers: Iterable[ClubMember]): RIO[DataSource, Unit] = ???
}
