package ccas.analysis.tables

import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{ClubId, PlayerId}
import ccas.utils.sql.SqlZioTypes.SqlTask
import ccas.utils.sql.SqlRepoUtils
import io.getquill.*
import io.getquill.jdbczio.Quill

import java.time.Instant

final case class ClubMember(
  clubId  : ClubId,
  playerId: PlayerId,
  since   : Instant,
  until   : Option[Instant],
) {
  def isCurrent: Boolean = until.isEmpty
}

object ClubMember extends SqlRepoUtils {
  inline given UpdateMeta[ClubMember] = updateMeta(_.clubId, _.playerId, _.since)

  override protected type Repo = ClubMemberRepository

  override protected def makeRepo(quill: Quill.Postgres[SnakeCase]): Repo = ClubMemberRepository(quill)

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

  case class ClubMemberRepository(quill: Quill.Postgres[SnakeCase]) {
    import quill.*

    inline def selectAllQuery = query[ClubMember]
    inline def selectClubQuery(clubId: ClubId) = selectAllQuery.filter(_.clubId == lift(clubId))
    inline def selectClubCurrentQuery(clubId: ClubId) = selectClubQuery(clubId).filter(_.until.isEmpty)
    inline def selectClubActiveQuery(clubId: ClubId) = {
      selectAllQuery
        .join(query[PlayerSnapshot])
        .on(_.playerId == _.playerId)
        .join(query[PlayerSnapshot].groupByMap(_.playerId)(x => x.playerId -> max(x.since)))
        .on { case ((_, ps), (playerId, since)) => ps.playerId == playerId && ps.since == since }
        .filter { case ((cm, ps), _) => cm.clubId == lift(clubId) && cm.until.isEmpty && ps.status == lift(Active) }
        .map(_._1._1)
    }
    inline def selectClubFormerQuery(clubId: ClubId) = selectClubQuery(clubId).filter(_.until.isDefined)
    private inline def insertLifted(item: ClubMember): Insert[ClubMember] = selectAllQuery.insertValue(item)
    inline def insertQuery(item: ClubMember) = insertLifted(lift(item))
    inline def insertBatchQuery(items: Iterable[ClubMember]) = liftQuery(items).foreach(insertLifted)
    private inline def updateLifted(item: ClubMember): Update[ClubMember] = {
      selectAllQuery
        .filter(_.clubId == item.clubId)
        .filter(_.playerId == item.playerId)
        .filter(_.since == item.since)
        .updateValue(item)
    }
    inline def updateQuery(item: ClubMember) = updateLifted(lift(item))
    inline def updateBatchQuery(items: Iterable[ClubMember]) = liftQuery(items).foreach(updateLifted)
    inline def deleteAllQuery = selectAllQuery.delete

    def selectAll: SqlTask[List[ClubMember]] = run(selectAllQuery)
    def selectClub(clubId: ClubId): SqlTask[List[ClubMember]] = run(selectClubQuery(clubId))
    def selectClubCurrent(clubId: ClubId): SqlTask[List[ClubMember]] = run(selectClubCurrentQuery(clubId))
    def selectClubActive(clubId: ClubId): SqlTask[List[ClubMember]] = run(selectClubActiveQuery(clubId))
    def selectClubFormer(clubId: ClubId): SqlTask[List[ClubMember]] = run(selectClubFormerQuery(clubId))
    def insert(item: ClubMember): SqlTask[Unit] = run(insertQuery(item)).unit
    def insertBatch(items: Iterable[ClubMember]): SqlTask[Unit] = run(insertBatchQuery(items)).unit
    def update(item: ClubMember): SqlTask[Unit] = run(updateQuery(item)).unit
    def updateBatch(items: Iterable[ClubMember]): SqlTask[Unit] = run(updateBatchQuery(items)).unit
    def deleteAll: SqlTask[Unit] = run(deleteAllQuery).unit
  }
}
