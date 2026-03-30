package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.ClubId
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.connectZIO

final case class RecruitmentAlias(clubId: ClubId, alias: String, since: Instant, criteriaId: Long) derives DbCodec

object RecruitmentAlias {
  private val selectCols = SqlLiteral("club_id, alias, since, criteria_id")

  def createTable: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS recruitment_alias (
              club_id      BIGINT NOT NULL,
              alias        TEXT NOT NULL,
              since        TIMESTAMPTZ NOT NULL,
              criteria_id  BIGINT NOT NULL,
              PRIMARY KEY (club_id, alias, since),
              FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT,
              FOREIGN KEY (criteria_id) REFERENCES recruitment_criteria (criteria_id) ON DELETE RESTRICT
            )""".update.run()
    }

  def selectLatest(clubId: ClubId, alias: String): ZIO[Transactor, SQLException, Option[RecruitmentAlias]] =
    connectZIO {
      sql"""SELECT $selectCols FROM recruitment_alias
            WHERE club_id = $clubId AND alias = $alias
            ORDER BY since DESC LIMIT 1"""
        .query[RecruitmentAlias].run().headOption
    }

  def selectClub(clubId: ClubId): ZIO[Transactor, SQLException, List[RecruitmentAlias]] =
    connectZIO {
      sql"""SELECT DISTINCT ON (alias) $selectCols FROM recruitment_alias
            WHERE club_id = $clubId
            ORDER BY alias, since DESC"""
        .query[RecruitmentAlias].run().toList
    }

  def countDistinct(clubId: ClubId): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"SELECT COUNT(DISTINCT alias) FROM recruitment_alias WHERE club_id = $clubId"
        .query[Int].run().head
    }

  def insert(item: RecruitmentAlias): ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO recruitment_alias (club_id, alias, since, criteria_id)
            VALUES (${item.clubId}, ${item.alias}, ${item.since}, ${item.criteriaId})"""
        .update.run()
    }

  def deleteAll: ZIO[Transactor, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM recruitment_alias".update.run()
    }
}
