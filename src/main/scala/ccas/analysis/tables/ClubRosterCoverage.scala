package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.ClubId
import ccas.utils.sql.AdvisoryLock
import ccas.utils.sql.AdvisoryLock.Space
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

/** When a club's full roster was first and last fetched. Membership writes it with the member rows the roster implied,
  * so the two never disagree; it is what a member row's ends are taken from (ADR 0021).
  */
final case class ClubRosterCoverage(clubId: ClubId, firstObservedAt: Instant, lastObservedAt: Instant)
    derives DbCodec

object ClubRosterCoverage {

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS club_roster_coverage (
              club_id           BIGINT PRIMARY KEY,
              first_observed_at TIMESTAMPTZ NOT NULL,
              last_observed_at  TIMESTAMPTZ NOT NULL,
              CHECK (first_observed_at <= last_observed_at),
              FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT
            )""".update.run()
    }

  def select(clubId: ClubId): ZIO[PostgresClient, SQLException, Option[ClubRosterCoverage]] =
    connectZIO {
      sql"""SELECT club_id, first_observed_at, last_observed_at FROM club_roster_coverage WHERE club_id = $clubId"""
        .query[ClubRosterCoverage].run().headOption
    }

  /** Which save of `clubId`'s roster is the latest, or `None` before the first. Every save writes this row, and
    * Postgres stamps each write with the id of the transaction that made it (`xmin`), so two reads differ exactly when
    * a save committed between them, whatever the clocks of the hosts that saved (#312).
    */
  def selectVersion(clubId: ClubId): ZIO[PostgresClient, SQLException, Option[Long]] =
    connectZIO(versionOf(clubId))

  /** Waits out any other save of `clubId`'s roster and holds off the rest until the calling transaction ends, then
    * reads [[selectVersion]]. Only a call inside the saving transaction holds anything.
    */
  def lockVersion(clubId: ClubId): ZIO[PostgresClient, SQLException, Option[Long]] =
    transactZIO {
      // A club id past `Int` can share its key with another club's, which only makes their saves queue.
      AdvisoryLock.acquire(Space.ClubRoster, List(ClubId.unwrap(clubId).hashCode))
      versionOf(clubId)
    }

  private def versionOf(clubId: ClubId)(using DbCon): Option[Long] =
    sql"SELECT xmin::text::bigint FROM club_roster_coverage WHERE club_id = $clubId".query[Long].run().headOption

  /** Records that `clubId`'s roster was fetched at `at`. Neither bound ever moves inward, so a late or repeated write
    * changes nothing it should not.
    */
  def observe(clubId: ClubId, at: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club_roster_coverage (club_id, first_observed_at, last_observed_at) VALUES ($clubId, $at, $at)
            ON CONFLICT (club_id) DO UPDATE SET
              first_observed_at = LEAST(club_roster_coverage.first_observed_at, EXCLUDED.first_observed_at),
              last_observed_at  = GREATEST(club_roster_coverage.last_observed_at, EXCLUDED.last_observed_at)"""
        .update.run()
    }
}
