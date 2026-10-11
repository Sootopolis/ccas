package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.ClubId
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

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
