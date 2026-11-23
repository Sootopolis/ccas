package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import scala.util.chaining.*

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

@Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
final case class Club(
  @Id clubId: ClubId,
  created: Instant,
  slug: ClubSlug,
  name: String,
  membersCount: Option[Int],
  latestMatchAt: Option[Instant],
  fetchedAt: Option[Instant]
) derives DbCodec

object Club {
  private val repo = ImmutableRepo[Club, ClubId]

  private[tables] val selectCols =
    SqlLiteral("club_id, created, slug, name, members_count, latest_match_at, fetched_at")

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      sql"""CREATE TABLE IF NOT EXISTS club (
              club_id          BIGINT PRIMARY KEY,
              created          TIMESTAMPTZ NOT NULL,
              slug             TEXT NOT NULL,
              name             TEXT NOT NULL,
              members_count    INT,
              latest_match_at  TIMESTAMPTZ,
              fetched_at       TIMESTAMPTZ
            )""".update.run()
    }

  def selectAll: ZIO[PostgresClient, SQLException, List[Club]] =
    connectZIO(repo.findAll.toList)

  def selectId(clubId: ClubId): ZIO[PostgresClient, SQLException, Option[Club]] =
    connectZIO(repo.findById(clubId))

  /** Every club that holds a name now — the source the CLI's completion cache is built from (#44). A club whose name
    * another club took holds none until we observe what it answers to, and offering the name would offer the taker.
    */
  def selectNamed: ZIO[PostgresClient, SQLException, List[Club]] =
    connectZIO {
      sql"""SELECT $selectCols FROM club
            WHERE club_id IN (SELECT club_id FROM club_name WHERE until IS NULL)""".query[Club].run().toList
    }

  /** Upserts a club. NB: `latest_match_at` and `fetched_at` are intentionally not updated on conflict — they are
    * managed separately by [[ccas.analysis.apps.clubdata.ClubDataApp]] via [[updateLatestMatchAt]] and
    * [[updateFetchedAt]] so other callers don't accidentally clobber the cached values with `None`.
    */
  def upsert(club: Club): ZIO[PostgresClient, SQLException, Int] =
    transactZIO(upsertFrag(club).run().tap(_ => recordName(club)))

  /** Updates only the cached match-activity timestamp. Use this from ClubDataApp. */
  def updateLatestMatchAt(clubId: ClubId, latestMatchAt: Option[Instant]): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"UPDATE club SET latest_match_at = $latestMatchAt WHERE club_id = $clubId".update.run()
    }

  /** Stamps a club as successfully refreshed by ClubDataApp. Used by the `--min-age [hours]` filter on subsequent runs
    * to decide whether to skip this club.
    */
  def updateFetchedAt(clubId: ClubId, at: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"UPDATE club SET fetched_at = $at WHERE club_id = $clubId".update.run()
    }

  /** Builds a [[Club]] from an [[ApiClub]] response, reading the slug from `apiClub.canonicalSlug` — the name
    * Chess.com answers to now, not necessarily the one the caller requested. `latestMatchAt` and `fetchedAt` are
    * left as `None` — they are populated separately by ClubDataApp and preserved by [[upsert]].
    */
  def fromApi(apiClub: ApiClub): Club =
    Club(
      clubId = apiClub.clubId,
      created = Instant.ofEpochSecond(apiClub.created),
      slug = apiClub.canonicalSlug,
      name = apiClub.name,
      membersCount = Some(apiClub.membersCount),
      latestMatchAt = None,
      fetchedAt = None
    )

  private def upsertFrag(club: Club): Update =
    sql"""INSERT INTO club (club_id, created, slug, name, members_count, latest_match_at, fetched_at)
          VALUES (${club.clubId}, ${club.created}, ${club.slug}, ${club.name}, ${club.membersCount}, ${club.latestMatchAt}, ${club.fetchedAt})
          ON CONFLICT (club_id) DO UPDATE SET
            slug = EXCLUDED.slug,
            name = EXCLUDED.name,
            members_count = EXCLUDED.members_count""".update

  // Every write of `club.slug` goes through here, so `club_name` never drifts from it.
  private def recordName(club: Club)(using DbTx): Int =
    ClubName.record(club.clubId, club.slug)
}
