package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import scala.util.chaining.*

import com.augustnagro.magnum.*
import zio.{Task, ZIO}

import ccas.analysis.apps.ref.RefHelpers
import ccas.api.club.ApiClub
import ccas.api.clubmatch.TeamMatchTeams
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.client.ChessComClient
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

  /** The name the club's own match board reports for it — Tier B of slug-rename recovery. Reads a [[ClubMatchRef]],
    * explicit row first and otherwise inferred from `club_match` and promoted via [[ClubMatchRef.findOrInfer]], then
    * takes the slug from that board's team URL. `None` is every way this comes to nothing quietly: no ref to read, or
    * a match Chess.com reports gone — Tier B's answer to both is "try Tier C" (ADR 0019, #258). A board whose team
    * URL names no club is a malformed response rather than an absence, so it fails; #275 would have the decoder say
    * that instead.
    */
  def slugFromMatchRef(clubId: ClubId, client: ChessComClient): ZIO[PostgresClient, Throwable, Option[ClubSlug]] =
    ClubMatchRef.findOrInfer(clubId).flatMap {
      case None      => ZIO.none
      case Some(ref) => teamsOfMatch(ref, client).flatMap(ZIO.foreach(_)(slugOfTeam(ref, _)))
    }

  private def teamsOfMatch(ref: ClubMatchRef, client: ChessComClient): Task[Option[TeamMatchTeams]] =
    RefHelpers
      .fetchTeamMatchTeamsResult(client, ref.matchId, ref.isLive)
      .flatMap(_.foldZIO(_ => ZIO.none, _.getValue.asSome, _.getValue.asSome))

  private def slugOfTeam(ref: ClubMatchRef, teams: TeamMatchTeams): Task[ClubSlug] =
    ZIO.attempt(ClubSlug.fromUrl(if (ref.isTeam1) { teams.team1.`@id` } else { teams.team2.`@id` }))

  /** Builds a [[Club]] from an [[ApiClub]] response, reading the slug from `apiClub.canonicalSlug` — the name
    * Chess.com answers to now, not necessarily the one the caller requested. `latestMatchAt` and `fetchedAt` are
    * left as `None` — they are populated separately by ClubDataApp and preserved by [[upsert]].
    */
  def fromApi(apiClub: ApiClub): Club =
    Club(
      apiClub.clubId, Instant.ofEpochSecond(apiClub.created), apiClub.canonicalSlug, apiClub.name,
      Some(apiClub.membersCount), None, None
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
