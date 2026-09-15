package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.{connectZIO, transactZIO}

/** One window over which a club was observed holding a slug. `until = None` means the most recent observation still
  * stands. Both bounds are discovery times, not transition times — see
  * `docs/adr/0016-identity-is-the-id-names-are-observations.md`, which also holds why the constraints are shaped as
  * they are.
  */
final case class ClubName(clubId: ClubId, slug: ClubSlug, since: Instant, until: Option[Instant]) derives DbCodec

object ClubName {
  private val selectCols = SqlLiteral("club_id, slug, since, until")

  /** Fails with a pointer to the runbook when `btree_gist` is missing: the exclusion constraint needs it, and the
    * extension is installed by hand rather than from the boot path (ADR 0016).
    */
  def createTable: ZIO[PostgresClient, SQLException, Int] =
    transactZIO {
      val hasBtreeGist =
        sql"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'btree_gist')".query[Boolean].run().head
      if (!hasBtreeGist) {
        throw SQLException(
          "club_name needs the btree_gist extension: run 'CREATE EXTENSION btree_gist;' against this database " +
            "(docs/adr/0016-identity-is-the-id-names-are-observations.md)"
        )
      }
      sql"""CREATE TABLE IF NOT EXISTS club_name (
              club_id  BIGINT      NOT NULL REFERENCES club (club_id) ON DELETE RESTRICT,
              slug     TEXT        NOT NULL,
              since    TIMESTAMPTZ NOT NULL,
              until    TIMESTAMPTZ,
              PRIMARY KEY (club_id, since),
              CONSTRAINT club_name_window CHECK (until IS NULL OR until > since),
              CONSTRAINT club_name_no_overlap
                EXCLUDE USING gist (club_id WITH =, tstzrange(since, until) WITH &&)
            )""".update.run()
      sql"CREATE UNIQUE INDEX IF NOT EXISTS club_name_current ON club_name (slug) WHERE until IS NULL".update.run()
    }

  /** Opens a current name for every club that has no `club_name` row at all — the first boot after the table lands,
    * and any club a pre-`club_name` binary inserted since. Tombstoned clubs hold no name, so they get none. Idempotent.
    */
  def backfill: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO club_name (club_id, slug, since)
            SELECT c.club_id, c.slug, now() FROM club c
            WHERE c.slug !~ ${Club.TombstoneSlugRegex}
              AND NOT EXISTS (SELECT 1 FROM club_name n WHERE n.club_id = c.club_id)
            ON CONFLICT DO NOTHING""".update.run()
    }

  def selectClub(clubId: ClubId): ZIO[PostgresClient, SQLException, List[ClubName]] =
    connectZIO {
      sql"SELECT $selectCols FROM club_name WHERE club_id = $clubId ORDER BY since".query[ClubName].run().toList
    }

  def selectCurrentHolder(slug: ClubSlug): ZIO[PostgresClient, SQLException, Option[Club]] =
    connectZIO {
      sql"""SELECT ${Club.selectCols} FROM club
            WHERE club_id = (SELECT club_id FROM club_name WHERE slug = $slug AND until IS NULL)"""
        .query[Club].run().headOption
    }

  /** Every club that has ever held `slug`, current holder included. Unindexed on purpose — ask
    * [[selectCurrentHolder]] first and come here only on a miss (ADR 0016).
    */
  def selectHolders(slug: ClubSlug): ZIO[PostgresClient, SQLException, List[Club]] =
    connectZIO {
      sql"""SELECT ${Club.selectCols} FROM club
            WHERE club_id IN (SELECT club_id FROM club_name WHERE slug = $slug)
            ORDER BY club_id""".query[Club].run().toList
    }

  /** Records the name `clubId` holds as of now: `Some(slug)`, or `None` once it holds none we know of (a tombstone).
    * Unless that already stands, this closes the club's current name and any other club's current hold on the slug
    * (the name moved), then opens the new name. Stamped with the database clock, which every writer shares. Takes a
    * `DbTx` so it runs in the transaction of the `club` write it describes.
    */
  private[tables] def record(clubId: ClubId, slugOption: Option[ClubSlug])(using DbTx): Int = {
    // One round trip in the common case — the name already stands — since every club refresh lands here. The exclusion
    // constraint allows a club one current name at most, so the subquery is a single slug or NULL.
    val (at, standing) =
      sql"""SELECT clock_timestamp(),
                   (SELECT slug FROM club_name WHERE club_id = $clubId AND until IS NULL)
                     IS NOT DISTINCT FROM $slugOption""".query[(Instant, Boolean)].run().head
    if (standing) { 0 }
    else { supersede(clubId, slugOption, at) }
  }

  // `IS DISTINCT FROM` lets `None` close whatever the club holds, and `slug = NULL` matches no other club. A row opened
  // at or after `at` would close into an empty window, which the CHECK rejects and which records nothing, so it is
  // dropped instead of closed.
  private def supersede(clubId: ClubId, slugOption: Option[ClubSlug], at: Instant)(using DbTx): Int = {
    val dropped =
      sql"""DELETE FROM club_name
            WHERE until IS NULL AND since >= $at
              AND ((club_id = $clubId AND slug IS DISTINCT FROM $slugOption)
                OR (slug = $slugOption AND club_id <> $clubId))""".update.run()
    val closed =
      sql"""UPDATE club_name SET until = $at
            WHERE until IS NULL
              AND ((club_id = $clubId AND slug IS DISTINCT FROM $slugOption)
                OR (slug = $slugOption AND club_id <> $clubId))""".update.run()
    val opened = slugOption.fold(0) { slug =>
      sql"INSERT INTO club_name (club_id, slug, since) VALUES ($clubId, $slug, $at)".update.run()
    }
    dropped + closed + opened
  }
}
