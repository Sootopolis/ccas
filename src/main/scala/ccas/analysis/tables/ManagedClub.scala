package ccas.analysis.tables

import java.sql.SQLException
import java.time.Instant

import com.augustnagro.magnum.*
import zio.ZIO

import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

/** A club the user actively manages with CCAS (membership / recruitment / stats / history), as opposed to the many
  * scouted/opponent clubs the DB ingests incidentally. A first-class, explicitly-set marker — never derived from
  * side-effect tables. Server-scoped for the single-user v0 (epic #40); when auth lands (#66) the PK becomes
  * `(user_id, club_id)` and these rows migrate to the bootstrap user.
  */
final case class ManagedClub(
  clubId: ClubId,
  markedAt: Instant
) derives DbCodec

/** A managed club joined to its `club` row for display (slug + name). */
final case class ManagedClubView(
  slug: ClubSlug,
  name: String,
  markedAt: Instant
) derives DbCodec

object ManagedClub {

  def createTable: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""CREATE TABLE IF NOT EXISTS managed_club (
              club_id    BIGINT PRIMARY KEY,
              marked_at  TIMESTAMPTZ NOT NULL,
              FOREIGN KEY (club_id) REFERENCES club (club_id) ON DELETE RESTRICT
            )""".update.run()
    }

  def selectByClubId(clubId: ClubId): ZIO[PostgresClient, SQLException, Option[ManagedClub]] =
    connectZIO {
      sql"SELECT club_id, marked_at FROM managed_club WHERE club_id = $clubId".query[ManagedClub].run().headOption
    }

  /** All managed clubs joined to their `club` row, newest-marked first. Tombstoned clubs are excluded — a managed
    * marker on a club whose slug went stale shouldn't surface a `_stale_<id>` placeholder.
    */
  def selectAllWithClub: ZIO[PostgresClient, SQLException, List[ManagedClubView]] =
    connectZIO {
      sql"""SELECT c.slug, c.name, mc.marked_at
            FROM managed_club mc
            JOIN club c ON c.club_id = mc.club_id
            ORDER BY mc.marked_at DESC""".query[ManagedClubView].run().toList
    }.map(_.filterNot(v => Club.isTombstoneSlug(v.slug)))

  /** Club ids of every managed, non-tombstoned club — the non-leaky source for per-managed-club scheduling (#102).
    * Tombstoned (`_stale_<id>`) clubs are excluded: they have no usable slug, so they are not valid job targets and a
    * consumer (e.g. #102) must never crawl them. The `!~` pattern mirrors [[Club.isTombstoneSlug]] (`^_stale_\d+$`).
    */
  def selectClubIds: ZIO[PostgresClient, SQLException, List[ClubId]] =
    connectZIO {
      sql"""SELECT c.club_id
            FROM managed_club mc
            JOIN club c ON c.club_id = mc.club_id
            WHERE c.slug !~ '^_stale_[0-9]+$$'""".query[ClubId].run().toList
    }

  /** Idempotently marks a club managed. Returns rows inserted (1 first time, 0 if already managed). */
  def markManaged(clubId: ClubId, markedAt: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO managed_club (club_id, marked_at)
            VALUES ($clubId, $markedAt)
            ON CONFLICT (club_id) DO NOTHING""".update.run()
    }

  def delete(clubId: ClubId): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"DELETE FROM managed_club WHERE club_id = $clubId".update.run()
    }
}
