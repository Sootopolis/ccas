package ccas.analysis.apps

import zio.{Clock, RIO}

import ccas.analysis.tables.{Club, ManagedClub, ManagedClubView}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.errors.NotFoundException
import ccas.utils.sql.PostgresClient

/** Synchronous CRUD for the [[ManagedClub]] marker — the explicit "I manage this club" act. Invoked from
  * `ManagedClubRoutes` (and so `ccas club add|remove|list`). No `ChessComClient`: the club must already exist
  * locally; this never fetches Chess.com.
  */
object ManagedClubApp {

  /** Marks an existing local club managed. Idempotent — re-marking an already-managed club is a no-op. A stale/renamed
    * slug 404s rather than triggering rename recovery (unlike `BlacklistApp`): managing a club requires it to already
    * exist locally (#101 scope — no Chess.com fetch), so an unknown slug is a genuine "not found".
    */
  def mark(clubSlug: ClubSlug): RIO[PostgresClient, Unit] =
    for {
      club <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club not found: $clubSlug"))
      now  <- Clock.instant
      _    <- ManagedClub.markManaged(club.clubId, now)
    } yield ()

  /** Clears a club's managed marker and returns its `ClubId`. Succeeds whether or not it was managed. Stays
    * analysis-pure (touches only `managed_club`); the caller (`ManagedClubRoutes`, #106) uses the returned id to
    * also clear the club's per-club `job_schedule` rows in the same transaction — `job_schedule` is a server-layer
    * table and `analysis` never imports `server`.
    */
  def unmark(clubSlug: ClubSlug): RIO[PostgresClient, ClubId] =
    for {
      club <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club not found: $clubSlug"))
      _    <- ManagedClub.delete(club.clubId)
    } yield club.clubId

  def list: RIO[PostgresClient, List[ManagedClubView]] =
    ManagedClub.selectAllWithClub
}
