package ccas.analysis.apps

import zio.{Clock, RIO}

import ccas.analysis.tables.{ManagedClub, ManagedClubView}
import ccas.api.misc.subtypes.ClubId
import ccas.utils.sql.PostgresClient

/** Synchronous CRUD for the [[ManagedClub]] marker — the explicit "I manage this club" act. Invoked from
  * `ManagedClubRoutes` (and so `ccas club add|remove|list`), which resolves the club first, so managing a club requires
  * it to already exist locally.
  */
object ManagedClubApp {

  /** Marks a club managed, answering whether it was not already. Idempotent. */
  def mark(clubId: ClubId): RIO[PostgresClient, Boolean] =
    Clock.instant.flatMap(ManagedClub.markManaged(clubId, _)).map(_ > 0)

  /** Clears a club's managed marker, answering whether it had one. Stays analysis-pure (touches only `managed_club`):
    * the caller (`ManagedClubRoutes`, #106) also clears the club's per-club `job_schedule` rows in the same
    * transaction, since `job_schedule` is a server-layer table and `analysis` never imports `server`.
    */
  def unmark(clubId: ClubId): RIO[PostgresClient, Boolean] =
    ManagedClub.delete(clubId).map(_ > 0)

  def list: RIO[PostgresClient, List[ManagedClubView]] =
    ManagedClub.selectAllWithClub
}
