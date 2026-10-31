package ccas.server.routes

import zio.http.*
import zio.json.{jsonField, DeriveJsonCodec, JsonCodec}

import ccas.analysis.apps.{ClubQuery, ClubRef, ManagedClubApp}
import ccas.analysis.tables.ManagedClubView
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.server.routes.RouteHelpers.*
import ccas.server.scheduler.JobSchedule
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient

/** Synchronous CRUD for the managed-club marker (delegates to [[ManagedClubApp]], not `JobRunner`). Mirrors
  * `BlacklistRoutes` / `RecruitmentCriteriaRoutes`.
  */
object ManagedClubRoutes {

  // --- Request/response types ---

  private[ccas] case class MarkManagedRequest(club: ClubQuery)
  object MarkManagedRequest {
    given JsonCodec[MarkManagedRequest] = DeriveJsonCodec.gen
  }

  // `slug` is absent for a managed club that has lost its name to another, which can still be reached by id (#385).
  private[ccas] case class ManagedClubResponse(
    clubId: Long,
    @jsonField("slug") slugOption: Option[String],
    name: String,
    markedAt: String
  ) {

    /** The club this row names, by its id and the name it holds now, if any. */
    def ref: ClubRef = ClubRef(ClubId(clubId), slugOption.map(ClubSlug(_)))
  }
  object ManagedClubResponse {
    given JsonCodec[ManagedClubResponse] = DeriveJsonCodec.gen

    def fromView(v: ManagedClubView): ManagedClubResponse =
      ManagedClubResponse(
        clubId = ClubId.unwrap(v.clubId),
        slugOption = v.slugOption.map(ClubSlug.unwrap),
        name = v.name,
        markedAt = v.markedAt.toString
      )
  }

  // --- Routes ---

  // Marking and unmarking answer with a `ClubResult` saying whether the marker changed.
  val routes: Routes[ChessComClient & PostgresClient, Nothing] = Routes(
    Method.GET / "api" / "managed-clubs" -> handler { (_: Request) =>
      ManagedClubApp.list.map(views => jsonResponse(Status.Ok, views.map(ManagedClubResponse.fromView)))
    },
    Method.POST / "api" / "managed-clubs" -> handler { (req: Request) =>
      for {
        body   <- parseJsonBody[MarkManagedRequest](req)
        result <- ClubRequest.run(body.club)(club => ManagedClubApp.mark(club.clubId))
      } yield jsonResponse(Status.Ok, result)
    },
    Method.DELETE / "api" / "managed-clubs" -> handler { (req: Request) =>
      // Unmanage + stop the club's per-club schedules atomically (#106), so a failure leaves neither the managed_club
      // marker nor the job_schedule rows half-removed. Resolution may ask Chess.com, so it runs before the transaction.
      // Both need only the id, so a club that has lost its name can still be unmanaged (#385).
      for {
        query <- ClubRequest.query(req)
        result <- ClubRequest.runOnReached(query) { clubId =>
          PostgresClient.withTransaction(ManagedClubApp.unmark(clubId) <* JobSchedule.deleteByClub(clubId))
        }
      } yield jsonResponse(Status.Ok, result)
    }
  ).handleErrorRequestCauseZIO(renderError)
}
