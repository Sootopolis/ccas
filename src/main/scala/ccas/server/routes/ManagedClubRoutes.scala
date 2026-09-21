package ccas.server.routes

import scala.util.chaining.*

import zio.http.*
import zio.json.{DeriveJsonCodec, JsonCodec}

import ccas.analysis.apps.{ClubQuery, ManagedClubApp}
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

  private[ccas] case class ManagedClubResponse(clubId: Long, slug: String, name: String, markedAt: String)
  object ManagedClubResponse {
    given JsonCodec[ManagedClubResponse] = DeriveJsonCodec.gen

    def fromView(v: ManagedClubView): ManagedClubResponse =
      ManagedClubResponse(
        clubId = ClubId.unwrap(v.clubId),
        slug = ClubSlug.unwrap(v.slug),
        name = v.name,
        markedAt = v.markedAt.toString
      )
  }

  // --- Routes ---

  // Marking and unmarking answer with a `ClubResult` saying whether the marker changed.
  val routes: Routes[ChessComClient & PostgresClient, Nothing] = Routes(
    Method.GET / "api" / "managed-clubs" -> handler { (_: Request) =>
      ManagedClubApp.list
        .map(views => jsonResponse(Status.Ok, views.map(ManagedClubResponse.fromView)))
        .pipe(withErrorHandling)
    },
    Method.POST / "api" / "managed-clubs" -> handler { (req: Request) =>
      (for {
        body   <- parseJsonBody[MarkManagedRequest](req)
        result <- ClubRequest.run(body.club)(club => ManagedClubApp.mark(club.clubId))
      } yield jsonResponse(Status.Ok, result))
        .pipe(withErrorHandling)
    },
    Method.DELETE / "api" / "managed-clubs" -> handler { (req: Request) =>
      // Unmanage + stop the club's per-club schedules atomically (#106), so a failure leaves neither the managed_club
      // marker nor the job_schedule rows half-removed. Resolution may ask Chess.com, so it runs before the transaction.
      (for {
        query <- ClubRequest.query(req)
        result <- ClubRequest.run(query) { club =>
          PostgresClient.withTransaction(ManagedClubApp.unmark(club.clubId) <* JobSchedule.deleteByClub(club.clubId))
        }
      } yield jsonResponse(Status.Ok, result))
        .pipe(withErrorHandling)
    }
  )
}
