package ccas.server.routes

import zio.http.*
import zio.json.JsonCodec

import ccas.analysis.apps.ClubQuery
import ccas.analysis.tables.Club
import ccas.api.misc.subtypes.ClubSlug
import ccas.server.routes.RouteHelpers.*
import ccas.utils.client.ChessComClient
import ccas.utils.errors.NotFoundException
import ccas.utils.sql.PostgresClient

object ClubRoutes {

  // --- Response types ---

  private[ccas] case class ClubInfo(slug: ClubSlug, name: String) derives JsonCodec

  private[ccas] case class ClubsResponse(clubs: List[ClubInfo]) derives JsonCodec

  // Shared with the route tests: `use-club` reads a wrong path as a server with no answer and quietly falls back.
  private[ccas] def resolvePath(query: ClubQuery): String = s"/api/clubs/resolve?${ClubRequest.queryString(query)}"

  // --- Routes ---

  /** `GET /api/clubs` is the club-slug source for the CLI's completion cache (#44): every club that holds a name now,
    * as `{slug, name}`, alphabetical by slug. `GET /api/clubs/resolve` says which club a query names without acting on
    * it (#271), resolved exactly as a command naming it would be — Chess.com included — so it previews that command.
    */
  val routes: Routes[ChessComClient & PostgresClient, Nothing] = Routes(
    Method.GET / "api" / "clubs" -> handler {
      Club.selectNamed
        .map(_.sortBy(_.slug.value).map(c => ClubInfo(c.slug, c.name)))
        .map(infos => jsonResponse(Status.Ok, ClubsResponse(infos)))
    },
    Method.GET / "api" / "clubs" / "resolve" -> handler { (req: Request) =>
      for {
        query <- ClubRequest.query(req)
        result <- ClubRequest.run(query) { club =>
          Club
            .selectId(club.clubId)
            .someOrFail(NotFoundException(s"Club not found: ${club.display}"))
            .map(row => ClubInfo(row.slug, row.name))
        }
      } yield jsonResponse(Status.Ok, result)
    }
  ).handleErrorRequestCauseZIO(renderError)
}
