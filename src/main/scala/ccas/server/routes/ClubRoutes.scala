package ccas.server.routes

import scala.util.chaining.*

import zio.http.*
import zio.json.JsonCodec

import ccas.analysis.tables.Club
import ccas.api.misc.subtypes.ClubSlug
import ccas.server.routes.RouteHelpers.*
import ccas.utils.sql.PostgresClient

object ClubRoutes {

  // --- Response types ---

  private[server] case class ClubInfo(slug: ClubSlug, name: String) derives JsonCodec

  private[server] case class ClubsResponse(clubs: List[ClubInfo]) derives JsonCodec

  // --- Routes ---

  /** Lightweight club-slug source for the CLI's completion cache (#44). Returns every club that holds a name now as
    * `{slug, name}`, alphabetical by slug.
    */
  val routes: Routes[PostgresClient, Nothing] = Routes(
    Method.GET / "api" / "clubs" -> handler {
      Club.selectNamed
        .map(_.sortBy(_.slug.value).map(c => ClubInfo(c.slug, c.name)))
        .map(infos => jsonResponse(Status.Ok, ClubsResponse(infos)))
        .pipe(withErrorHandling)
    }
  )
}
