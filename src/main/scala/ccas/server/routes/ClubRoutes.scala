package ccas.server.routes

import scala.util.chaining.*

import zio.http.*
import zio.json.{DeriveJsonCodec, JsonCodec}

import ccas.analysis.tables.Club
import ccas.api.misc.subtypes.ClubSlug
import ccas.server.routes.RouteHelpers.*
import ccas.utils.sql.PostgresClient

object ClubRoutes {

  // --- Response types ---

  private[server] case class ClubInfo(slug: ClubSlug, name: String)
  object ClubInfo {
    given JsonCodec[ClubInfo] = DeriveJsonCodec.gen
  }

  private[server] case class ClubsResponse(clubs: List[ClubInfo])
  object ClubsResponse {
    given JsonCodec[ClubsResponse] = DeriveJsonCodec.gen
  }

  // --- Routes ---

  /** Lightweight club-slug source for the CLI's completion cache (#44). Returns every non-tombstoned club as
    * `{slug, name}`, alphabetical by slug. Tombstoned `_stale_<id>` rows are excluded.
    */
  val routes: Routes[PostgresClient, Nothing] = Routes(
    Method.GET / "api" / "clubs" -> handler {
      Club.selectAll
        .map(_.filterNot(_.isTombstoned).sortBy(_.slug.value).map(c => ClubInfo(c.slug, c.name)))
        .map(infos => jsonResponse(Status.Ok, ClubsResponse(infos)))
        .pipe(withErrorHandling)
    }
  )
}
