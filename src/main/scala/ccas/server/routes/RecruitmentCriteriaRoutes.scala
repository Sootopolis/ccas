package ccas.server.routes

import scala.util.chaining.*

import zio.http.*
import zio.json.{DeriveJsonCodec, JsonCodec}

import ccas.analysis.apps.recruitment.{CriteriaSpec, RecruitmentCriteriaApp}
import ccas.api.misc.subtypes.ClubSlug
import ccas.server.routes.RouteHelpers.*
import ccas.utils.sql.PostgresClient

object RecruitmentCriteriaRoutes {

  // --- Request/response types ---

  private[server] case class SetCriteriaRequest(clubSlug: ClubSlug, alias: String, criteria: CriteriaSpec)
  object SetCriteriaRequest {
    given JsonCodec[SetCriteriaRequest] = DeriveJsonCodec.gen
  }

  private[server] case class SetCriteriaResponse(criteriaId: Long)
  object SetCriteriaResponse {
    given JsonCodec[SetCriteriaResponse] = DeriveJsonCodec.gen
  }

  private[server] case class AliasSummary(alias: String, since: String, criteriaId: Long)
  object AliasSummary {
    given JsonCodec[AliasSummary] = DeriveJsonCodec.gen
  }

  // --- Routes ---

  val routes: Routes[PostgresClient, Nothing] = Routes(
    Method.POST / "api" / "recruitment-criteria" -> handler { (req: Request) =>
      (for {
        body <- parseJsonBody[SetCriteriaRequest](req)
        id   <- RecruitmentCriteriaApp.set(body.clubSlug, body.alias, body.criteria.toCriteria)
      } yield jsonResponse(Status.Ok, SetCriteriaResponse(id)))
        .pipe(withErrorHandling)
    },
    Method.GET / "api" / "recruitment-criteria" / string("clubSlug") / string("alias") -> handler {
      (clubSlugStr: String, alias: String, _: Request) =>
        RecruitmentCriteriaApp.show(ClubSlug.wrap(clubSlugStr), alias)
          .map(criteria => jsonResponse(Status.Ok, CriteriaSpec.fromCriteria(criteria)))
          .pipe(withErrorHandling)
    },
    Method.GET / "api" / "recruitment-criteria" / string("clubSlug") -> handler {
      (clubSlugStr: String, _: Request) =>
        RecruitmentCriteriaApp.list(ClubSlug.wrap(clubSlugStr))
          .map(aliases => jsonResponse(Status.Ok, aliases.map(a => AliasSummary(a.alias, a.since.toString, a.criteriaId))))
          .pipe(withErrorHandling)
    }
  )
}
