package ccas.server.routes

import scala.util.chaining.*

import zio.http.*
import zio.json.{DeriveJsonCodec, JsonCodec}

import ccas.analysis.apps.ClubQuery
import ccas.analysis.apps.recruitment.{CriteriaSpec, RecruitmentCriteriaApp}
import ccas.server.routes.RouteHelpers.*
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient

object RecruitmentCriteriaRoutes {

  // --- Request/response types ---

  private[server] case class SetCriteriaRequest(club: ClubQuery, alias: String, criteria: CriteriaSpec)
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

  // Each answers with a `ClubResult` carrying the new criteria id, the alias's criteria, or the club's aliases.
  val routes: Routes[ChessComClient & PostgresClient, Nothing] = Routes(
    Method.POST / "api" / "recruitment-criteria" -> handler { (req: Request) =>
      (for {
        body <- parseJsonBody[SetCriteriaRequest](req)
        criteria = body.criteria.toCriteria
        _ <- RecruitmentCriteriaApp.validateSet(body.alias, criteria)
        result <- ClubRequest.run(body.club) { club =>
          RecruitmentCriteriaApp.set(club, body.alias, criteria).map(SetCriteriaResponse(_))
        }
      } yield jsonResponse(Status.Ok, result))
        .pipe(withErrorHandling)
    },
    Method.GET / "api" / "recruitment-criteria" / string("alias") -> handler { (alias: String, req: Request) =>
      (for {
        query  <- ClubRequest.query(req)
        result <- ClubRequest.run(query)(RecruitmentCriteriaApp.show(_, alias).map(CriteriaSpec.fromCriteria))
      } yield jsonResponse(Status.Ok, result))
        .pipe(withErrorHandling)
    },
    Method.GET / "api" / "recruitment-criteria" -> handler { (req: Request) =>
      (for {
        query <- ClubRequest.query(req)
        result <- ClubRequest.run(query) { club =>
          RecruitmentCriteriaApp
            .list(club.clubId)
            .map(_.map(a => AliasSummary(a.alias, a.since.toString, a.criteriaId)))
        }
      } yield jsonResponse(Status.Ok, result))
        .pipe(withErrorHandling)
    }
  )
}
