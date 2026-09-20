package ccas.server.routes

import java.time.{Instant, ZoneOffset}

import scala.util.chaining.*

import zio.http.*
import zio.json.{DeriveJsonCodec, JsonCodec}

import ccas.analysis.apps.ClubQuery
import ccas.analysis.apps.recruitment.BlacklistApp
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.server.routes.RouteHelpers.*
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient

object BlacklistRoutes {

  // --- Request/response types ---

  private[ccas] case class CreateBlacklistRequest(
    club: ClubQuery,
    usernames: List[Username],
    reason: Option[String],
    months: Option[Int]
  )
  object CreateBlacklistRequest {
    given JsonCodec[CreateBlacklistRequest] = DeriveJsonCodec.gen
  }

  private[ccas] case class BlacklistEntryResponse(
    playerId: Long,
    username: Option[String],
    addedAt: String,
    expiresAt: Option[String],
    reason: Option[String]
  )
  object BlacklistEntryResponse {
    given JsonCodec[BlacklistEntryResponse] = DeriveJsonCodec.gen

    def fromEntry(entry: BlacklistEntry): BlacklistEntryResponse =
      BlacklistEntryResponse(
        playerId = PlayerId.unwrap(entry.playerId),
        username = entry.username.map(Username.unwrap),
        addedAt = entry.addedAt.toString,
        expiresAt = entry.expiresAt.map(_.toString),
        reason = entry.reason
      )
  }

  // --- Routes ---

  // Each answers with a `ClubResult`: the entries, the usernames as blacklisted (a renamed one under its current
  // name), and whether a removal found an entry to remove.
  val routes: Routes[ChessComClient & PostgresClient, Nothing] = Routes(
    Method.GET / "api" / "blacklist" -> handler { (req: Request) =>
      (for {
        query <- ClubRequest.query(req)
        now = Instant.now()
        result <- ClubRequest.run(query) { club =>
          RecruitmentBlacklist.selectActiveByClub(club.clubId, now).map(_.map(BlacklistEntryResponse.fromEntry))
        }
      } yield jsonResponse(Status.Ok, result))
        .pipe(withErrorHandling)
    },
    Method.POST / "api" / "blacklist" -> handler { (req: Request) =>
      (for {
        body <- parseJsonBody[CreateBlacklistRequest](req)
        expiresAt = body.months.map(m => Instant.now().atZone(ZoneOffset.UTC).plusMonths(m.toLong).toInstant)
        result <- ClubRequest.run(body.club) { club =>
          BlacklistApp.addToBlacklist(club, body.usernames, body.reason, expiresAt).map(_.map(Username.unwrap))
        }
      } yield jsonResponse(Status.Ok, result))
        .pipe(withErrorHandling)
    },
    Method.DELETE / "api" / "blacklist" / string("username") -> handler { (usernameStr: String, req: Request) =>
      (for {
        query  <- ClubRequest.query(req)
        result <- ClubRequest.run(query)(BlacklistApp.removeFromBlacklist(_, Username.wrap(usernameStr)))
      } yield jsonResponse(Status.Ok, result))
        .pipe(withErrorHandling)
    }
  )
}
