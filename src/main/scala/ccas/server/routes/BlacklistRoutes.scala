package ccas.server.routes

import java.time.{Instant, ZoneOffset}
import scala.util.chaining.*

import ccas.utils.sql.PostgresClient
import zio.http.*
import zio.json.{DeriveJsonCodec, JsonCodec}

import ccas.analysis.apps.recruitment.BlacklistApp
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubSlug, PlayerId, Username}
import ccas.server.routes.RouteHelpers.*
import ccas.utils.client.ChessComClient
import ccas.utils.errors.NotFoundException

object BlacklistRoutes {

  // --- Request/response types ---

  private[ccas] case class CreateBlacklistRequest(
    clubSlug: ClubSlug,
    usernames: List[Username],
    reason: Option[String],
    months: Option[Int]
  )
  object CreateBlacklistRequest {
    given JsonCodec[CreateBlacklistRequest] = DeriveJsonCodec.gen
  }

  private[ccas] case class BlacklistEntryResponse(
    clubSlug: String,
    playerId: Long,
    username: Option[String],
    addedAt: String,
    expiresAt: Option[String],
    reason: Option[String]
  )
  object BlacklistEntryResponse {
    given JsonCodec[BlacklistEntryResponse] = DeriveJsonCodec.gen

    def fromEntry(entry: BlacklistEntry, clubSlug: ClubSlug): BlacklistEntryResponse =
      BlacklistEntryResponse(
        clubSlug = ClubSlug.unwrap(clubSlug),
        playerId = PlayerId.unwrap(entry.playerId),
        username = entry.username.map(Username.unwrap),
        addedAt = entry.addedAt.toString,
        expiresAt = entry.expiresAt.map(_.toString),
        reason = entry.reason
      )
  }

  // --- Routes ---

  val routes: Routes[ChessComClient & PostgresClient, Nothing] = Routes(
    Method.GET / "api" / "blacklist" / string("clubSlug") -> handler { (clubSlugStr: String, _: Request) =>
      val clubSlug = ClubSlug.wrap(clubSlugStr)
      (for {
        club <- Club.selectBySlug(clubSlug).someOrFail(NotFoundException(s"Club not found: $clubSlugStr"))
        now = Instant.now()
        entries <- RecruitmentBlacklist.selectActiveByClub(club.clubId, now)
      } yield jsonResponse(Status.Ok, entries.map(BlacklistEntryResponse.fromEntry(_, clubSlug))))
        .pipe(withErrorHandling)
    },
    Method.POST / "api" / "blacklist" -> handler { (req: Request) =>
      (for {
        body <- parseJsonBody[CreateBlacklistRequest](req)
        expiresAt = body.months.map(m => Instant.now().atZone(ZoneOffset.UTC).plusMonths(m.toLong).toInstant)
        _ <- BlacklistApp.addToBlacklist(body.clubSlug, body.usernames, body.reason, expiresAt)
      } yield Response.ok)
        .pipe(withErrorHandling)
    },
    Method.DELETE / "api" / "blacklist" / string("clubSlug") / string("username") -> handler {
      (clubSlugStr: String, usernameStr: String, _: Request) =>
        (for {
          _ <- BlacklistApp.removeFromBlacklist(ClubSlug.wrap(clubSlugStr), Username.wrap(usernameStr))
        } yield Response(status = Status.NoContent))
          .pipe(withErrorHandling)
    }
  )
}
