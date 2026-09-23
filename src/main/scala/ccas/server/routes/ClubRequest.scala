package ccas.server.routes

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import zio.{Chunk, IO, RIO, ZIO}
import zio.http.Request
import zio.json.{jsonField, DeriveJsonCodec, JsonCodec}

import ccas.analysis.apps.{ClubQuery, ClubResolution, NamedClub}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.BadRequestException
import ccas.utils.sql.PostgresClient

/** What a synchronous request that names a club did with it. `club` labels it for a human, as `ClubJobResult`'s does.
  * `result` is present exactly when `resolution` found a club to act on, so a caller reads a miss or an ambiguity from
  * the resolution rather than from a status code; a failure past resolution is still an HTTP error.
  */
private[ccas] final case class ClubResult[A](
  club: String,
  resolution: ClubResolution,
  @jsonField("result") resultOption: Option[A]
) {

  /** The club acted on and what came of it, or why nothing was done. */
  def toEither: Either[String, (NamedClub, A)] =
    resolution.runnable.flatMap(ref => resultOption.map(ref -> _).toRight(s"the server did not act on $club"))
}

object ClubResult {
  given [A: JsonCodec]: JsonCodec[ClubResult[A]] = DeriveJsonCodec.gen
}

/** The server half of naming a club. Every route that takes one reads it as a [[ClubQuery]] and resolves it the way a
  * job submit does — local reach, then Chess.com for a name only history knows — so a name means the same club
  * whichever command it is typed into (ADR 0016, #254). A body carries the query as JSON; a URL carries it as exactly
  * one of two query parameters.
  */
object ClubRequest {
  private val ClubIdParam = "clubId"
  private val SlugParam   = "slug"

  def queryString(query: ClubQuery): String = query match {
    case ClubQuery.ById(clubId) => s"$ClubIdParam=${ClubId.unwrap(clubId)}"
    case ClubQuery.BySlug(slug) => s"$SlugParam=${URLEncoder.encode(ClubSlug.unwrap(slug), StandardCharsets.UTF_8)}"
  }

  /** Every value given for each parameter, so a repeated one is refused rather than silently narrowed to its first. */
  def fromParams(clubIds: Chunk[String], slugs: Chunk[String]): Either[String, ClubQuery] =
    (clubIds.toList, slugs.toList) match {
      case (List(raw), Nil) =>
        raw.trim.toLongOption
          .filter(_ > 0L)
          .map(id => ClubQuery.ById(ClubId(id)))
          .toRight(s"'$ClubIdParam' must be a positive integer, got '$raw'")
      case (Nil, List(raw)) if raw.trim.nonEmpty => Right(ClubQuery.BySlug(ClubSlug(raw.trim)))
      case (Nil, List(_))                        => Left(s"'$SlugParam' must not be blank")
      case _ => Left(s"name the club with exactly one '$ClubIdParam' or '$SlugParam'")
    }

  def query(req: Request): IO[BadRequestException, ClubQuery] =
    ZIO
      .fromEither(fromParams(req.queryParams(ClubIdParam), req.queryParams(SlugParam)))
      .mapError(BadRequestException(_))

  def resolve(query: ClubQuery): RIO[ChessComClient & PostgresClient, ClubResolution] =
    ZIO.serviceWithZIO[ChessComClient](ClubResolution.resolveAndAdjudicate(_, query))

  /** The club's name once resolution found one, else the query as it arrived. */
  def label(query: ClubQuery, resolution: ClubResolution): String =
    resolution.runnable.fold(_ => query.describe, club => ClubSlug.unwrap(club.slug))

  /** Resolves `query` and runs `action` against the club it names, if it names one. */
  def run[R, A](query: ClubQuery)(
    action: NamedClub => RIO[R, A]
  ): RIO[R & ChessComClient & PostgresClient, ClubResult[A]] =
    for {
      resolution   <- resolve(query)
      resultOption <- ZIO.foreach(resolution.runnable.toOption)(action)
    } yield ClubResult(club = label(query, resolution), resolution = resolution, resultOption = resultOption)
}
