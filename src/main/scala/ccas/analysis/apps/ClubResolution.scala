package ccas.analysis.apps

import java.sql.SQLException

import zio.ZIO
import zio.json.{jsonDiscriminator, jsonHintNames, DeriveJsonCodec, JsonCodec, SnakeCase}

import ccas.analysis.tables.{Club, ClubName}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.sql.PostgresClient

/** A club as resolution reports it: its id, and the slug it holds now. */
final case class ClubRef(clubId: ClubId, slug: ClubSlug)

object ClubRef {
  given JsonCodec[ClubRef] = DeriveJsonCodec.gen

  def fromClub(club: Club): ClubRef = ClubRef(club.clubId, club.slug)
}

/** How a club the CLI is targeting resolved, from local data only; Chess.com adjudication of a former or ambiguous
  * name arrives later (#254). The lookup order and its rationale are in ADR 0016. Club-scoped submit responses carry
  * it as-is, so the CLI branches on the case rather than on a parallel status field.
  *
  * [[NotLocal]] is deliberately not `NotFound`: a DB miss is not proof the club is gone from Chess.com, since it may
  * never have been ingested. Every case but [[Known]] carries the *requested* slug, so a message built from it names
  * something the user recognises rather than an internal `_stale_<id>` placeholder.
  */
@jsonDiscriminator("kind")
@jsonHintNames(SnakeCase)
enum ClubResolution {
  case Known(club: ClubRef)
  case Renamed(club: ClubRef, requested: ClubSlug)
  case NotLocal(requested: ClubSlug)
  case Problematic(requested: ClubSlug)
  case Ambiguous(requested: ClubSlug, holders: List[ClubRef])

  /** The club a job should run against, or why the job must not run. */
  def runnable: Either[String, ClubRef] = this match {
    case Known(club)         => Right(club)
    case Renamed(club, _)    => Right(club)
    case NotLocal(requested) => Left(s"Club not found: ${ClubSlug.unwrap(requested)}")
    case Problematic(requested) =>
      Left(s"Club ${ClubSlug.unwrap(requested)} is unavailable — its canonical name could not be resolved")
    case Ambiguous(requested, holders) =>
      val candidates = holders.map { club =>
        val current =
          if (Club.isTombstoneSlug(club.slug)) { "no known name" }
          else { s"now ${ClubSlug.unwrap(club.slug)}" }
        s"#${ClubId.unwrap(club.clubId)} ($current)"
      }.mkString(", ")
      Left(
        s"Club ${ClubSlug.unwrap(requested)} is ambiguous — no club holds that name now, and it was held by $candidates"
      )
  }
}

object ClubResolution {
  given JsonCodec[ClubResolution] = DeriveJsonCodec.gen

  /** Resolve by id (rename-proof) when the caller has one; otherwise ask who holds the slug now, and only on a miss who
    * ever held it (ADR 0016). Local-only: no Chess.com request.
    */
  def resolve(clubIdOption: Option[ClubId], slug: ClubSlug): ZIO[PostgresClient, SQLException, ClubResolution] =
    clubIdOption match {
      case Some(clubId) => Club.selectId(clubId).map(fromCurrent(_, slug))
      case None =>
        ClubName.selectCurrentHolder(slug).flatMap {
          case Some(holder) => ZIO.succeed(fromCurrent(Some(holder), slug))
          case None         => ClubName.selectHolders(slug).map(fromFormer(_, slug))
        }
    }

  private def fromCurrent(clubOption: Option[Club], requested: ClubSlug): ClubResolution =
    clubOption match {
      case None                            => NotLocal(requested)
      case Some(club) if club.isTombstoned => Problematic(requested)
      case Some(club)                      => Known(ClubRef.fromClub(club))
    }

  private def fromFormer(holders: List[Club], requested: ClubSlug): ClubResolution =
    holders match {
      case Nil                              => NotLocal(requested)
      case club :: Nil if club.isTombstoned => Problematic(requested)
      case club :: Nil                      => Renamed(ClubRef.fromClub(club), requested)
      case several                          => Ambiguous(requested, several.map(ClubRef.fromClub))
    }
}
