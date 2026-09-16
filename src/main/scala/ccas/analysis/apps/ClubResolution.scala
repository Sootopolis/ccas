package ccas.analysis.apps

import java.sql.SQLException

import zio.{RIO, ZIO}
import zio.json.{jsonDiscriminator, jsonHintNames, DeriveJsonCodec, JsonCodec, SnakeCase}

import ccas.analysis.tables.{Club, ClubName}
import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.PostgresClient

/** How a caller named the club it wants. The two are exclusive by construction: an id resolves rename-proof and a name
  * has to be looked up, and a caller that had both could disagree with itself — which is the one thing resolution must
  * never have to guess about (ADR 0016).
  */
@jsonDiscriminator("kind")
@jsonHintNames(SnakeCase)
enum ClubQuery {
  case ById(clubId: ClubId)
  case BySlug(slug: ClubSlug)

  /** The club as the caller named it, for a message they have to recognise. */
  def describe: String = this match {
    case ById(clubId) => s"#${ClubId.unwrap(clubId)}"
    case BySlug(slug) => ClubSlug.unwrap(slug)
  }
}

object ClubQuery {
  given JsonCodec[ClubQuery] = DeriveJsonCodec.gen
}

/** A club as resolution reports it: its id, and the slug it holds now. */
final case class ClubRef(clubId: ClubId, slug: ClubSlug) {

  /** How to name this club to a user who asked for it by some other name: the id it will always answer to, plus the
    * name it holds now — a tombstoned club holds none.
    */
  def display: String = {
    val current = if (Club.isTombstoneSlug(slug)) { "no known name" } else { s"now ${ClubSlug.unwrap(slug)}" }
    s"#${ClubId.unwrap(clubId)} ($current)"
  }
}

object ClubRef {
  given JsonCodec[ClubRef] = DeriveJsonCodec.gen

  def fromClub(club: Club): ClubRef = ClubRef(club.clubId, club.slug)
}

/** How a club the CLI is targeting resolved: from local data ([[ClubResolution.resolve]]), then from Chess.com when
  * local data answered with history ([[ClubResolution.adjudicate]]). The lookup order and its rationale are in ADR
  * 0016. Club-scoped submit responses carry it as-is, so the CLI branches on the case rather than on a parallel status
  * field.
  *
  * [[NotLocal]] is deliberately not `NotFound`: a DB miss is not proof the club is gone from Chess.com, since it may
  * never have been ingested. Every case but [[Known]] says what was asked for, so a message built from it names
  * something the user recognises rather than an internal `_stale_<id>` placeholder — as a [[ClubQuery]] for the two
  * misses either lookup can end in, and as the slug itself for the three only a name can reach.
  */
@jsonDiscriminator("kind")
@jsonHintNames(SnakeCase)
enum ClubResolution {
  case Known(club: ClubRef)
  case Renamed(club: ClubRef, requested: ClubSlug)
  case NotLocal(requested: ClubQuery)
  case Problematic(requested: ClubQuery)
  case Ambiguous(requested: ClubSlug, holders: List[ClubRef])

  /** Chess.com says `club` holds the requested name now, and it is not a club local history pointed at — the name
    * moved. The job runs against the holder; `previous` is who held it locally, which the CLI names in its note.
    */
  case Moved(club: ClubRef, requested: ClubSlug, previous: List[ClubRef])

  /** The club a job should run against, or why the job must not run. */
  def runnable: Either[String, ClubRef] = this match {
    case Known(club)         => Right(club)
    case Renamed(club, _)    => Right(club)
    case Moved(club, _, _)   => Right(club)
    case NotLocal(requested) => Left(s"Club not found: ${requested.describe}")
    case Problematic(requested) =>
      Left(s"Club ${requested.describe} is unavailable — its canonical name could not be resolved")
    case Ambiguous(requested, holders) =>
      val candidates = holders.map(_.display).mkString(", ")
      Left(
        s"Club ${ClubSlug.unwrap(requested)} is ambiguous — no club holds that name now, and it was held by " +
          s"$candidates; name the one you meant with --club-id <id>"
      )
  }
}

object ClubResolution {
  given JsonCodec[ClubResolution] = DeriveJsonCodec.gen

  /** An id resolves straight through, rename-proof; a name asks who holds it now, and only on a miss who ever held it
    * (ADR 0016). Local-only: no Chess.com request.
    */
  def resolve(query: ClubQuery): ZIO[PostgresClient, SQLException, ClubResolution] =
    query match {
      case ClubQuery.ById(clubId) => Club.selectId(clubId).map(fromCurrent(_, query))
      case ClubQuery.BySlug(slug) =>
        ClubName.selectCurrentHolder(slug).flatMap {
          case Some(holder) => ZIO.succeed(fromCurrent(Some(holder), query))
          case None         => ClubName.selectHolders(slug).map(fromFormer(_, slug))
        }
    }

  private def fromCurrent(clubOption: Option[Club], requested: ClubQuery): ClubResolution =
    clubOption match {
      case None                            => NotLocal(requested)
      case Some(club) if club.isTombstoned => Problematic(requested)
      case Some(club)                      => Known(ClubRef.fromClub(club))
    }

  private def fromFormer(holders: List[Club], requested: ClubSlug): ClubResolution =
    holders match {
      case Nil                              => NotLocal(ClubQuery.BySlug(requested))
      case club :: Nil if club.isTombstoned => Problematic(ClubQuery.BySlug(requested))
      case club :: Nil                      => Renamed(ClubRef.fromClub(club), requested)
      case several                          => Ambiguous(requested, several.map(ClubRef.fromClub))
    }

  /** Asks Chess.com who holds the requested name when [[resolve]] answered out of history — one former holder
    * ([[Renamed]]) or several ([[Ambiguous]]). Whoever holds it now wins, known to us or not, and is persisted, so the
    * name resolves locally from here on; a reported 404 means nobody holds it and the local answer stands (#254).
    *
    * Every other case is returned untouched and fires no request: [[Known]] is already current, [[NotLocal]] is a name
    * we have never seen, and a [[Problematic]] club holds none for Chess.com to adjudicate until #254 step 4.
    *
    * A failed request degrades to the local answer: adjudication refines a usable answer rather than producing one, so
    * an unreachable Chess.com must not fail the submit.
    */
  def adjudicate(client: ChessComClient, resolution: ClubResolution): RIO[PostgresClient, ClubResolution] =
    historical(resolution) match {
      case None => ZIO.succeed(resolution)
      case Some((requested, previous)) =>
        ApiClub
          .getOptional(client, requested)
          .flatMap {
            case None => ZIO.succeed(resolution)
            case Some(apiClub) =>
              Club.upsertResolvingSlugConflict(Club.fromApi(apiClub), client)
                .as(fromHolder(apiClub, requested, previous))
          }
          .catchAll(e =>
            ZIO.logWarning(s"  Upstream check for '$requested' failed, keeping the local answer: ${e.getMessage}")
              .as(resolution)
          )
    }

  /** The answer a submit acts on: local reach first, then Chess.com for a name only local history knows. The two
    * halves are separable — `ClubDataApp` and `StatsApp` resolve without asking upstream — but every submit wants
    * both, so the pairing lives here rather than being re-composed at each gate.
    */
  def resolveAndAdjudicate(client: ChessComClient, query: ClubQuery): RIO[PostgresClient, ClubResolution] =
    resolve(query).flatMap(adjudicate(client, _))

  /** The requested name and who local history says held it, for the two answers worth adjudicating. */
  private def historical(resolution: ClubResolution): Option[(ClubSlug, List[ClubRef])] =
    resolution match {
      case Renamed(club, requested)      => Some((requested, List(club)))
      case Ambiguous(requested, holders) => Some((requested, holders))
      case _                             => None
    }

  // A holder local history passed over is `Moved` — the same job, but the CLI says the name changed hands. The rest is
  // the club we meant, answering under its canonical slug, so it is `Known` only if that is the name asked for:
  // Chess.com can answer a request under a name it has normalised away, still a former name to whoever typed it.
  private def fromHolder(apiClub: ApiClub, requested: ClubSlug, previous: List[ClubRef]): ClubResolution = {
    val holder = ClubRef(apiClub.clubId, apiClub.canonicalSlug)
    if (!previous.exists(_.clubId == holder.clubId)) { Moved(holder, requested, previous) }
    else if (holder.slug == requested) { Known(holder) }
    else { Renamed(holder, requested) }
  }
}
