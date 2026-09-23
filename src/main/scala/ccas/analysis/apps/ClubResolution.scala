package ccas.analysis.apps

import java.sql.SQLException

import zio.{RIO, ZIO}
import zio.json.{jsonDiscriminator, jsonField, jsonHintNames, DeriveJsonCodec, JsonCodec, SnakeCase}

import ccas.analysis.tables.{Club, ClubName}
import ccas.api.club.ApiClub
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.NotFoundException
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

/** A club and the name it holds now — the only shape a job can be addressed to, since a club is reached upstream by
  * name and nothing else (ADR 0016).
  */
final case class NamedClub(clubId: ClubId, slug: ClubSlug) derives JsonCodec {

  /** How to name this club to a user who asked for it by some other name. */
  def display: String = ClubRef.of(this).display
}

/** A club resolution can only name, not act on: its id, and the name it holds now if it holds one. It holds none
  * once another club has taken that name and we have not yet seen what it answers to (ADR 0016).
  */
final case class ClubRef(clubId: ClubId, @jsonField("slug") slugOption: Option[ClubSlug]) derives JsonCodec {

  /** How to name this club to a user who asked for it by some other name: the id it will always answer to, plus the
    * name it holds now.
    */
  def display: String = {
    val current = slugOption.fold("no known name")(slug => s"now ${ClubSlug.unwrap(slug)}")
    s"#${ClubId.unwrap(clubId)} ($current)"
  }

  /** The club to act on, when it is one we can still name. */
  def namedOption: Option[NamedClub] = slugOption.map(NamedClub(clubId, _))
}

object ClubRef {
  def of(club: NamedClub): ClubRef = ClubRef(club.clubId, Some(club.slug))
}

/** How a club the CLI is targeting resolved: from local data ([[ClubResolution.resolve]]), then from Chess.com when
  * local data answered with history ([[ClubResolution.adjudicate]]). The lookup order and its rationale are in ADR
  * 0016. Club-scoped submit responses carry it as-is, so the CLI branches on the case rather than on a parallel status
  * field.
  *
  * [[NotLocal]] is deliberately not `NotFound`: a DB miss is not proof the club is gone from Chess.com, since it may
  * never have been ingested. Every case but [[Known]] says what was asked for, so a message built from it names
  * something the user recognises rather than whatever the club answers to now — as a [[ClubQuery]] for the two misses
  * either lookup can end in, and as the slug itself for the three only a name can reach.
  */
@jsonDiscriminator("kind")
@jsonHintNames(SnakeCase)
enum ClubResolution {
  case Known(club: NamedClub)
  case Renamed(club: NamedClub, requested: ClubSlug)
  case NotLocal(requested: ClubQuery)
  case Problematic(requested: ClubQuery)
  case Ambiguous(requested: ClubSlug, holders: List[ClubRef])

  /** Chess.com says `club` holds the requested name now, and it is not a club local history pointed at — the name
    * moved. The job runs against the holder; `previous` is who held it locally, which the CLI names in its note.
    */
  case Moved(club: NamedClub, requested: ClubSlug, previous: List[ClubRef])

  /** The club a job should run against, or why the job must not run. */
  def runnable: Either[String, NamedClub] = this match {
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
      case ClubQuery.ById(clubId) =>
        ClubName.selectCurrentName(clubId).flatMap {
          case Some(slug) => ZIO.succeed(Known(NamedClub(clubId, slug)))
          case None       => Club.selectId(clubId).map(_.fold(NotLocal(query))(_ => Problematic(query)))
        }
      case ClubQuery.BySlug(slug) =>
        ClubName.selectCurrentHolder(slug).flatMap {
          case Some(holder) => ZIO.succeed(Known(NamedClub(holder.clubId, slug)))
          case None         => fromFormer(slug)
        }
    }

  // Nobody holds the name now, so each club that ever held it is reported by the name it holds instead — which it may
  // have lost the same way, leaving it nameable only by id.
  private def fromFormer(requested: ClubSlug): ZIO[PostgresClient, SQLException, ClubResolution] =
    for {
      clubIds <- ClubName.selectHolders(requested)
      names   <- ClubName.selectCurrentNames(clubIds)
      holders = clubIds.map(clubId => ClubRef(clubId, names.get(clubId)))
    } yield holders match {
      case Nil           => NotLocal(ClubQuery.BySlug(requested))
      case holder :: Nil => holder.namedOption.fold(Problematic(ClubQuery.BySlug(requested)))(Renamed(_, requested))
      case several       => Ambiguous(requested, several)
    }

  /** Asks Chess.com who holds the requested name when [[resolve]] answered out of history — one former holder
    * ([[Renamed]]) or several ([[Ambiguous]]). Whoever holds it now wins, known to us or not, and is persisted, so the
    * name resolves locally from here on; a reported 404 means nobody holds it and the local answer stands (#254).
    *
    * Every other case is returned untouched and fires no request: [[Known]] is already current, [[NotLocal]] is a name
    * we have never seen, and a [[Problematic]] club holds no name of its own — asking about the one that reached it
    * is #274.
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
              Club.upsert(Club.fromApi(apiClub)).as(fromHolder(apiClub, requested, previous))
          }
          .catchAll(e =>
            ZIO.logWarning(s"  Upstream check for '$requested' failed, keeping the local answer: ${e.getMessage}")
              .as(resolution)
          )
    }

  /** The answer a request that names a club acts on: local reach first, then Chess.com for a name only local history
    * knows. The two halves are separable — `ClubDataApp` and `StatsApp` resolve without asking upstream — but every
    * such request wants both, so the pairing lives here rather than being re-composed at each gate.
    */
  def resolveAndAdjudicate(client: ChessComClient, query: ClubQuery): RIO[PostgresClient, ClubResolution] =
    resolve(query).flatMap(adjudicate(client, _))

  /** [[resolveAndAdjudicate]] for a caller with no way to report a resolution but to fail on it: a standalone app's
    * command line.
    */
  def resolveRunnable(query: ClubQuery): RIO[ChessComClient & PostgresClient, NamedClub] =
    ZIO
      .serviceWithZIO[ChessComClient](resolveAndAdjudicate(_, query))
      .flatMap(resolution => ZIO.fromEither(resolution.runnable).mapError(NotFoundException(_)))

  /** The requested name and who local history says held it, for the two answers worth adjudicating. */
  private def historical(resolution: ClubResolution): Option[(ClubSlug, List[ClubRef])] =
    resolution match {
      case Renamed(club, requested)      => Some((requested, List(ClubRef.of(club))))
      case Ambiguous(requested, holders) => Some((requested, holders))
      case _                             => None
    }

  // A holder local history passed over is `Moved` — the same job, but the CLI says the name changed hands. The rest is
  // the club we meant, answering under its canonical slug, so it is `Known` only if that is the name asked for:
  // Chess.com can answer a request under a name it has normalised away, still a former name to whoever typed it.
  private def fromHolder(apiClub: ApiClub, requested: ClubSlug, previous: List[ClubRef]): ClubResolution = {
    val holder = NamedClub(apiClub.clubId, apiClub.canonicalSlug)
    if (!previous.exists(_.clubId == holder.clubId)) { Moved(holder, requested, previous) }
    else if (holder.slug == requested) { Known(holder) }
    else { Renamed(holder, requested) }
  }
}
