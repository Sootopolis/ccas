package ccas.analysis.apps

import java.sql.SQLException

import zio.ZIO

import ccas.analysis.tables.Club
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.errors.ClubProblem
import ccas.utils.sql.PostgresClient

/** The outcome of resolving a single club the CLI is targeting. This slice covers only the verdicts reachable from a
  * *local* (DB-only) resolution; the Chess.com-hitting verdicts (`Renamed` recovered from a rename, `OnChessComOnly`,
  * `NotFound`-confirmed-upstream) arrive with the opt-in upstream reach in a later #180 slice.
  *
  *   - [[Known]]: found in our DB under a usable slug — the job can run.
  *   - [[NotLocal]]: no row for this id/slug. Deliberately NOT called `NotFound`: a DB miss is not proof the club is
  *     gone from Chess.com (it may simply never have been ingested), and only an upstream check could say otherwise.
  *   - [[Problematic]]: found, but the row is tombstoned (`_stale_<id>`) — its canonical name couldn't be resolved
  *     (a rename or slug-conflict recovery failed), so it isn't a valid job target until repaired.
  *
  * `NotLocal` and `Problematic` carry the *requested* slug (what the caller knows the club as), not the internal
  * `_stale_<id>` placeholder, so a message built from them names something the user recognises.
  */
enum ClubVerdict {
  case Known(club: Club)
  case NotLocal(slug: ClubSlug)
  case Problematic(slug: ClubSlug)

  /** The wire discriminant for a non-`Known` verdict; `None` for `Known` (the job runs, so there's no problem). */
  def problem: Option[ClubProblem] = this match {
    case Known(_)       => None
    case NotLocal(_)    => Some(ClubProblem.NotFound)
    case Problematic(_) => Some(ClubProblem.Problematic)
  }

  /** Human-readable message paired with [[problem]] for the `error` field CLI/logs display; `None` for `Known`. */
  def message: Option[String] = this match {
    case Known(_)    => None
    case NotLocal(s) => Some(s"Club not found: ${ClubSlug.unwrap(s)}")
    case Problematic(s) =>
      Some(s"Club ${ClubSlug.unwrap(s)} is unavailable — its canonical name could not be resolved")
  }
}

object ClubResolution {

  /** Resolve a club by id (rename-proof) when the caller has one, else by slug — then classify the row. Local-only: no
    * Chess.com request, so the cost profile is one indexed SELECT, unchanged from the bare `Club.selectBySlug` it
    * supersedes at the submission gate.
    */
  def resolve(clubId: Option[ClubId], slug: ClubSlug): ZIO[PostgresClient, SQLException, ClubVerdict] =
    Club.resolveByIdOrSlug(clubId, slug).map {
      case None                                          => ClubVerdict.NotLocal(slug)
      // Carry the requested slug, not `club.slug` — the latter is the `_stale_<id>` placeholder the user never sees.
      case Some(club) if Club.isTombstoneSlug(club.slug) => ClubVerdict.Problematic(slug)
      case Some(club)                                    => ClubVerdict.Known(club)
    }
}
