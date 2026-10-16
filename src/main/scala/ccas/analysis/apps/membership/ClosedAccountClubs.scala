package ccas.analysis.apps.membership

import java.time.Instant

import zio.{Chunk, Clock, RIO, Task, ZIO}

import ccas.analysis.apps.membership.MembershipChange.{later, StoredRoster}
import ccas.analysis.tables.{ClubMember, PlayerClubList}
import ccas.analysis.tables.PlayerClubList.Listing
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}
import ccas.api.player.{ApiPlayer, ApiPlayerClubs}
import ccas.utils.{ApiConcurrency, ProgressDisplay}
import ccas.utils.client.{ChessComClient, NetworkUnavailableException}
import ccas.utils.sql.PostgresClient

/** Closed accounts' club lists, which keep the memberships a club's roster drops when an account closes (ADR 0022). A
  * list carries no player id, so it is fetched under a name the account's profile has just answered to.
  */
private[membership] object ClosedAccountClubs {

  /** A closed account whose list settled nothing about the club, and why. */
  final case class Unsettled(playerId: PlayerId, username: Username, reason: String)

  /** What the kept lists change about a club's rows: ended rows they show were hidden instead, and memberships they
    * show that no row records, opened hidden.
    */
  final case class Settlement(hidden: Chunk[ClubMember], opened: Chunk[ClubMember], unsettled: Chunk[Unsettled])

  /** Two requests each: enough to work through a club's closed accounts within weeks without lengthening a run much. */
  private val ListsPerRun = 50

  /** `profile`'s club list, kept, or `None` if fetching it failed. */
  def fetchFor(client: ChessComClient, profile: ApiPlayer): RIO[PostgresClient, Option[PlayerClubList]] =
    keep(listOf(client, profile), profile.username)

  /** What the lists kept for `clubId`'s closed accounts change about the rows of those not in `exclude`, once any due
    * are fetched. A list fetched this run is reported if it settles nothing.
    */
  def settle(
    client: ChessComClient,
    clubId: ClubId,
    storedRoster: StoredRoster,
    exclude: Set[PlayerId]
  ): RIO[ProgressDisplay & PostgresClient, Settlement] =
    for {
      fetched  <- gather(client = client, clubId = clubId, exclude = exclude)
      listings <- PlayerClubList.selectListings(clubId)
    } yield settlement(
      clubId = clubId,
      storedRoster = storedRoster,
      listings = listings.filterNot(listing => exclude.contains(listing.playerId)),
      fetched = fetched
    )

  /** Fetches and keeps the lists of up to [[ListsPerRun]] of `clubId`'s closed accounts, other than `exclude`, that
    * have none kept since their state last changed. A name that now answers for nobody, or for another player, is kept
    * as having found nothing, so it is not asked again until the player is seen again.
    */
  private def gather(
    client: ChessComClient,
    clubId: ClubId,
    exclude: Set[PlayerId]
  ): RIO[ProgressDisplay & PostgresClient, Chunk[(Username, PlayerClubList)]] = {
    val label = (n: Int, total: Int) => s"  Fetching closed accounts' club lists: $n/$total"
    PlayerClubList.selectToFetch(clubId, ListsPerRun).flatMap { candidates =>
      val toFetch = candidates.filterNot((playerId, _) => exclude.contains(playerId))
      ZIO.scoped {
        ProgressDisplay.foreachParProgress(toFetch, ApiConcurrency.fiberCap(client))(label) { (playerId, username) =>
          keep(verifiedListOf(client, playerId, username), username).map(_.map(username -> _))
        }
      }.map(_.flatten)
    }
  }

  private def settlement(
    clubId: ClubId,
    storedRoster: StoredRoster,
    listings: List[Listing],
    fetched: Chunk[(Username, PlayerClubList)]
  ): Settlement = {
    val fetchedNames = fetched.map((username, list) => list.playerId -> username).toMap
    val outcomes = Chunk.from(listings).map { listing =>
      listing.playerId -> outcome(clubId, storedRoster.unlistedByPlayerId.getOrElse(listing.playerId, Nil), listing)
    }
    val nameless = fetched.collect {
      case (username, list) if list.lastOnlineOption.isEmpty =>
        Unsettled(list.playerId, username, "no list answers for it under the name we hold")
    }
    val overlapping = outcomes.collect { case (playerId, Outcome.Overlaps) => playerId }.flatMap { playerId =>
      fetchedNames.get(playerId).map(Unsettled(playerId, _, "its list's join date overlaps a recorded membership"))
    }
    Settlement(
      hidden = outcomes.collect { case (_, Outcome.Hide(row)) => row },
      opened = outcomes.collect { case (_, Outcome.Open(row)) => row },
      unsettled = nameless ++ overlapping
    )
  }

  private enum Outcome {
    case Hide(row: ClubMember)
    case Open(row: ClubMember)
    case Overlaps
    case Unchanged
  }

  /** `rows` are the player's rows the roster does not list. A guess gives way to the membership the list shows. */
  private def outcome(clubId: ClubId, rows: List[ClubMember], listing: Listing): Outcome = {
    val (matching, others) = rows.filterNot(_.sinceApproximate).partition(_.since == listing.joined)
    val lastSeen           = later(listing.joined, listing.lastOnline)
    if (others.exists(_.until.forall(_.isAfter(listing.joined)))) { Outcome.Overlaps }
    else {
      matching.headOption match {
        case Some(row) if row.until.isEmpty => Outcome.Unchanged
        case Some(row) => Outcome.Hide(row.hidden(later(row.lastSeen.getOrElse(row.since), lastSeen)))
        case None =>
          val row = ClubMember.joined(clubId = clubId, playerId = listing.playerId, since = listing.joined)
          Outcome.Open(row.hidden(lastSeen))
      }
    }
  }

  private def verifiedListOf(client: ChessComClient, playerId: PlayerId, username: Username): Task[PlayerClubList] =
    client.getUncachedOptional[ApiPlayer](ApiPlayer.getUrl(username)).flatMap {
      case Some(profile) if profile.playerId == playerId => listOf(client, profile)
      case _ =>
        Clock.instant.map { at =>
          PlayerClubList(playerId = playerId, fetchedAt = at, lastOnlineOption = None, clubs = Map.empty)
        }
    }

  private def listOf(client: ChessComClient, profile: ApiPlayer): Task[PlayerClubList] =
    for {
      clubsOption <- client.getUncachedOptional[ApiPlayerClubs](ApiPlayerClubs.getUrl(profile.username))
      fetchedAt   <- Clock.instant
    } yield PlayerClubList(
      playerId = profile.playerId,
      fetchedAt = fetchedAt,
      lastOnlineOption = clubsOption.map(_ => profile.lastOnlineAt),
      clubs =
        clubsOption.fold(Map.empty)(_.clubs.map(club => club.clubName -> Instant.ofEpochSecond(club.joined)).toMap)
    )

  /** A failed fetch keeps nothing, so the list is asked for again next run; an outage still aborts the run. */
  private def keep(fetch: Task[PlayerClubList], username: Username): RIO[PostgresClient, Option[PlayerClubList]] =
    fetch.foldZIO(
      error =>
        NetworkUnavailableException.recoverUnless(error)(
          ZIO.logWarning(s"Cannot fetch the club list of $username: ${error.getMessage}").as(None)
        ),
      list => PlayerClubList.save(list).as(Some(list))
    )
}
