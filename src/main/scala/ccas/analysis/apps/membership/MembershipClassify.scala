package ccas.analysis.apps.membership

import java.time.Instant

import ccas.utils.sql.PostgresClient
import zio.{Chunk, RIO, ZIO}

import ccas.analysis.apps.UsernameRenameResolver
import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.*
import ccas.analysis.tables.*
import ccas.api.misc.enums.{PlayerStatusCategory, Title}
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.client.{ChessComClient, NetworkUnavailableException}
import ccas.utils.{ApiConcurrency, ProgressDisplay}


private[membership] object MembershipClassify {

  // --- Phase B: Classify API members ---

  final case class PhaseBResult(
    resolvedIds: Set[PlayerId],
    changes: Chunk[MemberChangeSummary],
    newPlayers: Chunk[Player],
    updatedPlayers: Chunk[Player],
    archivedSnapshots: Chunk[PlayerSnapshot],
    newMemberships: Chunk[ClubMember],
    closedMemberships: Chunk[ClubMember]
  )

  private case class PhaseBMemberResult(
    resolvedId: PlayerId,
    changes: Chunk[MemberChangeSummary],
    newPlayers: Chunk[Player],
    updatedPlayers: Chunk[Player],
    archivedSnapshots: Chunk[PlayerSnapshot],
    newMemberships: Chunk[ClubMember],
    closedMemberships: Chunk[ClubMember]
  )

  def classifyApiMembers(
    client: ChessComClient,
    clubId: ClubId,
    apiMap: Map[Username, Long],
    storedRoster: StoredRoster,
    now: Instant,
    trustUsernames: Boolean = true
  ): RIO[ProgressDisplay & PostgresClient, PhaseBResult] = {
    val label = (n: Int, total: Int) => s"  Classifying API members: $n/$total"
    ZIO.scoped {
      for {
        results <- ProgressDisplay.foreachParProgress(apiMap, ApiConcurrency.fiberCap(client))(label) {
          case (username, joinedEpoch) =>
            classifyOneMember(client, clubId, username, joinedEpoch, storedRoster, now, trustUsernames)
        }
      } yield PhaseBResult(
        resolvedIds = results.map(_.resolvedId).toSet,
        changes = results.flatMap(_.changes),
        newPlayers = results.flatMap(_.newPlayers),
        updatedPlayers = results.flatMap(_.updatedPlayers),
        archivedSnapshots = results.flatMap(_.archivedSnapshots),
        newMemberships = results.flatMap(_.newMemberships),
        closedMemberships = results.flatMap(_.closedMemberships)
      )
    }
  }

  private def classifyOneMember(
    client: ChessComClient,
    clubId: ClubId,
    username: Username,
    joinedEpoch: Long,
    storedRoster: StoredRoster,
    now: Instant,
    trustUsernames: Boolean
  ): RIO[PostgresClient, PhaseBMemberResult] = {
    val since = Instant.ofEpochSecond(joinedEpoch)
    def resolved(playerId: PlayerId) =
      PhaseBMemberResult(playerId, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty)

    storedRoster.membersByUsername.get(username) match {
      case Some(state) if state.member.sinceApproximate =>
        // Replace approximate with authoritative timestamp
        ClubMember.replaceSince(clubId, state.player.playerId, state.member.since, since)
          .as(resolved(state.player.playerId))

      case Some(state) if state.member.since == since =>
        // Unchanged member
        ZIO.succeed(resolved(state.player.playerId))

      case Some(state) =>
        // Rejoin: a different `since`. The old membership had ended by the time the new one began.
        val closedMember = storedRoster.close(member = state.member, at = since)
        val newMember    = ClubMember.joined(clubId = clubId, playerId = state.player.playerId, since = since)
        val rejoined = Rejoined(
          ts = since,
          previousLastSeen = closedMember.lastSeen.getOrElse(since),
          previousUntil = closedMember.until.getOrElse(since)
        )
        val change = MemberChangeSummary(state.player.playerId, username, Chunk(rejoined))
        ZIO.succeed(
          PhaseBMemberResult(
            resolvedId = state.player.playerId,
            changes = Chunk(change),
            newPlayers = Chunk.empty,
            updatedPlayers = Chunk.empty,
            archivedSnapshots = Chunk.empty,
            newMemberships = Chunk(newMember),
            closedMemberships = Chunk(closedMember)
          )
        )

      case None =>
        // Unknown by username — check the club's former members first, then fall back to API
        if (!trustUsernames) { fetchAndClassifyNewMember(client, clubId, username, since, storedRoster, now) }
        else {
          storedRoster.knownPlayersByUsername.get(username) match {
            case None => fetchAndClassifyNewMember(client, clubId, username, since, storedRoster, now)
            case Some(knownPlayer) =>
              // A former member joining again. A current one would have matched `membersByUsername` above:
              // both maps are keyed by the name each player holds now, and only one player holds a name.
              val playerId  = knownPlayer.playerId
              val newMember = ClubMember.joined(clubId = clubId, playerId = playerId, since = since)
              val change    = MemberChangeSummary(playerId, username, Chunk(JoinedClub(since)))
              ZIO.succeed(
                PhaseBMemberResult(
                  resolvedId = playerId,
                  changes = Chunk(change),
                  newPlayers = Chunk.empty,
                  updatedPlayers = Chunk.empty,
                  archivedSnapshots = Chunk.empty,
                  newMemberships = Chunk(newMember),
                  closedMemberships = Chunk.empty
                )
              )
          }
        }
    }
  }

  private def fetchAndClassifyNewMember(
    client: ChessComClient,
    clubId: ClubId,
    username: Username,
    since: Instant,
    storedRoster: StoredRoster,
    now: Instant
  ): RIO[PostgresClient, PhaseBMemberResult] =
    UsernameRenameResolver.fetchOrRecover(client, username).flatMap { apiPlayer =>
      val playerId       = apiPlayer.playerId
      val statusCategory = apiPlayer.status.category

      storedRoster.membersByPlayerId.get(playerId) match {
        case Some(state) =>
          // Username change: same player ID, different username
          val change =
            playerChanges(state.player, storedRoster.heldName(state), username, statusCategory, apiPlayer.title, now)
          val summary = MemberChangeSummary(playerId, username, change.changes)
          ZIO.succeed(
            PhaseBMemberResult(
              resolvedId = playerId,
              changes = Chunk(summary),
              newPlayers = Chunk.empty,
              updatedPlayers = Chunk.fromIterable(change.updated),
              archivedSnapshots = Chunk.fromIterable(change.archived),
              newMemberships = Chunk.empty,
              closedMemberships = Chunk.empty
            )
          )

        case None =>
          // Check if player exists in DB at all
          (Player.selectId(playerId) <*> PlayerName.selectCurrentName(playerId)).map {
            case (Some(existing), heldNameOption) =>
              // Player exists but not current club member — joined club
              val newMember = ClubMember.joined(clubId = clubId, playerId = playerId, since = since)
              val change    = playerChanges(existing, heldNameOption, username, statusCategory, apiPlayer.title, now)
              val summary   = MemberChangeSummary(playerId, username, JoinedClub(since) +: change.changes)
              PhaseBMemberResult(
                resolvedId = playerId,
                changes = Chunk(summary),
                newPlayers = Chunk.empty,
                updatedPlayers = Chunk.fromIterable(change.updated),
                archivedSnapshots = Chunk.fromIterable(change.archived),
                newMemberships = Chunk(newMember),
                closedMemberships = Chunk.empty
              )

            case (None, _) =>
              // Brand new player — create with all fields, no snapshot
              val player = Player(playerId, apiPlayer.joinedAt, username, statusCategory, apiPlayer.title, now)
              val member  = ClubMember.joined(clubId = clubId, playerId = playerId, since = since)
              val summary = MemberChangeSummary(playerId, username, Chunk(NewMember(since)))
              PhaseBMemberResult(
                resolvedId = playerId,
                changes = Chunk(summary),
                newPlayers = Chunk(player),
                updatedPlayers = Chunk.empty,
                archivedSnapshots = Chunk.empty,
                newMemberships = Chunk(member),
                closedMemberships = Chunk.empty
              )
          }
      }
    }

  // --- Phase C: Classify disappeared members ---

  final case class PhaseCResult(
    changes: Chunk[MemberChangeSummary],
    updatedPlayers: Chunk[Player],
    archivedSnapshots: Chunk[PlayerSnapshot],
    closedMemberships: Chunk[ClubMember]
  )

  // Says when the membership ended, if it did; `classifyDisappeared` turns that into the closed row.
  private case class PhaseCMemberResult(
    changes: Chunk[MemberChangeSummary],
    updatedPlayers: Chunk[Player],
    archivedSnapshots: Chunk[PlayerSnapshot],
    endedAtOption: Option[Instant]
  )

  def classifyDisappeared(
    client: ChessComClient,
    storedRoster: StoredRoster,
    resolvedIds: Set[PlayerId],
    apiMap: Map[Username, Long],
    now: Instant
  ): RIO[ProgressDisplay & PostgresClient, PhaseCResult] = {
    val disappeared = storedRoster.membersByPlayerId.values.filterNot(s => resolvedIds.contains(s.player.playerId))
    val label       = (n: Int, total: Int) => s"  Classifying disappeared members: $n/$total"
    ZIO.scoped {
      for {
        results <- ProgressDisplay.foreachParProgress(disappeared, ApiConcurrency.fiberCap(client))(label) { state =>
          classifyOneDisappeared(client, state, storedRoster.heldName(state), apiMap, now).map(state.member -> _)
        }
      } yield PhaseCResult(
        changes = results.flatMap((_, result) => result.changes),
        updatedPlayers = results.flatMap((_, result) => result.updatedPlayers),
        archivedSnapshots = results.flatMap((_, result) => result.archivedSnapshots),
        closedMemberships = results.flatMap((member, result) => result.endedAtOption.map(storedRoster.close(member, _)))
      )
    }
  }

  private def classifyOneDisappeared(
    client: ChessComClient,
    state: MemberState,
    heldNameOption: Option[Username],
    apiMap: Map[Username, Long],
    now: Instant
  ): RIO[PostgresClient, PhaseCMemberResult] = {
    // Short-circuit already-closed players: status flipped to non-Active via another path (other club's refresh,
    // Recruitment, History) but `ClubMember.until` was left open. Skip the wasted `ApiPlayer` fetch; close the
    // membership row at the stored closure timestamp (`player.since`). Tradeoff: misses rare Chess.com reinstatement;
    // any reopened account that interacts with our tracked clubs is rediscovered via Phase B next run.
    if (state.player.status != PlayerStatusCategory.Active) {
      ZIO.succeed(
        PhaseCMemberResult(
          changes = Chunk.empty,
          updatedPlayers = Chunk.empty,
          archivedSnapshots = Chunk.empty,
          endedAtOption = Some(state.player.since)
        )
      )
    } else {
      classifyOneDisappearedActive(client, state, heldNameOption, apiMap, now)
    }
  }

  private def classifyOneDisappearedActive(
    client: ChessComClient,
    state: MemberState,
    heldNameOption: Option[Username],
    apiMap: Map[Username, Long],
    now: Instant
  ): RIO[PostgresClient, PhaseCMemberResult] = {
    val playerId    = state.player.playerId
    val oldUsername = state.player.username

    client.getUncached[ApiPlayer](ApiPlayer.getUrl(oldUsername)).foldZIO(
      // A systemic outage must not be swallowed into the match-ref fallback (which would misclassify the member as
      // left/closed/unresolvable on incomplete data); re-raise so the run aborts before persist.
      error => NetworkUnavailableException.recoverUnless(error)(matchRefFallback(client, state, apiMap, now)),
      apiPlayer =>
        if (apiPlayer.playerId != playerId) {
          matchRefFallback(client, state, apiMap, now)
        } else {
          // Site 1 short-circuit guarantees `state.player.status == Active`; the original `isFreshClosure` guard
          // collapses to `statusCategory != Active` here.
          val statusCategory    = apiPlayer.status.category
          val lastOnlineInstant = Instant.ofEpochSecond(apiPlayer.lastOnline)
          val change =
            playerChanges(state.player, heldNameOption, apiPlayer.username, statusCategory, apiPlayer.title, now)

          if (statusCategory == PlayerStatusCategory.Active) {
            val allChanges = change.changes :+ LeftClub(now)
            ZIO.succeed(
              PhaseCMemberResult(
                changes = Chunk(MemberChangeSummary(playerId, apiPlayer.username, allChanges)),
                updatedPlayers = Chunk.fromIterable(change.updated),
                archivedSnapshots = Chunk.fromIterable(change.archived),
                endedAtOption = Some(now)
              )
            )
          } else {
            // AccountClosed conveys the Active → non-Active transition; drop the redundant StatusChange that
            // `playerChanges` also emits. Closed account can't be a member, so skip the /pub/player/{u}/clubs
            // sanity check.
            val filteredExtra = change.changes.filterNot(_.isInstanceOf[StatusChange])
            val allChanges    = filteredExtra :+ AccountClosed(lastOnlineInstant, statusCategory)
            ZIO.succeed(
              PhaseCMemberResult(
                changes = Chunk(MemberChangeSummary(playerId, apiPlayer.username, allChanges)),
                updatedPlayers = Chunk.fromIterable(change.updated),
                archivedSnapshots = Chunk.fromIterable(change.archived),
                endedAtOption = Some(lastOnlineInstant)
              )
            )
          }
        }
    )
  }

  private case class PlayerChangeResult(
    updated: Option[Player],
    archived: Option[PlayerSnapshot],
    changes: Chunk[MemberChange]
  )

  /** Compares new API data against the existing player state and the name it holds now. If anything changed, returns
    * the updated Player, an archive snapshot of the old state, and the changes.
    */
  private def playerChanges(
    player: Player,
    heldNameOption: Option[Username],
    username: Username,
    statusCategory: PlayerStatusCategory,
    title: Option[Title],
    now: Instant
  ): PlayerChangeResult =
    if (!player.stateMatches(heldNameOption, username, statusCategory, title)) {
      val archive = player.toSnapshot
      val updated = player.copy(username = username, status = statusCategory, title = title, since = now)
      val changes = Chunk.newBuilder[MemberChange]
      if (player.status != statusCategory) { changes += StatusChange(now, player.status) }
      if (player.username != username) { changes += UsernameChange(now, player.username) }
      PlayerChangeResult(Some(updated), Some(archive), changes.result())
    } else { PlayerChangeResult(None, None, Chunk.empty) }

  private def matchRefFallback(
    client: ChessComClient,
    state: MemberState,
    apiMap: Map[Username, Long],
    now: Instant
  ): RIO[PostgresClient, PhaseCMemberResult] = {
    val playerId = state.player.playerId

    def unresolvable: PhaseCMemberResult = PhaseCMemberResult(
      changes = Chunk(
        MemberChangeSummary(playerId, state.player.username, Chunk(Unresolvable(now, state.player.username)))
      ),
      updatedPlayers = Chunk.empty,
      archivedSnapshots = Chunk.empty,
      endedAtOption = Some(now)
    )

    // The resolver subsumes the prior bespoke `resolveUsernameFromMatchRef` + `resolveUsernameFromTournamentRef`
    // helpers — Tier B's tournamentFallback covers both. The verified `ApiPlayer` is returned by `resolveAndVerify`
    // so we no longer need a follow-up profile fetch + `onProfileFetchFailed` guard. Note `resolveAndVerify` does
    // NOT auto-reconcile: the caller propagates the rename through `updatedPlayers` so the membership reconcile
    // transaction commits the rename alongside membership changes.
    UsernameRenameResolver
      .resolveAndVerify(client, state.player.username, Some(playerId), tournamentFallback = true)
      .flatMap {
        case None                                      => ZIO.succeed(unresolvable)
        case Some((resolvedUsername, resolvedProfile)) =>
          onProfileResolved(state, apiMap, now, resolvedUsername, resolvedProfile)
      }
  }

  private def onProfileResolved(
    state: MemberState,
    apiMap: Map[Username, Long],
    now: Instant,
    resolvedUsername: Username,
    resolvedProfile: ApiPlayer
  ): RIO[PostgresClient, PhaseCMemberResult] = {
    val playerId          = state.player.playerId
    val oldUsername       = state.player.username
    val statusCategory    = resolvedProfile.status.category
    val lastOnlineInstant = Instant.ofEpochSecond(resolvedProfile.lastOnline)
    val archive           = state.player.toSnapshot
    val updated = state.player.copy(
      username = resolvedUsername,
      status = statusCategory,
      title = resolvedProfile.title,
      since = now
    )

    // Site 1 short-circuit guarantees `state.player.status == Active`, so the old `statusChangeOption` and
    // `isFreshClosure` guards (designed for an arbitrary stored status) collapse: any non-Active `statusCategory`
    // is a fresh closure here; a StatusChange would always be redundant with the AccountClosed below.
    val baseChanges = Chunk(UsernameChange(now, oldUsername))

    if (statusCategory == PlayerStatusCategory.Active) {
      val hasLeft    = !apiMap.contains(resolvedUsername)
      val leftOption = Option.when(hasLeft)(LeftClub(now))
      ZIO.succeed(
        PhaseCMemberResult(
          changes = Chunk(MemberChangeSummary(playerId, resolvedUsername, baseChanges ++ leftOption)),
          updatedPlayers = Chunk(updated),
          archivedSnapshots = Chunk(archive),
          endedAtOption = Option.when(hasLeft)(now)
        )
      )
    } else {
      // Closed/banned account can't be a club member. Skip the `checkClubMembership` round-trip and close the
      // membership row at `lastOnline` unconditionally; see the sibling branch in `classifyOneDisappearedActive`.
      val allChanges = baseChanges :+ AccountClosed(lastOnlineInstant, statusCategory)
      ZIO.succeed(
        PhaseCMemberResult(
          changes = Chunk(MemberChangeSummary(playerId, resolvedUsername, allChanges)),
          updatedPlayers = Chunk(updated),
          archivedSnapshots = Chunk(archive),
          endedAtOption = Some(lastOnlineInstant)
        )
      )
    }
  }
}
