package ccas.analysis.apps.membership

import java.time.Instant

import zio.{Chunk, RIO, ZIO}

import ccas.analysis.apps.UsernameRenameResolver
import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.*
import ccas.analysis.tables.*
import ccas.api.misc.enums.{PlayerStatusCategory, Title}
import ccas.api.misc.subtypes.{ClubId, PlayerId, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.{ApiConcurrency, ProgressDisplay}
import ccas.utils.client.{ChessComClient, NetworkUnavailableException}
import ccas.utils.sql.PostgresClient

private[membership] object MembershipClassify {

  // --- Phase B: Classify API members ---

  final case class PhaseBResult(
    resolvedIds: Set[PlayerId],
    changes: Chunk[MemberChangeSummary],
    newPlayers: Chunk[Player],
    updatedPlayers: Chunk[Player],
    archivedSnapshots: Chunk[PlayerSnapshot],
    newMemberships: Chunk[ClubMember],
    closedMemberships: Chunk[ClubMember],
    reopenedMemberships: Chunk[ClubMember]
  )

  private case class PhaseBMemberResult(
    resolvedId: PlayerId,
    changes: Chunk[MemberChangeSummary],
    newPlayers: Chunk[Player],
    updatedPlayers: Chunk[Player],
    archivedSnapshots: Chunk[PlayerSnapshot],
    newMemberships: Chunk[ClubMember],
    closedMemberships: Chunk[ClubMember],
    reopenedMemberships: Chunk[ClubMember],
    matchedByProfile: Boolean
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
        kept = withoutNamesGivenUp(results)
      } yield PhaseBResult(
        resolvedIds = kept.map(_.resolvedId).toSet,
        changes = kept.flatMap(_.changes),
        newPlayers = kept.flatMap(_.newPlayers),
        updatedPlayers = kept.flatMap(_.updatedPlayers),
        archivedSnapshots = kept.flatMap(_.archivedSnapshots),
        newMemberships = kept.flatMap(_.newMemberships),
        closedMemberships = kept.flatMap(_.closedMemberships),
        reopenedMemberships = kept.flatMap(_.reopenedMemberships)
      )
    }
  }

  /** A name a member gave up can be listed for whoever took it, and still reach that member by the name we hold
    * (docs/chess-com-api.md, "Usernames change, and a freed one can be taken"). The entry their profile answered for is
    * theirs, and the other is read on the next reconcile, once the rename is saved.
    */
  private def withoutNamesGivenUp(results: Chunk[PhaseBMemberResult]): Chunk[PhaseBMemberResult] = {
    val answered = results.filter(_.matchedByProfile).map(_.resolvedId).toSet
    results.filter(result => result.matchedByProfile || !answered.contains(result.resolvedId))
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
    val since     = Instant.ofEpochSecond(joinedEpoch)
    val unchanged = PlayerChangeResult(updated = None, archived = None, changes = Chunk.empty)

    storedRoster.membersByUsername.get(username) match {
      case Some(state) =>
        ZIO.succeed(classifyListedJoin(storedRoster, clubId, state.player.playerId, username, since, unchanged))

      case None =>
        // Unknown by username — check the club's former members first, then fall back to API
        if (!trustUsernames) { fetchAndClassifyNewMember(client, clubId, username, since, storedRoster, now) }
        else {
          storedRoster.knownPlayersByUsername.get(username) match {
            // A former member joining again. An active account with an open row would have matched `membersByUsername`
            // above: both maps are keyed by the name each player holds now, and only one holds it.
            case Some(knownPlayer) if knownPlayer.status == PlayerStatusCategory.Active =>
              ZIO.succeed(classifyListedJoin(storedRoster, clubId, knownPlayer.playerId, username, since, unchanged))
            // A closed account listed again has reopened, which only its profile records.
            case _ => fetchAndClassifyNewMember(client, clubId, username, since, storedRoster, now)
          }
        }
    }
  }

  /** A player listed as joined at `since`, whose latest row decides (ADR 0026). If it started then, it is the same
    * membership, and the row reopens unless it is open already. A join before its start, or one an ended row was last
    * seen after, cannot be placed, so it changes no row and is reported. Any other join opens a new row, and the latest
    * row ends at that join if it was still running then, since they had left by then.
    */
  private def classifyListedJoin(
    storedRoster: StoredRoster,
    clubId: ClubId,
    playerId: PlayerId,
    username: Username,
    since: Instant,
    change: PlayerChangeResult
  ): PhaseBMemberResult = {
    val rows =
      storedRoster.membersByPlayerId.get(playerId).map(_.member).toList ++
        storedRoster.unlistedByPlayerId.getOrElse(playerId, Nil)
    def result(
      memberChanges: Chunk[MemberChange],
      newMemberships: Chunk[ClubMember],
      closedMemberships: Chunk[ClubMember],
      reopenedMemberships: Chunk[ClubMember]
    ) = {
      val changes = memberChanges ++ change.changes
      PhaseBMemberResult(
        resolvedId = playerId,
        changes = Chunk.fromIterable(Option.when(changes.nonEmpty)(MemberChangeSummary(playerId, username, changes))),
        newPlayers = Chunk.empty,
        updatedPlayers = Chunk.fromIterable(change.updated),
        archivedSnapshots = Chunk.fromIterable(change.archived),
        newMemberships = newMemberships,
        closedMemberships = closedMemberships,
        reopenedMemberships = reopenedMemberships,
        matchedByProfile = false
      )
    }
    def opening(memberChange: MemberChange, closedMemberships: Chunk[ClubMember]) =
      result(
        memberChanges = Chunk(memberChange),
        newMemberships = Chunk(ClubMember.joined(clubId = clubId, playerId = playerId, since = since)),
        closedMemberships = closedMemberships,
        reopenedMemberships = Chunk.empty
      )
    rows.maxByOption(_.since) match {
      case None         => opening(JoinedClub(since), Chunk.empty)
      case Some(latest) =>
        // An ended row's last sighting is normally a roster listing it under its old join, so a fresh join should not
        // come before then. A hidden row's can be only its account's last visit, which does not show membership.
        val seenSince = latest.until.exists(_.isAfter(since)) && latest.lastSeen.exists(!_.isBefore(since))
        if (latest.since == since) {
          val reopened = latest.copy(lastSeen = None, until = None)
          result(
            memberChanges = Chunk.empty,
            newMemberships = Chunk.empty,
            closedMemberships = Chunk.empty,
            reopenedMemberships = Chunk(reopened).filter(_ != latest)
          )
        } else if (latest.since.isAfter(since) || seenSince) {
          result(
            memberChanges = Chunk(UnsettledJoin(since, latest)),
            newMemberships = Chunk.empty,
            closedMemberships = Chunk.empty,
            reopenedMemberships = Chunk.empty
          )
        } else if (latest.until.forall(_.isAfter(since))) {
          val closed = storedRoster.close(member = latest, at = since)
          opening(Rejoined(ts = since, previous = PreviousMembership.of(closed)), Chunk(closed))
        } else { opening(JoinedClub(since), Chunk.empty) }
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
          val change =
            playerChanges(state.player, storedRoster.heldName(state), username, statusCategory, apiPlayer.title, now)
          ZIO.succeed(classifyListedJoin(storedRoster, clubId, playerId, username, since, change))

        case None =>
          // Check if player exists in DB at all
          (Player.selectId(playerId) <*> PlayerName.selectCurrentName(playerId)).map {
            case (Some(existing), heldNameOption) =>
              val change = playerChanges(existing, heldNameOption, username, statusCategory, apiPlayer.title, now)
              classifyListedJoin(storedRoster, clubId, playerId, username, since, change)

            case (None, _) =>
              // Brand new player — create with all fields, no snapshot
              val player  = Player(playerId, apiPlayer.joinedAt, username, statusCategory, apiPlayer.title, now)
              val member  = ClubMember.joined(clubId = clubId, playerId = playerId, since = since)
              val summary = MemberChangeSummary(playerId, username, Chunk(NewMember(since)))
              PhaseBMemberResult(
                resolvedId = playerId,
                changes = Chunk(summary),
                newPlayers = Chunk(player),
                updatedPlayers = Chunk.empty,
                archivedSnapshots = Chunk.empty,
                newMemberships = Chunk(member),
                closedMemberships = Chunk.empty,
                reopenedMemberships = Chunk.empty,
                matchedByProfile = false
              )
          }
      }
    }.map(_.copy(matchedByProfile = true))

  // --- Phase C: Classify disappeared members ---

  final case class PhaseCResult(
    changes: Chunk[MemberChangeSummary],
    updatedPlayers: Chunk[Player],
    archivedSnapshots: Chunk[PlayerSnapshot],
    closedMemberships: Chunk[ClubMember],
    hiddenMemberships: Chunk[ClubMember]
  )

  /** The member's row as this roster leaves it, if it changed: ended, or hidden by a closed account. */
  private case class PhaseCMemberResult(
    changes: Chunk[MemberChangeSummary],
    updatedPlayers: Chunk[Player],
    archivedSnapshots: Chunk[PlayerSnapshot],
    rowOption: Option[ClubMember]
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
          classifyOneDisappeared(client, storedRoster, state, apiMap, now)
        }
        rows = results.flatMap(_.rowOption)
      } yield PhaseCResult(
        changes = results.flatMap(_.changes),
        updatedPlayers = results.flatMap(_.updatedPlayers),
        archivedSnapshots = results.flatMap(_.archivedSnapshots),
        closedMemberships = rows.filter(_.until.nonEmpty),
        hiddenMemberships = rows.filter(_.until.isEmpty)
      )
    }
  }

  private def classifyOneDisappeared(
    client: ChessComClient,
    storedRoster: StoredRoster,
    state: MemberState,
    apiMap: Map[Username, Long],
    now: Instant
  ): RIO[PostgresClient, PhaseCMemberResult] = {
    val playerId    = state.player.playerId
    val oldUsername = state.player.username

    client.getUncached[ApiPlayer](ApiPlayer.getUrl(oldUsername)).foldZIO(
      // A systemic outage must not be swallowed into the match-ref fallback (which would misclassify the member as
      // left/closed/unresolvable on incomplete data); re-raise so the run aborts before persist.
      error =>
        NetworkUnavailableException.recoverUnless(error)(matchRefFallback(client, storedRoster, state, apiMap, now)),
      apiPlayer =>
        if (apiPlayer.playerId != playerId) {
          matchRefFallback(client, storedRoster, state, apiMap, now)
        } else {
          val statusCategory = apiPlayer.status.category
          val change = playerChanges(
            player = state.player,
            heldNameOption = storedRoster.heldName(state),
            username = apiPlayer.username,
            statusCategory = statusCategory,
            title = apiPlayer.title,
            now = now
          )

          if (statusCategory == PlayerStatusCategory.Active) {
            val allChanges = change.changes :+ LeftClub(now)
            ZIO.succeed(
              PhaseCMemberResult(
                changes = Chunk(MemberChangeSummary(playerId, apiPlayer.username, allChanges)),
                updatedPlayers = Chunk.fromIterable(change.updated),
                archivedSnapshots = Chunk.fromIterable(change.archived),
                rowOption = Some(storedRoster.close(member = state.member, at = now))
              )
            )
          } else {
            closedAccount(
              client = client,
              storedRoster = storedRoster,
              state = state,
              profile = apiPlayer,
              otherChanges = change.changes.filterNot(_.isInstanceOf[StatusChange]),
              updatedPlayers = Chunk.fromIterable(change.updated),
              archivedSnapshots = Chunk.fromIterable(change.archived),
              now = now
            )
          }
        }
    )
  }

  /** A member the roster dropped because their account closed, with `otherChanges` besides. That ends the membership
    * only if the account's club list, kept as at closure, leaves the club out; without a list to say so, the membership
    * stands, hidden (ADR 0022).
    */
  private def closedAccount(
    client: ChessComClient,
    storedRoster: StoredRoster,
    state: MemberState,
    profile: ApiPlayer,
    otherChanges: Chunk[MemberChange],
    updatedPlayers: Chunk[Player],
    archivedSnapshots: Chunk[PlayerSnapshot],
    now: Instant
  ): RIO[PostgresClient, PhaseCMemberResult] =
    ClosedAccountClubs.fetchFor(client, profile).map { listOption =>
      val statusCategory = profile.status.category
      // The reconcile has just confirmed the club's slug, which is the one a list fetched now gives it.
      val leftFirst = listOption.exists { list =>
        list.lastOnlineOption.nonEmpty && !list.clubs.keySet.exists(storedRoster.slugs.contains)
      }
      val (changes, row) =
        if (leftFirst) {
          val leaving = otherChanges ++ statusChange(state.player, statusCategory, now) :+ LeftClub(now)
          (leaving, storedRoster.close(member = state.member, at = now))
        } else {
          // AccountClosed conveys the status change, so a StatusChange would repeat it.
          val lastSeen = storedRoster.hiddenAt(member = state.member, lastOnline = profile.lastOnlineAt)
          (otherChanges :+ AccountClosed(lastSeen, statusCategory), state.member.hidden(lastSeen))
        }
      PhaseCMemberResult(
        changes = Chunk(MemberChangeSummary(state.player.playerId, profile.username, changes)),
        updatedPlayers = updatedPlayers,
        archivedSnapshots = archivedSnapshots,
        rowOption = Some(row)
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
      changes ++= statusChange(player, statusCategory, now)
      if (player.username != username) { changes += UsernameChange(now, player.username) }
      PlayerChangeResult(Some(updated), Some(archive), changes.result())
    } else { PlayerChangeResult(None, None, Chunk.empty) }

  private def statusChange(player: Player, statusCategory: PlayerStatusCategory, now: Instant): Option[MemberChange] =
    Option.when(player.status != statusCategory)(StatusChange(now, player.status))

  private def matchRefFallback(
    client: ChessComClient,
    storedRoster: StoredRoster,
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
      rowOption = Some(storedRoster.close(member = state.member, at = now))
    )

    // The resolver subsumes the prior bespoke `resolveUsernameFromMatchRef` + `resolveUsernameFromTournamentRef`
    // helpers — Tier B's tournamentFallback covers both. The verified `ApiPlayer` is returned by `resolveAndVerify`
    // so we no longer need a follow-up profile fetch + `onProfileFetchFailed` guard. Note `resolveAndVerify` does
    // NOT auto-reconcile: the caller propagates the rename through `updatedPlayers` so the membership reconcile
    // transaction commits the rename alongside membership changes.
    UsernameRenameResolver
      .resolveAndVerify(client, state.player.username, Some(playerId), tournamentFallback = true)
      .flatMap {
        case None => ZIO.succeed(unresolvable)
        case Some((resolvedUsername, resolvedProfile)) =>
          onProfileResolved(client, storedRoster, state, apiMap, now, resolvedUsername, resolvedProfile)
      }
  }

  private def onProfileResolved(
    client: ChessComClient,
    storedRoster: StoredRoster,
    state: MemberState,
    apiMap: Map[Username, Long],
    now: Instant,
    resolvedUsername: Username,
    resolvedProfile: ApiPlayer
  ): RIO[PostgresClient, PhaseCMemberResult] = {
    val playerId       = state.player.playerId
    val oldUsername    = state.player.username
    val statusCategory = resolvedProfile.status.category
    val archive        = state.player.toSnapshot
    val updated = state.player.copy(
      username = resolvedUsername,
      status = statusCategory,
      title = resolvedProfile.title,
      since = now
    )
    val baseChanges = Chunk(UsernameChange(now, oldUsername))

    if (statusCategory == PlayerStatusCategory.Active) {
      val hasLeft    = !apiMap.contains(resolvedUsername)
      val leftOption = Option.when(hasLeft)(LeftClub(now))
      val changes    = baseChanges ++ statusChange(state.player, statusCategory, now) ++ leftOption
      ZIO.succeed(
        PhaseCMemberResult(
          changes = Chunk(MemberChangeSummary(playerId, resolvedUsername, changes)),
          updatedPlayers = Chunk(updated),
          archivedSnapshots = Chunk(archive),
          rowOption = Option.when(hasLeft)(storedRoster.close(member = state.member, at = now))
        )
      )
    } else {
      closedAccount(
        client = client,
        storedRoster = storedRoster,
        state = state,
        profile = resolvedProfile,
        otherChanges = baseChanges,
        updatedPlayers = Chunk(updated),
        archivedSnapshots = Chunk(archive),
        now = now
      )
    }
  }
}
