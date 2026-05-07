package ccas.analysis.apps.membership

import ccas.utils.sql.PostgresClient
import zio.{Chunk, RIO, Ref, Task, ZIO}

import ccas.analysis.apps.UsernameRenameResolver
import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.*
import ccas.analysis.tables.*
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.api.player.{ApiPlayer, ApiPlayerClubs}
import ccas.utils.client.ChessComClient
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
    dbState: DbState,
    now: java.time.Instant,
    trustUsernames: Boolean = true
  ): RIO[ProgressDisplay & PostgresClient, PhaseBResult] = {
    val total = apiMap.size
    ZIO.scoped {
      for {
        bar     <- ProgressDisplay.progressBar
        counter <- Ref.make(0)
        results <- ZIO.foreachPar(Chunk.from(apiMap)) { case (username, joinedEpoch) =>
          classifyOneMember(client, clubId, username, joinedEpoch, dbState, now, trustUsernames)
            <* counter.updateAndGet(_ + 1).flatMap(n => bar.print(n, total, s"  Classifying API members: $n/$total"))
        }.withParallelism(ApiConcurrency.fiberCap(client))
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
    dbState: DbState,
    now: java.time.Instant,
    trustUsernames: Boolean
  ): RIO[PostgresClient, PhaseBMemberResult] = {
    val since = java.time.Instant.ofEpochSecond(joinedEpoch)
    def resolved(playerId: PlayerId) =
      PhaseBMemberResult(playerId, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty, Chunk.empty)

    dbState.membersByUsername.get(username) match {
      case Some(state) if state.member.sinceApproximate =>
        // Replace approximate with authoritative timestamp
        ClubMember.replaceSince(clubId, state.player.playerId, state.member.since, since)
          .as(resolved(state.player.playerId))

      case Some(state) if state.member.since == since =>
        // Unchanged member
        ZIO.succeed(resolved(state.player.playerId))

      case Some(state) =>
        // Rejoin: different `since` timestamp
        val closedMember = state.member.copy(until = Some(now))
        val newMember    = ClubMember(clubId, state.player.playerId, since, None, sinceApproximate = false)
        val change = MemberChangeSummary(state.player.playerId, username, Chunk(Rejoined(since, state.member.since)))
        ZIO.succeed(
          PhaseBMemberResult(
            state.player.playerId,
            Chunk(change),
            Chunk.empty,
            Chunk.empty,
            Chunk.empty,
            Chunk(newMember),
            Chunk(closedMember)
          )
        )

      case None =>
        // Unknown by username — check trusted snapshots first, then fall back to API
        if (!trustUsernames) { fetchAndClassifyNewMember(client, clubId, username, since, dbState, now) }
        else {
          dbState.knownPlayersByUsername.get(username) match {
            case None => fetchAndClassifyNewMember(client, clubId, username, since, dbState, now)
            case Some(knownPlayer) =>
              val playerId = knownPlayer.playerId
              dbState.membersByPlayerId.get(playerId) match {
                case Some(state) =>
                  // Username change detected via trusted player lookup
                  val change =
                    MemberChangeSummary(playerId, username, Chunk(UsernameChange(now, state.player.username)))
                  val archive = state.player.toSnapshot
                  val updated = state.player.copy(username = username, since = now)
                  ZIO.succeed(
                    PhaseBMemberResult(
                      resolvedId = playerId,
                      changes = Chunk(change),
                      newPlayers = Chunk.empty,
                      updatedPlayers = Chunk(updated),
                      archivedSnapshots = Chunk(archive),
                      newMemberships = Chunk.empty,
                      closedMemberships = Chunk.empty
                    )
                  )
                case None =>
                  // Known player joined this club
                  val newMember = ClubMember(clubId, playerId, since, None, sinceApproximate = false)
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
  }

  private def fetchAndClassifyNewMember(
    client: ChessComClient,
    clubId: ClubId,
    username: Username,
    since: java.time.Instant,
    dbState: DbState,
    now: java.time.Instant
  ): RIO[PostgresClient, PhaseBMemberResult] =
    UsernameRenameResolver.fetchOrRecover(client, username).flatMap { apiPlayer =>
      val playerId       = apiPlayer.playerId
      val statusCategory = apiPlayer.status.category

      dbState.membersByPlayerId.get(playerId) match {
        case Some(state) =>
          // Username change: same player ID, different username
          val change  = playerChanges(state, username, statusCategory, apiPlayer.title, now)
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
          Player.selectId(playerId).map {
            case Some(existing) =>
              // Player exists but not current club member — joined club
              val newMember    = ClubMember(clubId, playerId, since, None, sinceApproximate = false)
              val changeChunks = Chunk.newBuilder[MemberChange]
              changeChunks += JoinedClub(since)

              val needsUpdate = !existing.stateMatches(username, statusCategory, apiPlayer.title)
              val (updatedOpt, archiveOpt) = if (needsUpdate) {
                if (existing.username != username) { changeChunks += UsernameChange(now, existing.username) }
                if (existing.status != statusCategory) { changeChunks += StatusChange(now, existing.status) }
                val archive = existing.toSnapshot
                val updated =
                  existing.copy(username = username, status = statusCategory, title = apiPlayer.title, since = now)
                (Chunk(updated), Chunk(archive))
              } else { (Chunk.empty, Chunk.empty) }

              val summary = MemberChangeSummary(playerId, username, changeChunks.result())
              PhaseBMemberResult(
                resolvedId = playerId,
                changes = Chunk(summary),
                newPlayers = Chunk.empty,
                updatedPlayers = updatedOpt,
                archivedSnapshots = archiveOpt,
                newMemberships = Chunk(newMember),
                closedMemberships = Chunk.empty
              )

            case None =>
              // Brand new player — create with all fields, no snapshot
              val player = Player(playerId, apiPlayer.joinedAt, username, statusCategory, apiPlayer.title, now)
              val member  = ClubMember(clubId, playerId, since, None, sinceApproximate = false)
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

  private case class PhaseCMemberResult(
    changes: Chunk[MemberChangeSummary],
    updatedPlayers: Chunk[Player],
    archivedSnapshots: Chunk[PlayerSnapshot],
    closedMemberships: Chunk[ClubMember]
  )

  def classifyDisappeared(
    client: ChessComClient,
    dbState: DbState,
    resolvedIds: Set[PlayerId],
    apiMap: Map[Username, Long],
    clubSlug: ClubSlug,
    now: java.time.Instant
  ): RIO[ProgressDisplay & PostgresClient, PhaseCResult] = {
    val disappearedList =
      dbState.membersByPlayerId.values.filterNot(s => resolvedIds.contains(s.player.playerId)).toList
    val total = disappearedList.size

    ZIO.scoped {
      for {
        bar     <- ProgressDisplay.progressBar
        counter <- Ref.make(0)
        results <- ZIO.foreachPar(Chunk.from(disappearedList)) { state =>
          classifyOneDisappeared(client, state, apiMap, clubSlug, now) <* counter.updateAndGet(_ + 1).flatMap { n =>
            bar.print(n, total, s"  Classifying disappeared members: $n/$total")
          }
        }.withParallelism(ApiConcurrency.fiberCap(client))
      } yield PhaseCResult(
        changes = results.flatMap(_.changes),
        updatedPlayers = results.flatMap(_.updatedPlayers),
        archivedSnapshots = results.flatMap(_.archivedSnapshots),
        closedMemberships = results.flatMap(_.closedMemberships)
      )
    }
  }

  private def classifyOneDisappeared(
    client: ChessComClient,
    state: MemberState,
    apiMap: Map[Username, Long],
    clubSlug: ClubSlug,
    now: java.time.Instant
  ): RIO[PostgresClient, PhaseCMemberResult] = {
    val playerId     = state.player.playerId
    val oldUsername  = state.player.username
    val closedMember = state.member.copy(until = Some(now))

    client.get[ApiPlayer](ApiPlayer.getUrl(oldUsername)).foldZIO(
      _ => matchRefFallback(client, state, closedMember, apiMap, clubSlug, now),
      apiPlayer =>
        if (apiPlayer.playerId != playerId) {
          matchRefFallback(client, state, closedMember, apiMap, clubSlug, now)
        } else {
          val statusCategory    = apiPlayer.status.category
          val lastOnlineInstant = java.time.Instant.ofEpochSecond(apiPlayer.lastOnline)
          val change            = playerChanges(state, apiPlayer.username, statusCategory, apiPlayer.title, now)

          val isFreshClosure =
            statusCategory != PlayerStatusCategory.Active &&
              state.player.status == PlayerStatusCategory.Active

          val primaryChange =
            if (statusCategory == PlayerStatusCategory.Active) { Some(LeftClub(now)) }
            else if (isFreshClosure) { Some(AccountClosed(lastOnlineInstant, statusCategory)) }
            else { None }

          // AccountClosed already conveys the Active → non-Active transition;
          // drop the redundant StatusChange that playerChanges also emits.
          val filteredExtra =
            if (isFreshClosure) { change.changes.filterNot(_.isInstanceOf[StatusChange]) }
            else { change.changes }

          val allChanges = filteredExtra ++ primaryChange
          val summaries =
            if (allChanges.isEmpty) { Chunk.empty }
            else { Chunk(MemberChangeSummary(playerId, apiPlayer.username, allChanges)) }

          if (statusCategory == PlayerStatusCategory.Active) {
            ZIO.succeed(
              PhaseCMemberResult(
                changes = summaries,
                updatedPlayers = Chunk.fromIterable(change.updated),
                archivedSnapshots = Chunk.fromIterable(change.archived),
                closedMemberships = Chunk(closedMember)
              )
            )
          } else {
            val closedAtLastOnline = state.member.copy(until = Some(lastOnlineInstant))
            checkClubMembership(client, clubSlug, apiPlayer.username).map { stillMember =>
              PhaseCMemberResult(
                changes = summaries,
                updatedPlayers = Chunk.fromIterable(change.updated),
                archivedSnapshots = Chunk.fromIterable(change.archived),
                closedMemberships = if (stillMember) Chunk.empty else Chunk(closedAtLastOnline)
              )
            }
          }
        }
    )
  }

  private case class PlayerChangeResult(
    updated: Option[Player],
    archived: Option[PlayerSnapshot],
    changes: Chunk[MemberChange]
  )

  /** Compares new API data against the existing player state. If anything changed, returns the updated Player, an
    * archive snapshot of the old state, and the changes.
    */
  private def playerChanges(
    state: MemberState,
    username: Username,
    statusCategory: PlayerStatusCategory,
    title: Option[ccas.api.misc.enums.Title],
    now: java.time.Instant
  ): PlayerChangeResult =
    if (!state.player.stateMatches(username, statusCategory, title)) {
      val archive = state.player.toSnapshot
      val updated = state.player.copy(username = username, status = statusCategory, title = title, since = now)
      val changes = Chunk.newBuilder[MemberChange]
      if (state.player.status != statusCategory) { changes += StatusChange(now, state.player.status) }
      if (state.player.username != username) { changes += UsernameChange(now, state.player.username) }
      PlayerChangeResult(Some(updated), Some(archive), changes.result())
    } else { PlayerChangeResult(None, None, Chunk.empty) }

  private def checkClubMembership(client: ChessComClient, clubSlug: ClubSlug, username: Username): Task[Boolean] =
    client.get[ApiPlayerClubs](ApiPlayerClubs.getUrl(username)).map { playerClubs =>
      playerClubs.clubs.exists(_.clubName == clubSlug)
    }.catchAll(_ => ZIO.succeed(false))

  private def matchRefFallback(
    client: ChessComClient,
    state: MemberState,
    closedMember: ClubMember,
    apiMap: Map[Username, Long],
    clubSlug: ClubSlug,
    now: java.time.Instant
  ): RIO[PostgresClient, PhaseCMemberResult] = {
    val playerId = state.player.playerId

    def unresolvable: PhaseCMemberResult = PhaseCMemberResult(
      changes = Chunk(
        MemberChangeSummary(playerId, state.player.username, Chunk(Unresolvable(now, state.player.username)))
      ),
      updatedPlayers = Chunk.empty,
      archivedSnapshots = Chunk.empty,
      closedMemberships = Chunk(closedMember)
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
          onProfileResolved(state, closedMember, apiMap, clubSlug, now, resolvedUsername, resolvedProfile, client)
      }
  }

  private def onProfileResolved(
    state: MemberState,
    closedMember: ClubMember,
    apiMap: Map[Username, Long],
    clubSlug: ClubSlug,
    now: java.time.Instant,
    resolvedUsername: Username,
    resolvedProfile: ApiPlayer,
    client: ChessComClient
  ): RIO[PostgresClient, PhaseCMemberResult] = {
    val playerId          = state.player.playerId
    val oldUsername       = state.player.username
    val statusCategory    = resolvedProfile.status.category
    val lastOnlineInstant = java.time.Instant.ofEpochSecond(resolvedProfile.lastOnline)
    val archive           = state.player.toSnapshot
    val updated = state.player.copy(
      username = resolvedUsername,
      status = statusCategory,
      title = resolvedProfile.title,
      since = now
    )

    val isFreshClosure =
      statusCategory != PlayerStatusCategory.Active &&
        state.player.status == PlayerStatusCategory.Active

    // Suppress the StatusChange when AccountClosed will be emitted — the closure covers it.
    val statusChangeOpt =
      Option.when(state.player.status != statusCategory && !isFreshClosure) {
        StatusChange(now, state.player.status)
      }
    val changes = Chunk(UsernameChange(now, oldUsername)) ++ statusChangeOpt

    if (statusCategory == PlayerStatusCategory.Active) {
      val stillMember = apiMap.contains(resolvedUsername)
      val leftOpt     = Option.unless(stillMember)(LeftClub(now))
      ZIO.succeed(
        PhaseCMemberResult(
          changes = Chunk(MemberChangeSummary(playerId, resolvedUsername, changes ++ leftOpt)),
          updatedPlayers = Chunk(updated),
          archivedSnapshots = Chunk(archive),
          closedMemberships = Chunk.fromIterable(Option.unless(stillMember)(closedMember))
        )
      )
    } else {
      val closedAtLastOnline = closedMember.copy(until = Some(lastOnlineInstant))
      val accountClosedOpt =
        Option.when(isFreshClosure)(AccountClosed(lastOnlineInstant, statusCategory))
      checkClubMembership(client, clubSlug, resolvedUsername).map { stillMember =>
        PhaseCMemberResult(
          changes = Chunk(MemberChangeSummary(playerId, resolvedUsername, changes ++ accountClosedOpt)),
          updatedPlayers = Chunk(updated),
          archivedSnapshots = Chunk(archive),
          closedMemberships = if (stillMember) Chunk.empty else Chunk(closedAtLastOnline)
        )
      }
    }
  }
}
