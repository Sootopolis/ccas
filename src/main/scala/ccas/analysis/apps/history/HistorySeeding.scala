package ccas.analysis.apps.history

import java.time.Instant

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction
import zio.{Chunk, RIO, Ref, ZIO}
import HistoryUtils.*

import ccas.analysis.apps.{
  ClubSlugRenameResolver,
  PlayerUpdater,
  UsernameRenameResolver,
  withClubSlugRenameRecovery,
  withPlayerRenameRecovery
}
import ccas.analysis.tables.*
import ccas.api.club.ApiClubMatches
import ccas.api.misc.subtypes.*
import ccas.api.player.{ApiPlayer, ApiPlayerMatches}
import ccas.utils.client.{ChessComClient, onNotFound}
import ccas.utils.ApiConcurrency
import ccas.utils.ProgressDisplay

private[history] object HistorySeeding {

  /** Retries resolution for clubs previously recorded in `unresolved_match_club`. Groups entries by slug so each unique
    * club is resolved at most once. On success, patches all matching `club_match` rows and removes the unresolved
    * entries. Returns total count of resolved entries.
    */
  def retryUnresolvedClubs(client: ChessComClient): RIO[ProgressDisplay & PostgresClient, Int] =
    for {
      unresolved <- UnresolvedMatchClub.selectAll
      result <-
        if (unresolved.isEmpty) { ZIO.succeed(0) }
        else {
          val grouped = unresolved.groupBy(_.slug)
          val total   = grouped.size
          ZIO.logInfo(s"  Retrying ${unresolved.size} unresolved clubs ($total unique)...") *>
            ZIO.scoped {
              for {
                bar         <- ProgressDisplay.progressBar
                counterRef  <- Ref.make(0)
                resolvedRef <- Ref.make(0)
                _ <- ZIO.foreachDiscard(grouped.toList) { case (slug, entries) =>
                  // Apps-layer counterpart of the deleted `Club.resolveOrFetch`. Slug-rename recovery is not wired
                  // through this entry point — see `ClubSlugRenameResolver.resolveOrFetch`'s scaladoc for why.
                  ClubSlugRenameResolver.resolveOrFetch(client, slug).flatMap {
                    case Some(clubId) =>
                      withTransaction {
                        ZIO.foreachDiscard(entries) { entry =>
                          ClubMatch.updateTeamClubId(entry.matchId, entry.isTeam1, clubId) *>
                            UnresolvedMatchClub.delete(entry.matchId, entry.isTeam1)
                        }
                      } *> resolvedRef.update(_ + entries.size)
                    case None => ZIO.unit
                  }.ignore *>
                    counterRef.updateAndGet(_ + 1)
                      .flatMap(n => bar.print(n, total, s"  Retrying unresolved clubs: $n/$total"))
                }
                resolved <- resolvedRef.get
              } yield resolved
            }
        }
    } yield result

  /** Retries resolution for players previously recorded in `unresolved_board_player`. Groups entries by username so
    * each unique stale username is fetched at most once on the happy path. On 404, falls back to per-entry rename
    * recovery: reads the current username from the board endpoint (cross-referenced with the opposing side's known
    * identity) and retries with the rediscovered name. On success, ensures the player row is up to date (archiving
    * prior state to `player_snapshot` when a rename is detected), patches `club_match_board`, and deletes the entry.
    * Returns total count of resolved entries.
    */
  def retryUnresolvedPlayers(client: ChessComClient): RIO[ProgressDisplay & PostgresClient, Int] =
    for {
      unresolved <- UnresolvedBoardPlayer.selectAll
      result <-
        if (unresolved.isEmpty) { ZIO.succeed(0) }
        else {
          val grouped = unresolved.groupBy(_.username)
          val total   = grouped.size
          ZIO.logInfo(s"  Retrying ${unresolved.size} unresolved players ($total unique)...") *>
            ZIO.scoped {
              for {
                bar         <- ProgressDisplay.progressBar
                counterRef  <- Ref.make(0)
                resolvedRef <- Ref.make(0)
                _ <- ZIO.foreachParDiscard(grouped.toList) { case (username, entries) =>
                  resolveByUsername(client, username, entries)
                    .onNotFound { _ =>
                      recoverEntriesAfter404(client, username, entries)
                    }
                    .catchAll { error =>
                      ZIO.logWarning(s"  Retry $username: ${error.getMessage}").as(0)
                    }
                    .flatMap(n => resolvedRef.update(_ + n)) *>
                    counterRef.updateAndGet(_ + 1)
                      .flatMap(n => bar.print(n, total, s"  Retrying unresolved players: $n/$total"))
                }.withParallelism(ApiConcurrency.fiberCap(client))
                resolved <- resolvedRef.get
              } yield resolved
            }
        }
    } yield result

  /** Fetches a player by `username` and reconciles the result against our `player` table via
    * [[PlayerUpdater.reconcile]] (archive-to-snapshot on drift, insert on absence), then links every passed `entries`
    * row via [[ClubMatchBoard.updatePlayerId]] before deleting from `unresolved_board_player`. Errors from the HTTP
    * fetch propagate so callers can catch 404s for rename recovery. Returns the number of entries fully resolved.
    */
  private def resolveByUsername(
    client: ChessComClient,
    username: Username,
    entries: Iterable[UnresolvedBoardPlayer]
  ): RIO[ProgressDisplay & PostgresClient, Int] =
    for {
      apiPlayer <- client.get[ApiPlayer](ApiPlayer.getUrl(username))
      playerId = apiPlayer.playerId
      _ <- withTransaction {
        PlayerUpdater.reconcile(apiPlayer, client) *>
          ZIO.foreachDiscard(entries) { entry =>
            ClubMatchBoard.updatePlayerId(entry.matchId, entry.board, entry.isTeam1, playerId) *>
              UnresolvedBoardPlayer.delete(entry.matchId, entry.board, entry.isTeam1)
          }
      }
    } yield entries.size

  /** Per-entry recovery for a 404 from [[resolveByUsername]]. For each entry, tries to rediscover the current username
    * via the board endpoint and retry resolution. Errors within each entry are caught and logged so one bad entry
    * doesn't abort the rest of the group. Returns the sum of entries resolved across the group.
    */
  private def recoverEntriesAfter404(
    client: ChessComClient,
    staleUsername: Username,
    entries: Iterable[UnresolvedBoardPlayer]
  ): RIO[ProgressDisplay & PostgresClient, Int] =
    ZIO.foldLeft(entries)(0) { (acc, entry) =>
      recoverRenamedUsername(client, entry).flatMap {
        case Some(newUsername) =>
          ZIO.logInfo(
            s"  Renamed $staleUsername → $newUsername (match ${entry.matchId} board ${entry.board})"
          ) *> resolveByUsername(client, newUsername, Chunk(entry))
        case None =>
          ZIO.logWarning(
            s"  Rename recovery failed for $staleUsername (match ${entry.matchId} board ${entry.board}); leaving row"
          ).as(0)
      }.catchAll { error =>
        ZIO.logWarning(
          s"  Rename recovery errored for $staleUsername (match ${entry.matchId} board ${entry.board}): ${error.getMessage}"
        ).as(0)
      }.map(acc + _)
    }

  /** Delegates to [[UsernameRenameResolver.resolveFromBoard]]. The resolver owns the board-endpoint primitive used
    * for both this unresolved-player retry path and HistoryApp Phase 2 / other rename-recovery sites.
    */
  private def recoverRenamedUsername(
    client: ChessComClient,
    entry: UnresolvedBoardPlayer
  ): RIO[PostgresClient, Option[Username]] =
    UsernameRenameResolver.resolveFromBoard(
      client, entry.username, entry.matchId, entry.board, entry.isTeam1, isLive = false
    )

  /** Fetches the club's match listing endpoint and inserts any not-yet-known match IDs as pending. Unchanged
    * responses skip the select-known-ids + filter + insert pipeline entirely — the listing can only grow, so an
    * unchanged response means no new matches.
    */
  private[history] def seedFromClubMatches(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug,
    unchangedCounter: Ref[Int]
  ): RIO[ProgressDisplay & PostgresClient, Int] = {
    def fetch(slug: ClubSlug): RIO[PostgresClient, Int] =
      client.getCacheable[ApiClubMatches](ApiClubMatches.getUrl(slug))
        .flatMap(_.foldZIO(_ => unchangedCounter.update(_ + 1).as(0))(insertPendingFromClubMatches(clubId, _)))
    fetch(clubSlug)
      .withClubSlugRenameRecovery(client, clubSlug, Some(clubId))(fetch)
      .catchAll { error =>
        ZIO.logWarning(s"  Failed to fetch club matches: ${error.getMessage}").as(0)
      }
  }

  private def insertPendingFromClubMatches(
    clubId: ClubId,
    clubMatches: ApiClubMatches
  ): RIO[PostgresClient, Int] = {
    val allDaily     = clubMatches.dailyFinished ++ clubMatches.dailyInProgress ++ clubMatches.dailyRegistered
    val dailyPending = allDaily.map(m => HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = false))
    val nonDaily = clubMatches.finished.filterNot(_.timeClass.isDaily) ++
      clubMatches.inProgress.filterNot(_.timeClass.isDaily) ++
      clubMatches.registered.filterNot(_.timeClass.isDaily)
    val livePending = nonDaily.map(m => HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = true))
    val all         = dailyPending ++ livePending
    for {
      knownIds <- ClubMatch.selectMatchIdsForClub(clubId)
      newOnly = all.filterNot(p => knownIds.contains(p.matchId))
      _ <- insertPendingMatches(newOnly)
    } yield newOnly.size
  }

  /** Queries each un-queried member's match list to find club match IDs. Skips already-queried members unless --full.
    * When `shared` is present, also skips members already queried by a prior club in the same batch, writing
    * `HistoryMemberQuery` for the current club so future runs have correct per-club history.
    */
  def seedFromMemberMatches(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug,
    allMembers: List[ClubMember],
    queriedIds: Set[PlayerId],
    playerById: Map[PlayerId, Player],
    settledMatchIds: Set[ClubMatchId],
    shared: Option[SharedContext],
    unchangedPlayerCounter: Ref[Int]
  ): RIO[ProgressDisplay & PostgresClient, MemberSeedResult] =
    for {
      sharedQueried <- shared.fold(ZIO.succeed(Set.empty[PlayerId]))(_.queriedPlayers.get)
      // Tombstoned Player rows have a sentinel `_stale_<playerId>` username — emitting them here would 404 against
      // Chess.com on every wave with no possible recovery (we have nothing better to query with). Filtering pre-
      // partition is cheap (in-memory predicate over the playerById map). The renamed player will rejoin the queue
      // automatically on the next run after some other path (board appearance, club roster refresh) rediscovers the
      // current handle and replaces the tombstone via PlayerUpdater.reconcile.
      candidates = allMembers
        .filterNot(m => queriedIds.contains(m.playerId))
        .flatMap(m => playerById.get(m.playerId).filterNot(_.isTombstoned).map(s => (m.playerId, s.username)))
        .distinctBy(_._1)
      (toQuery, sharedSkipped) = candidates.partition { case (pid, _) => !sharedQueried.contains(pid) }

      // For members skipped via shared dedup, still record HistoryMemberQuery for this club
      _ <- ZIO.foreachDiscard(sharedSkipped) { case (playerId, _) =>
        HistoryMemberQuery.upsert(HistoryMemberQuery(clubId, playerId, Instant.now()))
      }

      counterRef       <- Ref.make(0)
      seedRef          <- Ref.make(0)
      failedMembersRef <- Ref.make(List.empty[FailedMember])
      total = toQuery.size
      _ <- ZIO.scoped {
        for {
          bar <- ProgressDisplay.progressBar
          _ <- ZIO.foreachParDiscard(toQuery) { case (playerId, username) =>
            seedMatchesForPlayerAllClubs(
              client, clubId, clubSlug, playerId, username, settledMatchIds, shared, unchangedPlayerCounter
            )
              .foldZIO(
                error =>
                  failedMembersRef.update(_ :+ FailedMember(username, error.getMessage))
                    *> ZIO.logWarning(s"  $username: failed — ${error.getMessage}"),
                count =>
                  seedRef.update(_ + count) *> counterRef.updateAndGet(_ + 1).flatMap { n =>
                    bar.print(n, total, s"  Querying member matches: $n/$total")
                  }
              )
          }.withParallelism(ApiConcurrency.fiberCap(client))
        } yield ()
      }
      seeded        <- seedRef.get
      queried       <- counterRef.get
      failedMembers <- failedMembersRef.get
    } yield MemberSeedResult(seeded, queried, failedMembers)

  private[history] def isClubDailyMatch(m: ApiPlayerMatches.ApiPlayerMatch, clubSlug: ClubSlug): Boolean =
    m.club.path.segments.lastOption.map(ClubSlug.wrap).contains(clubSlug)
      && !m.`@id`.path.segments.contains("live")

  private[history] def isClubLiveMatch(m: ApiPlayerMatches.ApiPlayerMatch, clubSlug: ClubSlug): Boolean =
    m.club.path.segments.lastOption.map(ClubSlug.wrap).contains(clubSlug)
      && m.`@id`.path.segments.contains("live")

  /** Wraps `seedMatchesForPlayer` with cross-club fan-out when `shared` is present. Fetches the player's match list
    * once, seeds matches for the primary club (with settled filtering), then seeds for all other resolved clubs in the
    * batch (without settled filtering). Records `HistoryMemberQuery` for all clubs and adds the player to the shared
    * queried set. Returns the primary club's seeded count.
    *
    * On an unchanged response, skips the seed pipeline for every club but still stamps `HistoryMemberQuery` for all
    * of them so the wave loop doesn't re-query the player on the next iteration.
    */
  private def seedMatchesForPlayerAllClubs(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug,
    playerId: PlayerId,
    username: Username,
    settledMatchIds: Set[ClubMatchId],
    shared: Option[SharedContext],
    unchangedCounter: Ref[Int]
  ): RIO[PostgresClient, Int] =
    shared match {
      case None => seedMatchesForPlayer(client, clubId, clubSlug, playerId, username, settledMatchIds, unchangedCounter)
      case Some(sc) =>
        def go(uname: Username): RIO[PostgresClient, Int] =
          for {
            result     <- client.getCacheable[ApiPlayerMatches](ApiPlayerMatches.getUrl(uname))
            otherClubs <- sc.resolvedClubs.get.map(_.removed(clubSlug).toList)
            primaryCount <- result.foldZIO(_ =>
              unchangedCounter.update(_ + 1) *> stampQueriedAllClubs(clubId, otherClubs, playerId).as(0)
            )(seedAndStampAllClubs(clubId, clubSlug, settledMatchIds, otherClubs, playerId, _))
            _ <- sc.queriedPlayers.update(_ + playerId)
          } yield primaryCount
        go(username).withPlayerRenameRecovery(client, username, Some(playerId))(go)
    }

  private def stampQueriedAllClubs(
    primaryClubId: ClubId,
    otherClubs: List[(ClubSlug, ClubId)],
    playerId: PlayerId
  ): RIO[PostgresClient, Unit] = {
    val now = Instant.now()
    HistoryMemberQuery.upsert(HistoryMemberQuery(primaryClubId, playerId, now)) *>
      ZIO.foreachDiscard(otherClubs) { case (_, otherClubId) =>
        HistoryMemberQuery.upsert(HistoryMemberQuery(otherClubId, playerId, now))
      }
  }

  private def seedAndStampAllClubs(
    primaryClubId: ClubId,
    primarySlug: ClubSlug,
    settledMatchIds: Set[ClubMatchId],
    otherClubs: List[(ClubSlug, ClubId)],
    playerId: PlayerId,
    playerMatches: ApiPlayerMatches
  ): RIO[PostgresClient, Int] = {
    val allMatches = playerMatches.finished ++ playerMatches.inProgress ++ playerMatches.registered
    for {
      primary <- seedMatchesFromList(primaryClubId, primarySlug, allMatches, settledMatchIds)
      _       <- HistoryMemberQuery.upsert(HistoryMemberQuery(primaryClubId, playerId, Instant.now()))
      _ <- ZIO.foreachDiscard(otherClubs) { case (otherSlug, otherClubId) =>
        seedMatchesFromList(otherClubId, otherSlug, allMatches, Set.empty) *>
          HistoryMemberQuery.upsert(HistoryMemberQuery(otherClubId, playerId, Instant.now()))
      }
    } yield primary
  }

  /** Filters a player's match list for a specific club and inserts matching entries as pending. */
  private def seedMatchesFromList(
    clubId: ClubId,
    clubSlug: ClubSlug,
    allMatches: Chunk[ApiPlayerMatches.ApiPlayerMatch],
    settledMatchIds: Set[ClubMatchId]
  ): RIO[PostgresClient, Int] = {
    val dailyPending = allMatches.collect {
      case m if isClubDailyMatch(m, clubSlug) =>
        HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = false)
    }
    val livePending = allMatches.collect {
      case m if isClubLiveMatch(m, clubSlug) =>
        HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = true)
    }
    val all = (dailyPending ++ livePending).filterNot(p => settledMatchIds.contains(p.matchId))
    insertPendingMatches(all).as(all.size)
  }

  def seedMatchesForPlayer(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug,
    playerId: PlayerId,
    username: Username,
    settledMatchIds: Set[ClubMatchId],
    unchangedCounter: Ref[Int]
  ): RIO[PostgresClient, Int] = {
    // `def` not `val` so `Instant.now()` is captured when the stamp actually runs (post-fetch / post-seed),
    // matching the original `HistoryMemberQuery.upsert(... Instant.now())` call inside the for-comprehension.
    def stamp = HistoryMemberQuery.upsert(HistoryMemberQuery(clubId, playerId, Instant.now()))
    def fetch(uname: Username): RIO[PostgresClient, Int] =
      client.getCacheable[ApiPlayerMatches](ApiPlayerMatches.getUrl(uname)).flatMap {
        _.foldZIO(_ => unchangedCounter.update(_ + 1) *> stamp.as(0))(
          seedMatchesFromPlayerMatches(clubId, clubSlug, settledMatchIds, _).zipLeft(stamp)
        )
      }
    fetch(username).withPlayerRenameRecovery(client, username, Some(playerId))(fetch)
  }

  private def seedMatchesFromPlayerMatches(
    clubId: ClubId,
    clubSlug: ClubSlug,
    settledMatchIds: Set[ClubMatchId],
    playerMatches: ApiPlayerMatches
  ): RIO[PostgresClient, Int] = {
    val allMatches = playerMatches.finished ++ playerMatches.inProgress ++ playerMatches.registered
    seedMatchesFromList(clubId, clubSlug, allMatches, settledMatchIds)
  }

  private def insertPendingMatches(items: Iterable[HistoryPendingMatch]): RIO[PostgresClient, Unit] =
    ZIO.foreachDiscard(items.grouped(1000).toList)(HistoryPendingMatch.insertBatch)

  /** Re-queues stale matches (unfinished or recently completed) as pending for reprocessing. When `shared` is present,
    * filters out matches already processed by a prior club in this batch.
    */
  def seedStaleMatches(clubId: ClubId, shared: Option[SharedContext]): RIO[PostgresClient, Int] =
    for {
      ids <- ClubMatch.selectStaleForClub(clubId)
      alreadyProcessed <- shared.fold(ZIO.succeed(Set.empty[ClubMatchId]))(_.processedMatches.get)
      filtered = ids.filterNot(alreadyProcessed.contains)
      _ <- insertPendingMatches(filtered.map(id => HistoryPendingMatch(clubId, id, isLive = false)))
    } yield filtered.size
}
