package ccas.analysis.apps.history

import java.time.Instant

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction
import zio.{Chunk, RIO, Ref, ZIO}
import HistoryUtils.*

import ccas.analysis.tables.*
import ccas.api.club.ApiClubMatches
import ccas.api.misc.subtypes.*
import ccas.api.player.{ApiPlayer, ApiPlayerMatches}
import ccas.utils.client.ChessComClient
import ccas.utils.CcasLogger

private[history] object HistorySeeding {

  private val ApiParallelism = 16

  /** Retries resolution for clubs previously recorded in `unresolved_match_club`. Groups entries by slug so each unique
    * club is resolved at most once. On success, patches all matching `club_match` rows and removes the unresolved
    * entries. Returns total count of resolved entries.
    */
  def retryUnresolvedClubs(client: ChessComClient): RIO[CcasLogger & PostgresClient, Int] =
    for {
      unresolved <- UnresolvedMatchClub.selectAll
      result <-
        if (unresolved.isEmpty) { ZIO.succeed(0) }
        else {
          val grouped = unresolved.groupBy(_.slug)
          val total   = grouped.size
          CcasLogger.info(s"  Retrying ${unresolved.size} unresolved clubs ($total unique)...") *>
            ZIO.scoped {
              for {
                bar         <- CcasLogger.progressBar
                counterRef  <- Ref.make(0)
                resolvedRef <- Ref.make(0)
                _ <- ZIO.foreachDiscard(grouped.toList) { case (slug, entries) =>
                  Club.resolveOrFetch(client, slug).flatMap {
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
    * each unique player is fetched at most once. On success, ensures the player exists in the DB, patches all matching
    * `club_match_board` rows, and removes the unresolved entries. Returns total count of resolved entries.
    */
  def retryUnresolvedPlayers(client: ChessComClient): RIO[CcasLogger & PostgresClient, Int] =
    for {
      unresolved <- UnresolvedBoardPlayer.selectAll
      result <-
        if (unresolved.isEmpty) { ZIO.succeed(0) }
        else {
          val grouped = unresolved.groupBy(_.username)
          val total   = grouped.size
          CcasLogger.info(s"  Retrying ${unresolved.size} unresolved players ($total unique)...") *>
            ZIO.scoped {
              for {
                bar         <- CcasLogger.progressBar
                counterRef  <- Ref.make(0)
                resolvedRef <- Ref.make(0)
                _ <- ZIO.foreachParDiscard(grouped.toList) { case (username, entries) =>
                  (for {
                    apiPlayer <- client.get[ApiPlayer](ApiPlayer.getUrl(username))
                    playerId = apiPlayer.playerId
                    _ <- withTransaction {
                      for {
                        _ <- Player.insertIfNew(
                          Player(
                            playerId,
                            apiPlayer.joinedAt,
                            username,
                            apiPlayer.status.category,
                            apiPlayer.title,
                            Instant.now()
                          )
                        )
                        _ <- ZIO.foreachDiscard(entries) { entry =>
                          ClubMatchBoard.updatePlayerId(entry.matchId, entry.board, entry.isTeam1, playerId) *>
                            UnresolvedBoardPlayer.delete(entry.matchId, entry.board, entry.isTeam1)
                        }
                      } yield ()
                    }
                    _ <- resolvedRef.update(_ + entries.size)
                  } yield ()).ignore *>
                    counterRef.updateAndGet(_ + 1)
                      .flatMap(n => bar.print(n, total, s"  Retrying unresolved players: $n/$total"))
                }.withParallelism(ApiParallelism)
                resolved <- resolvedRef.get
              } yield resolved
            }
        }
    } yield result

  /** Fetches the club's match listing endpoint and inserts any not-yet-known match IDs as pending. */
  private[history] def seedFromClubMatches(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug
  ): RIO[CcasLogger & PostgresClient, Int] =
    (for {
      clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(clubSlug))
      allDaily     = clubMatches.dailyFinished ++ clubMatches.dailyInProgress ++ clubMatches.dailyRegistered
      dailyPending = allDaily.map(m => HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = false))
      nonDaily = clubMatches.finished.filterNot(_.timeClass.isDaily) ++
        clubMatches.inProgress.filterNot(_.timeClass.isDaily) ++
        clubMatches.registered.filterNot(_.timeClass.isDaily)
      livePending = nonDaily.map(m => HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = true))
      all         = dailyPending ++ livePending
      knownIds <- ClubMatch.selectMatchIdsForClub(clubId)
      newOnly = all.filterNot(p => knownIds.contains(p.matchId))
      _ <- insertPendingMatches(newOnly)
    } yield newOnly.size).catchAll { error =>
      CcasLogger.warn(s"  Failed to fetch club matches: ${error.getMessage}").as(0)
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
    shared: Option[SharedContext]
  ): RIO[CcasLogger & PostgresClient, MemberSeedResult] =
    for {
      sharedQueried <- shared.fold(ZIO.succeed(Set.empty[PlayerId]))(_.queriedPlayers.get)
      candidates = allMembers
        .filterNot(m => queriedIds.contains(m.playerId))
        .flatMap(m => playerById.get(m.playerId).map(s => (m.playerId, s.username)))
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
          bar <- CcasLogger.progressBar
          _ <- ZIO.foreachParDiscard(toQuery) { case (playerId, username) =>
            seedMatchesForPlayerAllClubs(client, clubId, clubSlug, playerId, username, settledMatchIds, shared)
              .foldZIO(
                error =>
                  failedMembersRef.update(_ :+ FailedMember(username, error.getMessage))
                    *> CcasLogger.warn(s"  $username: failed — ${error.getMessage}"),
                count =>
                  seedRef.update(_ + count) *> counterRef.updateAndGet(_ + 1).flatMap { n =>
                    bar.print(n, total, s"  Querying member matches: $n/$total")
                  }
              )
          }.withParallelism(ApiParallelism)
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
    */
  private def seedMatchesForPlayerAllClubs(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug,
    playerId: PlayerId,
    username: Username,
    settledMatchIds: Set[ClubMatchId],
    shared: Option[SharedContext]
  ): RIO[PostgresClient, Int] =
    shared match {
      case None => seedMatchesForPlayer(client, clubId, clubSlug, playerId, username, settledMatchIds)
      case Some(sc) =>
        for {
          playerMatches <- client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(username))
          allMatches = playerMatches.finished ++ playerMatches.inProgress ++ playerMatches.registered

          // Seed for primary club (with settled filtering)
          primaryCount <- seedMatchesFromList(clubId, clubSlug, allMatches, settledMatchIds)
          _ <- HistoryMemberQuery.upsert(HistoryMemberQuery(clubId, playerId, Instant.now()))

          // Seed for other resolved clubs (without settled filtering)
          otherClubs <- sc.resolvedClubs.get.map(_.removed(clubSlug).toList)
          _ <- ZIO.foreachDiscard(otherClubs) { case (otherSlug, otherClubId) =>
            seedMatchesFromList(otherClubId, otherSlug, allMatches, Set.empty) *>
              HistoryMemberQuery.upsert(HistoryMemberQuery(otherClubId, playerId, Instant.now()))
          }

          _ <- sc.queriedPlayers.update(_ + playerId)
        } yield primaryCount
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
    settledMatchIds: Set[ClubMatchId]
  ): RIO[PostgresClient, Int] =
    for {
      playerMatches <- client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(username))
      allMatches = playerMatches.finished ++ playerMatches.inProgress ++ playerMatches.registered
      count <- seedMatchesFromList(clubId, clubSlug, allMatches, settledMatchIds)
      _     <- HistoryMemberQuery.upsert(HistoryMemberQuery(clubId, playerId, Instant.now()))
    } yield count

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
