package ccas.analysis.apps.history

import java.time.Instant

import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction
import zio.{Chunk, RIO, Ref, ZIO}
import zio.http.URL
import HistoryUtils.*

import ccas.analysis.apps.PlayerUpdater
import ccas.analysis.tables.*
import ccas.api.club.ApiClubMatches
import ccas.api.clubmatch.{ApiDailyMatch, ApiMatchBoard}
import ccas.api.clubmatch.ApiDailyMatch.{ApiDailyMatchRegistered, MatchPlayerStarted}
import ccas.api.clubmatch.ApiMatchBoard.ApiBoardPlayer
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.*
import ccas.api.player.{ApiPlayer, ApiPlayerMatches}
import ccas.utils.client.{ChessComClient, HttpStatusException}
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
    * each unique stale username is fetched at most once on the happy path. On 404, falls back to per-entry rename
    * recovery: reads the current username from the board endpoint (cross-referenced with the opposing side's known
    * identity) and retries with the rediscovered name. On success, ensures the player row is up to date (archiving
    * prior state to `player_snapshot` when a rename is detected), patches `club_match_board`, and deletes the entry.
    * Returns total count of resolved entries.
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
                  resolveByUsername(client, username, entries)
                    .catchSome {
                      case e: HttpStatusException if e.statusCode == 404 =>
                        recoverEntriesAfter404(client, username, entries)
                    }
                    .catchAll { error =>
                      CcasLogger.warn(s"  Retry $username: ${error.getMessage}").as(0)
                    }
                    .flatMap(n => resolvedRef.update(_ + n)) *>
                    counterRef.updateAndGet(_ + 1)
                      .flatMap(n => bar.print(n, total, s"  Retrying unresolved players: $n/$total"))
                }.withParallelism(ApiParallelism)
                resolved <- resolvedRef.get
              } yield resolved
            }
        }
    } yield result

  /** Fetches a player by `username` and reconciles the result against our `player` table: archives prior state to
    * `player_snapshot` when the existing row differs (via [[PlayerUpdater.archiveAndUpdate]]), inserts a fresh row when
    * absent, and links every passed `entries` row via [[ClubMatchBoard.updatePlayerId]] before deleting from
    * `unresolved_board_player`. Errors from the HTTP fetch propagate so callers can catch 404s for rename recovery.
    * Returns the number of entries fully resolved.
    */
  private def resolveByUsername(
    client: ChessComClient,
    username: Username,
    entries: Iterable[UnresolvedBoardPlayer]
  ): RIO[CcasLogger & PostgresClient, Int] =
    for {
      apiPlayer <- client.get[ApiPlayer](ApiPlayer.getUrl(username))
      playerId       = apiPlayer.playerId
      statusCategory = apiPlayer.status.category
      now            = Instant.now()
      _ <- withTransaction {
        for {
          _ <- Player.selectIdForUpdate(playerId).flatMap {
            case Some(existing) =>
              ZIO.whenDiscard(!existing.stateMatches(username, statusCategory, apiPlayer.title)) {
                PlayerUpdater.archiveAndUpdate(existing, username, statusCategory, apiPlayer.title, now, client)
              }
            case None =>
              val since = if (statusCategory == PlayerStatusCategory.Active) { now }
              else { apiPlayer.lastOnlineAt }
              Player.insertIfNew(
                Player(playerId, apiPlayer.joinedAt, username, statusCategory, apiPlayer.title, since)
              ).unit
          }
          _ <- ZIO.foreachDiscard(entries) { entry =>
            ClubMatchBoard.updatePlayerId(entry.matchId, entry.board, entry.isTeam1, playerId) *>
              UnresolvedBoardPlayer.delete(entry.matchId, entry.board, entry.isTeam1)
          }
        } yield ()
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
  ): RIO[CcasLogger & PostgresClient, Int] =
    ZIO.foldLeft(entries)(0) { (acc, entry) =>
      recoverRenamedUsername(client, entry).flatMap {
        case Some(newUsername) =>
          CcasLogger.info(
            s"  Renamed $staleUsername → $newUsername (match ${entry.matchId} board ${entry.board})"
          ) *> resolveByUsername(client, newUsername, Chunk(entry))
        case None =>
          CcasLogger.warn(
            s"  Rename recovery failed for $staleUsername (match ${entry.matchId} board ${entry.board}); leaving row"
          ).as(0)
      }.catchAll { error =>
        CcasLogger.warn(
          s"  Rename recovery errored for $staleUsername (match ${entry.matchId} board ${entry.board}): ${error.getMessage}"
        ).as(0)
      }.map(acc + _)
    }

  /** Rediscovers the current username for a renamed player on a specific board. Fetches the board endpoint to see
    * both sides' current usernames, then identifies which of the two is ours by eliminating the opposing side: prefers
    * the DB-first path (the opposing player is already resolved in `club_match_board` → look up their current username
    * in `player`), falling back to the match endpoint if the opposing side is also unresolved. Returns `None` when the
    * recovery can't disambiguate, when the board endpoint is still serving cached stale data, or when the opposing
    * identity isn't present on the board.
    */
  private def recoverRenamedUsername(
    client: ChessComClient,
    entry: UnresolvedBoardPlayer
  ): RIO[CcasLogger & PostgresClient, Option[Username]] =
    for {
      boardData <- client.get[ApiMatchBoard](ApiMatchBoard.dailyUrl(entry.matchId, entry.board.toInt))
      boardUsernames = extractBoardUsernames(boardData)
      recovered <- ZIO.when(boardUsernames.size == 2) {
        opposingCurrentUsername(client, entry).map(_.flatMap { otherCurrent =>
          boardUsernames.filterNot(_ == otherCurrent) match {
            case ours :: Nil if ours != entry.username => Some(ours)
            case _                                     => None
          }
        })
      }
    } yield recovered.flatten

  /** Extracts the distinct usernames that appear on either side of any game on a board. For closed/deleted accounts
    * Chess.com returns a bare URL (`Left(URL)`) in place of the `ApiBoardPlayer` object; we still recover the username
    * from the URL's last path segment.
    */
  private def extractBoardUsernames(boardData: ApiMatchBoard): List[Username] =
    boardData.games.toList.flatMap { game =>
      List(
        extractSideUsername(game.white),
        extractSideUsername(game.black)
      )
    }.distinct

  private def extractSideUsername(side: Either[URL, ApiBoardPlayer]): Username =
    side match {
      case Right(player) => player.username
      case Left(url)     => Username.wrap(url.path.segments.last)
    }

  /** Finds the opposing player's current username on this board, preferring the DB-first path. If the opposing side
    * is already linked on `club_match_board`, reads their current username from `player`. Otherwise falls back to the
    * match endpoint's match-time username (which is possibly stale, but still authoritative for the common case where
    * only one side was renamed).
    */
  private def opposingCurrentUsername(
    client: ChessComClient,
    entry: UnresolvedBoardPlayer
  ): RIO[CcasLogger & PostgresClient, Option[Username]] =
    for {
      rows <- ClubMatchBoard.selectMatch(entry.matchId)
      opposingPidOpt = rows.find(_.board == entry.board).flatMap { row =>
        if (entry.isTeam1) { row.team2PlayerId } else { row.team1PlayerId }
      }
      result <- opposingPidOpt match {
        case Some(pid) => Player.selectId(pid).map(_.map(_.username))
        case None      => opposingUsernameFromMatchEndpoint(client, entry)
      }
    } yield result

  /** Fallback used when the opposing side is also unresolved in `club_match_board`: fetches the match endpoint and
    * reads the opposing player's match-time username for the given board. Registered matches have no boards assigned
    * yet, and cancelled matches only expose players without a `board` field, so both return `None` naturally.
    */
  private def opposingUsernameFromMatchEndpoint(
    client: ChessComClient,
    entry: UnresolvedBoardPlayer
  ): RIO[PostgresClient, Option[Username]] =
    client.get[ApiDailyMatch](ApiDailyMatch.getUrl(entry.matchId)).map {
      case _: ApiDailyMatchRegistered => None
      case dailyMatch =>
        val opposingTeam = if (entry.isTeam1) { dailyMatch.teams.team2 }
        else { dailyMatch.teams.team1 }
        opposingTeam.players.collectFirst {
          case p: MatchPlayerStarted if p.board.path.segments.lastOption.exists(_.toShort == entry.board) =>
            p.username
        }
    }

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
