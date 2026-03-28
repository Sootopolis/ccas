package ccas.analysis.apps.history

import java.time.{Instant, Duration as JDuration}
import com.augustnagro.magnum.{Transactor, sql}
import zio.{Promise, RIO, Ref, Scope, Task, UIO, URIO, ZEnvironment, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.{Client, URL}
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.tables.*
import ccas.api.club.ApiClubMatches
import ccas.api.clubmatch.{ApiDailyMatch, ApiLiveMatch}
import ccas.api.clubmatch.ApiDailyMatch.*
import ccas.api.misc.enums.*
import ccas.api.misc.subtypes.*
import ccas.api.player.{ApiPlayer, ApiPlayerClubs, ApiPlayerMatches}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.BadRequestException
import ccas.utils.sql.DataSourceLayer
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.{connectZIO, withTransaction}
import ccas.utils.{CcasLogger, OutputFile, ProgressBar, display}

/** Discovers and persists a chess club's match history by crawling the Chess.com API.
  *
  * Starting from a club's known members, it collects match IDs, fetches match data, and follows links to newly
  * discovered players via breadth-first search (BFS) waves — expanding the match graph until no new matches remain.
  *
  * ==Run Modes==
  *   - '''Default (incremental):''' Only queries members whose match lists haven't been fetched before. Only re-queues
  *     stale matches (unfinished or recently completed). This is the cheapest mode for regular updates.
  *   - '''`--full`:''' Clears member query history so every member's match list is re-fetched from the API. Use when
  *     you want a complete rebuild of the match graph (e.g., after a long gap or to pick up retroactive API changes).
  *   - '''`--refresh`:''' Re-queues ALL known matches for reprocessing, not just stale ones. Use to update match data
  *     across the board (e.g., to pick up score corrections or newly resolved fair-play removals).
  *
  * ==Workflow (4 Phases)==
  *   1. '''Initialize''' — Reconcile club membership, load current state (members, snapshots, processed counts),
  *      reset pending match statuses, and create a `HistoryRun` record.
  *   2. '''Seed''' — Collect match IDs into `history_pending_match` from three sources:
  *      the club matches endpoint, each member's match list, and stale/all existing matches.
  *      Also retries previously unresolved clubs and board players from prior runs.
  *   3. '''Process''' — BFS wave loop: fetch and persist match data in parallel batches, discover unknown players,
  *      seed their match lists, and repeat until no new pending matches remain.
  *   4. '''Finalize''' — Mark the `HistoryRun` complete, log summary stats, and write a report file.
  *
  * ==Unresolved Entities==
  * During processing, some entities may not be resolvable via the API:
  *   - '''Unresolved match clubs:''' If a team's club URL can't be resolved to a `ClubId` (club deleted or API error),
  *     the slug is recorded in `unresolved_match_club`. If ''neither'' team resolves to the target club, the match is
  *     marked `Unidentified` — data is saved but BFS expansion is skipped for that match.
  *   - '''Unresolved board players:''' If a player's username can't be resolved to a `PlayerId` (account closed or
  *     deleted), it is recorded in `unresolved_board_player`. The board row is saved with a `None` player ID.
  *
  * Both types are retried at the start of each run. Successfully resolved entries are patched in-place and removed
  * from their respective unresolved tables.
  *
  * ==Invocation==
  *   - '''CLI:''' `HistoryApp <club-slug> [--full] [--refresh]`
  *   - '''API:''' `POST /api/jobs/history` with `{"clubSlug": "...", "full": true/false, "refresh": true/false}`
  */
object HistoryApp extends ZIOAppDefault {
  private val help = "Usage: HistoryApp <club-slug> [--full] [--refresh]"

  // --- CLI entry point ---

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      args <- ZIOAppArgs.getArgs
      clubName <- args.headOption match {
        case None    => ZIO.fail(BadRequestException(help))
        case Some(s) => ZIO.succeed(ClubSlug.wrap(s))
      }
      full    = args.contains("--full")
      refresh = args.contains("--refresh")
      _ <- discover(clubName, full, refresh)
    } yield ()).provideSomeAuto(
      CcasLogger.live(showProgress = true),
      ChessComClient.live,
      Client.default,
      DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
    )

  private val BatchSize = 500

  private case class DiscoveredPlayer(playerId: PlayerId, username: Username)

  private case class MemberSeedResult(
    seeded: Int,
    queried: Int,
    failed: Int,
    failedMembers: List[(Username, String)]
  )

  private case class RunStats(
    membersQueried: Int = 0,
    membersSkipped: Int = 0,
    membersFailed: Int = 0,
    matchesSeeded: Int = 0,
    matchesProcessed: Int = 0,
    matchesFailed: Int = 0,
    matchesUnidentified: Int = 0,
    playersDiscovered: Int = 0,
    playersKnown: Int = 0,
    playersFailed: Int = 0,
    waveCount: Int = 0,
    pendingRemaining: Int = 0,
    waveDetails: List[(Int, Int)] = Nil,
    failedMatches: List[(MatchKey, String)] = Nil,
    failedMembers: List[(Username, String)] = Nil
  )

  /** Shared mutable state for concurrent Phase 3 processing: API response caches (deduplicated via Promises),
    * a player lookup map, counters for statistics, and error tracking.
    */
  private class ProcessingContext(
    val client: ChessComClient,
    val clubId: ClubId,
    val clubSlug: ClubSlug,
    val matchCache: Ref[Map[ClubMatchId, Promise[Throwable, ApiDailyMatch]]],
    val liveMatchCache: Ref[Map[ClubMatchId, Promise[Throwable, ApiLiveMatch]]],
    val discoveryCache: Ref[Map[String, Promise[Throwable, Option[PlayerId]]]],
    val clubCache: Ref[Map[String, Promise[Throwable, Option[ClubId]]]],
    val knownPlayers: Ref[Map[String, PlayerId]],
    val newPlayers: Ref[Set[DiscoveredPlayer]],
    val matchesProcessed: Ref[Int],
    val matchesFailed: Ref[Int],
    val matchesUnidentified: Ref[Int],
    val playersDiscovered: Ref[Int],
    val playersKnown: Ref[Int],
    val playersFailed: Ref[Int],
    val failedMatches: Ref[List[(MatchKey, String)]]
  )

  private object ProcessingContext {
    def make(
      client: ChessComClient,
      clubId: ClubId,
      clubSlug: ClubSlug,
      initialKnownPlayers: Map[String, PlayerId]
    ): ZIO[Any, Nothing, ProcessingContext] =
      for {
        matchCache          <- Ref.make(Map.empty[ClubMatchId, Promise[Throwable, ApiDailyMatch]])
        liveMatchCache      <- Ref.make(Map.empty[ClubMatchId, Promise[Throwable, ApiLiveMatch]])
        discoveryCache      <- Ref.make(Map.empty[String, Promise[Throwable, Option[PlayerId]]])
        clubCache           <- Ref.make(Map.empty[String, Promise[Throwable, Option[ClubId]]])
        knownPlayers        <- Ref.make(initialKnownPlayers)
        newPlayers          <- Ref.make(Set.empty[DiscoveredPlayer])
        matchesProcessed    <- Ref.make(0)
        matchesFailed       <- Ref.make(0)
        matchesUnidentified <- Ref.make(0)
        playersDiscovered   <- Ref.make(0)
        playersKnown        <- Ref.make(0)
        playersFailed       <- Ref.make(0)
        failedMatches       <- Ref.make(List.empty[(MatchKey, String)])
      } yield new ProcessingContext(
        client,
        clubId,
        clubSlug,
        matchCache,
        liveMatchCache,
        discoveryCache,
        clubCache,
        knownPlayers,
        newPlayers,
        matchesProcessed,
        matchesFailed,
        matchesUnidentified,
        playersDiscovered,
        playersKnown,
        playersFailed,
        failedMatches
      )
  }

  /** Main entry point: orchestrates the 4-phase discover workflow for a club's match history. */
  def discover(
    clubSlug: ClubSlug,
    full: Boolean = false,
    refresh: Boolean = false,
    trigger: RunTrigger = RunTrigger.Cli
  ): RIO[CcasLogger & ChessComClient & Transactor, Unit] =
    for {
      client     <- ZIO.service[ChessComClient]
      transactor <- ZIO.service[Transactor]
      logger     <- ZIO.service[CcasLogger]

      // === Phase 1: Initialize ===
      _ <- CcasLogger.info(s"=== HistoryApp: $clubSlug ===")
      _ <- CcasLogger.info("Phase 1: Initializing...")
      _ <- MembershipApp.reconcile(clubSlug, trackRun = false)
      club <- Club.selectBySlug(clubSlug)
        .someOrFail(IllegalStateException(s"Club '$clubSlug' not found after reconcile"))
      clubId = club.clubId
      (allMembers, latestSnaps, processedCount, queriedIds) <-
        ClubMember.selectClub(clubId) <&>
        PlayerSnapshot.selectLatest <&>
        ClubMatch.countForClub(clubId) <&>
        HistoryMemberQuery.selectClubPlayerIds(clubId)
      _ <- HistoryPendingMatch.resetStatuses(clubId)
      startedAt = Instant.now()
      runId <- HistoryRun.insert(clubId, trigger, startedAt)
      snapByPlayerId = latestSnaps.map(s => s.playerId -> s).toMap
      _ <- CcasLogger.info(
        s"  Members: ${allMembers.size}, Processed matches: $processedCount, Queried members: ${queriedIds.size}"
      )

      // Setup for interrupt safety: create ProcessingContext and Phase 2 tracking Refs before entering the
      // interrupt-guarded block so partial progress is always available to the onInterrupt handler.
      knownPlayersInit = latestSnaps.map(s => s.username.value -> s.playerId).toMap
      ctx           <- ProcessingContext.make(client, clubId, clubSlug, knownPlayersInit)
      seedClubRef   <- Ref.make(0)
      memberSeedRef <- Ref.make(MemberSeedResult(0, 0, 0, Nil))
      seedStaleRef  <- Ref.make(0)

      // === Phases 2-3: Seed + Process (interrupt-safe) ===
      waveStats <- {
        for {
          // Phase 2: Seed match IDs
          _ <- CcasLogger.info("Phase 2: Seeding match IDs...")
          (resolvedClubs, resolvedPlayers) <- retryUnresolvedClubs(client) <&> retryUnresolvedPlayers(client)
          _ <- ZIO.whenDiscard(resolvedClubs > 0 || resolvedPlayers > 0) {
            CcasLogger.info(s"  Resolved $resolvedClubs clubs, $resolvedPlayers players from previous runs")
          }
          _ <- ZIO.whenDiscard(full) {
            CcasLogger.info("  --full: clearing member query history") *> HistoryMemberQuery.deleteClub(clubId)
          }
          effectiveQueriedIds = if (full) { Set.empty[PlayerId] } else { queriedIds }

          seedClub <- seedFromClubMatches(client, clubId, clubSlug)
          _        <- seedClubRef.set(seedClub)
          _        <- CcasLogger.info(s"  Club matches endpoint: $seedClub new match IDs")

          memberSeed <-
            seedFromMemberMatches(client, clubId, clubSlug, allMembers, effectiveQueriedIds, snapByPlayerId)
          _ <- memberSeedRef.set(memberSeed)
          membersSkipped = allMembers.size - memberSeed.queried - memberSeed.failed
          _ <- CcasLogger.info(
            s"  Member match lists: ${memberSeed.seeded} new IDs (queried: ${memberSeed.queried}, skipped: $membersSkipped, failed: ${memberSeed.failed})"
          )

          seedStale <- seedStaleMatches(clubId, refresh)
          _         <- seedStaleRef.set(seedStale)
          _         <- CcasLogger.info(s"  Stale match refresh: $seedStale matches queued")

          // Phase 3: Process matches (BFS waves)
          _ <- CcasLogger.info("Phase 3: Processing matches...")
          ws <- processWaves(ctx)
        } yield ws
      }.onInterrupt {
        for {
          sc   <- seedClubRef.get
          ms   <- memberSeedRef.get
          ss   <- seedStaleRef.get
          skip = allMembers.size - ms.queried - ms.failed
          _ <- finalizeInterrupted(ctx, runId, startedAt, clubSlug, ms, skip, sc, ss)
                 .provideEnvironment(ZEnvironment(logger, transactor)).orDie
        } yield ()
      }

      // === Phase 4: Finalize ===
      seedClub   <- seedClubRef.get
      memberSeed <- memberSeedRef.get
      seedStale  <- seedStaleRef.get
      membersSkipped = allMembers.size - memberSeed.queried - memberSeed.failed
      completedAt = Instant.now()
      totalStats = waveStats.copy(
        membersQueried = memberSeed.queried,
        membersSkipped = membersSkipped,
        membersFailed = memberSeed.failed,
        matchesSeeded = seedClub + memberSeed.seeded + seedStale,
        failedMembers = memberSeed.failedMembers
      )
      _ <- HistoryRun.complete(runId, completedAt, totalStats.matchesProcessed, totalStats.playersDiscovered)
      _ <- logSummary(totalStats, startedAt, completedAt)
      _ <- OutputFile.writeAndLog("history", clubSlug, formatReport(totalStats, clubSlug, startedAt, completedAt))
    } yield ()

  // === Phase 2: Seeding ===

  /** Retries resolution for clubs previously recorded in `unresolved_match_club`. Groups entries by slug so each
    * unique club is resolved at most once. On success, patches all matching `club_match` rows and removes the
    * unresolved entries. Returns total count of resolved entries.
    */
  private def retryUnresolvedClubs(client: ChessComClient): RIO[CcasLogger & Transactor, Int] =
    for {
      unresolved <- UnresolvedMatchClub.selectAll
      result <- if (unresolved.isEmpty) { ZIO.succeed(0) } else {
        val grouped = unresolved.groupBy(_.slug)
        val total = grouped.size
        CcasLogger.info(s"  Retrying ${unresolved.size} unresolved clubs ($total unique)...") *>
        ZIO.scoped {
          for {
            bar        <- CcasLogger.progressBar
            counterRef <- Ref.make(0)
            resolvedRef <- Ref.make(0)
            _ <- ZIO.foreachDiscard(grouped.toList) { case (slug, entries) =>
              Club.resolveOrFetch(client, slug).flatMap {
                case Some(clubId) =>
                  ZIO.foreachDiscard(entries) { entry =>
                    ClubMatch.updateTeamClubId(entry.matchId, entry.isTeam1, clubId) *>
                      UnresolvedMatchClub.delete(entry.matchId, entry.isTeam1)
                  } *> resolvedRef.update(_ + entries.size)
                case None => ZIO.unit
              }.catchAll(_ => ZIO.unit) *>
                counterRef.updateAndGet(_ + 1).flatMap(n => bar.print(n, total, s"  Retrying unresolved clubs: $n/$total"))
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
  private def retryUnresolvedPlayers(client: ChessComClient): RIO[CcasLogger & Transactor, Int] =
    for {
      unresolved <- UnresolvedBoardPlayer.selectAll
      result <- if (unresolved.isEmpty) { ZIO.succeed(0) } else {
        val grouped = unresolved.groupBy(_.username)
        val total = grouped.size
        CcasLogger.info(s"  Retrying ${unresolved.size} unresolved players ($total unique)...") *>
        ZIO.scoped {
          for {
            bar        <- CcasLogger.progressBar
            counterRef <- Ref.make(0)
            resolvedRef <- Ref.make(0)
            _ <- ZIO.foreachDiscard(grouped.toList) { case (username, entries) =>
              (for {
                apiPlayer <- client.get[ApiPlayer](ApiPlayer.getUrl(username))
                playerId = apiPlayer.playerId
                _ <- Player.selectId(playerId).flatMap {
                  case Some(_) => ZIO.unit
                  case None =>
                    val joined = Instant.ofEpochSecond(apiPlayer.joined)
                    connectZIO {
                      sql"""INSERT INTO player (player_id, joined) VALUES ($playerId, $joined)
                            ON CONFLICT (player_id) DO NOTHING""".update.run()
                    } *> PlayerSnapshot.insert(PlayerSnapshot(
                      playerId, Instant.now(), username, apiPlayer.status.category, apiPlayer.title
                    )).unit
                }
                _ <- ZIO.foreachDiscard(entries) { entry =>
                  ClubMatchBoard.updatePlayerId(entry.matchId, entry.board, entry.isTeam1, playerId) *>
                    UnresolvedBoardPlayer.delete(entry.matchId, entry.board, entry.isTeam1)
                }
                _ <- resolvedRef.update(_ + entries.size)
              } yield ()).catchAll(_ => ZIO.unit) *>
                counterRef.updateAndGet(_ + 1).flatMap(n => bar.print(n, total, s"  Retrying unresolved players: $n/$total"))
            }
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
  ): RIO[CcasLogger & Transactor, Int] =
    (for {
      clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(clubSlug))
      allDaily = clubMatches.dailyFinished ++ clubMatches.dailyInProgress ++ clubMatches.dailyRegistered
      dailyPending = allDaily.map(m => HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = false))
      nonDaily = clubMatches.finished.filterNot(_.timeClass.isDaily) ++
        clubMatches.inProgress.filterNot(_.timeClass.isDaily) ++
        clubMatches.registered.filterNot(_.timeClass.isDaily)
      livePending = nonDaily.map(m => HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = true))
      all = dailyPending ++ livePending
      knownIds <- ClubMatch.selectMatchIdsForClub(clubId)
      newOnly = all.filterNot(p => knownIds.contains(p.matchId))
      _ <- insertPendingMatches(newOnly)
    } yield newOnly.size).catchAll { error =>
      CcasLogger.warn(s"  Failed to fetch club matches: ${error.getMessage}").as(0)
    }

  /** Queries each un-queried member's match list to find club match IDs. Skips already-queried members unless --full. */
  private def seedFromMemberMatches(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug,
    allMembers: List[ClubMember],
    queriedIds: Set[PlayerId],
    snapByPlayerId: Map[PlayerId, PlayerSnapshot]
  ): RIO[CcasLogger & Transactor, MemberSeedResult] = {
    val toQuery = allMembers
      .filterNot(m => queriedIds.contains(m.playerId))
      .flatMap(m => snapByPlayerId.get(m.playerId).map(s => (m.playerId, s.username)))
      .distinctBy(_._1)
    for {
      counterRef       <- Ref.make(0)
      seedRef          <- Ref.make(0)
      failRef          <- Ref.make(0)
      failedMembersRef <- Ref.make(List.empty[(Username, String)])
      total = toQuery.size
      _ <- ZIO.scoped {
        for {
          bar <- CcasLogger.progressBar
          _ <- ZIO.foreachParDiscard(toQuery) { case (playerId, username) =>
            seedMatchesForPlayer(client, clubId, clubSlug, playerId, username).foldZIO(
              error => failRef.update(_ + 1)
                *> failedMembersRef.update(_ :+ (username, error.getMessage))
                *> CcasLogger.warn(s"  $username: failed — ${error.getMessage}"),
              count => seedRef.update(_ + count) *> counterRef.updateAndGet(_ + 1).flatMap { n =>
                bar.print(n, total, s"  Querying member matches: $n/$total")
              }
            )
          }
        } yield ()
      }
      seeded        <- seedRef.get
      queried       <- counterRef.get
      failed        <- failRef.get
      failedMembers <- failedMembersRef.get
    } yield MemberSeedResult(seeded, queried, failed, failedMembers)
  }

  private[history] def isClubDailyMatch(m: ApiPlayerMatches.ApiPlayerMatch, clubSlug: ClubSlug): Boolean =
    m.club.path.segments.lastOption.map(ClubSlug.wrap).contains(clubSlug)
      && !m.`@id`.path.segments.contains("live")

  private[history] def isClubLiveMatch(m: ApiPlayerMatches.ApiPlayerMatch, clubSlug: ClubSlug): Boolean =
    m.club.path.segments.lastOption.map(ClubSlug.wrap).contains(clubSlug)
      && m.`@id`.path.segments.contains("live")

  private def seedMatchesForPlayer(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug,
    playerId: PlayerId,
    username: Username
  ): RIO[Transactor, Int] =
    for {
      playerMatches <- client.get[ApiPlayerMatches](ApiPlayerMatches.getUrl(username))
      allMatches = playerMatches.finished ++ playerMatches.inProgress ++ playerMatches.registered
      dailyPending = allMatches.collect {
        case m if isClubDailyMatch(m, clubSlug) =>
          HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = false)
      }
      livePending = allMatches.collect {
        case m if isClubLiveMatch(m, clubSlug) =>
          HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = true)
      }
      all = dailyPending ++ livePending
      _ <- insertPendingMatches(all)
      _ <- HistoryMemberQuery.upsert(HistoryMemberQuery(clubId, playerId, Instant.now()))
    } yield all.size

  private def insertPendingMatches(items: Iterable[HistoryPendingMatch]): RIO[Transactor, Unit] =
    ZIO.foreachDiscard(items.grouped(1000).toList)(HistoryPendingMatch.insertBatch)

  /** If --refresh, re-queues all known matches; otherwise only stale ones (unfinished or recently completed). */
  private def seedStaleMatches(clubId: ClubId, refresh: Boolean): RIO[Transactor, Int] =
    if (refresh) {
      for {
        matchIds <- ClubMatch.selectMatchIdsForClub(clubId)
        _        <- insertPendingMatches(matchIds.map(id => HistoryPendingMatch(clubId, id, isLive = false)))
      } yield matchIds.size
    } else {
      for {
        staleIds <- ClubMatch.selectStaleForClub(clubId)
        _        <- insertPendingMatches(staleIds.map(id => HistoryPendingMatch(clubId, id, isLive = false)))
      } yield staleIds.size
    }

  // === Phase 3: BFS Wave Processing ===

  /** BFS wave loop: processes pending matches in batches, discovers new players, seeds their match lists,
    * and repeats until no new pending matches remain. Returns accumulated stats.
    */
  private def processWaves(ctx: ProcessingContext): RIO[CcasLogger & Transactor, RunStats] = {
    def waveLoop(waveCount: Int, waveDetails: List[(Int, Int)]): RIO[CcasLogger & Transactor, RunStats] =
      for {
        pendingCount <- HistoryPendingMatch.countNew(ctx.clubId)
        result <- if (pendingCount == 0) { readStats(ctx, waveCount, waveDetails) } else {
          val wave = waveCount + 1
          for {
            _ <- CcasLogger.info(s"  Wave $wave: $pendingCount matches to process")
            _ <- ctx.newPlayers.set(Set.empty)
            beforeCount <- ctx.matchesProcessed.get

            waveCounter <- Ref.make(0)
            _ <- ZIO.scoped {
              CcasLogger.progressBar.flatMap(waveBar => processAllPending(ctx, waveBar, waveCounter, pendingCount))
            }

            afterCount <- ctx.matchesProcessed.get
            failedCount <- ctx.matchesFailed.get
            waveProcessed = afterCount - beforeCount
            _ <- CcasLogger.info(s"  Wave $wave complete: $waveProcessed processed, $failedCount failed total")

            // Seed matches for newly discovered players
            newPlayers <- ctx.newPlayers.get
            _ <- ZIO.whenDiscard(newPlayers.nonEmpty) {
              CcasLogger.info(s"  Querying match lists for ${newPlayers.size} discovered players...") *>
                ZIO.foreachParDiscard(newPlayers) { dp =>
                  seedMatchesForPlayer(ctx.client, ctx.clubId, ctx.clubSlug, dp.playerId, dp.username).catchAll {
                    error => CcasLogger.warn(s"  ${dp.username}: failed to seed — ${error.getMessage}")
                  }
                }
            }

            newPendingNew <- HistoryPendingMatch.countNew(ctx.clubId)
            updatedDetails = waveDetails :+ (wave, waveProcessed)
            r <-
              if (newPendingNew > 0 && newPlayers.nonEmpty) { waveLoop(wave, updatedDetails) }
              else {
                for {
                  pendingTotal <- HistoryPendingMatch.count(ctx.clubId)
                  stats <- readStats(ctx, wave, updatedDetails)
                } yield stats.copy(pendingRemaining = pendingTotal.toInt)
              }
          } yield r
        }
      } yield result

    waveLoop(0, Nil)
  }

  private def processAllPending(
    ctx: ProcessingContext,
    bar: ProgressBar,
    counter: Ref[Int],
    waveTotal: Long
  ): RIO[CcasLogger & Transactor, Unit] =
    for {
      batch <- HistoryPendingMatch.selectClubBatch(ctx.clubId, BatchSize)
      _ <- ZIO.whenDiscard(batch.nonEmpty) {
        processMatchBatch(ctx, batch, bar, counter, waveTotal) *> processAllPending(ctx, bar, counter, waveTotal)
      }
    } yield ()

  private def processMatchBatch(
    ctx: ProcessingContext,
    pending: List[HistoryPendingMatch],
    bar: ProgressBar,
    counter: Ref[Int],
    waveTotal: Long
  ): RIO[CcasLogger & Transactor, Unit] =
    ZIO.foreachParDiscard(pending) { pm =>
      processMatch(ctx, pm.matchId, pm.isLive)
        .zipLeft(ctx.matchesProcessed.update(_ + 1))
        .catchAll { error =>
          ctx.matchesFailed.update(_ + 1) *>
            ctx.failedMatches.update(_ :+ (MatchKey(pm.matchId, pm.isLive), error.getMessage)) *>
            HistoryPendingMatch.updateStatus(ctx.clubId, pm.matchId, pm.isLive, PendingMatchStatus.ApiError) *>
            CcasLogger.warn(s"    Match ${pm.matchId}${if (pm.isLive) " (live)" else ""}: ${error.getMessage}")
        } *> counter.updateAndGet(_ + 1).flatMap { n =>
        bar.print(n, waveTotal.toInt, s"    Processing matches: $n/$waveTotal")
      }
    }

  private def processMatch(
    ctx: ProcessingContext,
    matchId: ClubMatchId,
    isLive: Boolean
  ): RIO[CcasLogger & Transactor, Unit] =
    if (isLive) { processLiveMatch(ctx, matchId) }
    else { processDailyMatch(ctx, matchId) }

  /** Fetches a daily match from the API, resolves team clubs, builds and persists board rows.
    * Marks the match Unidentified if the target club is not found in either team (data saved, BFS skipped).
    */
  private def processDailyMatch(ctx: ProcessingContext, matchId: ClubMatchId): RIO[CcasLogger & Transactor, Unit] =
    for {
      dailyMatch <- fetchMatch(ctx, matchId)

      // Resolve both team club IDs symmetrically
      (team1ClubId, team2ClubId) <-
        resolveClubIdFromTeamUrl(ctx, dailyMatch.teams.team1.`@id`) <&>
        resolveClubIdFromTeamUrl(ctx, dailyMatch.teams.team2.`@id`)

      _ <- trackUnresolvedClub(matchId, isTeam1 = true, dailyMatch.teams.team1.`@id`, team1ClubId) <&>
           trackUnresolvedClub(matchId, isTeam1 = false, dailyMatch.teams.team2.`@id`, team2ClubId)

      clubMatch = buildClubMatchRow(matchId, dailyMatch, team1ClubId, team2ClubId)

      // Determine our team position from resolved IDs
      weAreTeam1: Option[Boolean] =
        if (team1ClubId.contains(ctx.clubId)) { Some(true) }
        else if (team2ClubId.contains(ctx.clubId)) { Some(false) }
        else { None }

      boardRows <- buildBoardRows(ctx, matchId, dailyMatch, weAreTeam1, clubMatch.startTime)

      _ <- withTransaction {
        for {
          _ <- ClubMatch.upsert(clubMatch)
          _ <- ClubMatchBoard.deleteMatch(matchId)
          _ <- ClubMatchBoard.insertBatch(boardRows)
          _ <- if (weAreTeam1.isDefined) {
            HistoryPendingMatch.delete(ctx.clubId, matchId, isLive = false)
          } else {
            HistoryPendingMatch.updateStatus(ctx.clubId, matchId, false, PendingMatchStatus.Unidentified)
          }
        } yield ()
      }
      _ <- ZIO.whenDiscard(weAreTeam1.isEmpty) {
        ctx.matchesUnidentified.update(_ + 1) *>
          CcasLogger.warn(s"    Match $matchId: club ${ctx.clubId} not found in either team (data saved, BFS skipped)")
      }
    } yield ()

  /** Fetches a live match and resolves its players for BFS expansion. No board rows are persisted for live matches. */
  private def processLiveMatch(ctx: ProcessingContext, matchId: ClubMatchId): RIO[CcasLogger & Transactor, Unit] =
    for {
      liveMatch <- fetchLiveMatch(ctx, matchId)

      // Resolve both team club IDs (for caching/persistence)
      _ <- resolveClubIdFromTeamUrl(ctx, liveMatch.teams.team1.`@id`) <&>
           resolveClubIdFromTeamUrl(ctx, liveMatch.teams.team2.`@id`)

      // Discover players from both teams (for BFS wave expansion)
      startTime = Some(Instant.ofEpochSecond(liveMatch.startTime))
      _ <- ZIO.foreachParDiscard(liveMatch.teams.team1.players ++ liveMatch.teams.team2.players) { p =>
        resolvePlayerId(ctx, p.username, isOurTeam = false, startTime)
      }

      _ <- HistoryPendingMatch.delete(ctx.clubId, matchId, isLive = true)
    } yield ()

  // === Board Row Construction ===

  /** Constructs ClubMatchBoard rows by resolving player IDs and normalizing game outcomes.
    * Players that can't be resolved are recorded in `unresolved_board_player` and the board row gets a None player ID.
    */
  private def buildBoardRows(
    ctx: ProcessingContext,
    matchId: ClubMatchId,
    dailyMatch: ApiDailyMatch,
    weAreTeam1: Option[Boolean],
    matchStartTime: Option[Instant]
  ): RIO[CcasLogger & Transactor, List[ClubMatchBoard]] =
    dailyMatch match {
      case _: ApiDailyMatchRegistered => ZIO.succeed(Nil)
      case _ =>
        val teams        = dailyMatch.teams
        val team1Fp = teams.team1.fairPlayRemovals.map(_.value)
        val team2Fp = teams.team2.fairPlayRemovals.map(_.value)

        val team1ByBoard: Map[Int, MatchPlayerStarted] = teams.team1.players.collect {
          case p: MatchPlayerStarted => p.board.path.segments.last.toInt -> p
        }.toMap
        val team2ByBoard: Map[Int, MatchPlayerStarted] = teams.team2.players.collect {
          case p: MatchPlayerStarted => p.board.path.segments.last.toInt -> p
        }.toMap

        val allBoards = (team1ByBoard.keySet ++ team2ByBoard.keySet).toList.sorted

        ZIO.foreachPar(allBoards) { boardNum =>
          for {
            t1Player <- ZIO.fromOption(team1ByBoard.get(boardNum))
              .orElseFail(Exception(s"Match $matchId board $boardNum: missing team1 player"))
            t2Player <- ZIO.fromOption(team2ByBoard.get(boardNum))
              .orElseFail(Exception(s"Match $matchId board $boardNum: missing team2 player"))

            t1Username = t1Player.username
            t2Username = t2Player.username
            t1FairPlay = team1Fp.contains(t1Username.value)
            t2FairPlay = team2Fp.contains(t2Username.value)

            t1Pid <- resolvePlayerId(ctx, t1Username, isOurTeam = weAreTeam1.contains(true), matchStartTime)
            t2Pid <- resolvePlayerId(ctx, t2Username, isOurTeam = weAreTeam1.contains(false), matchStartTime)
            _ <- ZIO.whenDiscard(t1Pid.isEmpty)(
              UnresolvedBoardPlayer.insert(matchId, boardNum, isTeam1 = true, t1Username).ignore
            )
            _ <- ZIO.whenDiscard(t2Pid.isEmpty)(
              UnresolvedBoardPlayer.insert(matchId, boardNum, isTeam1 = false, t2Username).ignore
            )
          } yield {
            val (g1Winner, g1Detail) =
              normalizeGameOutcome(t1Player.playedAsWhite, t2Player.playedAsBlack, whiteTeamIsTeam1 = true)
            val (g2Winner, g2Detail) =
              normalizeGameOutcome(t2Player.playedAsWhite, t1Player.playedAsBlack, whiteTeamIsTeam1 = false)
            val (t1Score, t2Score) = computeScoreX2(g1Winner, g2Winner, t1FairPlay, t2FairPlay)

            ClubMatchBoard(
              matchId = matchId,
              board = boardNum,
              team1PlayerId = t1Pid,
              team1FairPlay = t1FairPlay,
              team2PlayerId = t2Pid,
              team2FairPlay = t2FairPlay,
              game1Winner = g1Winner,
              game1Detail = g1Detail,
              game2Winner = g2Winner,
              game2Detail = g2Detail,
              team1ScoreX2 = t1Score,
              team2ScoreX2 = t2Score
            )
          }
        }
    }

  // === Game Outcome Normalization ===

  private[history] def normalizeGameOutcome(
    whiteResult: Option[GameResultDetail],
    blackResult: Option[GameResultDetail],
    whiteTeamIsTeam1: Boolean
  ): (Option[BoardGameWinner], Option[GameResultDetail]) =
    (whiteResult, blackResult) match {
      case (Some(GameResultDetail.Win), Some(loss)) =>
        val winner = if (whiteTeamIsTeam1) { BoardGameWinner.Team1 }
        else { BoardGameWinner.Team2 }
        (Some(winner), Some(loss))
      case (Some(loss), Some(GameResultDetail.Win)) =>
        val winner = if (whiteTeamIsTeam1) { BoardGameWinner.Team2 }
        else { BoardGameWinner.Team1 }
        (Some(winner), Some(loss))
      case (Some(draw), Some(_)) if draw.category == GameResult.Draw =>
        (Some(BoardGameWinner.Draw), Some(draw))
      case (None, None) =>
        (None, None)
      case _ =>
        // Mismatched state (e.g., one side played, other didn't) — treat as not played
        (None, None)
    }

  private[history] def computeScoreX2(
    game1Winner: Option[BoardGameWinner],
    game2Winner: Option[BoardGameWinner],
    team1FairPlay: Boolean,
    team2FairPlay: Boolean
  ): (Short, Short) = {
    def gameScore(winner: Option[BoardGameWinner]): (Int, Int) =
      winner match {
        case None                                      => (0, 0)
        case Some(_) if team1FairPlay && team2FairPlay => (1, 1)
        case Some(_) if team1FairPlay                  => (0, 2)
        case Some(_) if team2FairPlay                  => (2, 0)
        case Some(BoardGameWinner.Team1)               => (2, 0)
        case Some(BoardGameWinner.Team2)               => (0, 2)
        case Some(BoardGameWinner.Draw)                => (1, 1)
      }

    val (g1t1, g1t2) = gameScore(game1Winner)
    val (g2t1, g2t2) = gameScore(game2Winner)
    ((g1t1 + g2t1).toShort, (g1t2 + g2t2).toShort)
  }

  // === Player Resolution ===

  /** Resolves a username to a PlayerId, checking the in-memory cache first. If unknown, discovers the player via the
    * API (creating Player and PlayerSnapshot records). Returns None for unresolvable players (closed/deleted accounts).
    */
  private def resolvePlayerId(
    ctx: ProcessingContext,
    username: Username,
    isOurTeam: Boolean,
    matchStartTime: Option[Instant]
  ): RIO[CcasLogger & Transactor, Option[PlayerId]] = {
    val key = username.value
    ctx.knownPlayers.get.map(_.get(key)).flatMap {
      case Some(playerId) => ctx.playersKnown.update(_ + 1).as(Some(playerId))
      case None => discoverPlayer(ctx, username, key, isOurTeam, matchStartTime)
    }
  }

  /** Gates the full discovery flow (API fetch + DB insert) behind a Promise so that concurrent fibers resolving the
    * same unknown player share a single API call and a single DB insert.
    */
  private def discoverPlayer(
    ctx: ProcessingContext,
    username: Username,
    key: String,
    isOurTeam: Boolean,
    matchStartTime: Option[Instant]
  ): RIO[CcasLogger & Transactor, Option[PlayerId]] =
    for {
      promise <- Promise.make[Throwable, Option[PlayerId]]
      action <- ctx.discoveryCache.modify { m =>
        m.get(key) match {
          case Some(existing) => (existing.await, m)
          case None           => (doDiscoverPlayer(ctx, username, key, promise, isOurTeam, matchStartTime), m + (key -> promise))
        }
      }
      result <- action
    } yield result

  private def doDiscoverPlayer(
    ctx: ProcessingContext,
    username: Username,
    key: String,
    promise: Promise[Throwable, Option[PlayerId]],
    isOurTeam: Boolean,
    matchStartTime: Option[Instant]
  ): RIO[CcasLogger & Transactor, Option[PlayerId]] = {
    val work = for {
      apiPlayer <- ctx.client.get[ApiPlayer](ApiPlayer.getUrl(username))
      playerId       = apiPlayer.playerId
      statusCategory = apiPlayer.status.category
      now            = Instant.now()

      result <- Player.selectId(playerId).flatMap {
        case Some(_) =>
          // Player exists — username change. Add new snapshot.
          val snapshot = PlayerSnapshot(playerId, now, username, statusCategory, apiPlayer.title)
          PlayerSnapshot.insert(snapshot) *>
            ctx.knownPlayers.update(_ + (key -> playerId)).as(Some(playerId))

        case None =>
          // Brand new player — ON CONFLICT DO NOTHING handles concurrent inserts
          val joined = Instant.ofEpochSecond(apiPlayer.joined)
          connectZIO {
            sql"""INSERT INTO player (player_id, joined) VALUES ($playerId, $joined)
                  ON CONFLICT (player_id) DO NOTHING""".update.run()
          }.flatMap { inserted =>
            if (inserted == 0) {
              // Another fiber already created this player — treat as known
              ctx.knownPlayers.update(_ + (key -> playerId)).as(Some(playerId))
            } else {
              val snapshotSince = if (statusCategory == PlayerStatusCategory.Active) { now }
              else { Instant.ofEpochSecond(apiPlayer.lastOnline) }
              val snapshot = PlayerSnapshot(playerId, snapshotSince, username, statusCategory, apiPlayer.title)
              for {
                _ <- PlayerSnapshot.insert(snapshot)
                _ <- ZIO.whenDiscard(isOurTeam)(createClubMemberForDiscovered(ctx, apiPlayer, matchStartTime))
                _ <- ctx.knownPlayers.update(_ + (key -> playerId))
                _ <- ZIO.whenDiscard(isOurTeam)(ctx.newPlayers.update(_ + DiscoveredPlayer(playerId, username)))
                _ <- ctx.playersDiscovered.update(_ + 1)
              } yield Some(playerId)
            }
          }
      }
    } yield result

    // The doer handles success/failure and completes the promise.
    // On failure: log once, count once, resolve promise to None so awaiting fibers get None (not an exception).
    work.foldZIO(
      error => ctx.playersFailed.update(_ + 1)
        *> CcasLogger.warn(s"    Cannot resolve player $username: ${error.getMessage}")
        *> promise.succeed(None).as(None),
      result => promise.succeed(result).as(result)
    )
  }

  private def createClubMemberForDiscovered(
    ctx: ProcessingContext,
    apiPlayer: ApiPlayer,
    matchStartTime: Option[Instant]
  ): RIO[Transactor, Unit] = {
    val playerId       = apiPlayer.playerId
    val statusCategory = apiPlayer.status.category
    val clubId         = ctx.clubId

    val existsCheck = connectZIO {
      sql"SELECT COUNT(*) FROM club_member WHERE club_id = $clubId AND player_id = $playerId"
        .query[Long].run().head > 0
    }

    existsCheck.flatMap {
      case true => ZIO.unit
      case false =>
        val fromApi = for {
          playerClubs <- ctx.client.get[ApiPlayerClubs](ApiPlayerClubs.getUrl(apiPlayer.username))
          clubOpt = playerClubs.clubs.find(_.clubName == ctx.clubSlug)
          member = clubOpt match {
            case Some(apiClub) =>
              val since = Instant.ofEpochSecond(apiClub.joined)
              val until = if (statusCategory == PlayerStatusCategory.Active) { None }
              else { Some(Instant.ofEpochSecond(apiPlayer.lastOnline)) }
              ClubMember(clubId, playerId, since, until, sinceApproximate = false)
            case None =>
              val since = matchStartTime.getOrElse(Instant.ofEpochSecond(apiPlayer.joined))
              val until = if (statusCategory == PlayerStatusCategory.Active) { matchStartTime }
              else { Some(Instant.ofEpochSecond(apiPlayer.lastOnline)) }
              ClubMember(clubId, playerId, since, until, sinceApproximate = true)
          }
          _ <- ClubMember.insert(member)
        } yield ()

        fromApi.catchAll { _ =>
          val since = matchStartTime.getOrElse(Instant.ofEpochSecond(apiPlayer.joined))
          val until = if (statusCategory == PlayerStatusCategory.Active) { None }
          else { Some(Instant.ofEpochSecond(apiPlayer.lastOnline)) }
          ClubMember.insert(ClubMember(clubId, playerId, since, until, sinceApproximate = true)).unit
        }
    }
  }

  // === Caching ===

  private def fetchMatch(ctx: ProcessingContext, matchId: ClubMatchId): Task[ApiDailyMatch] =
    for {
      promise <- Promise.make[Throwable, ApiDailyMatch]
      action <- ctx.matchCache.modify { m =>
        m.get(matchId) match {
          case Some(existing) => (existing.await, m)
          case None =>
            val fetch =
              ctx.client.get[ApiDailyMatch](ApiDailyMatch.getUrl(matchId)).tapBoth(promise.fail, promise.succeed)
            (fetch, m + (matchId -> promise))
        }
      }
      result <- action
    } yield result

  private def fetchLiveMatch(ctx: ProcessingContext, matchId: ClubMatchId): Task[ApiLiveMatch] =
    for {
      promise <- Promise.make[Throwable, ApiLiveMatch]
      action <- ctx.liveMatchCache.modify { m =>
        m.get(matchId) match {
          case Some(existing) => (existing.await, m)
          case None =>
            val fetch =
              ctx.client.get[ApiLiveMatch](ApiLiveMatch.getUrl(matchId)).tapBoth(promise.fail, promise.succeed)
            (fetch, m + (matchId -> promise))
        }
      }
      result <- action
    } yield result

  // === Helpers ===

  /** Records a club slug in `unresolved_match_club` when team club URL resolution fails. */
  private def trackUnresolvedClub(
    matchId: ClubMatchId,
    isTeam1: Boolean,
    teamUrl: URL,
    resolvedId: Option[ClubId]
  ): RIO[Transactor, Unit] =
    ZIO.whenDiscard(resolvedId.isEmpty) {
      teamUrl.path.segments.lastOption match {
        case Some(segment) => UnresolvedMatchClub.insert(matchId, isTeam1, ClubSlug.wrap(segment)).ignore
        case None          => ZIO.unit
      }
    }

  /** Resolves a team URL to a ClubId via Promise-based cache. Falls back to DB lookup then API fetch. */
  private def resolveClubIdFromTeamUrl(ctx: ProcessingContext, teamUrl: URL): RIO[Transactor, Option[ClubId]] =
    teamUrl.path.segments.lastOption match {
      case None => ZIO.none
      case Some(segment) =>
        val slug = ClubSlug.wrap(segment)
        val key = slug.value
        for {
          promise <- Promise.make[Throwable, Option[ClubId]]
          action <- ctx.clubCache.modify { m =>
            m.get(key) match {
              case Some(existing) => (existing.await, m)
              case None =>
                val work = Club.resolveOrFetch(ctx.client, slug).tapBoth(promise.fail, promise.succeed)
                (work, m + (key -> promise))
            }
          }
          result <- action
        } yield result
    }

  private[history] def buildClubMatchRow(
    matchId: ClubMatchId,
    dailyMatch: ApiDailyMatch,
    team1ClubId: Option[ClubId],
    team2ClubId: Option[ClubId]
  ): ClubMatch = {
    val teams = dailyMatch.teams
    val (startTime, endTime) = dailyMatch match {
      case m: ApiDailyMatchFinished =>
        (Some(Instant.ofEpochSecond(m.startTime)), Some(Instant.ofEpochSecond(m.endTime)))
      case m: ApiDailyMatchCancelled =>
        (Some(Instant.ofEpochSecond(m.startTime)), Some(Instant.ofEpochSecond(m.endTime)))
      case m: ApiDailyMatchInProgress => (Some(Instant.ofEpochSecond(m.startTime)), None)
      case m: ApiDailyMatchRegistered => (m.startTime.map(Instant.ofEpochSecond), None)
    }
    val (team1Result, team2Result) = teams match {
      case t: ApiDailyMatchTeamsFinished  => (Some(t.team1.result), Some(t.team2.result))
      case t: ApiDailyMatchTeamsCancelled => (Some(t.team1.result), Some(t.team2.result))
      case _                              => (None, None)
    }

    ClubMatch(
      matchId = matchId,
      name = dailyMatch.name,
      url = dailyMatch.url.encode,
      status = dailyMatch.status,
      timeClass = dailyMatch.settings.timeClass,
      startTime = startTime,
      endTime = endTime,
      boards = dailyMatch.boards,
      team1ClubId = team1ClubId,
      team1Score = teams.team1.score,
      team1Result = team1Result,
      team2ClubId = team2ClubId,
      team2Score = teams.team2.score,
      team2Result = team2Result,
      fetchedAt = Instant.now()
    )
  }

  // === Reporting ===

  private def finalizeInterrupted(
    ctx: ProcessingContext,
    runId: Long,
    startedAt: Instant,
    clubSlug: ClubSlug,
    memberSeed: MemberSeedResult,
    membersSkipped: Int,
    seedClub: Int,
    seedStale: Int
  ): RIO[CcasLogger & Transactor, Unit] =
    for {
      partialStats     <- readStats(ctx, waveCount = 0, waveDetails = Nil)
      pendingRemaining <- HistoryPendingMatch.count(ctx.clubId)
      completedAt = Instant.now()
      totalStats = partialStats.copy(
        membersQueried = memberSeed.queried,
        membersSkipped = membersSkipped,
        membersFailed = memberSeed.failed,
        matchesSeeded = seedClub + memberSeed.seeded + seedStale,
        failedMembers = memberSeed.failedMembers,
        pendingRemaining = pendingRemaining.toInt
      )
      _ <- HistoryRun.complete(runId, completedAt, totalStats.matchesProcessed, totalStats.playersDiscovered)
      _ <- logSummary(totalStats, startedAt, completedAt)
      _ <- OutputFile.writeAndLog("history", clubSlug, formatReport(totalStats, clubSlug, startedAt, completedAt))
    } yield ()

  private def readStats(ctx: ProcessingContext, waveCount: Int, waveDetails: List[(Int, Int)]): UIO[RunStats] =
    for {
      matchesProcessed    <- ctx.matchesProcessed.get
      matchesFailed       <- ctx.matchesFailed.get
      matchesUnidentified <- ctx.matchesUnidentified.get
      playersDiscovered   <- ctx.playersDiscovered.get
      playersKnown        <- ctx.playersKnown.get
      playersFailed       <- ctx.playersFailed.get
      failedMatches       <- ctx.failedMatches.get
    } yield RunStats(
      matchesProcessed = matchesProcessed,
      matchesFailed = matchesFailed,
      matchesUnidentified = matchesUnidentified,
      playersDiscovered = playersDiscovered,
      playersKnown = playersKnown,
      playersFailed = playersFailed,
      waveCount = waveCount,
      waveDetails = waveDetails,
      failedMatches = failedMatches
    )

  private def logSummary(stats: RunStats, startedAt: Instant, completedAt: Instant): URIO[CcasLogger, Unit] = {
    val duration = JDuration.between(startedAt, completedAt)
    for {
      _ <- CcasLogger.info("=== History Discovery Complete ===")
      _ <- CcasLogger.info(s"Duration: ${duration.display}")
      _ <- CcasLogger.info(
        s"Members queried: ${stats.membersQueried} | skipped: ${stats.membersSkipped} | failed: ${stats.membersFailed}"
      )
      _ <- CcasLogger.info(s"Matches seeded: ${stats.matchesSeeded}")
      _ <- CcasLogger.info(s"Matches processed: ${stats.matchesProcessed} | failed: ${stats.matchesFailed} | unidentified: ${stats.matchesUnidentified}")
      _ <- CcasLogger.info(
        s"Players discovered: ${stats.playersDiscovered} | known: ${stats.playersKnown} | failed: ${stats.playersFailed}"
      )
      _ <- CcasLogger.info(s"Waves: ${stats.waveCount}")
      _ <- CcasLogger.info(s"Pending remaining: ${stats.pendingRemaining}")
    } yield ()
  }

  private def formatReport(
    stats: RunStats,
    clubSlug: ClubSlug,
    startedAt: Instant,
    completedAt: Instant
  ): String = {
    val duration = JDuration.between(startedAt, completedAt)
    val sb       = new StringBuilder

    sb.append(s"=== History Discovery Report: $clubSlug ===\n\n")
    sb.append(s"Started:   $startedAt\n")
    sb.append(s"Completed: $completedAt\n")
    sb.append(s"Duration:  ${duration.display}\n\n")

    sb.append("--- Members ---\n")
    sb.append(s"Queried: ${stats.membersQueried}\n")
    sb.append(s"Skipped: ${stats.membersSkipped}\n")
    sb.append(s"Failed:  ${stats.membersFailed}\n\n")

    sb.append("--- Matches ---\n")
    sb.append(s"Seeded:       ${stats.matchesSeeded}\n")
    sb.append(s"Processed:    ${stats.matchesProcessed}\n")
    sb.append(s"Failed:       ${stats.matchesFailed}\n")
    sb.append(s"Unidentified: ${stats.matchesUnidentified}\n")
    stats.waveDetails.foreach { case (wave, count) =>
      sb.append(s"  Wave $wave: $count matches\n")
    }
    sb.append(s"Pending:   ${stats.pendingRemaining}\n\n")

    sb.append("--- Players ---\n")
    sb.append(s"Discovered: ${stats.playersDiscovered}\n")
    sb.append(s"Known:      ${stats.playersKnown}\n")
    sb.append(s"Failed:     ${stats.playersFailed}\n\n")

    if (stats.failedMatches.nonEmpty) {
      sb.append("--- Failed Matches ---\n")
      stats.failedMatches.foreach { case (MatchKey(matchId, isLive), error) =>
        val kind = if (isLive) { " (live)" } else { "" }
        sb.append(s"  Match $matchId$kind: $error\n")
      }
      sb.append("\n")
    }

    if (stats.failedMembers.nonEmpty) {
      sb.append("--- Failed Member Queries ---\n")
      stats.failedMembers.foreach { case (username, error) =>
        sb.append(s"  $username: $error\n")
      }
      sb.append("\n")
    }

    sb.toString
  }
}
