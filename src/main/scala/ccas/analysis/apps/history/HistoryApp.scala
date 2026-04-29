package ccas.analysis.apps.history

import java.time.{Duration as JDuration, Instant}

import zio.{Chunk, NonEmptyChunk, RIO, Ref, Scope, URIO, ZEnvironment, ZIO, ZIOAppArgs, ZIOAppDefault}
import HistoryUtils.*

import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.tables.*
import ccas.analysis.tables.subtypes.HistoryRunId
import ccas.api.misc.subtypes.*
import ccas.utils.{display, CcasLogger, OutputFile}
import ccas.utils.client.{ChessComClient, HttpClientLayer}
import ccas.utils.errors.BadRequestException
import ccas.utils.sql.PostgresClient

/** Discovers and persists a chess club's match history by crawling the Chess.com API.
  *
  * Starting from a club's known members, it collects match IDs, fetches match data, and follows links to newly
  * discovered players via breadth-first search (BFS) waves — expanding the match graph until no new matches remain.
  *
  * ==Run Modes==
  *   - '''Default (incremental):''' Only queries members whose match lists haven't been fetched before. Skips seeding
  *     settled matches (finished and fetched past the stale window) to avoid redundant API calls for matches already
  *     stored by another club's run. Only re-queues stale matches. This is the cheapest mode for regular updates.
  *   - '''`--full`:''' Clears member query history so every member's match list is re-fetched from the API. Use when
  *     you want a complete rebuild of the match graph (e.g., after a long gap or to pick up retroactive API changes).
  *   - '''`--refresh [hours]`:''' After BFS processing, re-fetches settled matches (finished + past the stale window)
  *     directly from `club_match`, bypassing the pending table. If `hours` is specified, only refreshes settled matches
  *     whose `fetchedAt` is older than that many hours — enabling resumable partial refreshes (already-refreshed matches
  *     have updated `fetchedAt` and are naturally skipped). Without `hours`, refreshes all settled matches.
  *
  * ==Workflow (4 Phases)==
  *   1. '''Initialize''' — Reconcile club membership, load current state (members, snapshots, processed counts), reset
  *      pending match statuses, and create a `HistoryRun` record.
  *   2. '''Seed''' — Collect match IDs into `history_pending_match` from three sources: the club matches endpoint, each
  *      member's match list, and stale existing matches. Also retries previously unresolved clubs and board players
  *      from prior runs.
  *   3. '''Process''' — BFS wave loop: fetch and persist match data in parallel batches, discover unknown players, seed
  *      their match lists, and repeat until no new pending matches remain.
  *   3.5. '''Refresh''' (if `--refresh`) — Re-fetch settled matches directly from `club_match` in batches, bypassing the
  *      pending table. Failed matches keep their old `fetchedAt` and are retried on the next refresh.
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
  * Both types are retried at the start of each run. Successfully resolved entries are patched in-place and removed from
  * their respective unresolved tables.
  *
  * ==Multi-Club Deduplication==
  * When multiple club slugs are provided in a single CLI invocation, clubs are processed sequentially with a shared
  * `SharedContext` that eliminates redundant API calls across clubs:
  *   - '''Unresolved retries''' run once before the per-club loop instead of per-club.
  *   - '''Member match lists:''' When a member's match list is fetched for one club, matches are also seeded into the
  *     pending queues of all other clubs in the batch. Subsequent clubs skip API calls for those members, writing only
  *     a `HistoryMemberQuery` record so future incremental runs have correct per-club history.
  *   - '''Match processing:''' Matches fully processed by a prior club are skipped — the pending entry is deleted
  *     without re-fetching from the API. The `history_run.matches_processed` column counts total matches handled
  *     (processed + shared-skipped) to preserve consistent run-level totals.
  *   - '''Stale seeding:''' Matches already processed by a prior club in the batch are filtered out before being
  *     re-queued as pending.
  *
  * This deduplication only applies to the CLI multi-club path. API-submitted jobs run independently per club.
  *
  * ==Invocation==
  *   - '''CLI:''' `HistoryApp <club-slug> [club-slug ...] [--full] [--refresh [hours]]`
  *   - '''API:''' `POST /api/jobs/history` with `{"clubSlugs": ["..."], "full": true/false, "refreshMinHours": 24}`
  */
object HistoryApp extends ZIOAppDefault {
  private val help = "Usage: HistoryApp <club-slug> [club-slug ...] [--full] [--refresh [hours]]"

  // --- CLI entry point ---

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      rawArgs <- ZIOAppArgs.getArgs
      parsed  <- ZIO.fromEither(parseArgs(rawArgs)).mapError(BadRequestException(_))
      _       <- discoverBatch(parsed.slugs, parsed.full, parsed.refreshMinHours)
    } yield ()).provideSomeAuto(
      CcasLogger.live(showProgress = true),
      ChessComClient.live("history"),
      HttpClientLayer.live,
      PostgresClient.live(onInit = Tables.ensureTables)
    )

  private[history] case class HistoryAppArgs(
    slugs: NonEmptyChunk[ClubSlug],
    full: Boolean,
    refreshMinHours: Option[Int]
  )

  /** Parses CLI args into `HistoryAppArgs`. Strips `--refresh [hours]` (bare `--refresh` defaults to 0 hours, meaning
    * "always refresh"; `--refresh N` only refreshes matches whose `fetched_at` is older than N hours); `--full` forces
    * a full re-scan; remaining positional tokens become slugs. Empty slug list is an error.
    */
  private[history] def parseArgs(args: Chunk[String]): Either[String, HistoryAppArgs] = {
    val full = args.contains("--full")
    val (refreshMinHours, refreshStripped) = {
      val idx = args.indexOf("--refresh")
      if (idx < 0) { (Option.empty[Int], args) }
      else {
        val nextOpt = args.lift(idx + 1).flatMap(_.toIntOption)
        if (nextOpt.isDefined) { (nextOpt, args.patch(idx, Chunk.empty, 2)) }
        else { (Some(0), args.patch(idx, Chunk.empty, 1)) }
      }
    }
    val slugChunk = refreshStripped.filterNot(_.startsWith("--")).map(ClubSlug.wrap)
    NonEmptyChunk.fromChunk(slugChunk) match {
      case Some(slugs) => Right(HistoryAppArgs(slugs, full, refreshMinHours))
      case None        => Left(help)
    }
  }

  private case class InitResult(
    allMembers: List[ClubMember],
    playerById: Map[PlayerId, Player],
    queriedIds: Set[PlayerId],
    ctx: ProcessingContext,
    startedAt: Instant,
    runId: HistoryRunId
  )

  private def discoverBatch(
    slugs: NonEmptyChunk[ClubSlug],
    full: Boolean,
    refreshMinHours: Option[Int]
  ): RIO[CcasLogger & ChessComClient & PostgresClient, Unit] =
    if (slugs.size == 1) { discover(slugs.head, full, refreshMinHours).flatMap(outputResult) }
    else {
      for {
        client <- ZIO.service[ChessComClient]
        (resolvedClubs, resolvedPlayers) <-
          HistorySeeding.retryUnresolvedClubs(client) <&> HistorySeeding.retryUnresolvedPlayers(client)
        _ <- ZIO.whenDiscard(resolvedClubs > 0 || resolvedPlayers > 0) {
          CcasLogger.info(s"  Resolved $resolvedClubs clubs, $resolvedPlayers players from previous runs")
        }
        shared <- SharedContext.make
        _ <- ZIO.foreachDiscard(slugs) { slug =>
          discoverClub(slug, full, refreshMinHours, RunTrigger.Cli, None, Some(shared)).flatMap(outputResult)
        }
      } yield ()
    }

  private def outputResult(r: HistoryResult): RIO[CcasLogger, Unit] =
    logSummary(r.stats, r.startedAt, r.completedAt) *>
      OutputFile.writeAndLog("history", r.clubSlug, formatReport(r.stats, r.clubSlug, r.startedAt, r.completedAt))

  private def initialize(
    clubSlug: ClubSlug,
    full: Boolean,
    trigger: RunTrigger,
    jobRunId: Option[JobRunId]
  ): RIO[CcasLogger & ChessComClient & PostgresClient, InitResult] =
    for {
      _ <- CcasLogger.info(s"=== HistoryApp: $clubSlug ===")
      _ <- CcasLogger.info("Phase 1: Initializing...")
      _ <- MembershipApp.reconcile(clubSlug, trackRun = false)
      club <- Club.selectBySlug(clubSlug)
        .someOrFail(IllegalStateException(s"Club '$clubSlug' not found after reconcile"))
      clubId = club.clubId
      (allMembers, processedCount, queriedIds) <-
        ClubMember.selectClub(clubId) <&>
          ClubMatch.countForClub(clubId) <&>
          HistoryMemberQuery.selectClubPlayerIds(clubId)
      memberPlayers <- Player.selectByIds(allMembers.map(_.playerId))
      _ <- HistoryPendingMatch.resetStatuses(clubId)
      startedAt = Instant.now()
      runId <- HistoryRun.insert(clubId, trigger, startedAt, jobRunId)
      playerById = memberPlayers.map(p => p.playerId -> p).toMap
      _ <- CcasLogger.info(
        s"  Members: ${allMembers.size}, Processed matches: $processedCount, Queried members: ${queriedIds.size}"
      )
      knownPlayersInit = memberPlayers.map(p => p.username.value -> p.playerId).toMap
      client <- ZIO.service[ChessComClient]
      ctx    <- ProcessingContext.make(client, clubId, clubSlug, knownPlayersInit)
      effectiveQueriedIds =
        if (full) { Set.empty[PlayerId] }
        else { queriedIds }
    } yield InitResult(allMembers, playerById, effectiveQueriedIds, ctx, startedAt, runId)

  final case class HistoryResult(stats: RunStats, clubSlug: ClubSlug, startedAt: Instant, completedAt: Instant)

  /** Main entry point: orchestrates the 4-phase discover workflow for a club's match history. */
  def discover(
    clubSlug: ClubSlug,
    full: Boolean = false,
    refreshMinHours: Option[Int] = None,
    trigger: RunTrigger = RunTrigger.Cli,
    jobRunId: Option[JobRunId] = None
  ): RIO[CcasLogger & ChessComClient & PostgresClient, HistoryResult] =
    discoverClub(clubSlug, full, refreshMinHours, trigger, jobRunId, shared = None)

  private def discoverClub(
    clubSlug: ClubSlug,
    full: Boolean,
    refreshMinHours: Option[Int],
    trigger: RunTrigger,
    jobRunId: Option[JobRunId],
    shared: Option[SharedContext]
  ): RIO[CcasLogger & ChessComClient & PostgresClient, HistoryResult] = {
    require(shared.isEmpty || trigger == RunTrigger.Cli, "SharedContext requires sequential CLI execution")
    for {
      pgClient <- ZIO.service[PostgresClient]
      logger   <- ZIO.service[CcasLogger]

      // === Phase 1: Initialize ===
      InitResult(allMembers, playerById, queriedIds, ctx, startedAt, runId) <-
        initialize(clubSlug, full, trigger, jobRunId)
      _ <- ZIO.foreachDiscard(shared)(_.resolvedClubs.update(_ + (clubSlug -> ctx.clubId)))

      seedClubRef   <- Ref.make(0)
      memberSeedRef <- Ref.make(MemberSeedResult(0, 0, Nil))
      seedStaleRef  <- Ref.make(0)
      refreshedRef  <- Ref.make(0)

      // === Phases 2-3: Seed + Process (interrupt-safe) ===
      waveStats <- {
        for {
          // Phase 2: Seed match IDs
          _ <- CcasLogger.info("Phase 2: Seeding match IDs...")
          _ <- ZIO.whenDiscard(shared.isEmpty) {
            for {
              (resolvedClubs, resolvedPlayers) <-
                HistorySeeding.retryUnresolvedClubs(ctx.client) <&> HistorySeeding.retryUnresolvedPlayers(ctx.client)
              _ <- ZIO.whenDiscard(resolvedClubs > 0 || resolvedPlayers > 0) {
                CcasLogger.info(s"  Resolved $resolvedClubs clubs, $resolvedPlayers players from previous runs")
              }
            } yield ()
          }
          _ <- ZIO.whenDiscard(full) {
            CcasLogger.info("  --full: clearing member query history") *> HistoryMemberQuery.deleteClub(ctx.clubId)
          }
          settledMatchIds <- ClubMatch.selectSettledMatchIdsForClub(ctx.clubId)

          seedClub <- HistorySeeding.seedFromClubMatches(
            ctx.client, ctx.clubId, clubSlug, ctx.seedClubMatchesUnchanged
          )
          _ <- seedClubRef.set(seedClub)
          _ <- CcasLogger.info(s"  Club matches endpoint: $seedClub new match IDs")

          memberSeed <-
            HistorySeeding.seedFromMemberMatches(
              ctx.client,
              ctx.clubId,
              clubSlug,
              allMembers,
              queriedIds,
              playerById,
              settledMatchIds,
              shared,
              ctx.seedPlayerMatchesUnchanged
            )
          _ <- memberSeedRef.set(memberSeed)
          membersSkipped = allMembers.size - memberSeed.queried - memberSeed.failed
          _ <- CcasLogger.info(
            s"  Member match lists: ${memberSeed.seeded} new IDs (queried: ${memberSeed.queried}, skipped: $membersSkipped, failed: ${memberSeed.failed})"
          )

          seedStale <- HistorySeeding.seedStaleMatches(ctx.clubId, shared)
          _         <- seedStaleRef.set(seedStale)
          _         <- CcasLogger.info(s"  Stale match re-queue: $seedStale matches queued")

          // Phase 3: Process matches (BFS waves)
          _  <- CcasLogger.info("Phase 3: Processing matches...")
          ws <- HistoryProcessing.processWaves(ctx, settledMatchIds, shared)

          // Phase 3.5: Refresh settled matches (if --refresh)
          refreshed <- refreshMinHours match {
            case Some(hours) =>
              CcasLogger.info("Phase 3.5: Refreshing settled matches...") *>
                HistoryProcessing.refreshSettledMatches(ctx, hours)
            case None => ZIO.succeed(0)
          }
          _ <- refreshedRef.set(refreshed)
        } yield ws.copy(matchesRefreshed = refreshed)
      }.onInterrupt {
        for {
          sc <- seedClubRef.get
          ms <- memberSeedRef.get
          ss <- seedStaleRef.get
          rf <- refreshedRef.get
          skip = allMembers.size - ms.queried - ms.failed
          _ <- finalizeInterrupted(ctx, runId, startedAt, clubSlug, ms, skip, sc, ss, rf)
            .provideEnvironment(ZEnvironment(logger, pgClient)).orDie
        } yield ()
      }

      // === Phase 4: Finalize ===
      seedClub   <- seedClubRef.get
      memberSeed <- memberSeedRef.get
      seedStale  <- seedStaleRef.get
      membersSkipped = allMembers.size - memberSeed.queried - memberSeed.failed
      completedAt    = Instant.now()
      totalStats = waveStats.copy(
        membersQueried = memberSeed.queried,
        membersSkipped = membersSkipped,
        membersFailed = memberSeed.failed,
        matchesSeeded = seedClub + memberSeed.seeded + seedStale,
        failedMembers = memberSeed.failedMembers
      )
      _ <- HistoryRun.complete(
        runId, completedAt, totalStats.matchesProcessed + totalStats.matchesSharedSkip, totalStats.playersDiscovered,
        totalStats.refreshMatchUnchanged, totalStats.seedClubMatchesUnchanged, totalStats.seedPlayerMatchesUnchanged
      )
    } yield HistoryResult(totalStats, clubSlug, startedAt, completedAt)
  }

  // === Reporting ===

  private def finalizeInterrupted(
    ctx: ProcessingContext,
    runId: HistoryRunId,
    startedAt: Instant,
    clubSlug: ClubSlug,
    memberSeed: MemberSeedResult,
    membersSkipped: Int,
    seedClub: Int,
    seedStale: Int,
    refreshed: Int
  ): RIO[CcasLogger & PostgresClient, Unit] =
    for {
      partialStats     <- HistoryProcessing.readStats(ctx, waveCount = 0, waveDetails = Nil)
      pendingRemaining <- HistoryPendingMatch.count(ctx.clubId)
      completedAt = Instant.now()
      totalStats = partialStats.copy(
        membersQueried = memberSeed.queried,
        membersSkipped = membersSkipped,
        membersFailed = memberSeed.failed,
        matchesSeeded = seedClub + memberSeed.seeded + seedStale,
        matchesRefreshed = refreshed,
        failedMembers = memberSeed.failedMembers,
        pendingRemaining = pendingRemaining.toInt
      )
      _ <- HistoryRun.complete(
        runId, completedAt, totalStats.matchesProcessed + totalStats.matchesSharedSkip, totalStats.playersDiscovered,
        totalStats.refreshMatchUnchanged, totalStats.seedClubMatchesUnchanged, totalStats.seedPlayerMatchesUnchanged
      )
      _ <- logSummary(totalStats, startedAt, completedAt)
      _ <- OutputFile.writeAndLog("history", clubSlug, formatReport(totalStats, clubSlug, startedAt, completedAt))
    } yield ()

  private def logSummary(stats: RunStats, startedAt: Instant, completedAt: Instant): URIO[CcasLogger, Unit] = {
    val duration = JDuration.between(startedAt, completedAt)
    for {
      _ <- CcasLogger.info("=== History Discovery Complete ===")
      _ <- CcasLogger.info(s"Duration: ${duration.display}")
      _ <- CcasLogger.info(
        s"Members queried: ${stats.membersQueried} | skipped: ${stats.membersSkipped} | failed: ${stats.membersFailed}"
      )
      _ <- CcasLogger.info(s"Matches seeded: ${stats.matchesSeeded}")
      _ <- CcasLogger.info(
        s"Matches processed: ${stats.matchesProcessed} | boards updated: ${stats.matchesBoardsUpdated} | failed: ${stats.matchesFailed} | unidentified: ${stats.matchesUnidentified} | shared skip: ${stats.matchesSharedSkip}" +
          (if (stats.matchesRefreshed > 0) { s" | refreshed: ${stats.matchesRefreshed}" } else { "" })
      )
      _ <- CcasLogger.info(
        s"Players discovered: ${stats.playersDiscovered} | known: ${stats.playersKnown} | failed: ${stats.playersFailed}"
      )
      _ <- CcasLogger.info(s"Waves: ${stats.waveCount}")
      _ <- CcasLogger.info(s"Pending remaining: ${stats.pendingRemaining}")
      _ <- CcasLogger.info(
        s"Unchanged skips: refresh=${stats.refreshMatchUnchanged} | seedClub=${stats.seedClubMatchesUnchanged} | seedPlayer=${stats.seedPlayerMatchesUnchanged}"
      )
    } yield ()
  }

  def formatReport(
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
    sb.append(s"Boards updated: ${stats.matchesBoardsUpdated}\n")
    sb.append(s"Failed:       ${stats.matchesFailed}\n")
    sb.append(s"Unidentified: ${stats.matchesUnidentified}\n")
    sb.append(s"Shared skip:  ${stats.matchesSharedSkip}\n")
    if (stats.matchesRefreshed > 0) { sb.append(s"Refreshed:    ${stats.matchesRefreshed}\n") }
    stats.waveDetails.foreach { case WaveDetail(wave, count) =>
      sb.append(s"  Wave $wave: $count matches\n")
    }
    sb.append(s"Pending:   ${stats.pendingRemaining}\n\n")

    sb.append("--- Players ---\n")
    sb.append(s"Discovered: ${stats.playersDiscovered}\n")
    sb.append(s"Known:      ${stats.playersKnown}\n")
    sb.append(s"Failed:     ${stats.playersFailed}\n\n")

    sb.append("--- Unchanged skips ---\n")
    sb.append(s"Refresh match:       ${stats.refreshMatchUnchanged}\n")
    sb.append(s"Seed club matches:   ${stats.seedClubMatchesUnchanged}\n")
    sb.append(s"Seed player matches: ${stats.seedPlayerMatchesUnchanged}\n\n")

    if (stats.failedMatches.nonEmpty) {
      sb.append("--- Failed Matches ---\n")
      stats.failedMatches.foreach { case FailedMatch(MatchKey(matchId, isLive), error) =>
        val kind = if (isLive) { " (live)" }
        else { "" }
        sb.append(s"  Match $matchId$kind: $error\n")
      }
      sb.append("\n")
    }

    if (stats.failedMembers.nonEmpty) {
      sb.append("--- Failed Member Queries ---\n")
      stats.failedMembers.foreach { case FailedMember(username, error) =>
        sb.append(s"  $username: $error\n")
      }
      sb.append("\n")
    }

    sb.toString
  }
}
