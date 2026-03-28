package ccas.analysis.apps.history

import java.time.{Instant, Duration as JDuration}
import com.augustnagro.magnum.Transactor
import zio.{RIO, Ref, Scope, URIO, ZEnvironment, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.Client
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.*
import ccas.utils.client.ChessComClient
import ccas.utils.errors.BadRequestException
import ccas.utils.sql.DataSourceLayer
import ccas.utils.{CcasLogger, OutputFile, display}
import HistoryUtils.*

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

  private case class InitResult(
    allMembers: List[ClubMember],
    snapByPlayerId: Map[PlayerId, PlayerSnapshot],
    queriedIds: Set[PlayerId],
    ctx: ProcessingContext,
    startedAt: Instant,
    runId: Long
  )

  private def initialize(
    clubSlug: ClubSlug,
    full: Boolean,
    trigger: RunTrigger
  ): RIO[CcasLogger & ChessComClient & Transactor, InitResult] =
    for {
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
      knownPlayersInit = latestSnaps.map(s => s.username.value -> s.playerId).toMap
      client <- ZIO.service[ChessComClient]
      ctx <- ProcessingContext.make(client, clubId, clubSlug, knownPlayersInit)
      effectiveQueriedIds = if (full) { Set.empty[PlayerId] } else { queriedIds }
    } yield InitResult(allMembers, snapByPlayerId, effectiveQueriedIds, ctx, startedAt, runId)

  /** Main entry point: orchestrates the 4-phase discover workflow for a club's match history. */
  def discover(
    clubSlug: ClubSlug,
    full: Boolean = false,
    refresh: Boolean = false,
    trigger: RunTrigger = RunTrigger.Cli
  ): RIO[CcasLogger & ChessComClient & Transactor, Unit] =
    for {
      transactor <- ZIO.service[Transactor]
      logger     <- ZIO.service[CcasLogger]

      // === Phase 1: Initialize ===
      InitResult(allMembers, snapByPlayerId, queriedIds, ctx, startedAt, runId) <-
        initialize(clubSlug, full, trigger)

      seedClubRef   <- Ref.make(0)
      memberSeedRef <- Ref.make(MemberSeedResult(0, 0, 0, Nil))
      seedStaleRef  <- Ref.make(0)

      // === Phases 2-3: Seed + Process (interrupt-safe) ===
      waveStats <- {
        for {
          // Phase 2: Seed match IDs
          _ <- CcasLogger.info("Phase 2: Seeding match IDs...")
          (resolvedClubs, resolvedPlayers) <-
            HistorySeeding.retryUnresolvedClubs(ctx.client) <&> HistorySeeding.retryUnresolvedPlayers(ctx.client)
          _ <- ZIO.whenDiscard(resolvedClubs > 0 || resolvedPlayers > 0) {
            CcasLogger.info(s"  Resolved $resolvedClubs clubs, $resolvedPlayers players from previous runs")
          }
          _ <- ZIO.whenDiscard(full) {
            CcasLogger.info("  --full: clearing member query history") *> HistoryMemberQuery.deleteClub(ctx.clubId)
          }

          seedClub <- HistorySeeding.seedFromClubMatches(ctx.client, ctx.clubId, clubSlug)
          _        <- seedClubRef.set(seedClub)
          _        <- CcasLogger.info(s"  Club matches endpoint: $seedClub new match IDs")

          memberSeed <-
            HistorySeeding.seedFromMemberMatches(ctx.client, ctx.clubId, clubSlug, allMembers, queriedIds, snapByPlayerId)
          _ <- memberSeedRef.set(memberSeed)
          membersSkipped = allMembers.size - memberSeed.queried - memberSeed.failed
          _ <- CcasLogger.info(
            s"  Member match lists: ${memberSeed.seeded} new IDs (queried: ${memberSeed.queried}, skipped: $membersSkipped, failed: ${memberSeed.failed})"
          )

          seedStale <- HistorySeeding.seedStaleMatches(ctx.clubId, refresh)
          _         <- seedStaleRef.set(seedStale)
          _         <- CcasLogger.info(s"  Stale match refresh: $seedStale matches queued")

          // Phase 3: Process matches (BFS waves)
          _ <- CcasLogger.info("Phase 3: Processing matches...")
          ws <- HistoryProcessing.processWaves(ctx)
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
      partialStats     <- HistoryProcessing.readStats(ctx, waveCount = 0, waveDetails = Nil)
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
