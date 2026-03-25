package ccas.analysis.apps.history

import java.time.{Instant, Duration as JDuration}
import com.augustnagro.magnum.{Transactor, sql}
import zio.{Promise, RIO, Ref, Scope, Task, UIO, ZEnvironment, ZIO, ZIOAppArgs, ZIOAppDefault}
import zio.http.{Client, URL}
import ccas.analysis.apps.membership.MembershipApp
import ccas.analysis.tables.*
import ccas.api.club.ApiClubMatches
import ccas.api.clubmatch.{ApiDailyMatch, ApiLiveMatch, TeamMatchTeams}
import ccas.api.clubmatch.ApiDailyMatch.*
import ccas.api.misc.enums.*
import ccas.api.misc.subtypes.*
import ccas.api.player.{ApiPlayer, ApiPlayerClubs, ApiPlayerMatches}
import ccas.utils.client.ChessComClient
import ccas.utils.errors.ExternalException
import ccas.utils.sql.DataSourceLayer
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.SqlZioTypes.{connectZIO, withTransaction}
import ccas.utils.{OutputFile, ProgressBar}

object HistoryApp extends ZIOAppDefault {

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
    playersDiscovered: Int = 0,
    playersKnown: Int = 0,
    playersFailed: Int = 0,
    waveCount: Int = 0,
    pendingRemaining: Int = 0,
    waveDetails: List[(Int, Int)] = Nil,
    failedMatches: List[((ClubMatchId, Boolean), String)] = Nil,
    failedMembers: List[(Username, String)] = Nil
  )

  // --- Shared state for Phase 3 processing ---

  private class ProcessingContext(
    val client: ChessComClient,
    val clubId: ClubId,
    val clubSlug: ClubSlug,
    val matchCache: Ref[Map[ClubMatchId, Promise[Throwable, ApiDailyMatch]]],
    val liveMatchCache: Ref[Map[ClubMatchId, Promise[Throwable, ApiLiveMatch]]],
    val discoveryCache: Ref[Map[String, Promise[Throwable, Option[PlayerId]]]],
    val knownPlayers: Ref[Map[String, PlayerId]],
    val newPlayers: Ref[Set[DiscoveredPlayer]],
    val matchesProcessed: Ref[Int],
    val matchesFailed: Ref[Int],
    val playersDiscovered: Ref[Int],
    val playersKnown: Ref[Int],
    val playersFailed: Ref[Int],
    val failedMatches: Ref[List[((ClubMatchId, Boolean), String)]]
  )

  private object ProcessingContext {
    def make(
      client: ChessComClient,
      clubId: ClubId,
      clubSlug: ClubSlug,
      initialKnownPlayers: Map[String, PlayerId]
    ): ZIO[Any, Nothing, ProcessingContext] =
      for {
        matchCache        <- Ref.make(Map.empty[ClubMatchId, Promise[Throwable, ApiDailyMatch]])
        liveMatchCache    <- Ref.make(Map.empty[ClubMatchId, Promise[Throwable, ApiLiveMatch]])
        discoveryCache    <- Ref.make(Map.empty[String, Promise[Throwable, Option[PlayerId]]])
        knownPlayers      <- Ref.make(initialKnownPlayers)
        newPlayers        <- Ref.make(Set.empty[DiscoveredPlayer])
        matchesProcessed  <- Ref.make(0)
        matchesFailed     <- Ref.make(0)
        playersDiscovered <- Ref.make(0)
        playersKnown      <- Ref.make(0)
        playersFailed     <- Ref.make(0)
        failedMatches     <- Ref.make(List.empty[((ClubMatchId, Boolean), String)])
      } yield new ProcessingContext(
        client,
        clubId,
        clubSlug,
        matchCache,
        liveMatchCache,
        discoveryCache,
        knownPlayers,
        newPlayers,
        matchesProcessed,
        matchesFailed,
        playersDiscovered,
        playersKnown,
        playersFailed,
        failedMatches
      )
  }

  // --- CLI entry point ---

  override def run: RIO[ZIOAppArgs & Scope, Unit] =
    (for {
      args <- ZIOAppArgs.getArgs
      clubName <- args.headOption match {
        case None    => ZIO.fail(ExternalException("Usage: HistoryApp <club-slug> [--full] [--refresh]"))
        case Some(s) => ZIO.succeed(ClubSlug.wrap(s))
      }
      full    = args.contains("--full")
      refresh = args.contains("--refresh")
      _ <- discover(clubName, full, refresh)
    } yield ()).provideSomeAuto(
      ChessComClient.live(),
      Client.default,
      DataSourceLayer.liveFromPrefix(onInit = Tables.ensureTables)
    )

  // --- Main workflow ---

  def discover(
    clubSlug: ClubSlug,
    full: Boolean = false,
    refresh: Boolean = false,
    trigger: RunTrigger = RunTrigger.Cli
  ): RIO[ChessComClient & Transactor, Unit] =
    for {
      client     <- ZIO.service[ChessComClient]
      transactor <- ZIO.service[Transactor]

      // === Phase 1: Initialize ===
      _ <- ZIO.logInfo(s"=== HistoryApp: $clubSlug ===")
      _ <- ZIO.logInfo("Phase 1: Initializing...")
      _ <- MembershipApp.reconcile(clubSlug, trackRun = false)
      club <- Club.selectBySlug(clubSlug)
        .someOrFail(ExternalException(s"Club '$clubSlug' not found"))
      clubId = club.clubId
      (allMembers, latestSnaps, processedCount, queriedIds) <-
        ClubMember.selectClub(clubId) <&>
        PlayerSnapshot.selectLatest <&>
        ClubMatch.countForClub(clubId) <&>
        HistoryMemberQuery.selectClubPlayerIds(clubId)
      startedAt = Instant.now()
      runId <- HistoryRun.insert(clubId, trigger, startedAt)
      snapByPlayerId = latestSnaps.map(s => s.playerId -> s).toMap
      _ <- ZIO.logInfo(
        s"  Members: ${allMembers.size}, Processed matches: $processedCount, Queried members: ${queriedIds.size}"
      )

      // === Phase 2: Seed match IDs ===
      _ <- ZIO.logInfo("Phase 2: Seeding match IDs...")
      _ <- ZIO.whenDiscard(full) {
        ZIO.logInfo("  --full: clearing member query history") *> HistoryMemberQuery.deleteClub(clubId)
      }
      effectiveQueriedIds =
        if (full) { Set.empty[PlayerId] }
        else { queriedIds }

      seedClub <- seedFromClubMatches(client, clubId, clubSlug)
      _        <- ZIO.logInfo(s"  Club matches endpoint: $seedClub new match IDs")

      memberSeed <-
        seedFromMemberMatches(client, clubId, clubSlug, allMembers, effectiveQueriedIds, snapByPlayerId)
      membersSkipped = allMembers.size - memberSeed.queried - memberSeed.failed
      _ <- ZIO.logInfo(
        s"  Member match lists: ${memberSeed.seeded} new IDs (queried: ${memberSeed.queried}, skipped: $membersSkipped, failed: ${memberSeed.failed})"
      )

      seedStale <- seedStaleMatches(clubId, refresh)
      _         <- ZIO.logInfo(s"  Stale match refresh: $seedStale matches queued")

      // === Phase 3: Process matches (BFS waves) ===
      _ <- ZIO.logInfo("Phase 3: Processing matches...")
      knownPlayersInit = latestSnaps.map(s => Username.unwrap(s.username).toLowerCase -> s.playerId).toMap
      ctx       <- ProcessingContext.make(client, clubId, clubSlug, knownPlayersInit)
      waveStats <- processWaves(ctx).onInterrupt(
        finalizeInterrupted(ctx, runId, startedAt, clubSlug, memberSeed, membersSkipped, seedClub, seedStale)
          .provideEnvironment(ZEnvironment(transactor))
          .orDie
      )

      // === Phase 4: Finalize ===
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

  private def seedFromClubMatches(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug
  ): RIO[Transactor, Int] =
    (for {
      clubMatches <- client.get[ApiClubMatches](ApiClubMatches.getUrl(clubSlug))
      allDaily = clubMatches.dailyFinished ++ clubMatches.dailyInProgress ++ clubMatches.dailyRegistered
      dailyPending = allDaily.map(m => HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = false))
      nonDaily = clubMatches.finished.filterNot(_.timeClass.isDaily) ++
        clubMatches.inProgress.filterNot(_.timeClass.isDaily) ++
        clubMatches.registered.filterNot(_.timeClass.isDaily)
      livePending = nonDaily.map(m => HistoryPendingMatch(clubId, ClubMatchId.fromUrl(m.`@id`), isLive = true))
      all = dailyPending ++ livePending
      _ <- insertPendingMatches(all)
    } yield all.size).catchAll { error =>
      ZIO.logWarning(s"  Failed to fetch club matches: ${error.getMessage}").as(0)
    }

  private def seedFromMemberMatches(
    client: ChessComClient,
    clubId: ClubId,
    clubSlug: ClubSlug,
    allMembers: List[ClubMember],
    queriedIds: Set[PlayerId],
    snapByPlayerId: Map[PlayerId, PlayerSnapshot]
  ): RIO[Transactor, MemberSeedResult] = {
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
          bar <- ProgressBar.scoped
          _ <- ZIO.foreachParDiscard(toQuery) { case (playerId, username) =>
            seedMatchesForPlayer(client, clubId, clubSlug, playerId, username).foldZIO(
              error => failRef.update(_ + 1)
                *> failedMembersRef.update(_ :+ (username, error.getMessage))
                *> bar.logWarning(s"  $username: failed — ${error.getMessage}"),
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
    m.club.path.segments.lastOption.exists(_.equalsIgnoreCase(ClubSlug.unwrap(clubSlug))) &&
      !m.`@id`.path.segments.contains("live")

  private[history] def isClubLiveMatch(m: ApiPlayerMatches.ApiPlayerMatch, clubSlug: ClubSlug): Boolean =
    m.club.path.segments.lastOption.exists(_.equalsIgnoreCase(ClubSlug.unwrap(clubSlug))) &&
      m.`@id`.path.segments.contains("live")

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
      _ <- HistoryMemberQuery.insert(HistoryMemberQuery(clubId, playerId, Instant.now()))
    } yield all.size

  private def insertPendingMatches(items: Iterable[HistoryPendingMatch]): RIO[Transactor, Unit] =
    ZIO.foreachDiscard(items.grouped(1000).toList)(HistoryPendingMatch.insertBatch)

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

  private def processWaves(ctx: ProcessingContext): RIO[Transactor, RunStats] = {
    def waveLoop(waveCount: Int, waveDetails: List[(Int, Int)]): RIO[Transactor, RunStats] =
      for {
        pendingCount <- HistoryPendingMatch.count(ctx.clubId)
        result <- if (pendingCount == 0) { readStats(ctx, waveCount, waveDetails) } else {
          val wave = waveCount + 1
          for {
            _ <- ZIO.logInfo(s"  Wave $wave: $pendingCount matches to process")
            _ <- ctx.newPlayers.set(Set.empty)
            beforeCount <- ctx.matchesProcessed.get

            waveCounter <- Ref.make(0)
            _ <- ZIO.scoped {
              ProgressBar.scoped.flatMap(waveBar => processAllPending(ctx, waveBar, waveCounter, pendingCount))
            }

            afterCount <- ctx.matchesProcessed.get
            failedCount <- ctx.matchesFailed.get
            waveProcessed = afterCount - beforeCount
            _ <- ZIO.logInfo(s"  Wave $wave complete: $waveProcessed processed, $failedCount failed total")

            // Seed matches for newly discovered players
            newPlayers <- ctx.newPlayers.get
            _ <- ZIO.whenDiscard(newPlayers.nonEmpty) {
              ZIO.logInfo(s"  Querying match lists for ${newPlayers.size} discovered players...") *>
                ZIO.foreachParDiscard(newPlayers) { dp =>
                  seedMatchesForPlayer(ctx.client, ctx.clubId, ctx.clubSlug, dp.playerId, dp.username).catchAll {
                    error => ZIO.logWarning(s"  ${dp.username}: failed to seed — ${error.getMessage}")
                  }
                }
            }

            newPending <- HistoryPendingMatch.count(ctx.clubId)
            updatedDetails = waveDetails :+ (wave, waveProcessed)
            r <-
              if (newPending > 0 && newPlayers.nonEmpty) { waveLoop(wave, updatedDetails) }
              else { readStats(ctx, wave, updatedDetails).map(_.copy(pendingRemaining = newPending.toInt)) }
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
  ): RIO[Transactor, Unit] =
    for {
      batch     <- HistoryPendingMatch.selectClubBatch(ctx.clubId, BatchSize)
      failedKeys <- ctx.failedMatches.get.map(_.map(_._1).toSet)
      filtered = batch.filterNot(m => failedKeys.contains((m.matchId, m.isLive)))
      _ <- ZIO.whenDiscard(filtered.nonEmpty) {
        processMatchBatch(ctx, filtered, bar, counter, waveTotal) *> processAllPending(ctx, bar, counter, waveTotal)
      }
    } yield ()

  private def processMatchBatch(
    ctx: ProcessingContext,
    pending: List[HistoryPendingMatch],
    bar: ProgressBar,
    counter: Ref[Int],
    waveTotal: Long
  ): RIO[Transactor, Unit] =
    ZIO.foreachParDiscard(pending) { pm =>
      processMatch(ctx, pm.matchId, pm.isLive, bar)
        .zipLeft(ctx.matchesProcessed.update(_ + 1))
        .catchAll { error =>
          ctx.matchesFailed.update(_ + 1) *>
            ctx.failedMatches.update(_ :+ ((pm.matchId, pm.isLive), error.getMessage)) *>
            bar.logWarning(s"    Match ${pm.matchId}${if (pm.isLive) " (live)" else ""}: ${error.getMessage}")
        } *> counter.updateAndGet(_ + 1).flatMap { n =>
        bar.print(n, waveTotal.toInt, s"    Processing matches: $n/$waveTotal")
      }
    }

  private def processMatch(
    ctx: ProcessingContext,
    matchId: ClubMatchId,
    isLive: Boolean,
    bar: ProgressBar
  ): RIO[Transactor, Unit] =
    if (isLive) { processLiveMatch(ctx, matchId, bar) }
    else { processDailyMatch(ctx, matchId, bar) }

  private def processDailyMatch(ctx: ProcessingContext, matchId: ClubMatchId, bar: ProgressBar): RIO[Transactor, Unit] =
    for {
      dailyMatch <- fetchMatch(ctx, matchId)
      weAreTeam1 <- ZIO.fromOption(findOurTeam(dailyMatch.teams, ctx.clubSlug))
        .orElse(findOurTeamByClubId(dailyMatch.teams, ctx.clubId))
        .orElseFail(ExternalException(s"Club ${ctx.clubId} not found in match $matchId teams"))

      opponentTeam = if (weAreTeam1) { dailyMatch.teams.team2 } else { dailyMatch.teams.team1 }

      opponentClubId <- resolveClubIdFromTeamUrl(opponentTeam.`@id`)
      clubMatch = buildClubMatchRow(matchId, dailyMatch, ctx.clubId, weAreTeam1, opponentClubId)

      boardRows <- buildBoardRows(ctx, matchId, dailyMatch, weAreTeam1, clubMatch.startTime, bar)

      _ <- withTransaction {
        for {
          _ <- ClubMatch.upsert(clubMatch)
          _ <- ClubMatchBoard.deleteMatch(matchId)
          _ <- ClubMatchBoard.insertBatch(boardRows)
          _ <- HistoryPendingMatch.delete(ctx.clubId, matchId, isLive = false)
        } yield ()
      }
    } yield ()

  private def processLiveMatch(ctx: ProcessingContext, matchId: ClubMatchId, bar: ProgressBar): RIO[Transactor, Unit] =
    for {
      liveMatch <- fetchLiveMatch(ctx, matchId)
      weAreTeam1 <- ZIO.fromOption(findOurTeam(liveMatch.teams, ctx.clubSlug))
        .orElse(findOurTeamByClubId(liveMatch.teams, ctx.clubId))
        .orElseFail(ExternalException(s"Club ${ctx.clubId} not found in live match $matchId teams"))

      opponentTeam = if (weAreTeam1) { liveMatch.teams.team2 } else { liveMatch.teams.team1 }
      _ <- resolveClubIdFromTeamUrl(opponentTeam.`@id`)

      // Discover players from both teams (for BFS wave expansion)
      startTime = Some(Instant.ofEpochSecond(liveMatch.startTime))
      _ <- ZIO.foreachParDiscard(liveMatch.teams.team1.players ++ liveMatch.teams.team2.players) { p =>
        resolvePlayerId(ctx, p.username, isOurTeam = false, startTime, bar)
      }

      _ <- HistoryPendingMatch.delete(ctx.clubId, matchId, isLive = true)
    } yield ()

  // === Board Row Construction ===

  private def buildBoardRows(
    ctx: ProcessingContext,
    matchId: ClubMatchId,
    dailyMatch: ApiDailyMatch,
    weAreTeam1: Boolean,
    matchStartTime: Option[Instant],
    bar: ProgressBar
  ): RIO[Transactor, List[ClubMatchBoard]] =
    dailyMatch match {
      case _: ApiDailyMatchRegistered => ZIO.succeed(Nil)
      case _ =>
        val teams        = dailyMatch.teams
        val team1FpLower = teams.team1.fairPlayRemovals.map(u => Username.unwrap(u).toLowerCase)
        val team2FpLower = teams.team2.fairPlayRemovals.map(u => Username.unwrap(u).toLowerCase)

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
              .orElseFail(ExternalException(s"Match $matchId board $boardNum: missing team1 player"))
            t2Player <- ZIO.fromOption(team2ByBoard.get(boardNum))
              .orElseFail(ExternalException(s"Match $matchId board $boardNum: missing team2 player"))

            t1Username = t1Player.username
            t2Username = t2Player.username
            t1FairPlay = team1FpLower.contains(Username.unwrap(t1Username).toLowerCase)
            t2FairPlay = team2FpLower.contains(Username.unwrap(t2Username).toLowerCase)

            t1Pid <-
              if (weAreTeam1) { resolvePlayerId(ctx, t1Username, isOurTeam = true, matchStartTime, bar) }
              else { ZIO.none }
            t2Pid <-
              if (!weAreTeam1) { resolvePlayerId(ctx, t2Username, isOurTeam = true, matchStartTime, bar) }
              else { ZIO.none }
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
              team1Username = t1Username,
              team1FairPlay = t1FairPlay,
              team2PlayerId = t2Pid,
              team2Username = t2Username,
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

  private def resolvePlayerId(
    ctx: ProcessingContext,
    username: Username,
    isOurTeam: Boolean,
    matchStartTime: Option[Instant],
    bar: ProgressBar
  ): RIO[Transactor, Option[PlayerId]] = {
    val key = Username.unwrap(username).toLowerCase
    ctx.knownPlayers.get.map(_.get(key)).flatMap {
      case Some(playerId) => ctx.playersKnown.update(_ + 1).as(Some(playerId))
      case None if !isOurTeam => ZIO.none
      case None => discoverPlayer(ctx, username, key, matchStartTime, bar)
    }
  }

  /** Gates the full discovery flow (API fetch + DB insert) behind a Promise so that concurrent fibers resolving the
    * same unknown player share a single API call and a single DB insert.
    */
  private def discoverPlayer(
    ctx: ProcessingContext,
    username: Username,
    key: String,
    matchStartTime: Option[Instant],
    bar: ProgressBar
  ): RIO[Transactor, Option[PlayerId]] =
    for {
      promise <- Promise.make[Throwable, Option[PlayerId]]
      action <- ctx.discoveryCache.modify { m =>
        m.get(key) match {
          case Some(existing) => (existing.await, m)
          case None           => (doDiscoverPlayer(ctx, username, key, promise, matchStartTime, bar), m + (key -> promise))
        }
      }
      result <- action
    } yield result

  private def doDiscoverPlayer(
    ctx: ProcessingContext,
    username: Username,
    key: String,
    promise: Promise[Throwable, Option[PlayerId]],
    matchStartTime: Option[Instant],
    bar: ProgressBar
  ): RIO[Transactor, Option[PlayerId]] = {
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
                _ <- createClubMemberForDiscovered(ctx, apiPlayer, matchStartTime)
                _ <- ctx.knownPlayers.update(_ + (key -> playerId))
                _ <- ctx.newPlayers.update(_ + DiscoveredPlayer(playerId, username))
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
        *> bar.logWarning(s"    Cannot resolve player $username: ${error.getMessage}")
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

  private[history] def findOurTeam(teams: TeamMatchTeams, clubSlug: ClubSlug): Option[Boolean] = {
    val name = ClubSlug.unwrap(clubSlug)
    if (teams.team1.`@id`.path.segments.lastOption.exists(_.equalsIgnoreCase(name))) { Some(true) }
    else if (teams.team2.`@id`.path.segments.lastOption.exists(_.equalsIgnoreCase(name))) { Some(false) }
    else { None }
  }

  private def findOurTeamByClubId(teams: TeamMatchTeams, clubId: ClubId): RIO[Transactor, Boolean] =
    for {
      t1Id <- resolveClubIdFromTeamUrl(teams.team1.`@id`)
      result <-
        if (t1Id.contains(clubId)) { ZIO.succeed(true) }
        else {
          resolveClubIdFromTeamUrl(teams.team2.`@id`).flatMap {
            case Some(id) if id == clubId => ZIO.succeed(false)
            case _                        => ZIO.fail(ExternalException("Club not found in either team"))
          }
        }
    } yield result

  private def resolveClubIdFromTeamUrl(teamUrl: URL): RIO[Transactor, Option[ClubId]] =
    teamUrl.path.segments.lastOption match {
      case None          => ZIO.none
      case Some(segment) => Club.selectBySlug(ClubSlug.wrap(segment)).map(_.map(_.clubId))
    }

  private[history] def buildClubMatchRow(
    matchId: ClubMatchId,
    dailyMatch: ApiDailyMatch,
    clubId: ClubId,
    weAreTeam1: Boolean,
    opponentClubId: Option[ClubId]
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
    val (team1ClubId, team2ClubId) = if (weAreTeam1) { (Some(clubId), opponentClubId) }
    else { (opponentClubId, Some(clubId)) }

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
      team1Name = teams.team1.name,
      team1Score = teams.team1.score,
      team1Result = team1Result,
      team2ClubId = team2ClubId,
      team2Name = teams.team2.name,
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
  ): RIO[Transactor, Unit] =
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
      matchesProcessed  <- ctx.matchesProcessed.get
      matchesFailed     <- ctx.matchesFailed.get
      playersDiscovered <- ctx.playersDiscovered.get
      playersKnown      <- ctx.playersKnown.get
      playersFailed     <- ctx.playersFailed.get
      failedMatches     <- ctx.failedMatches.get
    } yield RunStats(
      matchesProcessed = matchesProcessed,
      matchesFailed = matchesFailed,
      playersDiscovered = playersDiscovered,
      playersKnown = playersKnown,
      playersFailed = playersFailed,
      waveCount = waveCount,
      waveDetails = waveDetails,
      failedMatches = failedMatches
    )

  private def logSummary(stats: RunStats, startedAt: Instant, completedAt: Instant): UIO[Unit] = {
    val duration = JDuration.between(startedAt, completedAt)
    for {
      _ <- ZIO.logInfo("=== History Discovery Complete ===")
      _ <- ZIO.logInfo(s"Duration: ${duration.toMinutes}m ${duration.toSecondsPart}s")
      _ <- ZIO.logInfo(
        s"Members queried: ${stats.membersQueried} / skipped: ${stats.membersSkipped} / failed: ${stats.membersFailed}"
      )
      _ <- ZIO.logInfo(s"Matches seeded: ${stats.matchesSeeded}")
      _ <- ZIO.logInfo(s"Matches processed: ${stats.matchesProcessed} / failed: ${stats.matchesFailed}")
      _ <- ZIO.logInfo(
        s"Players discovered: ${stats.playersDiscovered} / known: ${stats.playersKnown} / failed: ${stats.playersFailed}"
      )
      _ <- ZIO.logInfo(s"Waves: ${stats.waveCount}")
      _ <- ZIO.logInfo(s"Pending remaining: ${stats.pendingRemaining}")
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
    sb.append(s"Duration:  ${duration.toMinutes}m ${duration.toSecondsPart}s\n\n")

    sb.append("--- Members ---\n")
    sb.append(s"Queried: ${stats.membersQueried}\n")
    sb.append(s"Skipped: ${stats.membersSkipped}\n")
    sb.append(s"Failed:  ${stats.membersFailed}\n\n")

    sb.append("--- Matches ---\n")
    sb.append(s"Seeded:    ${stats.matchesSeeded}\n")
    sb.append(s"Processed: ${stats.matchesProcessed}\n")
    sb.append(s"Failed:    ${stats.matchesFailed}\n")
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
      stats.failedMatches.foreach { case ((matchId, isLive), error) =>
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
