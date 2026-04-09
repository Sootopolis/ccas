package ccas.analysis.apps.history

import java.time.{Duration as JDuration, Instant}

import zio.{Promise, RIO, Ref, Task, UIO, ZIO}
import zio.http.URL
import HistoryUtils.*

import ccas.analysis.GameScoring
import ccas.analysis.apps.PlayerUpdater
import ccas.analysis.tables.*
import ccas.api.clubmatch.{ApiDailyMatch, ApiLiveMatch, ApiMatchBoard}
import ccas.api.clubmatch.ApiDailyMatch.*
import ccas.api.clubmatch.ApiMatchBoard.{ApiBoardGame, ApiBoardPlayer}
import ccas.api.misc.enums.*
import ccas.api.misc.subtypes.*
import ccas.api.player.{ApiPlayer, ApiPlayerClubs}
import ccas.utils.{CcasLogger, ProgressBar}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.withTransaction

private[history] object HistoryProcessing {

  private val BatchSize        = 500
  private val MatchParallelism = 16

  // === BFS Wave Processing ===

  /** BFS wave loop: processes pending matches in batches, discovers new players, seeds their match lists, and repeats
    * until no new pending matches remain. Returns accumulated stats.
    */
  def processWaves(
    ctx: ProcessingContext,
    settledMatchIds: Set[ClubMatchId],
    shared: Option[SharedContext] = None
  ): RIO[CcasLogger & PostgresClient, RunStats] = {
    def waveLoop(waveCount: Int, waveDetails: List[(Int, Int)]): RIO[CcasLogger & PostgresClient, RunStats] =
      for {
        pendingCount <- HistoryPendingMatch.countNew(ctx.clubId)
        result <-
          if (pendingCount == 0) { readStats(ctx, waveCount, waveDetails) }
          else {
            val wave = waveCount + 1
            for {
              _           <- CcasLogger.info(s"  Wave $wave: $pendingCount matches to process")
              _           <- ctx.newPlayers.set(Set.empty)
              beforeCount <- ctx.matchesProcessed.get

              waveCounter <- Ref.make(0)
              _ <- ZIO.scoped {
                CcasLogger.progressBar.flatMap(waveBar =>
                  processAllPending(ctx, waveBar, waveCounter, pendingCount, shared)
                )
              }

              afterCount  <- ctx.matchesProcessed.get
              failedCount <- ctx.matchesFailed.get
              waveProcessed = afterCount - beforeCount
              _ <- CcasLogger.info(s"  Wave $wave complete: $waveProcessed processed, $failedCount failed total")

              // Seed matches for newly discovered players (track in shared context so later clubs don't re-query)
              newPlayers <- ctx.newPlayers.get
              _ <- ZIO.whenDiscard(newPlayers.nonEmpty) {
                CcasLogger.info(s"  Querying match lists for ${newPlayers.size} discovered players...") *>
                  ZIO.foreachParDiscard(newPlayers) { dp =>
                    HistorySeeding
                      .seedMatchesForPlayer(
                        ctx.client, ctx.clubId, ctx.clubSlug, dp.playerId, dp.username, settledMatchIds
                      )
                      .zipLeft(ZIO.foreachDiscard(shared)(_.queriedPlayers.update(_ + dp.playerId)))
                      .catchAll(error => CcasLogger.warn(s"  ${dp.username}: failed to seed — ${error.getMessage}"))
                  }.withParallelism(MatchParallelism)
              }

              newPendingNew <- HistoryPendingMatch.countNew(ctx.clubId)
              updatedDetails = waveDetails :+ (wave, waveProcessed)
              r <-
                if (newPendingNew > 0 && newPlayers.nonEmpty) { waveLoop(wave, updatedDetails) }
                else {
                  for {
                    pendingTotal <- HistoryPendingMatch.count(ctx.clubId)
                    stats        <- readStats(ctx, wave, updatedDetails)
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
    waveTotal: Long,
    shared: Option[SharedContext]
  ): RIO[CcasLogger & PostgresClient, Unit] =
    for {
      batch <- HistoryPendingMatch.selectClubBatch(ctx.clubId, BatchSize)
      _ <- ZIO.whenDiscard(batch.nonEmpty) {
        processMatchBatch(ctx, batch, bar, counter, waveTotal, shared) *>
          processAllPending(ctx, bar, counter, waveTotal, shared)
      }
    } yield ()

  private def processMatchBatch(
    ctx: ProcessingContext,
    pending: List[HistoryPendingMatch],
    bar: ProgressBar,
    counter: Ref[Int],
    waveTotal: Long,
    shared: Option[SharedContext]
  ): RIO[CcasLogger & PostgresClient, Unit] =
    ZIO.foreachParDiscard(pending) { pm =>
      processMatch(ctx, pm.matchId, pm.isLive, shared)
        .catchAll { error =>
          ctx.matchesFailed.update(_ + 1) *>
            ctx.failedMatches.update(_ :+ (MatchKey(pm.matchId, pm.isLive), error.getMessage)) *>
            HistoryPendingMatch.updateStatus(ctx.clubId, pm.matchId, pm.isLive, PendingMatchStatus.ApiError) *>
            CcasLogger.warn(s"    Match ${pm.matchId}${if (pm.isLive) " (live)" else ""}: ${error.getMessage}")
        } *> counter.updateAndGet(_ + 1).flatMap { n =>
        bar.print(n, waveTotal.toInt, s"    Processing matches: $n/$waveTotal")
      }
    }.withParallelism(MatchParallelism)

  private def processMatch(
    ctx: ProcessingContext,
    matchId: ClubMatchId,
    isLive: Boolean,
    shared: Option[SharedContext]
  ): RIO[CcasLogger & PostgresClient, Unit] =
    for {
      alreadyProcessed <- shared.fold(ZIO.succeed(false))(_.processedMatches.get.map(_.contains(matchId)))
      _ <-
        if (alreadyProcessed) {
          // Match was fully processed by a prior club in this batch — skip API calls, just clean up pending
          HistoryPendingMatch.delete(ctx.clubId, matchId, isLive) *>
            ctx.matchesSharedSkip.update(_ + 1)
        } else {
          (if (isLive) { processLiveMatch(ctx, matchId, shared) }
           else { processDailyMatch(ctx, matchId, shared) }) *>
            ctx.matchesProcessed.update(_ + 1)
        }
    } yield ()

  /** Fetches a daily match from the API, resolves team clubs, builds and persists board rows. Marks the match
    * Unidentified if the target club is not found in either team (data saved, BFS skipped).
    */
  private def processDailyMatch(
    ctx: ProcessingContext,
    matchId: ClubMatchId,
    shared: Option[SharedContext]
  ): RIO[CcasLogger & PostgresClient, Unit] =
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

      // Check if board scores are unchanged — if so, skip board API calls and row rebuild
      expectedScores = computeExpectedScores(dailyMatch)
      existingBoards <- ClubMatchBoard.selectMatch(matchId)
      boardSkip = scoresMatch(expectedScores, existingBoards)

      _ <- if (boardSkip) {
        withTransaction {
          for {
            _ <- ClubMatch.upsert(clubMatch)
            _ <-
              if (weAreTeam1.isDefined) {
                HistoryPendingMatch.delete(ctx.clubId, matchId, isLive = false)
              } else {
                HistoryPendingMatch.updateStatus(ctx.clubId, matchId, false, PendingMatchStatus.Unidentified)
              }
          } yield ()
        }
      } else {
        for {
          boardAndGames <- buildBoardAndGameRows(ctx, matchId, dailyMatch, weAreTeam1, clubMatch.startTime)
          (boardRows, gameRowLists) = boardAndGames.unzip
          gameRows = gameRowLists.flatten
          _ <- withTransaction {
            for {
              _ <- ClubMatch.upsert(clubMatch)
              _ <- ClubMatchBoard.deleteMatch(matchId)
              _ <- ClubMatchBoard.insertBatch(boardRows)
              _ <- ClubMatchGame.insertBatch(gameRows)
              _ <-
                if (weAreTeam1.isDefined) {
                  HistoryPendingMatch.delete(ctx.clubId, matchId, isLive = false)
                } else {
                  HistoryPendingMatch.updateStatus(ctx.clubId, matchId, false, PendingMatchStatus.Unidentified)
                }
            } yield ()
          }
          _ <- ctx.matchesBoardsUpdated.update(_ + 1)
        } yield ()
      }
      _ <- ZIO.whenDiscard(weAreTeam1.isEmpty) {
        ctx.matchesUnidentified.update(_ + 1) *>
          CcasLogger.warn(s"    Match $matchId: club ${ctx.clubId} not found in either team (data saved, BFS skipped)")
      }
      _ <- ZIO.foreachDiscard(shared)(_.processedMatches.update(_ + matchId))
    } yield ()

  /** Fetches a live match and resolves its players for BFS expansion. No board rows are persisted for live matches. */
  private def processLiveMatch(
    ctx: ProcessingContext,
    matchId: ClubMatchId,
    shared: Option[SharedContext]
  ): RIO[CcasLogger & PostgresClient, Unit] =
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
      _ <- ZIO.foreachDiscard(shared)(_.processedMatches.update(_ + matchId))
    } yield ()

  // === Settled Match Refresh ===

  /** Refreshes settled matches by re-fetching from the API — no pending table involvement. A match is settled when it is
    * finished and its `fetchedAt` is at least `StaleWindowDays` (90) past `endTime`; non-finished and recently finished
    * matches are always handled by stale seeding in Phase 2 instead. Uses cursor-based pagination (`ORDER BY match_id`)
    * so each match is attempted at most once per run; failed matches keep their old `fetchedAt` and are retried on the
    * next `--refresh` invocation.
    */
  def refreshSettledMatches(
    ctx: ProcessingContext,
    minAgeHours: Int
  ): RIO[CcasLogger & PostgresClient, Int] = {
    val cutoffTime =
      if (minAgeHours <= 0) { Instant.now() }
      else { Instant.now().minus(JDuration.ofHours(minAgeHours.toLong)) }
    for {
      total <- ClubMatch.countSettledForRefresh(ctx.clubId, cutoffTime)
      refreshed <-
        if (total == 0) { CcasLogger.info("  No settled matches to refresh").as(0) }
        else {
          CcasLogger.info(s"  $total settled matches to refresh") *>
            ZIO.scoped {
              for {
                bar     <- CcasLogger.progressBar
                counter <- Ref.make(0)
                failed  <- Ref.make(0)
                _       <- refreshLoop(ctx, cutoffTime, bar, counter, failed, total.toInt, ClubMatchId(0))
                f       <- failed.get
                _       <- ZIO.whenDiscard(f > 0)(CcasLogger.info(s"  Refresh: $f failed (will retry next run)"))
              } yield total.toInt - f
            }
        }
    } yield refreshed
  }

  private def refreshLoop(
    ctx: ProcessingContext,
    cutoffTime: Instant,
    bar: ProgressBar,
    counter: Ref[Int],
    failed: Ref[Int],
    total: Int,
    cursor: ClubMatchId
  ): RIO[CcasLogger & PostgresClient, Unit] =
    for {
      batch <- ClubMatch.selectSettledForRefreshBatch(ctx.clubId, cutoffTime, BatchSize, cursor)
      _ <- ZIO.whenDiscard(batch.nonEmpty) {
        ZIO.foreachParDiscard(batch) { matchId =>
          refreshSingleMatch(ctx, matchId)
            .catchAll { error =>
              failed.update(_ + 1) *>
                CcasLogger.warn(s"    Refresh $matchId: ${error.getMessage}")
            } *> counter.updateAndGet(_ + 1).flatMap { n =>
            bar.print(n, total, s"    Refreshing: $n/$total")
          }
        }.withParallelism(MatchParallelism) *>
          refreshLoop(ctx, cutoffTime, bar, counter, failed, total, batch.last)
      }
    } yield ()

  private def refreshSingleMatch(
    ctx: ProcessingContext,
    matchId: ClubMatchId
  ): RIO[CcasLogger & PostgresClient, Unit] =
    for {
      dailyMatch <- fetchMatch(ctx, matchId)

      (team1ClubId, team2ClubId) <-
        resolveClubIdFromTeamUrl(ctx, dailyMatch.teams.team1.`@id`) <&>
          resolveClubIdFromTeamUrl(ctx, dailyMatch.teams.team2.`@id`)

      _ <- trackUnresolvedClub(matchId, isTeam1 = true, dailyMatch.teams.team1.`@id`, team1ClubId) <&>
        trackUnresolvedClub(matchId, isTeam1 = false, dailyMatch.teams.team2.`@id`, team2ClubId)

      clubMatch = buildClubMatchRow(matchId, dailyMatch, team1ClubId, team2ClubId)

      weAreTeam1: Option[Boolean] =
        if (team1ClubId.contains(ctx.clubId)) { Some(true) }
        else if (team2ClubId.contains(ctx.clubId)) { Some(false) }
        else { None }

      expectedScores = computeExpectedScores(dailyMatch)
      existingBoards <- ClubMatchBoard.selectMatch(matchId)
      boardSkip = scoresMatch(expectedScores, existingBoards)

      _ <- if (boardSkip) {
        ClubMatch.upsert(clubMatch)
      } else {
        for {
          boardAndGames <- buildBoardAndGameRows(ctx, matchId, dailyMatch, weAreTeam1, clubMatch.startTime)
          (boardRows, gameRowLists) = boardAndGames.unzip
          _ <- withTransaction {
            for {
              _ <- ClubMatch.upsert(clubMatch)
              _ <- ClubMatchBoard.deleteMatch(matchId)
              _ <- ClubMatchBoard.insertBatch(boardRows)
              _ <- ClubMatchGame.insertBatch(gameRowLists.flatten)
            } yield ()
          }
          _ <- ctx.matchesBoardsUpdated.update(_ + 1)
        } yield ()
      }
    } yield ()

  // === Board Row Construction ===

  /** Constructs ClubMatchBoard and ClubMatchGame rows by resolving player IDs, normalizing game outcomes, and
    * optionally fetching board-level API data for game IDs, times, and ratings. Players that can't be resolved are
    * recorded in `unresolved_board_player` and the board row gets a None player ID.
    */
  private def buildBoardAndGameRows(
    ctx: ProcessingContext,
    matchId: ClubMatchId,
    dailyMatch: ApiDailyMatch,
    weAreTeam1: Option[Boolean],
    matchStartTime: Option[Instant]
  ): RIO[CcasLogger & PostgresClient, List[(ClubMatchBoard, List[ClubMatchGame])]] =
    dailyMatch match {
      case _: ApiDailyMatchRegistered => ZIO.succeed(Nil)
      case _ =>
        val teams   = dailyMatch.teams
        val team1Fp = teams.team1.fairPlayRemovals.map(_.value)
        val team2Fp = teams.team2.fairPlayRemovals.map(_.value)

        val team1ByBoard: Map[Short, MatchPlayerStarted] = teams.team1.players.collect { case p: MatchPlayerStarted =>
          p.board.path.segments.last.toShort -> p
        }.toMap
        val team2ByBoard: Map[Short, MatchPlayerStarted] = teams.team2.players.collect { case p: MatchPlayerStarted =>
          p.board.path.segments.last.toShort -> p
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

            boardData <- ctx.client.get[ApiMatchBoard](ApiMatchBoard.dailyUrl(matchId, boardNum)).option
          } yield {
            val (g1Winner, g1Detail) =
              normalizeGameOutcome(t1Player.playedAsWhite, t2Player.playedAsBlack, whiteTeamIsTeam1 = true)
            val (g2Winner, g2Detail) =
              normalizeGameOutcome(t2Player.playedAsWhite, t1Player.playedAsBlack, whiteTeamIsTeam1 = false)
            val (t1Score, t2Score) = computeScoreX2(g1Winner, g2Winner, t1FairPlay, t2FairPlay)

            val board = ClubMatchBoard(
              matchId = matchId,
              board = boardNum,
              team1PlayerId = t1Pid,
              team1FairPlay = t1FairPlay,
              team2PlayerId = t2Pid,
              team2FairPlay = t2FairPlay,
              team1ScoreX2 = t1Score,
              team2ScoreX2 = t2Score
            )

            // Partition board API games by team perspective (team1-white vs team2-white)
            val (game1Data, game2Data) = boardData match {
              case Some(bd) =>
                val (t1w, t2w) = bd.games.partition(g => g.white.exists(_.username == t1Username))
                (t1w.headOption, t2w.headOption)
              case None => (None, None)
            }

            val games = List(
              buildGameRow(matchId, boardNum, team1IsWhite = true, g1Winner, g1Detail, game1Data),
              buildGameRow(matchId, boardNum, team1IsWhite = false, g2Winner, g2Detail, game2Data)
            ).flatten

            (board, games)
          }
        }
    }

  /** Builds a ClubMatchGame row if there's a match-level outcome or board-level game data. Extracts game ID,
    * start/end times, and post-game ratings from the board API data when available.
    */
  private def buildGameRow(
    matchId: ClubMatchId,
    board: Short,
    team1IsWhite: Boolean,
    winner: Option[BoardGameWinner],
    detail: Option[GameResultDetail],
    boardGame: Option[ApiBoardGame]
  ): Option[ClubMatchGame] =
    Option.unless(winner.isEmpty && boardGame.isEmpty) {
      val gameId = boardGame.map(g => g.url.path.segments.last.toLong)
      val startTime = boardGame.flatMap(_.startTime)
      val endTime = boardGame.flatMap(_.endTime)

      // team1 is white when team1IsWhite=true, so white=team1, black=team2
      // team1 is black when team1IsWhite=false, so white=team2, black=team1
      val (t1Rating, t2Rating) = boardGame match {
        case Some(g) if team1IsWhite => (finishedRating(g.white), finishedRating(g.black))
        case Some(g)                 => (finishedRating(g.black), finishedRating(g.white))
        case None                    => (None, None)
      }

      ClubMatchGame(
        matchId = matchId,
        board = board,
        team1IsWhite = team1IsWhite,
        gameId = gameId,
        startTime = startTime,
        endTime = endTime,
        winner = winner,
        detail = detail,
        team1Rating = t1Rating,
        team2Rating = t2Rating
      )
    }

  private def finishedRating(p: Either[URL, ApiBoardPlayer]): Option[Elo] =
    p.toOption.filter(_.result.isDefined).map(_.rating)

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
    def gameScore(winner: Option[BoardGameWinner]): (Int, Int) = {
      val t1 = GameScoring.classifyGame(winner, team1FairPlay, team2FairPlay).fold(0)(GameScoring.scoreX2)
      val t2 = winner.fold(0)(_ => 2 - t1)
      (t1, t2)
    }

    val (g1t1, g1t2) = gameScore(game1Winner)
    val (g2t1, g2t2) = gameScore(game2Winner)
    ((g1t1 + g2t1).toShort, (g1t2 + g2t2).toShort)
  }

  /** Computes expected board scores from match-level data alone (no board endpoint needed).
    * Returns a map of boardNum → (team1ScoreX2, team2ScoreX2).
    */
  private[history] def computeExpectedScores(dailyMatch: ApiDailyMatch): Map[Short, (Short, Short)] =
    dailyMatch match {
      case _: ApiDailyMatchRegistered => Map.empty
      case _ =>
        val teams   = dailyMatch.teams
        val team1Fp = teams.team1.fairPlayRemovals.map(_.value)
        val team2Fp = teams.team2.fairPlayRemovals.map(_.value)

        val team1ByBoard: Map[Short, MatchPlayerStarted] = teams.team1.players.collect { case p: MatchPlayerStarted =>
          p.board.path.segments.last.toShort -> p
        }.toMap
        val team2ByBoard: Map[Short, MatchPlayerStarted] = teams.team2.players.collect { case p: MatchPlayerStarted =>
          p.board.path.segments.last.toShort -> p
        }.toMap

        val allBoards = team1ByBoard.keySet ++ team2ByBoard.keySet
        allBoards.flatMap { boardNum =>
          for {
            t1 <- team1ByBoard.get(boardNum)
            t2 <- team2ByBoard.get(boardNum)
          } yield {
            val t1FairPlay = team1Fp.contains(t1.username.value)
            val t2FairPlay = team2Fp.contains(t2.username.value)
            val (g1Winner, _) = normalizeGameOutcome(t1.playedAsWhite, t2.playedAsBlack, whiteTeamIsTeam1 = true)
            val (g2Winner, _) = normalizeGameOutcome(t2.playedAsWhite, t1.playedAsBlack, whiteTeamIsTeam1 = false)
            boardNum -> computeScoreX2(g1Winner, g2Winner, t1FairPlay, t2FairPlay)
          }
        }.toMap
    }

  /** Returns true if the expected board scores match the existing DB rows exactly. */
  private[history] def scoresMatch(
    expected: Map[Short, (Short, Short)],
    existing: List[ClubMatchBoard]
  ): Boolean =
    expected.size == existing.size && existing.forall { b =>
      expected.get(b.board).contains((b.team1ScoreX2, b.team2ScoreX2))
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
  ): RIO[CcasLogger & PostgresClient, Option[PlayerId]] = {
    val key = username.value
    ctx.knownPlayers.get.map(_.get(key)).flatMap {
      case Some(playerId) => ctx.playersKnown.update(_ + 1).as(Some(playerId))
      case None           => discoverPlayer(ctx, username, key, isOurTeam, matchStartTime)
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
  ): RIO[CcasLogger & PostgresClient, Option[PlayerId]] =
    for {
      promise <- Promise.make[Throwable, Option[PlayerId]]
      action <- ctx.discoveryCache.modify { m =>
        m.get(key) match {
          case Some(existing) => (existing.await, m)
          case None => (doDiscoverPlayer(ctx, username, key, promise, isOurTeam, matchStartTime), m + (key -> promise))
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
  ): RIO[CcasLogger & PostgresClient, Option[PlayerId]] = {
    val work = for {
      apiPlayer <- ctx.client.get[ApiPlayer](ApiPlayer.getUrl(username))
      playerId       = apiPlayer.playerId
      statusCategory = apiPlayer.status.category
      now            = Instant.now()

      isNew <- withTransaction {
        Player.selectIdForUpdate(playerId).flatMap {
          case Some(existing) =>
            val changed = !existing.stateMatches(username, statusCategory, apiPlayer.title)
            ZIO.whenDiscard(changed) {
              PlayerUpdater.archiveAndUpdate(existing, username, statusCategory, apiPlayer.title, now, ctx.client)
            }.as(false)

          case None =>
            val since = if (statusCategory == PlayerStatusCategory.Active) now else apiPlayer.lastOnlineAt
            Player.insertIfNew(Player(playerId, apiPlayer.joinedAt, username, statusCategory, apiPlayer.title, since))
              .flatMap { inserted =>
                if (inserted == 0) ZIO.succeed(false)
                else {
                  ZIO.whenDiscard(isOurTeam)(createClubMemberForDiscovered(ctx, apiPlayer, matchStartTime)).as(true)
                }
              }
        }
      }
      result <- {
        ctx.knownPlayers.update(_ + (key -> playerId)) *>
          ZIO.whenDiscard(isNew && isOurTeam)(ctx.newPlayers.update(_ + DiscoveredPlayer(playerId, username))) *>
          ZIO.whenDiscard(isNew)(ctx.playersDiscovered.update(_ + 1))
      }.as(Some(playerId))
    } yield result

    // The doer handles success/failure and completes the promise.
    // On failure: log once, count once, resolve promise to None so awaiting fibers get None (not an exception).
    work.foldZIO(
      error =>
        ctx.playersFailed.update(_ + 1)
          *> CcasLogger.warn(s"    Cannot resolve player $username: ${error.getMessage}")
          *> promise.succeed(None).as(None),
      result => promise.succeed(result).as(result)
    )
  }

  private def createClubMemberForDiscovered(
    ctx: ProcessingContext,
    apiPlayer: ApiPlayer,
    matchStartTime: Option[Instant]
  ): RIO[PostgresClient, Unit] = {
    val playerId       = apiPlayer.playerId
    val statusCategory = apiPlayer.status.category
    val clubId         = ctx.clubId

    ClubMember.exists(clubId, playerId).flatMap {
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
  ): RIO[PostgresClient, Unit] =
    ZIO.whenDiscard(resolvedId.isEmpty) {
      teamUrl.path.segments.lastOption match {
        case Some(segment) => UnresolvedMatchClub.insert(matchId, isTeam1, ClubSlug.wrap(segment)).ignore
        case None          => ZIO.unit
      }
    }

  /** Resolves a team URL to a ClubId via Promise-based cache. Falls back to DB lookup then API fetch. */
  private def resolveClubIdFromTeamUrl(ctx: ProcessingContext, teamUrl: URL): RIO[PostgresClient, Option[ClubId]] =
    teamUrl.path.segments.lastOption match {
      case None => ZIO.none
      case Some(segment) =>
        val slug = ClubSlug.wrap(segment)
        val key  = slug.value
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
    ClubMatch(
      matchId = matchId,
      name = dailyMatch.name,
      status = dailyMatch.status,
      timeClass = dailyMatch.settings.timeClass,
      startTime = startTime,
      endTime = endTime,
      boards = dailyMatch.boards.toShort,
      team1ClubId = team1ClubId,
      team1ScoreX2 = (teams.team1.score * 2).toShort,
      team2ClubId = team2ClubId,
      team2ScoreX2 = (teams.team2.score * 2).toShort,
      fetchedAt = Instant.now()
    )
  }

  // === Stats ===

  def readStats(ctx: ProcessingContext, waveCount: Int, waveDetails: List[(Int, Int)]): UIO[RunStats] =
    for {
      matchesProcessed    <- ctx.matchesProcessed.get
      matchesFailed       <- ctx.matchesFailed.get
      matchesUnidentified <- ctx.matchesUnidentified.get
      matchesBoardsUpdated    <- ctx.matchesBoardsUpdated.get
      matchesSharedSkip   <- ctx.matchesSharedSkip.get
      playersDiscovered   <- ctx.playersDiscovered.get
      playersKnown        <- ctx.playersKnown.get
      playersFailed       <- ctx.playersFailed.get
      failedMatches       <- ctx.failedMatches.get
    } yield RunStats(
      matchesProcessed = matchesProcessed,
      matchesFailed = matchesFailed,
      matchesUnidentified = matchesUnidentified,
      matchesBoardsUpdated = matchesBoardsUpdated,
      matchesSharedSkip = matchesSharedSkip,
      playersDiscovered = playersDiscovered,
      playersKnown = playersKnown,
      playersFailed = playersFailed,
      waveCount = waveCount,
      waveDetails = waveDetails,
      failedMatches = failedMatches
    )
}
