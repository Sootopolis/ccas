package ccas.analysis.apps.recruitment

import java.net.URI
import java.time.Instant
import zio.http.URL
import zio.{Ref, RIO, ZIO, ZLayer}
import zio.test.{Spec, TestAspect, ZIOSpecDefault, ZTestLogger, assertTrue}
import ccas.analysis.apps.TestTimes
import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.*
import ccas.analysis.tables.subtypes.RecruitmentRunId
import ccas.api.misc.enums.{ClubMatchStatus, PlayerStatus, PlayerStatusCategory, TimeClass}
import ccas.api.misc.subtypes.{ClubMatchId, ClubSlug, Elo, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.ProgressDisplay
import ccas.utils.client.{BodyStore, ChessComClient}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}

/** Exercises rename recovery on the recruitment-side player fetches wired in PR for issue #22, and what an evaluation
  * does with the handle it recovers.
  *
  * Most tests record the player holding the stale handle and then the canonical (post-rename) one, so the resolver's
  * Tier A history lookup can rediscover the canonical name without needing a board endpoint trick.
  */
object TestRecruitmentRenameRecovery extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentRenameRecovery")(
    fetchTmStatsRecoversFromArchive404,
    checkTmStatsReadsCachedArchivesUnderRecoveredHandle,
    checkOpponentMatchWrapRecoversFromMatches404,
    checkClubsWrapRecoversFromClubs404,
    checkOngoingGamesWrapRecoversFromGames404,
    checkDailyStatsWrapRecoversFromStats404,
    checkTmStatsWrapRecoversFromArchive404,
    statsFiltersReadGamesUnderAnsweredHandle,
    recoveredHandleIsNotWrittenBack,
    failedEvaluationLeavesStoredNameAlone,
    gatherClubCandidatesRecoversViaTierBMatchRef,
    recruitConfirmsRenamedCandidateById,
    twoNamesInTurnWriteAndFindOnce,
    twoNamesAtOnceWriteAndFindOnce,
    failedSecondEvaluationKeepsFirstRow,
    recruitCountsPlayerListedUnderTwoNamesOnce
  ).provideShared(
    FreshSchemaLayer("test_recruitment_rename_recovery", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false))
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private val staleU = Username("alice-old")
  private val freshU = Username("alice-new")
  private val pid    = pid0 // PlayerId(200) per RecruitmentTestSupport

  private val freshProfile = Map("player/alice-new" -> apiPlayerJson(200, "alice-new"))

  /** The profile lags the rename and still answers under alice-old, while the endpoints after it have moved on. */
  private val laggingProfile = Map(
    "player/alice-old" -> apiPlayerJson(200, "alice-old"),
    "player/alice-new" -> apiPlayerJson(200, "alice-new")
  )

  /** Records the stale handle and then the canonical one in `player_name`, so Tier A succeeds. */
  private def seedRenameHistory: RIO[PostgresClient, Unit] = {
    val joined = Instant.parse("2019-01-01T00:00:00Z")
    for {
      _ <- seedDb
      _ <- Player.insertIfNew(Player(pid, joined, staleU, PlayerStatusCategory.Active, None, joined))
      _ <- Player.updateCurrentState(
        Player(pid, joined, freshU, PlayerStatusCategory.Active, None, Instant.parse("2020-01-01T00:00:00Z"))
      )
    } yield ()
  }

  /** [[seedRenameHistory]], then a run under `makeCriteria()` to evaluate in. */
  private def seedRenameHistoryAndRun: RIO[PostgresClient, RecruitmentRunId] =
    for {
      _          <- seedRenameHistory
      criteriaId <- seedCriteria(makeCriteria())
      runId <- RecruitmentRun.insert(
        clubId = clubId,
        criteriaId = criteriaId,
        trigger = RunTrigger.Cli,
        startedAt = Times.t0,
        target = None,
        jobRunIdOption = None
      )
    } yield runId

  /** ApiPlayer pre-set to the stale handle to mimic mid-pipeline state. The filter wrap passes
    * `apiPlayer.playerId` as the resolver hint.
    */
  private val staleApiPlayer: ApiPlayer = ApiPlayer(
    playerId = pid,
    username = staleU,
    name = None,
    country = URL.fromURI(URI("https://api.chess.com/pub/country/US")).get,
    location = None,
    status = PlayerStatus.Basic,
    joined = Instant.parse("2020-01-01T00:00:00Z").getEpochSecond,
    lastOnline = Instant.parse("2020-01-01T00:00:00Z").getEpochSecond,
    title = None,
    avatar = None,
    followers = 0,
    isStreamer = false,
    verified = false,
    league = None,
    fide = None
  )

  private val staleCandidate: CandidateContext =
    CandidateContext(staleU, apiPlayerOption = Some(staleApiPlayer), isNewPlayer = false, cacheOption = None)

  private def runContext(client: ChessComClient): RIO[Any, RunContext] =
    for {
      discoveredOpponents <- Ref.make(Set.empty[Username])
      failedAdminSlugs    <- Ref.make(Set.empty[ClubSlug])
    } yield RunContext(
      client = client,
      criteria = makeCriteria().copy(
        maxClubs = Some(50),
        dailyMaxTimeoutPercent = Some(10.0),
        dailyMinTmGamesFinished = Some(0)
      ),
      clubId = clubId,
      alias = "default",
      clubMatchIds = Set.empty,
      formerMemberIds = Set.empty,
      adminExcludedPlayerIds = Set.empty,
      excludedSlugs = Set.empty,
      now = Instant.now(),
      discoveredOpponents = discoveredOpponents,
      failedAdminSlugs = failedAdminSlugs
    )

  /** Serves `archive` as every month of alice-new's 90-day archive window ending at `now`. */
  private def freshArchives(now: Instant, archive: String): Map[String, String] =
    RecruitmentStatsHelpers.recentArchiveMonths(now, 90).map { ym =>
      s"player/alice-new/games/${ym.getYear}/${"%02d".format(ym.getMonthValue)}" -> archive
    }.toMap

  // --- fetchTmStats ---

  private def fetchTmStatsRecoversFromArchive404 = test("fetchTmStats: archive 404 → resolver Tier A → retries with fresh username") {
    val now = Instant.parse("2026-04-01T00:00:00Z")
    // Build one archive month with a non-timeout daily team-match game so opponent extraction has signal.
    val game = archiveGameJson(
      white = "alice-new",
      black = "opponent",
      whiteResult = "win",
      blackResult = "checkmated",
      endTime = now.minusSeconds(86400).getEpochSecond,
      matchUrl = Some("https://www.chess.com/match/9999"),
      timeClass = "daily"
    )
    val months    = RecruitmentStatsHelpers.recentArchiveMonths(now, 90)
    val responses = freshProfile ++ freshArchives(now, archiveJson(List(game)))
    val criteria = makeCriteria().copy(dailyMinTmGamesFinished = Some(0), dailyMaxTmTimeoutPercent = Some(50.0))
    for {
      _      <- seedRenameHistory
      client <- fakeChessComClient(responses, failures = Set("alice-old"))
      result <- RecruitmentStatsHelpers.fetchTmStats(client, staleU, pid, criteria, overallTimeoutPct = 5.0, now, recentArchivesOption = None)
    } yield assertTrue(
      result.gamesFinished == months.size,
      result.opponentUsernames == Set(Username("opponent")),
      // Predicates must use the post-rename effective username (alice-new), not the stale input. If they used the
      // stale name, `playerResult` would treat alice-new as the "opponent" and miscount.
      result.timeoutPct.contains(0.0),
      result.username == freshU
    )
  }

  private def checkTmStatsReadsCachedArchivesUnderRecoveredHandle = test("CheckTmStats: archives cached by a recovering CheckDailyStats are read under the recovered handle") {
    // CheckTmStats reuses CheckDailyStats' archives without fetching, so only the handle CheckDailyStats wrote back
    // tells it which side of each game is the candidate's. Under the stale one, the timeout counts as the opponent's.
    val now = Instant.now()
    val game = archiveGameJson(
      white = "alice-new",
      black = "opponent",
      whiteResult = "timeout",
      blackResult = "win",
      endTime = now.minusSeconds(86400).getEpochSecond,
      matchUrl = Some("https://www.chess.com/match/9999"),
      timeClass = "daily"
    )
    val responses = Map[String, String](
      "player/alice-new"       -> apiPlayerJson(200, "alice-new"),
      "player/alice-new/stats" -> apiPlayerStatsJson(timeoutPct = 5.0)
    ) ++ freshArchives(now, archiveJson(List(game)))
    for {
      _      <- seedRenameHistory
      client <- fakeChessComClient(responses = responses, failures = Set("alice-old"))
      runCtx <- runContext(client).map(_.copy(now = now))
      daily  <- RecruitmentFilterDefs.CheckDailyStats.apply(FilterEnv(runCtx, staleCandidate))
      tm     <- RecruitmentFilterDefs.CheckTmStats.apply(FilterEnv(runCtx, daily.candidate))
    } yield assertTrue(
      daily.candidate.recentArchivesOption.isDefined,
      tm.candidate.cacheOption.flatMap(_.tmTimeoutPct90d).contains(100.0)
    )
  }

  // --- Filter wraps ---

  private def checkOpponentMatchWrapRecoversFromMatches404 = test("CheckOpponentMatch: matches 404 → wrap recovers via Tier A and carries the fresh handle") {
    val responses = Map(
      "player/alice-new"         -> apiPlayerJson(200, "alice-new"),
      "player/alice-new/matches" -> emptyPlayerMatchesJson
    )
    for {
      _      <- seedRenameHistory
      client <- fakeChessComClient(responses, failures = Set("alice-old"))
      runCtx <- runContext(client)
      cand   = staleCandidate
      result <- RecruitmentFilterDefs.CheckOpponentMatch.apply(FilterEnv(runCtx, cand))
    } yield assertTrue(
      !result.rejected,
      result.candidate.playerMatchesOption.isDefined,
      result.candidate.username == freshU
    )
  }

  private def checkClubsWrapRecoversFromClubs404 = test("CheckClubs: clubs 404 → wrap recovers via Tier A and carries the fresh handle") {
    val responses = Map(
      "player/alice-new"       -> apiPlayerJson(200, "alice-new"),
      "player/alice-new/clubs" -> apiPlayerClubsJson(List("test-club"))
    )
    for {
      _      <- seedRenameHistory
      client <- fakeChessComClient(responses, failures = Set("alice-old"))
      runCtx <- runContext(client)
      cand   = staleCandidate
      result <- RecruitmentFilterDefs.CheckClubs.apply(FilterEnv(runCtx, cand))
    } yield assertTrue(
      !result.rejected,
      result.candidate.playerClubsOption.isDefined,
      result.candidate.username == freshU
    )
  }

  private def checkOngoingGamesWrapRecoversFromGames404 = test("CheckOngoingGames: games 404 → wrap recovers via Tier A and carries the fresh handle") {
    val responses = Map(
      "player/alice-new"       -> apiPlayerJson(200, "alice-new"),
      "player/alice-new/games" -> emptyCurrentGamesJson
    )
    for {
      _      <- seedRenameHistory
      client <- fakeChessComClient(responses, failures = Set("alice-old"))
      runCtx <- runContext(client)
      cand   = staleCandidate
      result <- RecruitmentFilterDefs.CheckOngoingGames.apply(FilterEnv(runCtx, cand))
    } yield assertTrue(
      !result.rejected,
      result.candidate.username == freshU
    )
  }

  private def checkDailyStatsWrapRecoversFromStats404 = test("CheckDailyStats: stats 404 → wrap recovers and carries the fresh handle") {
    val responses = Map(
      "player/alice-new"       -> apiPlayerJson(200, "alice-new"),
      "player/alice-new/stats" -> apiPlayerStatsJson(dailyElo = 1500, timeoutPct = 0.0),
      "player/alice-new/clubs" -> apiPlayerClubsJson()
    )
    for {
      _      <- seedRenameHistory
      client <- fakeChessComClient(responses, failures = Set("alice-old"))
      runCtx <- runContext(client)
      cand   = staleCandidate
      result <- RecruitmentFilterDefs.CheckDailyStats.apply(FilterEnv(runCtx, cand))
    } yield assertTrue(
      !result.rejected,
      result.candidate.cacheOption.exists(_.dailyElo.contains(Elo(1500))),
      result.candidate.username == freshU
    )
  }

  private def checkTmStatsWrapRecoversFromArchive404 = test("CheckTmStats: archive 404 → wrap recovers via Tier A and carries the fresh handle") {
    for {
      _      <- seedRenameHistory
      client <- fakeChessComClient(responses = freshProfile, failures = Set("alice-old"))
      runCtx <- runContext(client)
      cand = staleCandidate.copy(
        cacheOption = Some(PlayerRecruitmentCache.empty(playerId = pid, fetchedAt = runCtx.now, clubCount = None))
      )
      result <- RecruitmentFilterDefs.CheckTmStats.apply(FilterEnv(runCtx, cand))
    } yield assertTrue(result.candidate.username == freshU)
  }

  private def statsFiltersReadGamesUnderAnsweredHandle = test("CheckDailyStats and CheckTmStats: a player renamed since we stored them has their games read under the handle Chess.com answered to, not ours") {
    val now = Instant.now()
    def timeoutGame(matchUrl: Option[String]): String = archiveGameJson(
      white = "alice-new",
      black = "opponent",
      whiteResult = "timeout",
      blackResult = "win",
      endTime = now.minusSeconds(86400).getEpochSecond,
      matchUrl = matchUrl
    )
    val games     = List(timeoutGame(matchUrl = None), timeoutGame(matchUrl = Some("https://www.chess.com/match/9999")))
    val responses = Map("player/alice-new/stats" -> apiPlayerStatsJson(timeoutPct = 5.0)) ++
      freshArchives(now, archiveJson(games))
    val joined = Instant.parse("2019-01-01T00:00:00Z")
    for {
      _ <- seedDb
      // We still hold the name the player has left. Nothing recovers: the profile already answered under the new one.
      _      <- Player.insertIfNew(Player(pid, joined, staleU, PlayerStatusCategory.Active, None, joined))
      client <- fakeChessComClient(responses = responses)
      runCtx <- runContext(client).map(_.copy(now = now))
      cand   = staleCandidate.copy(username = freshU, apiPlayerOption = Some(staleApiPlayer.copy(username = freshU)))
      daily  <- RecruitmentFilterDefs.CheckDailyStats.apply(FilterEnv(runCtx, cand))
      tm     <- RecruitmentFilterDefs.CheckTmStats.apply(FilterEnv(runCtx, daily.candidate))
    } yield assertTrue(
      daily.candidate.cacheOption.exists(_.lastDailyTimeoutAt.isDefined),
      tm.candidate.cacheOption.flatMap(_.tmTimeoutPct90d).contains(100.0)
    )
  }

  private def recoveredHandleIsNotWrittenBack = test("evaluateCandidate: a handle recovered mid-evaluation is not overwritten by the one the profile answered to") {
    for {
      runId   <- seedRenameHistoryAndRun
      client  <- fakeChessComClient(responses = laggingProfile, notFound = Set("player/alice-old/matches"))
      found   <- evalCandidates(client, runId, List(staleU), makeCriteria())
      current <- PlayerName.selectCurrentName(pid)
    } yield assertTrue(
      found == List(freshU),
      current.contains(freshU)
    )
  }

  private def failedEvaluationLeavesStoredNameAlone = test("evaluateCandidate: an evaluation that fails after recovering a rename leaves the stored name alone") {
    // CheckDailyStats recovers alice-new for the stats, then fails on the archives, so the `Error` row is written from
    // the candidate as it entered CheckDailyStats, still under alice-old.
    val now = Instant.now()
    val responses = laggingProfile ++ Map("player/alice-new/stats" -> apiPlayerStatsJson(timeoutPct = 5.0)) ++
      freshArchives(now, "NOT VALID JSON")
    for {
      runId   <- seedRenameHistoryAndRun
      client  <- fakeChessComClient(responses = responses, notFound = Set("player/alice-old/stats"))
      _       <- evalCandidates(client, runId, List(staleU), makeCriteria())
      rows    <- RecruitmentCandidate.selectByRun(runId)
      current <- PlayerName.selectCurrentName(pid)
    } yield assertTrue(
      rows.map(_.outcome) == List(CandidateOutcome.Error),
      current.contains(freshU)
    )
  }

  // --- Club slug recovery (PR 2) ---

  private def gatherClubCandidatesRecoversViaTierBMatchRef = test("gatherClubCandidates: club 404 → Tier B match-ref recovers canonical slug") {
    val staleSlug    = ClubSlug("renamed-old")
    val freshSlug    = ClubSlug("renamed-new")
    val staleClubId  = sourceClubId // ClubId(600) per RecruitmentTestSupport
    val matchId      = ClubMatchId(8001)
    // Match endpoint exposes team1 URL → we put fresh-slug there so Tier B can read it.
    val matchJson = apiDailyMatchJson(
      matchId = 8001L,
      team1Club = "renamed-new",
      team2Club = "other-club",
      team1Players = List(("p1", 1)),
      team2Players = List(("p2", 1))
    )
    val responses = Map(
      s"club/${freshSlug.value}"         -> apiClubJson(600L, freshSlug.value, admins = Nil, membersCount = 5),
      s"club/${freshSlug.value}/members" -> apiClubMembersJson(List(("memberA", 0L), ("memberB", 0L))),
      s"match/8001"                      -> matchJson
    )
    for {
      _ <- seedDb
      // DB still believes the stale slug — so deriveHint, resolving it locally, finds clubId.
      _ <- Club.upsert(Club(staleClubId, Instant.parse("2020-01-01T00:00:00Z"), staleSlug, "Renamed", Some(5), None, None))
      // ClubMatchRef seeds Tier B's match-endpoint trick.
      _ <- ClubMatch.upsert(
        ClubMatch(
          matchId = matchId,
          name = "Renamed match",
          status = ClubMatchStatus.Finished,
          timeClass = TimeClass.Daily,
          startTime = Some(Instant.parse("2020-01-01T00:00:00Z")),
          endTime = Some(Instant.parse("2020-01-02T00:00:00Z")),
          boards = 1,
          team1ClubIdOption = Some(staleClubId),
          team1ScoreX2 = 2,
          team2ClubIdOption = None,
          team2ScoreX2 = 0,
          fetchedAt = Instant.parse("2020-01-01T00:00:00Z"),
          processedBodyHash = None
        )
      )
      _      <- ClubMatchRef.upsert(ClubMatchRef(staleClubId, matchId, isLive = false, isTeam1 = true))
      client <- fakeChessComClient(responses, failures = Set(staleSlug.value))
      result <- RecruitmentExplore.gatherClubCandidates(
        client = client,
        clubSlug = staleSlug,
        excludeSourceAdmins = false,
        existingUsernames = Set.empty,
        evaluatedUsernames = Set.empty
      )
      // DB row's slug should be updated to the fresh handle by the resolver's resolveAndPersist.
      updatedClub <- Club.selectId(staleClubId)
    } yield assertTrue(
      result == List(Username("memberA"), Username("memberB")),
      updatedClub.exists(_.slug == freshSlug)
    )
  }

  // --- Confirmation ---

  private def recruitConfirmsRenamedCandidateById = test("recruit: a candidate found under a renamed-away handle is confirmed by player id and logged under the verified handle") {
    val joined = TestTimes.t0.getEpochSecond
    val responses = Map(
      s"club/$clubSlug"          -> apiClubJson(clubId.value, clubSlug.value),
      s"club/$clubSlug/members"  -> apiClubMembersJson(List(("existing", joined))),
      "club/source-club"         -> apiClubJson(sourceClubId.value, "source-club"),
      "club/source-club/members" -> apiClubMembersJson(List(("alice-old", joined))),
      "player/existing"          -> apiPlayerJson(199, "existing"),
      "player/alice-new"         -> apiPlayerJson(200, "alice-new")
    )
    for {
      _       <- seedRenameHistory
      _       <- seedCriteria(makeCriteria())
      client  <- fakeChessComClient(responses, failures = Set("alice-old"))
      run     <- runRecruit(client, sourceClubs = List(ClubSlug("source-club")))
      invited <- RecruitmentCandidate.selectInvitedByRun(run.runId)
      logged  <- ZTestLogger.logOutput.map(_.map(_.message()))
    } yield assertTrue(
      run.candidatesFound == 1,
      invited.map(_.playerId) == List(pid),
      logged.contains("  alice-new"),
      !logged.contains("  alice-old")
    )
  }.provideSomeLayer[PostgresClient & BodyStore & ProgressDisplay](ZTestLogger.default)

  // --- One player, two names ---

  private def twoNamesInTurnWriteAndFindOnce = test("evaluateCandidate: a player listed under an old and then a current name is written and found once") {
    for {
      runId  <- seedRenameHistoryAndRun
      client <- fakeChessComClient(responses = freshProfile, failures = Set("alice-old"))
      found  <- evalCandidates(client, runId, List(staleU, freshU), makeCriteria())
      rows   <- RecruitmentCandidate.selectByRun(runId)
    } yield assertTrue(
      found == List(freshU),
      rows.map(row => (row.playerId, row.outcome)) == List((pid, CandidateOutcome.Deferred))
    )
  }

  private def twoNamesAtOnceWriteAndFindOnce = test("evaluateCandidate: a player's two names evaluated at once are written and found once") {
    for {
      runId  <- seedRenameHistoryAndRun
      client <- fakeChessComClient(responses = freshProfile, failures = Set("alice-old"))
      runCtx <- runContext(client)
      filters = RecruitmentFilters.buildFilterChain(runCtx.criteria)
      found <- ZIO.foreachPar(List(staleU, freshU))(RecruitmentFilters.evaluateCandidate(runId, _, runCtx, filters))
      rows  <- RecruitmentCandidate.selectByRun(runId)
    } yield assertTrue(
      found.flatten.map(_.playerId) == List(pid),
      rows.map(row => (row.playerId, row.outcome)) == List((pid, CandidateOutcome.Deferred))
    )
  }

  private def failedSecondEvaluationKeepsFirstRow = test("evaluateCandidate: an evaluation that fails for a player the run already holds keeps the first row and carries on") {
    for {
      runId <- seedRenameHistoryAndRun
      // The row the evaluation under the player's other name already wrote.
      _ <- RecruitmentCandidate.insert(
        RecruitmentCandidate(
          runId = runId,
          playerId = pid,
          evaluatedAt = Times.t0,
          outcome = CandidateOutcome.Deferred,
          rejectionReason = None
        )
      )
      client <- fakeChessComClient(
        responses = freshProfile + ("player/alice-new/stats" -> "NOT VALID JSON"),
        failures = Set("alice-old")
      )
      found <- evalCandidates(client, runId, List(staleU), makeCriteria())
      rows  <- RecruitmentCandidate.selectByRun(runId)
    } yield assertTrue(
      found.isEmpty,
      rows.map(row => (row.playerId, row.outcome)) == List((pid, CandidateOutcome.Deferred))
    )
  }

  private def recruitCountsPlayerListedUnderTwoNamesOnce = test("recruit: a player a source lists under two names completes the run and is counted once") {
    val joined = TestTimes.t0.getEpochSecond
    // One club listing both names stands in for the usual route in: an old name from our database beside the current
    // one from Chess.com. Either way both land in one chunk.
    val responses = Map(
      s"club/$clubSlug"          -> apiClubJson(clubId.value, clubSlug.value),
      s"club/$clubSlug/members"  -> apiClubMembersJson(List(("existing", joined))),
      "club/source-club"         -> apiClubJson(sourceClubId.value, "source-club"),
      "club/source-club/members" -> apiClubMembersJson(List(("alice-old", joined), ("alice-new", joined))),
      "player/existing"          -> apiPlayerJson(199, "existing"),
      "player/alice-new"         -> apiPlayerJson(200, "alice-new")
    )
    for {
      _      <- seedRenameHistory
      _      <- seedCriteria(makeCriteria())
      client <- fakeChessComClient(responses = responses, failures = Set("alice-old"))
      run    <- runRecruit(client = client, sourceClubs = List(ClubSlug("source-club")))
      rows   <- RecruitmentCandidate.selectByRun(run.runId)
    } yield assertTrue(
      run.candidatesFound == 1,
      rows.map(row => (row.playerId, row.outcome)) == List((pid, CandidateOutcome.Invited))
    )
  }
}
