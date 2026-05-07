package ccas.analysis.apps.recruitment

import java.net.URI
import java.time.Instant

import zio.http.URL
import zio.json.JsonDecoder
import zio.Ref
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.*
import ccas.api.misc.enums.{PlayerStatus, PlayerStatusCategory}
import ccas.api.misc.subtypes.{ClubMatchId, ClubSlug, Elo, Username}
import ccas.api.player.{ApiPlayer, ApiPlayerArchive}
import ccas.utils.sql.FreshSchemaLayer

/** Exercises rename recovery on the recruitment-side player fetches wired in PR for issue #22.
  *
  * Each test seeds a Player row at the canonical (post-rename) handle plus a PlayerSnapshot at the stale handle so
  * the resolver's Tier A snapshot lookup can rediscover the canonical name without needing a board endpoint trick.
  */
object TestRecruitmentRenameRecovery extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentRenameRecovery")(
    fetchTmStatsRecoversFromArchive404,
    fetchTmStatsCachedPathUsesCanonicalUname,
    checkOpponentMatchWrapRecoversFromMatches404,
    checkClubsWrapRecoversFromClubs404,
    checkOngoingGamesWrapRecoversFromGames404,
    checkDailyStatsWrapRecoversFromStats404,
    gatherClubCandidatesRecoversViaTierBMatchRef
  ).provideShared(
    FreshSchemaLayer("test_recruitment_rename_recovery", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private val staleU = Username("alice-old")
  private val freshU = Username("alice-new")
  private val pid    = pid0 // PlayerId(200) per RecruitmentTestSupport

  /** Inserts the canonical Player row and a snapshot at the stale username so Tier A succeeds. */
  private def seedRenameHistory: zio.RIO[ccas.utils.sql.PostgresClient, Unit] =
    for {
      _ <- seedDb
      _ <- Player.insertIfNew(Player(pid, Instant.parse("2020-01-01T00:00:00Z"), freshU, PlayerStatusCategory.Active, None, Instant.parse("2020-01-01T00:00:00Z")))
      _ <- PlayerSnapshot.insert(PlayerSnapshot(pid, Instant.parse("2019-01-01T00:00:00Z"), staleU, PlayerStatusCategory.Active, None))
    } yield ()

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
    CandidateContext(staleU, apiPlayer = Some(staleApiPlayer), isNewPlayer = false, cache = None)

  private def runContext(client: ccas.utils.client.ChessComClient): zio.RIO[Any, RunContext] =
    for {
      discoveredOpponents <- Ref.make(Set.empty[Username])
      failedAdminSlugs    <- Ref.make(Set.empty[ccas.api.misc.subtypes.ClubSlug])
    } yield RunContext(
      client,
      makeCriteria().copy(maxClubs = Some(50), dailyMaxTimeoutPercent = Some(10.0), dailyMinTmGamesFinished = Some(0)),
      clubId,
      "default",
      Set.empty,
      Set.empty,
      Set.empty,
      Set.empty,
      Instant.now(),
      discoveredOpponents,
      failedAdminSlugs
    )

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
    val archive = archiveJson(List(game))
    val months  = RecruitmentStatsHelpers.recentArchiveMonths(now, 90)
    val responses = Map[String, String](
      "player/alice-new" -> apiPlayerJson(200, "alice-new")
    ) ++ months.map(ym => s"player/alice-new/games/${ym.getYear}/${"%02d".format(ym.getMonthValue)}" -> archive).toMap
    val criteria = makeCriteria().copy(dailyMinTmGamesFinished = Some(0), dailyMaxTmTimeoutPercent = Some(50.0))
    for {
      _      <- seedRenameHistory
      client <- fakeChessComClient(responses, failures = Set("alice-old"))
      result <- RecruitmentStatsHelpers.fetchTmStats(client, staleU, pid, criteria, overallTimeoutPct = 5.0, now, recentArchives = None)
    } yield assertTrue(
      result.gamesFinished == months.size,
      result.opponentUsernames == Set(Username("opponent")),
      // Predicates must use the post-rename effective username (alice-new), not the stale input. If they used the
      // stale name, `playerResult` would treat alice-new as the "opponent" and miscount.
      result.timeoutPct.contains(0.0)
    )
  }

  private def fetchTmStatsCachedPathUsesCanonicalUname = test("fetchTmStats: cached recentArchives + stale `username` arg → predicates use Player row's canonical handle, not the input") {
    // Regression for the StatsHelpers cached-path bug: CheckTmStats receives a CandidateContext whose `username`
    // field was set by FetchAndCheckPlayer and never refreshed; if CheckDailyStats triggered a rename recovery the
    // cached archives are keyed by the post-rename canonical handle but `env.candidate.username` is still pre-rename.
    // The predicates must derive the effective username from the Player row, not trust the input.
    val now = Instant.parse("2026-04-01T00:00:00Z")
    // Single archive containing one team-match daily timeout — opponent extraction depends on identifying which side
    // is "us". If predicates use the stale `username` ("alice-old") it'll never match white/black="alice-new", and
    // the timeout count + opponent set will both be empty.
    val game = archiveGameJson(
      white = "alice-new",
      black = "opponent",
      whiteResult = "timeout",
      blackResult = "win",
      endTime = now.minusSeconds(86400).getEpochSecond,
      matchUrl = Some("https://www.chess.com/match/9999"),
      timeClass = "daily"
    )
    val archive: ApiPlayerArchive = JsonDecoder[ApiPlayerArchive].decodeJson(archiveJson(List(game))).fold(
      e => throw new RuntimeException(s"Test fixture broke: $e"),
      identity
    )
    val criteria = makeCriteria().copy(dailyMinTmGamesFinished = Some(0), dailyMaxTmTimeoutPercent = Some(50.0))
    for {
      _      <- seedRenameHistory
      // No HTTP — recentArchives populated, fetchTmStats should not hit the network.
      client <- fakeChessComClient(Map.empty)
      result <- RecruitmentStatsHelpers.fetchTmStats(
        client,
        username = staleU, // pre-rename input — what env.candidate.username carries when CheckTmStats runs
        playerIdHint = pid,
        criteria = criteria,
        overallTimeoutPct = 5.0,
        now = now,
        recentArchives = Some(List(archive))
      )
    } yield assertTrue(
      result.gamesFinished == 1,
      // Timeout count would be 0 if the predicate compared against the stale `staleU` instead of the canonical
      // `freshU` from the Player row.
      result.timeoutPct.contains(100.0),
      result.lastTimeoutAt.isDefined,
      result.opponentUsernames == Set(Username("opponent"))
    )
  }

  // --- Filter wraps ---

  private def checkOpponentMatchWrapRecoversFromMatches404 = test("CheckOpponentMatch: matches 404 → wrap recovers via Tier A") {
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
      result.candidate.playerMatches.isDefined
    )
  }

  private def checkClubsWrapRecoversFromClubs404 = test("CheckClubs: clubs 404 → wrap recovers via Tier A") {
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
      result.candidate.playerClubs.isDefined
    )
  }

  private def checkOngoingGamesWrapRecoversFromGames404 = test("CheckOngoingGames: games 404 → wrap recovers via Tier A") {
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
    } yield assertTrue(!result.rejected)
  }

  private def checkDailyStatsWrapRecoversFromStats404 = test("CheckDailyStats: stats 404 → wrap recovers + applyDailyStats uses fresh handle for archive URL") {
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
      result.candidate.cache.exists(_.dailyElo.contains(Elo(1500)))
    )
  }

  // --- Club slug recovery (PR 2) ---

  private def gatherClubCandidatesRecoversViaTierBMatchRef = test("gatherClubCandidates: club 404 → Tier B match-ref recovers canonical slug") {
    val staleSlug    = ClubSlug("renamed-old")
    val freshSlug    = ClubSlug("renamed-new")
    val staleClubId  = sourceClubId // ClubId(600) per RecruitmentTestSupport
    val matchId      = ClubMatchId(8001)
    // Match endpoint exposes team1 URL → we put fresh-slug there so resolveStaleSlug can read it.
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
      // DB still believes the stale slug — so deriveHint via Club.selectBySlug(stale) finds clubId.
      _ <- Club.upsert(Club(staleClubId, Instant.parse("2020-01-01T00:00:00Z"), staleSlug, "Renamed", Some(5), None, None))
      // ClubMatchRef seeds Tier B's match-endpoint trick.
      _ <- ClubMatch.upsert(ClubMatch(
        matchId, "Renamed match", ccas.api.misc.enums.ClubMatchStatus.Finished,
        ccas.api.misc.enums.TimeClass.Daily,
        Some(Instant.parse("2020-01-01T00:00:00Z")), Some(Instant.parse("2020-01-02T00:00:00Z")),
        boards = 1, team1ClubId = Some(staleClubId), team1ScoreX2 = 2,
        team2ClubId = None, team2ScoreX2 = 0, fetchedAt = Instant.parse("2020-01-01T00:00:00Z")
      ))
      _      <- ClubMatchRef.upsert(ClubMatchRef(staleClubId, matchId, isLive = false, isTeam1 = true))
      client <- fakeChessComClient(responses, failures = Set(staleSlug.value))
      result <- RecruitmentExplore.gatherClubCandidates(
        client,
        staleSlug,
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
}
