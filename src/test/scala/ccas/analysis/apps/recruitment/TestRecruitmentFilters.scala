package ccas.analysis.apps.recruitment

import java.time.{Duration, Instant}

import ccas.utils.sql.PostgresClient
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.RIO

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubId, ClubSlug, Elo, Username}
import ccas.utils.sql.FreshSchemaLayer

object TestRecruitmentFilters extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentFilters")(
    suiteEvaluateCandidates,
    suiteFilterChain,
    suiteCacheFilters
  ).provideShared(
    FreshSchemaLayer("test_recruitment_filters", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite: Evaluate candidates
  // ==========================================================================

  private def suiteEvaluateCandidates = suite("evaluateCandidates")(
    test("persists Player, PlayerSnapshot, and RecruitmentCandidate") {
      val responses = Map(
        "player/alice" -> apiPlayerJson(200, "alice"),
        "player/bob"   -> apiPlayerJson(201, "bob")
      )
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        client     <- fakeChessComClient(responses)
        invited    <- evalCandidates(client, runId, List(Username("alice"), Username("bob")), criteria)
        // Check invited list
        _ = assertTrue(invited.size == 2)
        // Check Player table (now includes current username/status/title)
        playerA <- Player.selectId(pid0)
        playerB <- Player.selectId(pid1)
        // Check RecruitmentCandidate table
        candidates <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        playerA.isDefined,
        playerB.isDefined,
        playerA.get.username == Username("alice"),
        playerB.get.username == Username("bob"),
        candidates.size == 2,
        candidates.forall(_.outcome == CandidateOutcome.Deferred)
      )
    },
    test("respects target limit") {
      val responses = Map(
        "player/alice"   -> apiPlayerJson(200, "alice"),
        "player/bob"     -> apiPlayerJson(201, "bob"),
        "player/charlie" -> apiPlayerJson(202, "charlie")
      )
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        client     <- fakeChessComClient(responses)
        invited <- evalCandidates(
          client,
          runId,
          List(Username("alice"), Username("bob"), Username("charlie")),
          criteria,
          target = 2
        )
        candidates <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        invited.size == 2,
        candidates.size == 2
      )
    },
    test("skips candidate on API failure (no record written)") {
      val responses = Map(
        "player/alice" -> apiPlayerJson(200, "alice")
      )
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        client     <- fakeChessComClient(responses, failures = Set("bob"))
        invited    <- evalCandidates(client, runId, List(Username("alice"), Username("bob")), criteria)
        candidates <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        invited.size == 1,
        invited.head == Username("alice"),
        candidates.size == 1,
        candidates.head.outcome == CandidateOutcome.Deferred
      )
    },
    test("mid-pipeline error persists candidate with Error outcome") {
      // Player profile fetches OK (apiPlayer set), but stats returns invalid JSON
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> "NOT VALID JSON"
      )
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        client     <- fakeChessComClient(responses)
        _          <- evalCandidates(client, runId, List(Username("alice")), criteria)
        candidates <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        candidates.size == 1,
        candidates.head.outcome == CandidateOutcome.Error,
        candidates.head.playerId == pid0,
        candidates.head.rejectionReason.isDefined
      )
    }
  )

  // ==========================================================================
  // Suite: Filter chain
  // ==========================================================================

  /** Returns the filter chain outcome (in-memory) for a single candidate. Passing candidates are persisted as Deferred
    * (pre-confirmation), but this helper reports the filter's judgment: Invited if the candidate passed, Rejected
    * otherwise.
    */
  private def evalSingle(
    responses: Map[String, String],
    criteria: RecruitmentCriteria,
    username: String = "alice"
  ): RIO[PostgresClient, CandidateOutcome] =
    for {
      _          <- seedDb
      criteriaId <- seedCriteria(criteria)
      runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
      client     <- fakeChessComClient(responses)
      invited    <- evalCandidates(client, runId, List(Username.wrap(username)), criteria)
    } yield
      if (invited.contains(Username.wrap(username))) CandidateOutcome.Invited
      else CandidateOutcome.Rejected

  private def suiteFilterChain = suite("filter chain")(
    test("rejects closed account") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", status = "closed"))
      for { outcome <- evalSingle(responses, makeCriteria()) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by minDaysSinceRegistration") {
      // Player joined 5 days ago, config requires 30 days
      val recentJoin = Instant.now().minus(java.time.Duration.ofDays(5)).getEpochSecond
      val responses  = Map("player/alice" -> apiPlayerJson(200, "alice", joined = recentJoin))
      val criteria   = makeCriteria().copy(minDaysSinceRegistration = Some(30))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player meeting minDaysSinceRegistration") {
      // Player joined 60 days ago, config requires 30 days
      val oldJoin   = Instant.now().minus(java.time.Duration.ofDays(60)).getEpochSecond
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", joined = oldJoin))
      val criteria  = makeCriteria().copy(minDaysSinceRegistration = Some(30))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by nationality exclude mode") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", country = "XX"))
      val criteria  = makeCriteria().copy(nationalityExclude = true, nationalityCountries = List("XX", "YY"))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player not in nationality exclude list") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", country = "ZZ"))
      val criteria  = makeCriteria().copy(nationalityExclude = true, nationalityCountries = List("XX", "YY"))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by nationality include mode when not in list") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", country = "ZZ"))
      val criteria  = makeCriteria().copy(nationalityExclude = false, nationalityCountries = List("XX", "YY"))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player in nationality include list") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice", country = "YY"))
      val criteria  = makeCriteria().copy(nationalityExclude = false, nationalityCountries = List("XX", "YY"))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by dailyMinElo") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 800)
      )
      val criteria = makeCriteria().copy(dailyMinElo = Some(Elo(1000)))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by dailyMaxElo") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 2200)
      )
      val criteria = makeCriteria().copy(dailyMaxElo = Some(Elo(2000)))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player within Elo range") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 1500)
      )
      val criteria = makeCriteria().copy(dailyMinElo = Some(Elo(1000)), dailyMaxElo = Some(Elo(2000)))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by dailyMaxTimeoutPercent") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(timeoutPct = 15.0)
      )
      val criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(10.0))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by dailyMinGamesFinished") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(wins = 5, losses = 3, draws = 2) // 10 games
      )
      val criteria = makeCriteria().copy(dailyMinGamesFinished = Some(50))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("dailyMinGamesFinished counts team match games from archives") {
      // Archive has 2 TM games + 1 non-TM game = 3 daily games in 90d window
      val now        = Instant.now()
      val recent     = now.minus(java.time.Duration.ofDays(10)).getEpochSecond
      val ym         = java.time.YearMonth.from(java.time.LocalDate.ofInstant(now, java.time.ZoneOffset.UTC))
      val archiveKey = s"player/alice/games/${ym.getYear}/${f"${ym.getMonthValue}%02d"}"
      val games = List(
        archiveGameJson("alice", "bob", endTime = recent, matchUrl = Some("https://api.chess.com/pub/match/111")),
        archiveGameJson(
          "carol",
          "alice",
          whiteResult = "checkmated",
          blackResult = "win",
          endTime = recent,
          matchUrl = Some("https://api.chess.com/pub/match/222")
        ),
        archiveGameJson("alice", "dave", endTime = recent)
      )
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(wins = 100, losses = 50, draws = 10),
        archiveKey           -> archiveJson(games)
      )
      // Require 3 games — should pass because TM games are included
      val criteria = makeCriteria().copy(dailyMinGamesFinished = Some(3))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("dailyMinGamesFinished excludes non-daily games from archives") {
      val now        = Instant.now()
      val recent     = now.minus(java.time.Duration.ofDays(10)).getEpochSecond
      val ym         = java.time.YearMonth.from(java.time.LocalDate.ofInstant(now, java.time.ZoneOffset.UTC))
      val archiveKey = s"player/alice/games/${ym.getYear}/${f"${ym.getMonthValue}%02d"}"
      val games = List(
        archiveGameJson("alice", "bob", endTime = recent, timeClass = "daily"),
        archiveGameJson("alice", "carol", endTime = recent, timeClass = "blitz"),
        archiveGameJson("alice", "dave", endTime = recent, timeClass = "rapid")
      )
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(wins = 100, losses = 50, draws = 10),
        archiveKey           -> archiveJson(games)
      )
      // Only 1 daily game in archives — require 2, should reject
      val criteria = makeCriteria().copy(dailyMinGamesFinished = Some(2))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("archive fetch failure is recorded in ApiFetchFailure") {
      val now        = Instant.now()
      val ym         = java.time.YearMonth.from(java.time.LocalDate.ofInstant(now, java.time.ZoneOffset.UTC))
      val archiveKey = s"player/alice/games/${ym.getYear}/${f"${ym.getMonthValue}%02d"}"
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(wins = 100, losses = 50, draws = 10),
        archiveKey           -> "NOT VALID JSON"
      )
      // dailyMinGamesFinished triggers archive fetch
      val criteria = makeCriteria().copy(dailyMinGamesFinished = Some(1))
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        client     <- fakeChessComClient(responses)
        _          <- evalCandidates(client, runId, List(Username.wrap("alice")), criteria)
        failures   <- ApiFetchFailure.selectRecent(now.minus(Duration.ofMinutes(1)))
        cands      <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(
        failures.nonEmpty,
        failures.exists(_.errorType == "JsonDecodingException"),
        cands.head.outcome == CandidateOutcome.Error
      )
    },
    test("extractLastDailyTimeout ignores non-daily timeClass games") {
      val now        = Instant.now()
      val recent     = now.minus(java.time.Duration.ofDays(10)).getEpochSecond
      val ym         = java.time.YearMonth.from(java.time.LocalDate.ofInstant(now, java.time.ZoneOffset.UTC))
      val archiveKey = s"player/alice/games/${ym.getYear}/${f"${ym.getMonthValue}%02d"}"
      val games = List(
        // Blitz timeout — should NOT count as lastDailyTimeoutAt
        archiveGameJson(
          "alice",
          "bob",
          whiteResult = "timeout",
          blackResult = "win",
          endTime = recent,
          timeClass = "blitz"
        ),
        // Daily win — no timeout
        archiveGameJson("alice", "carol", endTime = recent, timeClass = "daily")
      )
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(timeoutPct = 5.0, wins = 100, losses = 50, draws = 10),
        archiveKey           -> archiveJson(games)
      )
      // timeoutPct > 0 triggers archive fetch; not high enough to reject
      val criteria = makeCriteria().copy(dailyMaxTimeoutPercent = Some(10.0))
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        client     <- fakeChessComClient(responses)
        _          <- evalCandidates(client, runId, List(Username.wrap("alice")), criteria)
        cache      <- PlayerRecruitmentCache.selectId(pid0)
      } yield assertTrue(
        cache.isDefined,
        cache.get.lastDailyTimeoutAt.isEmpty // blitz timeout should not be stored
      )
    },
    test("rejects by dailyMaxHoursPerMove") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(timePerMove = 86400) // 24 hours
      )
      val criteria = makeCriteria().copy(dailyMaxHoursPerMove = Some(12))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts player within dailyMaxHoursPerMove") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(timePerMove = 36000) // 10 hours
      )
      val criteria = makeCriteria().copy(dailyMaxHoursPerMove = Some(12))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by maxClubs") {
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/clubs" -> apiPlayerClubsJson(List("club-1", "club-2", "club-3", "club-4", "club-5", "club-6"))
      )
      val criteria = makeCriteria().copy(maxClubs = Some(5))
      for { outcome <- evalSingle(responses, criteria) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("rejects by excludeClubs") {
      val bannedClubId = ClubId(777)
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/clubs" -> apiPlayerClubsJson(List("good-club", "banned-club"))
      )
      val criteria = makeCriteria(excludeClubs = List(bannedClubId))
      for {
        _          <- seedDb
        _          <- Club.upsert(Club(bannedClubId, Times.t0, ClubSlug("banned-club"), "Banned Club"))
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        client     <- fakeChessComClient(responses)
        _          <- evalCandidates(client, runId, List(Username.wrap("alice")), criteria)
        cands      <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(cands.head.outcome == CandidateOutcome.Rejected)
    },
    test("rejects when player has match against target club") {
      val matchId = "https://api.chess.com/pub/match/12345"
      val responses = Map(
        "player/alice"            -> apiPlayerJson(200, "alice"),
        "player/alice/matches"    -> apiPlayerMatchesJson(registeredIds = List(matchId)),
        s"club/$clubSlug/matches" -> apiClubMatchesJson(registeredIds = List(matchId))
      )
      for { outcome <- evalSingle(responses, makeCriteria()) } yield assertTrue(outcome == CandidateOutcome.Rejected)
    },
    test("accepts when player matches don't overlap target club") {
      val responses = Map(
        "player/alice"            -> apiPlayerJson(200, "alice"),
        "player/alice/matches"    -> apiPlayerMatchesJson(registeredIds = List("https://api.chess.com/pub/match/999")),
        s"club/$clubSlug/matches" -> apiClubMatchesJson(registeredIds = List("https://api.chess.com/pub/match/888"))
      )
      for { outcome <- evalSingle(responses, makeCriteria()) } yield assertTrue(outcome == CandidateOutcome.Invited)
    },
    test("rejects by daysSinceLastInvited") {
      val criteria = makeCriteria().copy(daysSinceLastInvited = Some(30))
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        // Seed player row for FK constraint
        _ <- seedPlayer(pid0)
        // Create a prior run with alice invited recently
        priorRunId <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Instant.now())
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(priorRunId, pid0, Instant.now(), CandidateOutcome.Invited, None)
        )
        // Now evaluate alice again
        runId  <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        client <- fakeChessComClient(Map("player/alice" -> apiPlayerJson(200, "alice")))
        _      <- evalCandidates(client, runId, List(Username("alice")), criteria)
        cands  <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(cands.head.outcome == CandidateOutcome.Rejected)
    },
    test("gatherClubCandidates excludes existing and evaluated usernames") {
      val responses = Map(
        "club/source-club" -> apiClubJson(sourceClubId.value, "source-club"),
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("existing-member", Times.t0.getEpochSecond),
            ("already-evaluated", Times.t0.getEpochSecond),
            ("fresh-candidate", Times.t0.getEpochSecond)
          )
        )
      )

      for {
        client <- fakeChessComClient(responses)
        candidates <- RecruitmentExplore.gatherClubCandidates(
          client,
          ClubSlug("source-club"),
          excludeSourceAdmins = false,
          existingUsernames = Set(Username("existing-member")),
          evaluatedUsernames = Set(Username("already-evaluated"))
        )
      } yield assertTrue(
        candidates == List(Username("fresh-candidate"))
      )
    },
    test("gatherClubCandidates excludes admins when enabled") {
      val responses = Map(
        "club/source-club" -> apiClubJson(sourceClubId.value, "source-club", admins = List("admin-user")),
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("admin-user", Times.t0.getEpochSecond),
            ("regular-user", Times.t0.getEpochSecond)
          )
        )
      )

      for {
        client <- fakeChessComClient(responses)
        candidates <- RecruitmentExplore.gatherClubCandidates(
          client,
          ClubSlug("source-club"),
          excludeSourceAdmins = true,
          existingUsernames = Set.empty,
          evaluatedUsernames = Set.empty
        )
      } yield assertTrue(
        candidates.size == 1,
        candidates.head == Username("regular-user")
      )
    },
    test("gatherClubCandidates keeps admins when disabled") {
      val responses = Map(
        "club/source-club" -> apiClubJson(sourceClubId.value, "source-club", admins = List("admin-user")),
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("admin-user", Times.t0.getEpochSecond),
            ("regular-user", Times.t0.getEpochSecond)
          )
        )
      )

      for {
        client <- fakeChessComClient(responses)
        candidates <- RecruitmentExplore.gatherClubCandidates(
          client,
          ClubSlug("source-club"),
          excludeSourceAdmins = false,
          existingUsernames = Set.empty,
          evaluatedUsernames = Set.empty
        )
      } yield assertTrue(
        candidates.size == 2,
        candidates.toSet == Set(Username("admin-user"), Username("regular-user"))
      )
    },
    test("cache is populated after evaluation") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
      val criteria  = makeCriteria()
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        client     <- fakeChessComClient(responses)
        _          <- evalCandidates(client, runId, List(Username("alice")), criteria)
        cached     <- PlayerRecruitmentCache.selectId(pid0)
      } yield assertTrue(
        cached.isDefined,
        cached.get.clubCount.contains(0),
        cached.get.ongoingGames.contains(0),
        cached.get.dailyElo.contains(1200),
        cached.get.lastDailyTimeoutAt.isEmpty,
        cached.get.lastTmTimeoutAt.isEmpty
      )
    },
    test("former member rejected when excludeFormerMembers = true") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
      val criteria  = makeCriteria(excludeFormerMembers = true)
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        // Seed alice as a former member of the club (player row needed for FK)
        _      <- seedPlayer(pid0)
        _      <- ClubMember.insert(ClubMember(clubId, pid0, Times.t0, Some(Times.t1)))
        runId  <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        client <- fakeChessComClient(responses)
        _      <- evalCandidates(client, runId, List(Username("alice")), criteria)
        cands  <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(cands.head.outcome == CandidateOutcome.Rejected)
    },
    test("former member accepted when excludeFormerMembers = false") {
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))
      val criteria  = makeCriteria(excludeFormerMembers = false)
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        // Seed alice as a former member of the club
        _      <- seedPlayer(pid0)
        _      <- ClubMember.insert(ClubMember(clubId, pid0, Times.t0, Some(Times.t1)))
        runId  <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        client <- fakeChessComClient(responses)
        _      <- evalCandidates(client, runId, List(Username("alice")), criteria)
        cands  <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(cands.head.outcome == CandidateOutcome.Deferred)
    }
  )

  // ==========================================================================
  // Suite: Cache-aware filters
  // ==========================================================================

  /** Helper: run evalSingle but seed a cache row (and its player FK) before evaluation. */
  private def evalSingleWithCache(
    responses: Map[String, String],
    criteria: RecruitmentCriteria,
    cache: PlayerRecruitmentCache,
    username: String = "alice"
  ): RIO[PostgresClient, Option[CandidateOutcome]] =
    for {
      _ <- seedDb
      // Seed player row for FK constraint, then seed cache
      _          <- seedPlayer(cache.playerId)
      _          <- PlayerRecruitmentCache.upsert(cache)
      criteriaId <- seedCriteria(criteria)
      runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
      client     <- fakeChessComClient(responses)
      _          <- evalCandidates(client, runId, List(Username.wrap(username)), criteria)
      cands      <- RecruitmentCandidate.selectByRun(runId)
    } yield cands.headOption.map(_.outcome)

  private def suiteCacheFilters = suite("cache-aware filters")(
    test("zero-tolerance daily timeout rejects from cache") {
      val now = Instant.now()
      val staleCache = PlayerRecruitmentCache(
        playerId = pid0,
        fetchedAt = now.minus(java.time.Duration.ofDays(30)), // very old cache
        dailyElo = Some(Elo(1500)),
        dailyScoreRate = None,
        dailyTimeoutPct = Some(0.0),
        dailyGamesFinished = Some(200),
        clubCount = Some(5),
        ongoingGames = Some(3),
        ongoingTeamMatches = Some(2),
        tmGamesFinished90d = Some(10),
        tmTimeoutPct90d = Some(0.0),
        lastDailyTimeoutAt = Some(now.minus(java.time.Duration.ofDays(100))), // had a timeout once
        lastTmTimeoutAt = None
      )
      val criteria  = makeCriteria().copy(dailyMaxTimeoutPercent = Some(0.0))
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))

      for {
        outcome <- evalSingleWithCache(responses, criteria, staleCache)
      } yield assertTrue(outcome.isEmpty) // cache-only rejection: no candidate row persisted
    },
    test("maxClubs cache rejection at 48h old cache") {
      val now = Instant.now()
      val cache48h = PlayerRecruitmentCache(
        playerId = pid0,
        fetchedAt = now.minus(java.time.Duration.ofHours(48)),
        dailyElo = Some(Elo(1500)),
        dailyScoreRate = None,
        dailyTimeoutPct = Some(0.0),
        dailyGamesFinished = Some(200),
        clubCount = Some(120), // way over limit
        ongoingGames = Some(3),
        ongoingTeamMatches = Some(2),
        tmGamesFinished90d = Some(10),
        tmTimeoutPct90d = Some(0.0),
        lastDailyTimeoutAt = None,
        lastTmTimeoutAt = None
      )
      val criteria  = makeCriteria().copy(maxClubs = Some(50))
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))

      for {
        outcome <- evalSingleWithCache(responses, criteria, cache48h)
      } yield assertTrue(outcome.isEmpty) // cache-only rejection: no candidate row persisted
    },
    test("stale cache falls through to API checks") {
      val now = Instant.now()
      val staleCache = PlayerRecruitmentCache(
        playerId = pid0,
        fetchedAt = now.minus(java.time.Duration.ofDays(31)),
        dailyElo = Some(Elo(500)),
        dailyScoreRate = None,
        dailyTimeoutPct = Some(50.0),
        dailyGamesFinished = Some(5),
        clubCount = Some(120),
        ongoingGames = Some(0),
        ongoingTeamMatches = Some(0),
        tmGamesFinished90d = Some(0),
        tmTimeoutPct90d = None,
        lastDailyTimeoutAt = None,
        lastTmTimeoutAt = None
      )
      val criteria = makeCriteria().copy(
        maxClubs = Some(50),
        dailyMinElo = Some(Elo(1000)),
        dailyMaxTimeoutPercent = Some(10.0)
      )
      val responses = Map(
        "player/alice"       -> apiPlayerJson(200, "alice"),
        "player/alice/stats" -> apiPlayerStatsJson(dailyElo = 1500, timeoutPct = 2.0),
        "player/alice/clubs" -> apiPlayerClubsJson(List("club-a", "club-b"))
      )

      for {
        outcome <- evalSingleWithCache(responses, criteria, staleCache)
      } yield assertTrue(outcome.contains(CandidateOutcome.Deferred))
    },
    test("cache-only rejection does not block re-evaluation via daysSinceRejected") {
      val now = Instant.now()
      val cache = PlayerRecruitmentCache(
        playerId = pid0,
        fetchedAt = now.minus(Duration.ofHours(12)),
        dailyElo = Some(Elo(1500)),
        dailyScoreRate = None,
        dailyTimeoutPct = Some(0.0),
        dailyGamesFinished = Some(200),
        clubCount = Some(120), // over limit
        ongoingGames = Some(3),
        ongoingTeamMatches = Some(2),
        tmGamesFinished90d = Some(10),
        tmTimeoutPct90d = Some(0.0),
        lastDailyTimeoutAt = None,
        lastTmTimeoutAt = None
      )
      val criteria  = makeCriteria(daysSinceRejected = Some(30)).copy(maxClubs = Some(50))
      val responses = Map("player/alice" -> apiPlayerJson(200, "alice"))

      for {
        // First run: cache-only rejection (no candidate row written)
        outcome1 <- evalSingleWithCache(responses, criteria, cache)
        // Verify no Rejected row exists for daysSinceRejected to find
        rejected <- RecruitmentCandidate.selectLatestRejectedByAlias(pid0, clubId, "default")
      } yield assertTrue(
        outcome1.isEmpty,
        rejected.isEmpty
      )
    }
  )
}
