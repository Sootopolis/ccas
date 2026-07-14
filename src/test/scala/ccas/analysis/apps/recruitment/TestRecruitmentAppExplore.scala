package ccas.analysis.apps.recruitment

import java.time.{Instant, YearMonth, ZoneOffset}

import zio.{durationInt, Promise, Schedule, ZLayer}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.*
import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.ProgressDisplay

object TestRecruitmentAppExplore extends ZIOSpecDefault {

  private val discoverableClubId   = ClubId(701)
  private val discoverableClubSlug = ClubSlug("discoverable-club")
  // PlayerId(252) is in seedDb's cleanup list, so the shared-member row is reset between tests.
  private val sharedMemberPid = PlayerId(252)

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentAppExplore")(
    suiteExploreMode
  ).provideShared(
    FreshSchemaLayer("test_recruitment_app_explore", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false))
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite: Explore mode
  // ==========================================================================

  private def suiteExploreMode = suite("explore mode")(
    test("isGrim pure logic") {
      assertTrue(
        !isGrim(SourceState(Nil, 99, 99, 99)), // below threshold
        isGrim(SourceState(Nil, 10, 10, 100)), // consecutive threshold hit
        !isGrim(SourceState(Nil, 40, 39, 5)),  // high ratio but low consecutive — not grim
        !isGrim(SourceState(Nil, 0, 0, 0))     // fresh source
      )
    },
    test("explore=false does not explore beyond source clubs") {
      val responses = Map(
        s"club/$clubSlug"             -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members"     -> apiClubMembersJson(Nil),
        s"club/$discoverableClubSlug" -> apiClubJson(discoverableClubId.value, discoverableClubSlug.value),
        s"club/$discoverableClubSlug/members" -> apiClubMembersJson(
          List(("explorer", Times.t0.getEpochSecond))
        ),
        "player/explorer" -> apiPlayerJson(210, "explorer")
      )
      val criteria = makeCriteria()

      for {
        _      <- seedDb
        _      <- Club.upsert(Club(discoverableClubId, Times.t0, discoverableClubSlug, "Discoverable Club", None, None, None))
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        result <- runRecruit(client, explore = false)
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
      } yield assertTrue(
        candidates.isEmpty
      )
    },
    test("explore=true discovers candidates from match board opponents") {
      val oppPlayerId = PlayerId(211)
      val matchId     = ClubMatchId(9001)
      val responses = Map(
        s"club/$clubSlug"         -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members" -> apiClubMembersJson(Nil),
        s"club/$clubSlug/matches" -> apiClubMatchesJson(),
        "player/opp-player"       -> apiPlayerJson(oppPlayerId.value, "opp-player")
      )
      for {
        _      <- seedDb
        _      <- seedCriteria(makeCriteria())
        _      <- Player.insert(Player(oppPlayerId, Times.t0, Username("opp-player"), Active, None, Times.t0))
        _      <- seedPlayer(PlayerId(999))
        _      <- seedMatchWithBoard(matchId, Some(clubId), PlayerId(999), oppPlayerId)
        client <- fakeChessComClient(responses)
        result <- runRecruit(client, explore = true)
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
      } yield assertTrue(
        candidates.size == 1,
        candidates.head.outcome == CandidateOutcome.Invited,
        candidates.head.playerId == oppPlayerId
      )
    },
    test("explore=true respects invite cap across sources") {
      val source1 = ClubSlug("source-1")
      val source2 = ClubSlug("source-2")
      val responses = Map(
        s"club/$clubSlug"         -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members" -> apiClubMembersJson(Nil),
        s"club/$source1"          -> apiClubJson(ClubId(801).value, source1.value),
        s"club/$source1/members" -> apiClubMembersJson(
          List(("cap-a", Times.t0.getEpochSecond), ("cap-b", Times.t0.getEpochSecond))
        ),
        s"club/$source2" -> apiClubJson(ClubId(802).value, source2.value),
        s"club/$source2/members" -> apiClubMembersJson(
          List(("cap-c", Times.t0.getEpochSecond), ("cap-d", Times.t0.getEpochSecond))
        ),
        "player/cap-a" -> apiPlayerJson(220, "cap-a"),
        "player/cap-b" -> apiPlayerJson(221, "cap-b"),
        "player/cap-c" -> apiPlayerJson(222, "cap-c"),
        "player/cap-d" -> apiPlayerJson(223, "cap-d")
      )
      val criteria = makeCriteria()

      for {
        _      <- seedDb
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        result <- runRecruit(client, target = Some(3), sourceClubs = List(source1, source2))
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
        invited  = candidates.filter(_.outcome == CandidateOutcome.Invited)
        deferred = candidates.filter(_.outcome == CandidateOutcome.Deferred)
      } yield assertTrue(
        invited.size == 3,
        result.candidatesFound == 3,
        deferred.size + invited.size >= 3,
        deferred.size + invited.size <= 4
      )
    },
    test("interrupted recruit persists partial results") {
      val intSource      = ClubSlug("int-source")
      val candidateNames = (0 to 4).map(i => s"int-cand-$i").toList
      val responses = Map(
        s"club/$clubSlug"         -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members" -> apiClubMembersJson(Nil),
        s"club/$intSource"        -> apiClubJson(intSourceClubId.value, intSource.value),
        s"club/$intSource/members" -> apiClubMembersJson(
          candidateNames.map(n => (n, Times.t0.getEpochSecond))
        )
      ) ++ candidateNames.zipWithIndex.map { (name, i) =>
        s"player/$name" -> apiPlayerJson(300 + i, name)
      }.toMap
      val criteria = makeCriteria()

      for {
        _       <- seedDb
        _       <- seedCriteria(criteria)
        reached <- Promise.make[Nothing, Unit]
        gate    <- Promise.make[Nothing, Unit]
        client  <- fakeChessComClientWithBlock(responses, blockAfterN = 4, reached, gate)
        fiber <- runRecruit(client, sourceClubs = List(intSource)).fork
        _      <- reached.await
        // Anchor the interrupt on a persisted candidate row, not a fixed sleep. The 5th-profile
        // block pins how many profiles have been fetched, but a candidate is only written by
        // persistCandidateResults once its full filter chain (stats / clubs / etc.) completes,
        // and that finishing time is what flakes under load. The run row is inserted before any
        // API call, so selectLatest is Some by the time reached.await returns.
        runId  <- RecruitmentRun.selectLatest(clubId).map(_.get.runId)
        _ <- RecruitmentCandidate.selectByRun(runId)
          .repeat(Schedule.spaced(20.millis) && Schedule.recurUntil[List[RecruitmentCandidate]](_.nonEmpty))
          .timeoutFail(new RuntimeException("no candidate persisted before interrupt"))(5.seconds)
        _      <- fiber.interrupt
        latest <- RecruitmentRun.selectLatest(clubId)
        cands  <- RecruitmentCandidate.selectByRun(runId)
        deferred = cands.filter(_.outcome == CandidateOutcome.Deferred)
      } yield assertTrue(
        latest.isDefined,
        latest.get.completedAt.isDefined,
        latest.get.candidatesFound == 0,
        deferred.nonEmpty
      )
    } @@ TestAspect.withLiveClock,
    test("excess invited candidates stay Deferred and carry forward (auto-confirm)") {
      val source = ClubSlug("defer-source")
      // 6 candidates, target=2 → exactly 2 Invited; the 4 chunk-overshoot passers stay Deferred (NOT deleted) so they
      // carry to the next run via selectDeferredByClub.
      val candidateNames = (0 to 5).map(i => s"defer-cand-$i").toList
      val responses = Map(
        s"club/$clubSlug"         -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members" -> apiClubMembersJson(Nil),
        s"club/$source"           -> apiClubJson(ClubId(901).value, source.value),
        s"club/$source/members" -> apiClubMembersJson(
          candidateNames.map(n => (n, Times.t0.getEpochSecond))
        )
      ) ++ candidateNames.zipWithIndex.map { (name, i) =>
        s"player/$name" -> apiPlayerJson(400 + i, name)
      }.toMap
      val criteria = makeCriteria()

      for {
        _      <- seedDb
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        result <- runRecruit(client, target = Some(2), sourceClubs = List(source))
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
        invited  = candidates.filter(_.outcome == CandidateOutcome.Invited)
        deferred = candidates.filter(_.outcome == CandidateOutcome.Deferred)
      } yield assertTrue(
        invited.size == 2,
        result.candidatesFound == 2,
        deferred.size == candidateNames.size - 2
      )
    },
    test("deferred-confirm caps invited at target, leaving excess Deferred for next run") {
      // Regression for the 31-vs-30 bug: a chunk overshoot persists more Deferred than target, and the old uncapped
      // confirm flipped ALL of them. Now /found (selectDeferredByRun) and the confirm both cap at the run's stored
      // target, and the excess stays Deferred to carry forward.
      val source         = ClubSlug("confirm-source")
      val candidateNames = (0 to 5).map(i => s"confirm-cand-$i").toList
      val responses = Map(
        s"club/$clubSlug"         -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members" -> apiClubMembersJson(Nil),
        s"club/$source"           -> apiClubJson(ClubId(902).value, source.value),
        s"club/$source/members" -> apiClubMembersJson(
          candidateNames.map(n => (n, Times.t0.getEpochSecond))
        )
      ) ++ candidateNames.zipWithIndex.map { (name, i) =>
        s"player/$name" -> apiPlayerJson(410 + i, name)
      }.toMap
      val criteria = makeCriteria()

      for {
        _      <- seedDb
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        result <- runRecruit(client, target = Some(2), sourceClubs = List(source), autoConfirm = false)
        allBefore         <- RecruitmentCandidate.selectByRun(result.runId)
        allDeferredBefore = allBefore.filter(_.outcome == CandidateOutcome.Deferred)
        foundBefore       <- RecruitmentCandidate.selectDeferredByRun(result.runId)
        firstFlip         <- RecruitmentCandidate.confirmDeferredByRun(result.runId)
        invited           <- RecruitmentCandidate.selectInvitedByRun(result.runId)
        // Re-POST must be idempotent (JobRoutes contract): flip 0, invited stays at target — it must NOT sweep up the
        // still-Deferred overshoot.
        secondFlip        <- RecruitmentCandidate.confirmDeferredByRun(result.runId)
        invitedAfterRepost <- RecruitmentCandidate.selectInvitedByRun(result.runId)
        allAfter          <- RecruitmentCandidate.selectByRun(result.runId)
        deferredAfter     = allAfter.filter(_.outcome == CandidateOutcome.Deferred)
      } yield assertTrue(
        allDeferredBefore.size == candidateNames.size, // all 6 evaluated, nothing deleted
        foundBefore.size == 2,                         // /found capped at target
        firstFlip == 2,                                // confirm flips exactly target
        invited.size == 2,                             // confirm capped at target
        secondFlip == 0,                               // re-POST flips nothing (idempotent)
        invitedAfterRepost.size == 2,                  // still exactly target after re-POST
        deferredAfter.size == candidateNames.size - 2  // excess (4) carried forward as Deferred
      )
    },
    test("deferred candidates from prior run are prioritised in next run") {
      val source = ClubSlug("prio-source")
      // Seed a prior run with a Deferred candidate, then run again
      val responses = Map(
        s"club/$clubSlug"         -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members" -> apiClubMembersJson(Nil),
        s"club/$source"           -> apiClubJson(ClubId(902).value, source.value),
        s"club/$source/members" -> apiClubMembersJson(
          List(("prio-new", Times.t0.getEpochSecond))
        ),
        "player/prio-deferred" -> apiPlayerJson(500, "prio-deferred"),
        "player/prio-new"      -> apiPlayerJson(501, "prio-new")
      )
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        client     <- fakeChessComClient(responses)
        // Seed prior run with a Deferred candidate (need Player row)
        _          <- Player.insert(Player(PlayerId(500), Times.t0, Username("prio-deferred"), Active, None, Times.t0))
        priorRunId <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None, None)
        _ <- RecruitmentRun.update(
          RecruitmentRun(priorRunId, clubId, criteriaId, RunTrigger.Cli, Times.t0, Some(Times.t1), 0, None, None)
        )
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(priorRunId, PlayerId(500), Times.t0, CandidateOutcome.Deferred, None)
        )

        // Verify selectDeferredByClub finds it
        deferredBefore <- RecruitmentCandidate.selectDeferredByClub(clubId)

        // Run recruitment — deferred candidate should be picked up as priority
        result <- runRecruit(client, target = Some(10), sourceClubs = List(source))

        // The deferred candidate should now have an Invited outcome in the new run
        newCandidates <- RecruitmentCandidate.selectByRun(result.runId)
        newInvited     = newCandidates.filter(_.outcome == CandidateOutcome.Invited)
        newInvitedPids = newInvited.map(_.playerId).toSet

        // After the new run, selectDeferredByClub should no longer return the candidate
        deferredAfter <- RecruitmentCandidate.selectDeferredByClub(clubId)
      } yield assertTrue(
        deferredBefore.size == 1,
        deferredBefore.head.playerId == PlayerId(500),
        newInvitedPids.contains(PlayerId(500)),
        deferredAfter.isEmpty
      )
    },
    test("selectDeferredByClub excludes candidates resolved in later runs") {
      val criteria = makeCriteria()

      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        _ <- Player.insert(Player(PlayerId(600), Times.t0, Username("resolved-player"), Active, None, Times.t0))

        // Run 1: candidate is Deferred
        runId1 <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None, None)
        _ <- RecruitmentRun.update(
          RecruitmentRun(runId1, clubId, criteriaId, RunTrigger.Cli, Times.t0, Some(Times.t1), 0, None, None)
        )
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(runId1, PlayerId(600), Times.t0, CandidateOutcome.Deferred, None)
        )

        // Should find the deferred candidate
        deferredBefore <- RecruitmentCandidate.selectDeferredByClub(clubId)

        // Run 2: same candidate is Invited (later timestamp)
        runId2 <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t2, None, None)
        _ <- RecruitmentRun.update(
          RecruitmentRun(runId2, clubId, criteriaId, RunTrigger.Cli, Times.t2, Some(Times.t3), 1, None, None)
        )
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(runId2, PlayerId(600), Times.t2, CandidateOutcome.Invited, None)
        )

        // Should no longer find the deferred candidate
        deferredAfter <- RecruitmentCandidate.selectDeferredByClub(clubId)
      } yield assertTrue(
        deferredBefore.size == 1,
        deferredAfter.isEmpty
      )
    },
    test("empty source club does not abandon later source clubs (regression: dropped pending sources)") {
      val emptySource = ClubSlug("drop-empty-source")
      val liveSource  = ClubSlug("drop-live-source")
      val freshPid    = PlayerId(250)
      val responses = Map(
        s"club/$clubSlug" -> apiClubJson(clubId.value, clubSlug.value),
        // "shared-member" is already a member of the target club → it lands in existingUsernames
        s"club/$clubSlug/members"    -> apiClubMembersJson(List(("shared-member", Times.t0.getEpochSecond))),
        s"club/$emptySource"         -> apiClubJson(ClubId(801).value, emptySource.value),
        // emptySource's only member is an existing target member → 0 candidates after exclusion
        s"club/$emptySource/members" -> apiClubMembersJson(List(("shared-member", Times.t0.getEpochSecond))),
        s"club/$liveSource"          -> apiClubJson(ClubId(802).value, liveSource.value),
        s"club/$liveSource/members"  -> apiClubMembersJson(List(("fresh-cand", Times.t0.getEpochSecond))),
        // membership reconcile fetches each target-club member's profile
        "player/shared-member"       -> apiPlayerJson(sharedMemberPid.value, "shared-member"),
        "player/fresh-cand"          -> apiPlayerJson(freshPid.value, "fresh-cand")
      )
      for {
        _      <- seedDb
        _      <- seedCriteria(makeCriteria())
        client <- fakeChessComClient(responses)
        // exploreConcurrency=1 activates emptySource first; it yields 0 candidates so the pool is empty.
        // Pre-fix the still-pending liveSource was dropped and fresh-cand was never evaluated.
        result     <- runRecruit(client, target = Some(5), sourceClubs = List(emptySource, liveSource))
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
      } yield assertTrue(
        candidates.exists(c => c.playerId == freshPid && c.outcome == CandidateOutcome.Invited)
      )
    },
    test("discovered opponent who is an existing member does not spin the explore loop (regression)") {
      val tmSource = ClubSlug("tm-source")
      val candPid  = PlayerId(251)
      // Derive the archive month and game time from one instant. recruit's own clock runs slightly later, so guard
      // the month-boundary race by also seeding the next month — whichever month recruit treats as "current" hits.
      val nowInst = Instant.now()
      val gameEnd = nowInst.getEpochSecond - 3600
      val ym0     = YearMonth.from(nowInst.atOffset(ZoneOffset.UTC))
      val ym1     = ym0.plusMonths(1)
      // cand-x played a daily team match against "shared-member", who is already a member of the target club.
      // That opponent lands in discoveredOpponents but is filtered out at activation (existingUsernames).
      val archive = archiveJson(
        List(
          archiveGameJson(
            white = "cand-x",
            black = "shared-member",
            matchUrl = Some("https://api.chess.com/pub/match/12345"),
            timeClass = "daily",
            endTime = gameEnd
          )
        )
      )
      val responses = Map(
        s"club/$clubSlug"         -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members" -> apiClubMembersJson(List(("shared-member", Times.t0.getEpochSecond))),
        s"club/$tmSource"         -> apiClubJson(ClubId(801).value, tmSource.value),
        s"club/$tmSource/members" -> apiClubMembersJson(List(("cand-x", Times.t0.getEpochSecond))),
        // membership reconcile fetches each target-club member's profile
        "player/shared-member"    -> apiPlayerJson(sharedMemberPid.value, "shared-member"),
        "player/cand-x"           -> apiPlayerJson(candPid.value, "cand-x"),
        s"player/cand-x/games/${ym0.getYear}/${ym0.getMonthValue}" -> archive,
        s"player/cand-x/games/${ym1.getYear}/${ym1.getMonthValue}" -> archive
      )
      // CheckTmStats only joins the chain when a TM criterion is set; that filter populates discoveredOpponents.
      val criteria = makeCriteria().copy(dailyMinTmGamesFinished = Some(1))
      for {
        _      <- seedDb
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        // Pre-fix: replenish re-yields "shared-member" every cycle while activation drops it to 0 → infinite loop.
        // The timeout turns that hang into a deterministic failure; post-fix the run completes immediately.
        result <- runRecruit(client, target = Some(5), explore = true, sourceClubs = List(tmSource))
          .timeout(30.seconds)
        // cand-x's Player row is upserted by FetchAndCheckPlayer regardless of final candidate outcome, so this
        // confirms the discovery path ran without depending on candidate-row persistence (cacheRejected can skip it).
        candPlayer <- Player.selectByUsername(Username("cand-x"))
      } yield assertTrue(
        result.isDefined,
        candPlayer.isDefined
      )
    }
  )
}
