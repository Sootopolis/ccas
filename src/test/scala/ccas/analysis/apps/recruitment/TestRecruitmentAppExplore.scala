package ccas.analysis.apps.recruitment

import zio.{durationInt, Promise, Schedule, ZLayer}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.*
import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.TestCcasLogger

object TestRecruitmentAppExplore extends ZIOSpecDefault {

  private val discoverableClubId   = ClubId(701)
  private val discoverableClubSlug = ClubSlug("discoverable-club")

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentAppExplore")(
    suiteExploreMode
  ).provideShared(
    FreshSchemaLayer("test_recruitment_app_explore", onInit = Tables.ensureTables),
    ZLayer.succeed(TestCcasLogger.noop)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite: Explore mode
  // ==========================================================================

  private def suiteExploreMode = suite("explore mode")(
    test("isGrim pure logic") {
      assertTrue(
        !isGrim(SourceState(Nil, 49, 49, 49)), // below threshold
        isGrim(SourceState(Nil, 10, 10, 50)),  // consecutive threshold hit
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
    test("excess invited candidates are reclassified as Deferred") {
      val source = ClubSlug("defer-source")
      // 6 candidates, target=2 → should get exactly 2 Invited, rest Deferred or Rejected
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
        invited.size + deferred.size >= 2
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
        priorRunId <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None)
        _ <- RecruitmentRun.update(
          RecruitmentRun(priorRunId, clubId, criteriaId, RunTrigger.Cli, Times.t0, Some(Times.t1), 0, None)
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
        runId1 <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None)
        _ <- RecruitmentRun.update(
          RecruitmentRun(runId1, clubId, criteriaId, RunTrigger.Cli, Times.t0, Some(Times.t1), 0, None)
        )
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(runId1, PlayerId(600), Times.t0, CandidateOutcome.Deferred, None)
        )

        // Should find the deferred candidate
        deferredBefore <- RecruitmentCandidate.selectDeferredByClub(clubId)

        // Run 2: same candidate is Invited (later timestamp)
        runId2 <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t2, None)
        _ <- RecruitmentRun.update(
          RecruitmentRun(runId2, clubId, criteriaId, RunTrigger.Cli, Times.t2, Some(Times.t3), 1, None)
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
    }
  )
}
