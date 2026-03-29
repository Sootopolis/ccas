package ccas.analysis.apps.recruitment

import java.time.{Duration, Instant}

import com.augustnagro.magnum.Transactor
import zio.{durationInt, Promise, Scope, ZIO}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.CcasLogger
import ccas.utils.sql.FreshSchemaLayer

object TestRecruitmentApp extends ZIOSpecDefault {

  private val discoverableClubId   = ClubId(701)
  private val discoverableClubSlug = ClubSlug("discoverable-club")

  private val refMatchId = ClubMatchId(8001)

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentApp")(
    suiteDbCrud,
    suiteFullWorkflow,
    suiteExploreMode,
    suiteReport,
    suiteMatchRefWriting
  ).provideShared(
    FreshSchemaLayer("test_recruitment_app", onInit = Tables.ensureTables),
    Scope.default,
    CcasLogger.live(showProgress = false)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite: DB CRUD
  // ==========================================================================

  private def suiteDbCrud = suite("DB CRUD")(
    test("RecruitmentCriteria insert and selectId") {
      val criteria = makeCriteria(excludeClubs = List(ClubId(700)))
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        loaded     <- RecruitmentCriteria.selectId(criteriaId)
      } yield assertTrue(
        loaded.isDefined,
        loaded.get.excludeClubs == List(ClubId(700))
      )
    },
    test("RecruitmentAlias selectClub") {
      for {
        _    <- seedDb
        cid1 <- RecruitmentCriteria.insert(makeCriteria())
        cid2 <- RecruitmentCriteria.insert(makeCriteria())
        _    <- RecruitmentAlias.insert(RecruitmentAlias(clubId, "cfg1", Instant.now(), cid1))
        _    <- RecruitmentAlias.insert(RecruitmentAlias(clubId, "cfg2", Instant.now(), cid2))
        all  <- RecruitmentAlias.selectClub(clubId)
      } yield assertTrue(all.size == 2)
    },
    test("RecruitmentCriteria insert is insert-only (new ID each time)") {
      val criteria = makeCriteria()
      for {
        _       <- seedDb
        cid1    <- RecruitmentCriteria.insert(criteria)
        cid2    <- RecruitmentCriteria.insert(criteria.copy(dailyMinElo = Some(1500)))
        loaded1 <- RecruitmentCriteria.selectId(cid1)
        loaded2 <- RecruitmentCriteria.selectId(cid2)
      } yield assertTrue(
        cid1 != cid2,
        loaded1.get.dailyMinElo.isEmpty,
        loaded2.get.dailyMinElo.contains(1500)
      )
    },
    test("RecruitmentCriteria array round-trip") {
      val criteria = makeCriteria(
        excludeClubs = List(ClubId(800))
      ).copy(nationalityCountries = List("US", "GB", "DE"))
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        loaded     <- RecruitmentCriteria.selectId(criteriaId)
      } yield assertTrue(
        loaded.get.excludeClubs == List(ClubId(800)),
        loaded.get.nationalityCountries == List("US", "GB", "DE")
      )
    },
    test("RecruitmentRun insert returns generated runId and selectId retrieves") {
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        loaded     <- RecruitmentRun.selectId(runId)
      } yield assertTrue(
        runId > 0,
        loaded.isDefined,
        loaded.get.clubId == clubId,
        loaded.get.criteriaId == criteriaId,
        loaded.get.candidatesFound == 0,
        loaded.get.completedAt.isEmpty
      )
    },
    test("RecruitmentRun update sets completedAt and candidatesFound") {
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        _ <- RecruitmentRun.update(
          RecruitmentRun(runId, clubId, criteriaId, RunTrigger.Cli, Times.t0, Some(Times.t1), 5)
        )
        loaded <- RecruitmentRun.selectId(runId)
      } yield assertTrue(
        loaded.get.completedAt.isDefined,
        loaded.get.candidatesFound == 5
      )
    },
    test("RecruitmentRun selectLatest returns most recent") {
      for {
        _          <- seedDb
        criteriaId <- seedCriteria(makeCriteria())
        _          <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        runId2     <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t1)
        latest     <- RecruitmentRun.selectLatest(clubId)
      } yield assertTrue(
        latest.isDefined,
        latest.get.runId == runId2
      )
    },
    test("RecruitmentCandidate insert and selectByRun") {
      for {
        _          <- seedDb
        _          <- seedPlayer(pid0)
        _          <- seedPlayer(pid1)
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid0, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid1, Times.t0, CandidateOutcome.Rejected, Some("too few games"))
          )
        all <- RecruitmentCandidate.selectByRun(runId)
      } yield assertTrue(all.size == 2)
    },
    test("RecruitmentCandidate selectInvitedByRun filters by outcome") {
      for {
        _          <- seedDb
        _          <- seedPlayer(pid0)
        _          <- seedPlayer(pid1)
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid0, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid1, Times.t0, CandidateOutcome.Rejected, Some("reason"))
          )
        invited <- RecruitmentCandidate.selectInvitedByRun(runId)
      } yield assertTrue(
        invited.size == 1,
        invited.head.playerId == pid0
      )
    },
    test("CandidateOutcome enum round-trip for all variants") {
      val enumPids = CandidateOutcome.values.toList.zipWithIndex.map((_, i) => PlayerId.wrap(250L + i))
      for {
        _ <- seedDb
        _ <- ZIO.foreachDiscard(enumPids)(seedPlayer)
        outcomes = CandidateOutcome.values.toList
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        _ <- ZIO.foreachDiscard(outcomes.zip(enumPids)) { (outcome, pid) =>
          RecruitmentCandidate
            .insert(
              RecruitmentCandidate(runId, pid, Times.t0, outcome, Some(s"reason-${pid}"))
            )
        }
        candidates <- RecruitmentCandidate.selectByRun(runId)
        loadedOutcomes = candidates.map(_.outcome).toSet
      } yield assertTrue(
        candidates.size == outcomes.size,
        loadedOutcomes == outcomes.toSet
      )
    },
    test("RecruitmentCandidate selectLatestInvited") {
      for {
        _          <- seedDb
        _          <- seedPlayer(pid0)
        criteriaId <- seedCriteria(makeCriteria())
        runId1     <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        runId2     <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t1)
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId1, pid0, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId2, pid0, Times.t1, CandidateOutcome.Invited, None)
          )
        latest <- RecruitmentCandidate.selectLatestInvited(pid0)
      } yield assertTrue(
        latest.isDefined,
        latest.get.runId == runId2
      )
    },
    test("defaultDaily round-trips through DB via insert/selectId") {
      val criteria = RecruitmentCriteria.defaultDaily
      for {
        _          <- seedDb
        criteriaId <- RecruitmentCriteria.insert(criteria)
        loaded     <- RecruitmentCriteria.selectId(criteriaId)
      } yield assertTrue(
        loaded.isDefined,
        loaded.get.copy(criteriaId = 0) == criteria
      )
    },
    test("ApiFetchFailure insert and selectRecent") {
      val now = Instant.now()
      val failure = ApiFetchFailure(
        occurredAt = now,
        url = "https://api.chess.com/pub/player/alice/games/2026/03",
        errorType = "UserFacingException",
        errorMessage = Some("HTTP 404"),
        responseBody = None
      )
      for {
        _        <- seedDb
        _        <- ApiFetchFailure.insert(failure)
        recent   <- ApiFetchFailure.selectRecent(now.minus(Duration.ofMinutes(1)))
        tooEarly <- ApiFetchFailure.selectRecent(now.plus(Duration.ofMinutes(1)))
      } yield assertTrue(
        recent.size == 1,
        recent.head.url == failure.url,
        recent.head.errorType == "UserFacingException",
        recent.head.errorMessage.contains("HTTP 404"),
        tooEarly.isEmpty
      )
    }
  )

  // ==========================================================================
  // Suite: Full workflow (end-to-end)
  // ==========================================================================

  private def suiteFullWorkflow = suite("full workflow")(
    test("recruit end-to-end") {
      val responses = Map(
        s"club/$clubSlug" -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members" -> apiClubMembersJson(
          List(
            ("existing", Times.t0.getEpochSecond)
          )
        ),
        "club/source-club" -> apiClubJson(sourceClubId.value, "source-club"),
        "club/source-club/members" -> apiClubMembersJson(
          List(
            ("existing", Times.t0.getEpochSecond),
            ("candidate-a", Times.t0.getEpochSecond),
            ("candidate-b", Times.t0.getEpochSecond)
          )
        ),
        "player/existing"    -> apiPlayerJson(199, "existing"),
        "player/candidate-a" -> apiPlayerJson(200, "candidate-a"),
        "player/candidate-b" -> apiPlayerJson(201, "candidate-b")
      )
      val criteria = makeCriteria()

      for {
        _      <- seedDb
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        logger <- ZIO.service[CcasLogger]
        result <- RecruitmentApp.recruit(clubSlug, "default", sourceClubs = List(ClubSlug("source-club")), trigger = RunTrigger.Api)
          .provideEnvironment(zio.ZEnvironment(client, xa, logger))
        // Verify run record
        run <- RecruitmentRun.selectId(result.runId)
        // Verify candidates
        invited <- RecruitmentCandidate.selectInvitedByRun(result.runId)
        // Verify Player/PlayerSnapshot persistence
        playerA <- Player.selectId(pid0)
        playerB <- Player.selectId(pid1)
      } yield assertTrue(
        run.isDefined,
        run.get.completedAt.isDefined,
        run.get.candidatesFound == 2,
        invited.size == 2,
        playerA.isDefined,
        playerB.isDefined
      )
    }
  )

  // ==========================================================================
  // Suite: Explore mode
  // ==========================================================================

  private def suiteExploreMode = suite("explore mode")(
    test("isGrim pure logic") {
      import ccas.analysis.apps.recruitment.{SourceState, isGrim}
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
        _      <- Club.upsert(Club(discoverableClubId, Times.t0, discoverableClubSlug, "Discoverable Club"))
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        logger <- ZIO.service[CcasLogger]
        result <- RecruitmentApp.recruit(clubSlug, "default", sourceClubs = Nil, explore = false, trigger = RunTrigger.Api)
          .provideEnvironment(zio.ZEnvironment(client, xa, logger))
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
      } yield assertTrue(
        candidates.isEmpty
      )
    },
    test("explore=true discovers candidates from DB clubs") {
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
        _      <- Club.upsert(Club(discoverableClubId, Times.t0, discoverableClubSlug, "Discoverable Club"))
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        logger <- ZIO.service[CcasLogger]
        result <- RecruitmentApp.recruit(clubSlug, "default", sourceClubs = Nil, explore = true, trigger = RunTrigger.Api)
          .provideEnvironment(zio.ZEnvironment(client, xa, logger))
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
      } yield assertTrue(
        candidates.size == 1,
        candidates.head.outcome == CandidateOutcome.Invited,
        candidates.head.playerId == PlayerId(210)
      )
    },
    test("explore=true discovers candidates from match opponents") {
      val clubMatchesWithOpponent =
        s"""{"finished": [{"name": "match", "@id": "https://api.chess.com/pub/match/99", "opponent": "https://api.chess.com/pub/club/opponent-club", "time_class": "daily", "start_time": ${Times.t0.getEpochSecond}, "result": "win"}], "in_progress": [], "registered": []}"""
      val opponentClubId = ClubId(702)
      val responses = Map(
        s"club/$clubSlug"         -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members" -> apiClubMembersJson(Nil),
        s"club/$clubSlug/matches" -> clubMatchesWithOpponent,
        "club/opponent-club"      -> apiClubJson(opponentClubId.value, "opponent-club"),
        "club/opponent-club/members" -> apiClubMembersJson(
          List(("opp-player", Times.t0.getEpochSecond))
        ),
        "player/opp-player" -> apiPlayerJson(211, "opp-player")
      )
      val criteria = makeCriteria()

      for {
        _      <- seedDb
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        logger <- ZIO.service[CcasLogger]
        result <- RecruitmentApp.recruit(clubSlug, "default", sourceClubs = Nil, explore = true, trigger = RunTrigger.Api)
          .provideEnvironment(zio.ZEnvironment(client, xa, logger))
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
      } yield assertTrue(
        candidates.size == 1,
        candidates.head.outcome == CandidateOutcome.Invited,
        candidates.head.playerId == PlayerId(211)
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
        xa     <- ZIO.service[Transactor]
        logger <- ZIO.service[CcasLogger]
        result <- RecruitmentApp.recruit(clubSlug, "default", target = Some(3), sourceClubs = List(source1, source2), trigger = RunTrigger.Api)
          .provideEnvironment(zio.ZEnvironment(client, xa, logger))
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
        xa      <- ZIO.service[Transactor]
        logger  <- ZIO.service[CcasLogger]
        fiber <- RecruitmentApp.recruit(clubSlug, "default", sourceClubs = List(intSource))
          .provideEnvironment(zio.ZEnvironment(client, xa, logger))
          .fork
        _      <- reached.await
        _      <- ZIO.sleep(200.millis)
        _      <- fiber.interrupt
        latest <- RecruitmentRun.selectLatest(clubId)
        runId = latest.get.runId
        cands    <- RecruitmentCandidate.selectByRun(runId)
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
        xa     <- ZIO.service[Transactor]
        logger <- ZIO.service[CcasLogger]
        result <- RecruitmentApp.recruit(clubSlug, "default", target = Some(2), sourceClubs = List(source), trigger = RunTrigger.Api)
          .provideEnvironment(zio.ZEnvironment(client, xa, logger))
        candidates <- RecruitmentCandidate.selectByRun(result.runId)
        invited  = candidates.filter(_.outcome == CandidateOutcome.Invited)
        deferred = candidates.filter(_.outcome == CandidateOutcome.Deferred)
      } yield assertTrue(
        invited.size == 2,
        result.candidatesFound == 2,
        // Some candidates may be deferred (those that passed filters but exceeded target)
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
        xa         <- ZIO.service[Transactor]

        // Seed prior run with a Deferred candidate (need Player + Snapshot)
        _ <- seedPlayer(PlayerId(500))
        _ <- PlayerSnapshot.insert(
          PlayerSnapshot(
            PlayerId(500),
            Times.t0,
            Username.wrap("prio-deferred"),
            ccas.api.misc.enums.PlayerStatusCategory.Active,
            None
          )
        )
        priorRunId <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        _ <- RecruitmentRun.update(
          RecruitmentRun(priorRunId, clubId, criteriaId, RunTrigger.Cli, Times.t0, Some(Times.t1), 0)
        )
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(priorRunId, PlayerId(500), Times.t0, CandidateOutcome.Deferred, None)
        )

        // Verify selectDeferredByClub finds it
        deferredBefore <- RecruitmentCandidate.selectDeferredByClub(clubId)

        // Run recruitment — deferred candidate should be picked up as priority
        logger <- ZIO.service[CcasLogger]
        result <- RecruitmentApp.recruit(clubSlug, "default", target = Some(10), sourceClubs = List(source), trigger = RunTrigger.Api)
          .provideEnvironment(zio.ZEnvironment(client, xa, logger))

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
        _          <- seedPlayer(PlayerId(600))
        _ <- PlayerSnapshot.insert(
          PlayerSnapshot(
            PlayerId(600),
            Times.t0,
            Username.wrap("resolved-player"),
            ccas.api.misc.enums.PlayerStatusCategory.Active,
            None
          )
        )

        // Run 1: candidate is Deferred
        runId1 <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        _ <- RecruitmentRun.update(
          RecruitmentRun(runId1, clubId, criteriaId, RunTrigger.Cli, Times.t0, Some(Times.t1), 0)
        )
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(runId1, PlayerId(600), Times.t0, CandidateOutcome.Deferred, None)
        )

        // Should find the deferred candidate
        deferredBefore <- RecruitmentCandidate.selectDeferredByClub(clubId)

        // Run 2: same candidate is Invited (later timestamp)
        runId2 <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t2)
        _ <- RecruitmentRun.update(
          RecruitmentRun(runId2, clubId, criteriaId, RunTrigger.Cli, Times.t2, Some(Times.t3), 1)
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

  // ==========================================================================
  // Suite: Report mode
  // ==========================================================================

  private def suiteReport = suite("report mode")(
    test("showReport displays invited candidates") {
      for {
        _          <- seedDb
        _          <- seedPlayer(pid0)
        _          <- seedPlayer(pid1)
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        _ <- RecruitmentRun.update(
          RecruitmentRun(runId, clubId, criteriaId, RunTrigger.Cli, Times.t0, Some(Times.t1), 2)
        )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid0, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid1, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentApp.showReport(clubSlug, Some(runId.toString))
      } yield assertTrue(true)
    },
    test("showReport with latest run") {
      for {
        _          <- seedDb
        _          <- seedPlayer(pid0)
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0)
        _ <- RecruitmentRun.update(
          RecruitmentRun(runId, clubId, criteriaId, RunTrigger.Cli, Times.t0, Some(Times.t1), 1)
        )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId, pid0, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentApp.showReport(clubSlug, None)
      } yield assertTrue(true)
    }
  )

  // ==========================================================================
  // Suite: Match ref writing
  // ==========================================================================

  private def suiteMatchRefWriting = suite("match ref writing")(
    test("player ref resolved via DB when club_match_board data exists") {
      val candidatePid = PlayerId(300)
      val responses = Map(
        s"club/$clubSlug"               -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members"       -> apiClubMembersJson(Nil),
        s"player/ref-db-player"         -> apiPlayerJson(candidatePid.value, "ref-db-player"),
        s"player/ref-db-player/matches" -> emptyPlayerMatchesJson
      )
      val criteria = makeCriteria()
      for {
        _ <- seedDb
        _ <- seedCriteria(criteria)
        // Seed player rows (FK targets for club_match_board)
        _ <- seedPlayer(candidatePid)
        _ <- seedPlayer(PlayerId(999))
        // Seed club_match and club_match_board for the candidate
        _ <- ClubMatch.upsert(
          ClubMatch(
            refMatchId,
            "Test Match",
            "https://chess.com/match/8001",
            ccas.api.misc.enums.ClubMatchStatus.Finished,
            ccas.api.misc.enums.TimeClass.Daily,
            Some(Times.t0),
            Some(Times.t1),
            1,
            Some(clubId),
            10.0,
            Some(ccas.api.misc.enums.ClubMatchResult.Win),
            None,
            5.0,
            Some(ccas.api.misc.enums.ClubMatchResult.Lose),
            Times.t0
          )
        )
        _ <- ClubMatchBoard.insertBatch(
          List(
            ClubMatchBoard(
              refMatchId,
              1,
              Some(candidatePid),
              false,
              Some(PlayerId(999)),
              false,
              None,
              None,
              None,
              None,
              2,
              0
            )
          )
        )
        client <- fakeChessComClient(responses)
        runId  <- RecruitmentRun.insert(clubId, 1L, RunTrigger.Cli, Instant.now())
        _      <- evalCandidates(client, runId, List(Username("ref-db-player")), criteria)
        ref    <- PlayerMatchRef.selectId(candidatePid)
      } yield assertTrue(
        ref.isDefined,
        ref.get.matchId == refMatchId,
        ref.get.isTeam1,
        ref.get.boardIdx == 1
      )
    },
    test("player ref resolved via API when DB has no match data") {
      val candidatePid = PlayerId(301)
      val matchId      = 8002L
      val responses = Map(
        s"club/$clubSlug"         -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members" -> apiClubMembersJson(Nil),
        s"player/ref-api-player"  -> apiPlayerJson(candidatePid.value, "ref-api-player"),
        s"player/ref-api-player/matches" -> apiPlayerMatchesJsonWithFinished(
          finishedMatches = List((matchId, 3))
        ),
        s"match/$matchId" -> apiDailyMatchJson(
          matchId,
          team1Club = "alpha-club",
          team2Club = "beta-club",
          team1Players = List(("ref-api-player", 3)),
          team2Players = List(("opponent-x", 3))
        )
      )
      val criteria = makeCriteria()
      for {
        _      <- seedDb
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        runId  <- RecruitmentRun.insert(clubId, 1L, RunTrigger.Cli, Instant.now())
        _      <- evalCandidates(client, runId, List(Username("ref-api-player")), criteria)
        ref    <- PlayerMatchRef.selectId(candidatePid)
      } yield assertTrue(
        ref.isDefined,
        ref.get.matchId == ClubMatchId(matchId),
        ref.get.isTeam1,
        ref.get.boardIdx == 3
      )
    },
    test("club ref resolved via API during recruitment init") {
      val matchId = 8001L
      val responses = Map(
        s"club/$clubSlug"         -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members" -> apiClubMembersJson(Nil),
        s"club/$clubSlug/matches" -> apiClubMatchesJson(finishedIds = List(matchId)),
        s"match/$matchId" -> apiDailyMatchJson(
          matchId,
          team1Club = clubSlug.value,
          team2Club = "opponent-club",
          team1Players = List(("player-a", 1)),
          team2Players = List(("player-b", 1))
        ),
        "club/source-club"         -> apiClubJson(sourceClubId.value, "source-club"),
        "club/source-club/members" -> apiClubMembersJson(Nil)
      )
      val criteria = makeCriteria()
      for {
        _      <- seedDb
        _      <- seedCriteria(criteria)
        client <- fakeChessComClient(responses)
        xa     <- ZIO.service[Transactor]
        logger <- ZIO.service[CcasLogger]
        _ <- RecruitmentApp.recruit(clubSlug, "default", sourceClubs = List(ClubSlug("source-club")), explore = false, trigger = RunTrigger.Api)
          .provideEnvironment(zio.ZEnvironment(client, xa, logger))
        ref <- ClubMatchRef.selectId(clubId)
      } yield assertTrue(
        ref.isDefined,
        ref.get.matchId == ClubMatchId(matchId),
        ref.get.isTeam1
      )
    },
    test("ref failure does not affect recruitment outcome") {
      val candidatePid = PlayerId(302)
      // Provide finished matches but NO match endpoint response → API fallback fails with 404
      val responses = Map(
        s"club/$clubSlug"         -> apiClubJson(clubId.value, clubSlug.value),
        s"club/$clubSlug/members" -> apiClubMembersJson(Nil),
        s"player/ref-fail-player" -> apiPlayerJson(candidatePid.value, "ref-fail-player"),
        s"player/ref-fail-player/matches" -> apiPlayerMatchesJsonWithFinished(
          finishedMatches = List((9999L, 1))
        )
        // match/9999 intentionally missing → 404
      )
      val criteria = makeCriteria()
      for {
        _       <- seedDb
        _       <- seedCriteria(criteria)
        client  <- fakeChessComClient(responses)
        runId   <- RecruitmentRun.insert(clubId, 1L, RunTrigger.Cli, Instant.now())
        invited <- evalCandidates(client, runId, List(Username("ref-fail-player")), criteria)
        ref     <- PlayerMatchRef.selectId(candidatePid)
      } yield assertTrue(
        invited.contains(Username("ref-fail-player")),
        ref.isEmpty
      )
    }
  )
}
