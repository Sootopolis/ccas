package ccas.analysis.apps.recruitment

import java.time.{Duration, Instant}

import com.augustnagro.magnum.sql
import zio.{ZIO, ZLayer}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.*
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, Elo, PlayerId, Username}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.connectZIO
import ccas.utils.ProgressDisplay

object TestRecruitmentAppCore extends ZIOSpecDefault {

  private val refMatchId = ClubMatchId(8001)

  override def spec: Spec[Any, Throwable] = suite("TestRecruitmentAppCore")(
    suiteDbCrud,
    suiteFullWorkflow,
    suiteReport,
    suiteMatchRefWriting
  ).provideShared(
    FreshSchemaLayer("test_recruitment_app_core", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false))
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
        cid2    <- RecruitmentCriteria.insert(criteria.copy(dailyMinElo = Some(Elo(1500))))
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
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None)
        loaded     <- RecruitmentRun.selectId(runId)
      } yield assertTrue(
        runId.value > 0L,
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
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None)
        _ <- RecruitmentRun.update(
          RecruitmentRun(runId, clubId, criteriaId, RunTrigger.Cli, Times.t0, Some(Times.t1), 5, None)
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
        _          <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None)
        runId2     <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t1, None)
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
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None)
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
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None)
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
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None)
        _ <- ZIO.foreachDiscard(outcomes.zip(enumPids)) { (outcome, pid) =>
          RecruitmentCandidate
            .insert(
              RecruitmentCandidate(runId, pid, Times.t0, outcome, Some(s"reason-$pid"))
            )
        }
        candidates <- RecruitmentCandidate.selectByRun(runId)
        loadedOutcomes = candidates.map(_.outcome).toSet
      } yield assertTrue(
        candidates.size == outcomes.size,
        loadedOutcomes == outcomes.toSet
      )
    },
    test("RecruitmentCandidate selectLatestInvitedByClub returns latest for matching club") {
      for {
        _          <- seedDb
        _          <- seedPlayer(pid0)
        criteriaId <- seedCriteria(makeCriteria())
        runId1     <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None)
        runId2     <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t1, None)
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId1, pid0, Times.t0, CandidateOutcome.Invited, None)
          )
        _ <- RecruitmentCandidate
          .insert(
            RecruitmentCandidate(runId2, pid0, Times.t1, CandidateOutcome.Invited, None)
          )
        latest <- RecruitmentCandidate.selectLatestInvitedByClub(pid0, clubId)
      } yield assertTrue(
        latest.isDefined,
        latest.get.runId == runId2
      )
    },
    test("RecruitmentCandidate selectLatestInvitedByClub ignores other clubs") {
      val otherClub = Club(sourceClubId, Times.t0, ClubSlug("source-club"), "Source Club", None, None, None)
      for {
        _          <- seedDb
        _          <- Club.upsert(otherClub)
        _          <- seedPlayer(pid0)
        criteriaId <- seedCriteria(makeCriteria())
        otherRunId <- RecruitmentRun.insert(sourceClubId, criteriaId, RunTrigger.Cli, Times.t0, None)
        _ <- RecruitmentCandidate.insert(
          RecruitmentCandidate(otherRunId, pid0, Times.t0, CandidateOutcome.Invited, None)
        )
        forOtherClub <- RecruitmentCandidate.selectLatestInvitedByClub(pid0, sourceClubId)
        forOurClub   <- RecruitmentCandidate.selectLatestInvitedByClub(pid0, clubId)
      } yield assertTrue(
        forOtherClub.isDefined,
        forOurClub.isEmpty
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
    },
    test("ApiFetchFailure deduplicates response bodies") {
      val now  = Instant.now()
      val body = """{"code":3024,"message":"An internal error has occurred."}"""
      val failure1 = ApiFetchFailure(now, "https://api.chess.com/pub/match/1", "HttpStatusException", Some("404"), Some(body))
      val failure2 = ApiFetchFailure(now, "https://api.chess.com/pub/match/2", "HttpStatusException", Some("404"), Some(body))
      for {
        _       <- seedDb
        _       <- ApiFetchFailure.insert(failure1)
        _       <- ApiFetchFailure.insert(failure2)
        recent  <- ApiFetchFailure.selectRecent(now.minus(Duration.ofMinutes(1)))
        bodyRow <- connectZIO(sql"SELECT count(*) FROM api_response_body".query[Long].run().head)
      } yield assertTrue(
        recent.size == 2,
        recent.forall(_.responseBody.contains(body)),
        bodyRow == 1L
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
        result <- runRecruit(client, sourceClubs = List(ClubSlug("source-club")))
        run    <- RecruitmentRun.selectId(result.runId)
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
  // Suite: Report mode
  // ==========================================================================

  private def suiteReport = suite("report mode")(
    test("showReport displays invited candidates") {
      for {
        _          <- seedDb
        _          <- seedPlayer(pid0)
        _          <- seedPlayer(pid1)
        criteriaId <- seedCriteria(makeCriteria())
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None)
        _ <- RecruitmentRun.update(
          RecruitmentRun(runId, clubId, criteriaId, RunTrigger.Cli, Times.t0, Some(Times.t1), 2, None)
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
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Times.t0, None)
        _ <- RecruitmentRun.update(
          RecruitmentRun(runId, clubId, criteriaId, RunTrigger.Cli, Times.t0, Some(Times.t1), 1, None)
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
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        _ <- seedPlayer(candidatePid)
        _ <- seedPlayer(PlayerId(999))
        _ <- seedMatchWithBoard(refMatchId, Some(clubId), candidatePid, PlayerId(999))
        client <- fakeChessComClient(responses)
        runId  <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Instant.now(), None)
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
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        client     <- fakeChessComClient(responses)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Instant.now(), None)
        _          <- evalCandidates(client, runId, List(Username("ref-api-player")), criteria)
        ref        <- PlayerMatchRef.selectId(candidatePid)
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
        _      <- runRecruit(client, sourceClubs = List(ClubSlug("source-club")))
        ref    <- ClubMatchRef.selectId(clubId)
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
        _          <- seedDb
        criteriaId <- seedCriteria(criteria)
        client     <- fakeChessComClient(responses)
        runId      <- RecruitmentRun.insert(clubId, criteriaId, RunTrigger.Cli, Instant.now(), None)
        invited    <- evalCandidates(client, runId, List(Username("ref-fail-player")), criteria)
        ref        <- PlayerMatchRef.selectId(candidatePid)
      } yield assertTrue(
        invited.contains(Username("ref-fail-player")),
        ref.isEmpty
      )
    }
  )
}
