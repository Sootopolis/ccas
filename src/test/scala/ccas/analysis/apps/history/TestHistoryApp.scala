package ccas.analysis.apps.history

import java.time.Instant

import zio.http.URL
import zio.json.readJsonLinesAs
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.clubmatch.ApiDailyMatch
import ccas.api.misc.enums.{BoardGameWinner, ClubMatchResult, ClubMatchStatus, GameResultDetail, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug}
import ccas.api.player.ApiPlayerMatches.ApiPlayerMatch

object TestHistoryApp extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestHistoryApp")(
    testClubMatchIdFromUrl,
    testFindOurTeamTeam1,
    testFindOurTeamTeam2,
    testFindOurTeamNotFound,
    testBuildClubMatchRowFinished,
    testBuildClubMatchRowTeam2IsOurs,
    testBuildClubMatchRowInProgress,
    testBuildClubMatchRowRegistered,
    testIsClubDailyMatchAcceptsOwnClub,
    testIsClubDailyMatchRejectsOtherClub,
    testIsClubDailyMatchRejectsLiveMatch,
    testIsClubDailyMatchCaseInsensitive,
    suiteNormalizeGameOutcome,
    suiteComputeScoreX2
  )

  private def url(s: String): URL = URL.decode(s).toOption.get

  // --- ClubMatchId.fromUrl ---

  private def testClubMatchIdFromUrl = test("ClubMatchId.fromUrl extracts match ID from API URL") {
    val apiUrl = url("https://api.chess.com/pub/match/1650919")
    val id     = ClubMatchId.fromUrl(apiUrl)
    assertTrue(ClubMatchId.unwrap(id) == 1650919L)
  }

  // --- findOurTeamIdx ---

  private val matchFixture =
    readJsonLinesAs[ApiDailyMatch]("data/test/api/matchFinished.json").runHead.someOrFailException

  private def testFindOurTeamTeam1 = test("findOurTeam returns true when club is team1") {
    matchFixture.map { m =>
      assertTrue(HistoryApp.findOurTeam(m.teams, ClubSlug("turk-chess-players")) == Some(true))
    }
  }

  private def testFindOurTeamTeam2 = test("findOurTeam returns false when club is team2") {
    matchFixture.map { m =>
      assertTrue(HistoryApp.findOurTeam(m.teams, ClubSlug("the-great-british-empire")) == Some(false))
    }
  }

  private def testFindOurTeamNotFound = test("findOurTeam returns None for unknown club") {
    matchFixture.map { m =>
      assertTrue(HistoryApp.findOurTeam(m.teams, ClubSlug("not-a-club")) == None)
    }
  }

  // --- buildClubMatchRow ---

  private def testBuildClubMatchRowFinished = test("buildClubMatchRow correctly maps a finished match") {
    matchFixture.map { m =>
      val matchId = ClubMatchId.fromUrl(m.`@id`)
      val clubId  = ClubId(100)
      val oppId   = Some(ClubId(200))
      val row     = HistoryApp.buildClubMatchRow(matchId, m, clubId, weAreTeam1 = true, opponentClubId = oppId)

      assertTrue(
        row.matchId == ClubMatchId(1650919),
        row.name == "TURK CHESS PLAYERS vs The Great British Empire. U1300 14.07.2024",
        row.status == ClubMatchStatus.Finished,
        row.timeClass == TimeClass.Daily,
        row.startTime.contains(Instant.ofEpochSecond(1720908242L)),
        row.endTime.contains(Instant.ofEpochSecond(1735309563L)),
        row.boards == 13,
        row.team1ClubId.contains(clubId),
        row.team1Name == "TURK CHESS PLAYERS",
        row.team1Score == 10.0,
        row.team1Result.contains(ClubMatchResult.Lose),
        row.team2ClubId == oppId,
        row.team2Name == "The Great British Empire.",
        row.team2Score == 16.0,
        row.team2Result.contains(ClubMatchResult.Win)
      )
    }
  }

  private def testBuildClubMatchRowTeam2IsOurs = test("buildClubMatchRow swaps club IDs when we are team2") {
    matchFixture.map { m =>
      val matchId = ClubMatchId.fromUrl(m.`@id`)
      val clubId  = ClubId(200)
      val oppId   = Some(ClubId(100))
      val row     = HistoryApp.buildClubMatchRow(matchId, m, clubId, weAreTeam1 = false, opponentClubId = oppId)

      assertTrue(
        row.team1ClubId == oppId,
        row.team2ClubId.contains(clubId)
      )
    }
  }

  private val inProgressFixture =
    readJsonLinesAs[ApiDailyMatch]("data/test/api/matchInProgress.json").runHead.someOrFailException

  private val registeredFixture =
    readJsonLinesAs[ApiDailyMatch]("data/test/api/matchRegistered.json").runHead.someOrFailException

  private def testBuildClubMatchRowInProgress =
    test("buildClubMatchRow maps in-progress match with no endTime/results") {
      inProgressFixture.map { m =>
        val matchId = ClubMatchId.fromUrl(m.`@id`)
        val row     = HistoryApp.buildClubMatchRow(matchId, m, ClubId(100), weAreTeam1 = true, opponentClubId = None)

        assertTrue(
          row.status == ClubMatchStatus.InProgress,
          row.startTime.isDefined,
          row.endTime.isEmpty,
          row.team1Result.isEmpty,
          row.team2Result.isEmpty
        )
      }
    }

  private def testBuildClubMatchRowRegistered = test("buildClubMatchRow maps registered match with optional startTime") {
    registeredFixture.map { m =>
      val matchId = ClubMatchId.fromUrl(m.`@id`)
      val row     = HistoryApp.buildClubMatchRow(matchId, m, ClubId(100), weAreTeam1 = true, opponentClubId = None)

      assertTrue(
        row.status == ClubMatchStatus.Registration,
        row.endTime.isEmpty,
        row.team1Result.isEmpty,
        row.team2Result.isEmpty
      )
    }
  }

  // --- isClubDailyMatch ---

  private def makePlayerMatch(clubUrl: String, idUrl: String): ApiPlayerMatch =
    ApiPlayerMatch(
      name = "test",
      url = url("https://www.chess.com/club/matches/123"),
      `@id` = url(idUrl),
      club = url(clubUrl),
      results = None,
      board = None
    )

  private def testIsClubDailyMatchAcceptsOwnClub = test("isClubDailyMatch accepts match for target club") {
    val m = makePlayerMatch(
      "https://api.chess.com/pub/club/devon-chess",
      "https://api.chess.com/pub/match/1597947"
    )
    assertTrue(HistoryApp.isClubDailyMatch(m, ClubSlug("devon-chess")))
  }

  private def testIsClubDailyMatchRejectsOtherClub = test("isClubDailyMatch rejects match for different club") {
    val m = makePlayerMatch(
      "https://api.chess.com/pub/club/other-club",
      "https://api.chess.com/pub/match/1597947"
    )
    assertTrue(!HistoryApp.isClubDailyMatch(m, ClubSlug("devon-chess")))
  }

  private def testIsClubDailyMatchRejectsLiveMatch = test("isClubDailyMatch rejects live match") {
    val m = makePlayerMatch(
      "https://api.chess.com/pub/club/devon-chess",
      "https://api.chess.com/pub/match/live/1597947"
    )
    assertTrue(!HistoryApp.isClubDailyMatch(m, ClubSlug("devon-chess")))
  }

  private def testIsClubDailyMatchCaseInsensitive = test("isClubDailyMatch is case-insensitive on club name") {
    val m = makePlayerMatch(
      "https://api.chess.com/pub/club/Devon-Chess",
      "https://api.chess.com/pub/match/1597947"
    )
    assertTrue(HistoryApp.isClubDailyMatch(m, ClubSlug("devon-chess")))
  }

  // --- normalizeGameOutcome ---

  private def suiteNormalizeGameOutcome = suite("normalizeGameOutcome")(
    test("white wins → winner is white's team, detail is loss reason") {
      val (winner, detail) = HistoryApp.normalizeGameOutcome(
        Some(GameResultDetail.Win),
        Some(GameResultDetail.Checkmated),
        whiteTeamIsTeam1 = true
      )
      assertTrue(
        winner.contains(BoardGameWinner.Team1),
        detail.contains(GameResultDetail.Checkmated)
      )
    },
    test("black wins → winner is black's team, detail is loss reason") {
      val (winner, detail) = HistoryApp.normalizeGameOutcome(
        Some(GameResultDetail.Resigned),
        Some(GameResultDetail.Win),
        whiteTeamIsTeam1 = true
      )
      assertTrue(
        winner.contains(BoardGameWinner.Team2),
        detail.contains(GameResultDetail.Resigned)
      )
    },
    test("draw → winner is Draw, detail is draw reason") {
      val (winner, detail) = HistoryApp.normalizeGameOutcome(
        Some(GameResultDetail.Stalemate),
        Some(GameResultDetail.Stalemate),
        whiteTeamIsTeam1 = true
      )
      assertTrue(
        winner.contains(BoardGameWinner.Draw),
        detail.contains(GameResultDetail.Stalemate)
      )
    },
    test("not played → both None") {
      val (winner, detail) = HistoryApp.normalizeGameOutcome(None, None, whiteTeamIsTeam1 = true)
      assertTrue(winner.isEmpty, detail.isEmpty)
    },
    test("whiteTeamIsTeam1=false flips winner mapping") {
      val (winner, detail) = HistoryApp.normalizeGameOutcome(
        Some(GameResultDetail.Win),
        Some(GameResultDetail.Timeout),
        whiteTeamIsTeam1 = false
      )
      assertTrue(
        winner.contains(BoardGameWinner.Team2),
        detail.contains(GameResultDetail.Timeout)
      )
    },
    test("whiteTeamIsTeam1=false black wins → Team1") {
      val (winner, detail) = HistoryApp.normalizeGameOutcome(
        Some(GameResultDetail.Resigned),
        Some(GameResultDetail.Win),
        whiteTeamIsTeam1 = false
      )
      assertTrue(
        winner.contains(BoardGameWinner.Team1),
        detail.contains(GameResultDetail.Resigned)
      )
    }
  )

  // --- computeScoreX2 ---

  private def suiteComputeScoreX2 = suite("computeScoreX2")(
    test("normal scoring: team1 wins both games") {
      val (t1, t2) = HistoryApp.computeScoreX2(
        Some(BoardGameWinner.Team1),
        Some(BoardGameWinner.Team1),
        team1FairPlay = false,
        team2FairPlay = false
      )
      assertTrue(t1 == 4.toShort, t2 == 0.toShort)
    },
    test("normal scoring: split results") {
      val (t1, t2) = HistoryApp.computeScoreX2(
        Some(BoardGameWinner.Team1),
        Some(BoardGameWinner.Team2),
        team1FairPlay = false,
        team2FairPlay = false
      )
      assertTrue(t1 == 2.toShort, t2 == 2.toShort)
    },
    test("normal scoring: both draws") {
      val (t1, t2) = HistoryApp.computeScoreX2(
        Some(BoardGameWinner.Draw),
        Some(BoardGameWinner.Draw),
        team1FairPlay = false,
        team2FairPlay = false
      )
      assertTrue(t1 == 2.toShort, t2 == 2.toShort)
    },
    test("team1 banned: team2 gets 2 per game") {
      val (t1, t2) = HistoryApp.computeScoreX2(
        Some(BoardGameWinner.Team2),
        Some(BoardGameWinner.Team2),
        team1FairPlay = true,
        team2FairPlay = false
      )
      assertTrue(t1 == 0.toShort, t2 == 4.toShort)
    },
    test("team2 banned: team1 gets 2 per game") {
      val (t1, t2) = HistoryApp.computeScoreX2(
        Some(BoardGameWinner.Team2),
        Some(BoardGameWinner.Team2),
        team1FairPlay = false,
        team2FairPlay = true
      )
      assertTrue(t1 == 4.toShort, t2 == 0.toShort)
    },
    test("both players banned: each gets 1 per game") {
      val (t1, t2) = HistoryApp.computeScoreX2(
        Some(BoardGameWinner.Team1),
        Some(BoardGameWinner.Team1),
        team1FairPlay = true,
        team2FairPlay = true
      )
      assertTrue(t1 == 2.toShort, t2 == 2.toShort)
    },
    test("game not played: both get 0") {
      val (t1, t2) = HistoryApp.computeScoreX2(
        None,
        None,
        team1FairPlay = false,
        team2FairPlay = false
      )
      assertTrue(t1 == 0.toShort, t2 == 0.toShort)
    },
    test("one game played, one not") {
      val (t1, t2) = HistoryApp.computeScoreX2(
        Some(BoardGameWinner.Team1),
        None,
        team1FairPlay = false,
        team2FairPlay = false
      )
      assertTrue(t1 == 2.toShort, t2 == 0.toShort)
    }
  )
}
