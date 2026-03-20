package ccas.analysis.apps.history

import java.time.Instant

import zio.http.URL
import zio.json.readJsonLinesAs
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.clubmatch.ApiDailyMatch
import ccas.api.misc.enums.{ClubMatchResult, ClubMatchStatus, GameResultDetail, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubUrlName}
import ccas.api.player.ApiPlayerMatches.ApiPlayerMatch

object TestHistoryApp extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestHistoryApp")(
    testClubMatchIdFromUrl,
    testFindOurTeamIdxTeam1,
    testFindOurTeamIdxTeam2,
    testFindOurTeamIdxNotFound,
    testBuildClubMatchRowFinished,
    testBuildClubMatchRowTeam2IsOurs,
    testBuildClubMatchRowInProgress,
    testBuildClubMatchRowRegistered,
    testIsClubDailyMatchAcceptsOwnClub,
    testIsClubDailyMatchRejectsOtherClub,
    testIsClubDailyMatchRejectsLiveMatch,
    testIsClubDailyMatchCaseInsensitive,
    testScoreX2Computation
  )

  private def url(s: String): URL = URL.decode(s).toOption.get

  // --- ClubMatchId.fromUrl ---

  private def testClubMatchIdFromUrl = test("ClubMatchId.fromUrl extracts match ID from API URL") {
    val apiUrl = url("https://api.chess.com/pub/match/1650919")
    val id = ClubMatchId.fromUrl(apiUrl)
    assertTrue(ClubMatchId.unwrap(id) == 1650919L)
  }

  // --- findOurTeamIdx ---

  private val matchFixture =
    readJsonLinesAs[ApiDailyMatch]("data/test/api/matchFinished.json").runHead.someOrFailException

  private def testFindOurTeamIdxTeam1 = test("findOurTeamIdx returns 1 when club is team1") {
    matchFixture.map { m =>
      assertTrue(HistoryApp.findOurTeamIdx(m, ClubUrlName("turk-chess-players")) == Some(1))
    }
  }

  private def testFindOurTeamIdxTeam2 = test("findOurTeamIdx returns 2 when club is team2") {
    matchFixture.map { m =>
      assertTrue(HistoryApp.findOurTeamIdx(m, ClubUrlName("the-great-british-empire")) == Some(2))
    }
  }

  private def testFindOurTeamIdxNotFound = test("findOurTeamIdx returns None for unknown club") {
    matchFixture.map { m =>
      assertTrue(HistoryApp.findOurTeamIdx(m, ClubUrlName("not-a-club")) == None)
    }
  }

  // --- buildClubMatchRow ---

  private def testBuildClubMatchRowFinished = test("buildClubMatchRow correctly maps a finished match") {
    matchFixture.map { m =>
      val matchId = ClubMatchId.fromUrl(m.`@id`)
      val clubId = ClubId(100)
      val oppId = Some(ClubId(200))
      val row = HistoryApp.buildClubMatchRow(matchId, m, clubId, ourTeamIdx = 1, opponentClubId = oppId)

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
      val clubId = ClubId(200)
      val oppId = Some(ClubId(100))
      val row = HistoryApp.buildClubMatchRow(matchId, m, clubId, ourTeamIdx = 2, opponentClubId = oppId)

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

  private def testBuildClubMatchRowInProgress = test("buildClubMatchRow maps in-progress match with no endTime/results") {
    inProgressFixture.map { m =>
      val matchId = ClubMatchId.fromUrl(m.`@id`)
      val row = HistoryApp.buildClubMatchRow(matchId, m, ClubId(100), ourTeamIdx = 1, opponentClubId = None)

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
      val row = HistoryApp.buildClubMatchRow(matchId, m, ClubId(100), ourTeamIdx = 1, opponentClubId = None)

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
    assertTrue(HistoryApp.isClubDailyMatch(m, ClubUrlName("devon-chess")))
  }

  private def testIsClubDailyMatchRejectsOtherClub = test("isClubDailyMatch rejects match for different club") {
    val m = makePlayerMatch(
      "https://api.chess.com/pub/club/other-club",
      "https://api.chess.com/pub/match/1597947"
    )
    assertTrue(!HistoryApp.isClubDailyMatch(m, ClubUrlName("devon-chess")))
  }

  private def testIsClubDailyMatchRejectsLiveMatch = test("isClubDailyMatch rejects live match") {
    val m = makePlayerMatch(
      "https://api.chess.com/pub/club/devon-chess",
      "https://api.chess.com/pub/match/live/1597947"
    )
    assertTrue(!HistoryApp.isClubDailyMatch(m, ClubUrlName("devon-chess")))
  }

  private def testIsClubDailyMatchCaseInsensitive = test("isClubDailyMatch is case-insensitive on club name") {
    val m = makePlayerMatch(
      "https://api.chess.com/pub/club/Devon-Chess",
      "https://api.chess.com/pub/match/1597947"
    )
    assertTrue(HistoryApp.isClubDailyMatch(m, ClubUrlName("devon-chess")))
  }

  // --- Score computation ---

  private def testScoreX2Computation = test("scoreX2 computation: 2*(white+black)") {
    def compute(w: Option[GameResultDetail], b: Option[GameResultDetail]): Short =
      ((w.fold(0.0)(_.score) + b.fold(0.0)(_.score)) * 2).toShort

    assertTrue(
      compute(Some(GameResultDetail.Win), Some(GameResultDetail.Win)) == 4.toShort,       // 2 * (1.0 + 1.0)
      compute(Some(GameResultDetail.Win), Some(GameResultDetail.Checkmated)) == 2.toShort, // 2 * (1.0 + 0.0)
      compute(Some(GameResultDetail.Stalemate), Some(GameResultDetail.Agreed)) == 2.toShort, // 2 * (0.5 + 0.5)
      compute(Some(GameResultDetail.Win), None) == 2.toShort,                              // 2 * (1.0 + 0.0)
      compute(None, None) == 0.toShort,                                                    // no games played
      compute(Some(GameResultDetail.Resigned), Some(GameResultDetail.Timeout)) == 0.toShort // both lost
    )
  }
}
