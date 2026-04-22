package ccas.analysis.apps.history

import java.time.Instant

import zio.Chunk
import zio.http.URL
import zio.json.readJsonLinesAs
import zio.test.{assertTrue, Spec, ZIOSpecDefault}

import ccas.api.clubmatch.ApiDailyMatch
import ccas.api.misc.enums.{BoardGameWinner, ClubMatchStatus, GameResultDetail, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug}
import ccas.api.player.ApiPlayerMatches.ApiPlayerMatch

object TestHistoryApp extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestHistoryApp")(
    testClubMatchIdFromUrl,
    testBuildClubMatchRowFinished,
    testBuildClubMatchRowWithNoneClubId,
    testBuildClubMatchRowInProgress,
    testBuildClubMatchRowRegistered,
    testIsClubDailyMatchAcceptsOwnClub,
    testIsClubDailyMatchRejectsOtherClub,
    testIsClubDailyMatchRejectsLiveMatch,
    testIsClubDailyMatchCaseInsensitive,
    suiteNormalizeGameOutcome,
    suiteComputeScoreX2,
    suiteParseRefreshArg
  )

  private def url(s: String): URL = URL.decode(s).toOption.get

  // --- ClubMatchId.fromUrl ---

  private def testClubMatchIdFromUrl = test("ClubMatchId.fromUrl extracts match ID from API URL") {
    val apiUrl = url("https://api.chess.com/pub/match/1650919")
    val id     = ClubMatchId.fromUrl(apiUrl)
    assertTrue(ClubMatchId.unwrap(id) == 1650919L)
  }

  // --- buildClubMatchRow ---

  private val matchFixture =
    readJsonLinesAs[ApiDailyMatch]("data/test/api/matchFinished.json").runHead.someOrFailException

  private def testBuildClubMatchRowFinished = test("buildClubMatchRow correctly maps a finished match") {
    matchFixture.map { m =>
      val matchId     = ClubMatchId.fromUrl(m.`@id`)
      val team1ClubId = Some(ClubId(100))
      val team2ClubId = Some(ClubId(200))
      val row         = HistoryBoardBuilder.buildClubMatchRow(matchId, m, team1ClubId, team2ClubId)

      assertTrue(
        row.matchId == ClubMatchId(1650919),
        row.name == "TURK CHESS PLAYERS vs The Great British Empire. U1300 14.07.2024",
        row.status == ClubMatchStatus.Finished,
        row.timeClass == TimeClass.Daily,
        row.startTime.contains(Instant.ofEpochSecond(1720908242L)),
        row.endTime.contains(Instant.ofEpochSecond(1735309563L)),
        row.boards == 13,
        row.team1ClubId == team1ClubId,
        row.team1ScoreX2 == 20,
        row.team2ClubId == team2ClubId,
        row.team2ScoreX2 == 32
      )
    }
  }

  private def testBuildClubMatchRowWithNoneClubId =
    test("buildClubMatchRow passes through None club IDs") {
      matchFixture.map { m =>
        val matchId = ClubMatchId.fromUrl(m.`@id`)
        val row     = HistoryBoardBuilder.buildClubMatchRow(matchId, m, None, Some(ClubId(200)))

        assertTrue(
          row.team1ClubId.isEmpty,
          row.team2ClubId.contains(ClubId(200))
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
        val row     = HistoryBoardBuilder.buildClubMatchRow(matchId, m, Some(ClubId(100)), None)

        assertTrue(
          row.status == ClubMatchStatus.InProgress,
          row.startTime.isDefined,
          row.endTime.isEmpty
        )
      }
    }

  private def testBuildClubMatchRowRegistered = test("buildClubMatchRow maps registered match with optional startTime") {
    registeredFixture.map { m =>
      val matchId = ClubMatchId.fromUrl(m.`@id`)
      val row     = HistoryBoardBuilder.buildClubMatchRow(matchId, m, Some(ClubId(100)), None)

      assertTrue(
        row.status == ClubMatchStatus.Registration,
        row.endTime.isEmpty
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
    assertTrue(HistorySeeding.isClubDailyMatch(m, ClubSlug("devon-chess")))
  }

  private def testIsClubDailyMatchRejectsOtherClub = test("isClubDailyMatch rejects match for different club") {
    val m = makePlayerMatch(
      "https://api.chess.com/pub/club/other-club",
      "https://api.chess.com/pub/match/1597947"
    )
    assertTrue(!HistorySeeding.isClubDailyMatch(m, ClubSlug("devon-chess")))
  }

  private def testIsClubDailyMatchRejectsLiveMatch = test("isClubDailyMatch rejects live match") {
    val m = makePlayerMatch(
      "https://api.chess.com/pub/club/devon-chess",
      "https://api.chess.com/pub/match/live/1597947"
    )
    assertTrue(!HistorySeeding.isClubDailyMatch(m, ClubSlug("devon-chess")))
  }

  private def testIsClubDailyMatchCaseInsensitive = test("isClubDailyMatch is case-insensitive on club name") {
    val m = makePlayerMatch(
      "https://api.chess.com/pub/club/Devon-Chess",
      "https://api.chess.com/pub/match/1597947"
    )
    assertTrue(HistorySeeding.isClubDailyMatch(m, ClubSlug("devon-chess")))
  }

  // ==========================================================================
  // Suite: normalizeGameOutcome
  // ==========================================================================

  private def suiteNormalizeGameOutcome = suite("normalizeGameOutcome")(
    testWhiteWinsWinnerIsWhitesTeam,
    testBlackWinsWinnerIsBlacksTeam,
    testDrawWinnerIsDraw,
    testNotPlayedBothNone,
    testFalseFlipsWinnerMapping,
    testFalseBlackWinsTeam1
  )

  private def testWhiteWinsWinnerIsWhitesTeam = test("white wins → winner is white's team, detail is loss reason") {
    val (winner, detail) = HistoryBoardBuilder.normalizeGameOutcome(
      Some(GameResultDetail.Win),
      Some(GameResultDetail.Checkmated),
      whiteTeamIsTeam1 = true
    )
    assertTrue(
      winner.contains(BoardGameWinner.Team1),
      detail.contains(GameResultDetail.Checkmated)
    )
  }

  private def testBlackWinsWinnerIsBlacksTeam = test("black wins → winner is black's team, detail is loss reason") {
    val (winner, detail) = HistoryBoardBuilder.normalizeGameOutcome(
      Some(GameResultDetail.Resigned),
      Some(GameResultDetail.Win),
      whiteTeamIsTeam1 = true
    )
    assertTrue(
      winner.contains(BoardGameWinner.Team2),
      detail.contains(GameResultDetail.Resigned)
    )
  }

  private def testDrawWinnerIsDraw = test("draw → winner is Draw, detail is draw reason") {
    val (winner, detail) = HistoryBoardBuilder.normalizeGameOutcome(
      Some(GameResultDetail.Stalemate),
      Some(GameResultDetail.Stalemate),
      whiteTeamIsTeam1 = true
    )
    assertTrue(
      winner.contains(BoardGameWinner.Draw),
      detail.contains(GameResultDetail.Stalemate)
    )
  }

  private def testNotPlayedBothNone = test("not played → both None") {
    val (winner, detail) = HistoryBoardBuilder.normalizeGameOutcome(None, None, whiteTeamIsTeam1 = true)
    assertTrue(winner.isEmpty, detail.isEmpty)
  }

  private def testFalseFlipsWinnerMapping = test("whiteTeamIsTeam1=false flips winner mapping") {
    val (winner, detail) = HistoryBoardBuilder.normalizeGameOutcome(
      Some(GameResultDetail.Win),
      Some(GameResultDetail.Timeout),
      whiteTeamIsTeam1 = false
    )
    assertTrue(
      winner.contains(BoardGameWinner.Team2),
      detail.contains(GameResultDetail.Timeout)
    )
  }

  private def testFalseBlackWinsTeam1 = test("whiteTeamIsTeam1=false black wins → Team1") {
    val (winner, detail) = HistoryBoardBuilder.normalizeGameOutcome(
      Some(GameResultDetail.Resigned),
      Some(GameResultDetail.Win),
      whiteTeamIsTeam1 = false
    )
    assertTrue(
      winner.contains(BoardGameWinner.Team1),
      detail.contains(GameResultDetail.Resigned)
    )
  }

  // ==========================================================================
  // Suite: computeScoreX2
  // ==========================================================================

  private def suiteComputeScoreX2 = suite("computeScoreX2")(
    testTeam1WinsBothGames,
    testSplitResults,
    testBothDraws,
    testTeam1BannedTeam2Gets2PerGame,
    testTeam2BannedTeam1Gets2PerGame,
    testBothBannedEachGets1PerGame,
    testGameNotPlayedBothGet0,
    testOneGamePlayedOneNot
  )

  private def testTeam1WinsBothGames = test("normal scoring: team1 wins both games") {
    val (t1, t2) = HistoryBoardBuilder.computeScoreX2(
      Some(BoardGameWinner.Team1),
      Some(BoardGameWinner.Team1),
      team1FairPlay = false,
      team2FairPlay = false
    )
    assertTrue(t1 == 4.toShort, t2 == 0.toShort)
  }

  private def testSplitResults = test("normal scoring: split results") {
    val (t1, t2) = HistoryBoardBuilder.computeScoreX2(
      Some(BoardGameWinner.Team1),
      Some(BoardGameWinner.Team2),
      team1FairPlay = false,
      team2FairPlay = false
    )
    assertTrue(t1 == 2.toShort, t2 == 2.toShort)
  }

  private def testBothDraws = test("normal scoring: both draws") {
    val (t1, t2) = HistoryBoardBuilder.computeScoreX2(
      Some(BoardGameWinner.Draw),
      Some(BoardGameWinner.Draw),
      team1FairPlay = false,
      team2FairPlay = false
    )
    assertTrue(t1 == 2.toShort, t2 == 2.toShort)
  }

  private def testTeam1BannedTeam2Gets2PerGame = test("team1 banned: team2 gets 2 per game") {
    val (t1, t2) = HistoryBoardBuilder.computeScoreX2(
      Some(BoardGameWinner.Team2),
      Some(BoardGameWinner.Team2),
      team1FairPlay = true,
      team2FairPlay = false
    )
    assertTrue(t1 == 0.toShort, t2 == 4.toShort)
  }

  private def testTeam2BannedTeam1Gets2PerGame = test("team2 banned: team1 gets 2 per game") {
    val (t1, t2) = HistoryBoardBuilder.computeScoreX2(
      Some(BoardGameWinner.Team2),
      Some(BoardGameWinner.Team2),
      team1FairPlay = false,
      team2FairPlay = true
    )
    assertTrue(t1 == 4.toShort, t2 == 0.toShort)
  }

  private def testBothBannedEachGets1PerGame = test("both players banned: each gets 1 per game") {
    val (t1, t2) = HistoryBoardBuilder.computeScoreX2(
      Some(BoardGameWinner.Team1),
      Some(BoardGameWinner.Team1),
      team1FairPlay = true,
      team2FairPlay = true
    )
    assertTrue(t1 == 2.toShort, t2 == 2.toShort)
  }

  private def testGameNotPlayedBothGet0 = test("game not played: both get 0") {
    val (t1, t2) = HistoryBoardBuilder.computeScoreX2(
      None,
      None,
      team1FairPlay = false,
      team2FairPlay = false
    )
    assertTrue(t1 == 0.toShort, t2 == 0.toShort)
  }

  private def testOneGamePlayedOneNot = test("one game played, one not") {
    val (t1, t2) = HistoryBoardBuilder.computeScoreX2(
      Some(BoardGameWinner.Team1),
      None,
      team1FairPlay = false,
      team2FairPlay = false
    )
    assertTrue(t1 == 2.toShort, t2 == 0.toShort)
  }

  // --- parseRefreshArg ---

  private def suiteParseRefreshArg = suite("parseRefreshArg")(
    test("no --refresh flag returns None and unchanged args") {
      val args                 = Chunk("club-a", "--full")
      val (refresh, remaining) = HistoryApp.parseRefreshArg(args)
      assertTrue(refresh.isEmpty, remaining == args)
    },
    test("--refresh without hours returns Some(0) and strips flag") {
      val (refresh, remaining) = HistoryApp.parseRefreshArg(Chunk("club-a", "--refresh", "--full"))
      assertTrue(refresh.contains(0), remaining == Chunk("club-a", "--full"))
    },
    test("--refresh with hours returns Some(hours) and strips both") {
      val (refresh, remaining) = HistoryApp.parseRefreshArg(Chunk("club-a", "--refresh", "24"))
      assertTrue(refresh.contains(24), remaining == Chunk("club-a"))
    },
    test("--refresh at end of args returns Some(0)") {
      val (refresh, remaining) = HistoryApp.parseRefreshArg(Chunk("club-a", "--refresh"))
      assertTrue(refresh.contains(0), remaining == Chunk("club-a"))
    },
    test("--refresh followed by non-integer keeps next arg as positional") {
      val (refresh, remaining) = HistoryApp.parseRefreshArg(Chunk("--refresh", "club-a"))
      assertTrue(refresh.contains(0), remaining == Chunk("club-a"))
    }
  )
}
