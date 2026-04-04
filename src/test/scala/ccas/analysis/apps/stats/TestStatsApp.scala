package ccas.analysis.apps.stats

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, ClubMatch, ClubMatchBoard, ClubMatchGame, Player, Tables}
import ccas.api.misc.enums.*
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer

object TestStatsApp extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestStatsApp")(
    testMemberStatsReturnsResult,
    testMemberStatsEmptyClub,
    testMemberStatsNotFound,
    testMemberStatsTeam2Perspective,
    testMemberStatsMatchesButNoBoards,
    testPlayerOfPeriodFilters,
    testPlayerOfPeriodNoMatchCount,
    testPlayerOfPeriodInvertedRange
  ).provideShared(
    FreshSchemaLayer("test_stats_app", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(30))
    val t2: Instant = t0.plus(Duration.ofDays(60))
    val t3: Instant = t0.plus(Duration.ofDays(90))
  }

  private val clubId   = ClubId(100)
  private val clubSlug = ClubSlug("test-club")
  private val oppId    = ClubId(200)
  private val pid1     = PlayerId.wrap(1L)
  private val matchId1 = ClubMatchId(1001L)
  private val matchId2 = ClubMatchId(1002L)
  private val matchId3 = ClubMatchId(1003L)
  private val pid2     = PlayerId.wrap(2L)

  private def testMemberStatsReturnsResult = test("memberStats returns StatsResult with correct counts") {
    for {
      _ <- Club.upsert(Club(clubId, Times.t0, clubSlug, "Test Club"))
      _ <- Club.upsert(Club(oppId, Times.t0, ClubSlug("opp"), "Opponent"))
      _ <- Player.insertIfNew(
        Player(pid1, Times.t0, Username.wrap("alice"), PlayerStatusCategory.Active, None, Times.t0)
      )
      _ <- ClubMatch.upsert(
        ClubMatch(matchId1, "Match 1", ClubMatchStatus.Finished, TimeClass.Daily,
          Some(Times.t0), Some(Times.t1), 10, Some(clubId), 10, Some(oppId), 10, Times.t1)
      )
      _ <- ClubMatchBoard.insert(
        ClubMatchBoard(matchId1, 1, Some(pid1), false, None, false, 1, 1)
      )
      _ <- ClubMatchGame.insertBatch(List(
        ClubMatchGame(matchId1, 1, true, None, None, None, Some(BoardGameWinner.Team1), None, None, None)
      ))
      result <- StatsApp.memberStats(clubSlug)
    } yield assertTrue(
      result.contributions.size == 1,
      result.contributions.head.username == Username.wrap("alice"),
      result.contributions.head.raw.wins == 1,
      result.boardCount == 1,
      result.matchCount == 1L
    )
  }

  private def testMemberStatsEmptyClub = test("memberStats returns empty result for club with no matches") {
    val emptySlug = ClubSlug("empty-club")
    for {
      _ <- Club.upsert(Club(ClubId(300), Times.t0, emptySlug, "Empty Club"))
      result <- StatsApp.memberStats(emptySlug)
    } yield assertTrue(
      result.contributions.isEmpty,
      result.boardCount == 0,
      result.matchCount == 0L
    )
  }

  private def testMemberStatsNotFound = test("memberStats fails with NotFoundException for unknown club") {
    for {
      exit <- StatsApp.memberStats(ClubSlug("nonexistent")).exit
    } yield assertTrue(exit.isFailure)
  }

  private def testMemberStatsTeam2Perspective = test("memberStats includes boards when club is team2 with winner flipping") {
    val team2Slug = ClubSlug("team2-club")
    val team2Id   = ClubId(400)
    for {
      _ <- Club.upsert(Club(team2Id, Times.t0, team2Slug, "Team2 Club"))
      _ <- Player.insertIfNew(
        Player(pid2, Times.t0, Username.wrap("dave"), PlayerStatusCategory.Active, None, Times.t0)
      )
      // Match where team2-club is team2; opponent (clubId=100) wins as Team1
      _ <- ClubMatch.upsert(
        ClubMatch(matchId3, "Match 3", ClubMatchStatus.Finished, TimeClass.Daily,
          Some(Times.t0), Some(Times.t1), 10, Some(clubId), 10, Some(team2Id), 10, Times.t1)
      )
      // dave is on team2 side
      _ <- ClubMatchBoard.insert(
        ClubMatchBoard(matchId3, 1, None, false, Some(pid2), false, 1, 1)
      )
      // game1: Team2 wins (from match perspective). After flip for team2-club, this becomes Team1 = win for dave.
      _ <- ClubMatchGame.insertBatch(List(
        ClubMatchGame(matchId3, 1, true, None, None, None, Some(BoardGameWinner.Team2), None, None, None)
      ))
      result <- StatsApp.memberStats(team2Slug)
    } yield assertTrue(
      result.contributions.size == 1,
      result.contributions.head.username == Username.wrap("dave"),
      result.contributions.head.raw.wins == 1,
      result.contributions.head.raw.losses == 0
    )
  }

  private def testMemberStatsMatchesButNoBoards = test("memberStats returns empty contributions when matches exist but no boards") {
    val noBoardSlug = ClubSlug("noboard-club")
    val noBoardId   = ClubId(500)
    for {
      _ <- Club.upsert(Club(noBoardId, Times.t0, noBoardSlug, "No Board Club"))
      _ <- ClubMatch.upsert(
        ClubMatch(ClubMatchId(2001L), "Match NB", ClubMatchStatus.Finished, TimeClass.Daily,
          Some(Times.t0), Some(Times.t1), 10, Some(noBoardId), 10, Some(oppId), 10, Times.t1)
      )
      result <- StatsApp.memberStats(noBoardSlug)
    } yield assertTrue(
      result.contributions.isEmpty,
      result.boardCount == 0,
      result.matchCount == 1L
    )
  }

  private def testPlayerOfPeriodFilters = test("playerOfPeriod respects time boundaries") {
    for {
      // matchId1 ends at t1 (within [t0, t2)), matchId2 ends at t3 (outside)
      _ <- ClubMatch.upsert(
        ClubMatch(matchId2, "Match 2", ClubMatchStatus.Finished, TimeClass.Daily,
          Some(Times.t2), Some(Times.t3), 10, Some(clubId), 10, Some(oppId), 10, Times.t3)
      )
      _ <- ClubMatchBoard.insert(
        ClubMatchBoard(matchId2, 1, Some(pid1), false, None, false, 1, 1)
      )
      _ <- ClubMatchGame.insertBatch(List(
        ClubMatchGame(matchId2, 1, true, None, None, None, Some(BoardGameWinner.Team1), None, None, None)
      ))
      result <- StatsApp.playerOfPeriod(clubSlug, Times.t0, Times.t2)
    } yield assertTrue(
      result.contributions.size == 1,
      result.boardCount == 1, // only matchId1 is in range
      result.contributions.head.raw.wins == 1
    )
  }

  private def testPlayerOfPeriodNoMatchCount = test("playerOfPeriod returns matchCount=0") {
    for {
      result <- StatsApp.playerOfPeriod(clubSlug, Times.t0, Times.t2)
    } yield assertTrue(result.matchCount == 0L)
  }

  private def testPlayerOfPeriodInvertedRange = test("playerOfPeriod returns empty for inverted date range") {
    for {
      result <- StatsApp.playerOfPeriod(clubSlug, Times.t2, Times.t0)
    } yield assertTrue(
      result.contributions.isEmpty,
      result.boardCount == 0
    )
  }
}
