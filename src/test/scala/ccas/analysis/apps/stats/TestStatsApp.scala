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
    testPlayerOfPeriodFilters,
    testPlayerOfPeriodNoMatchCount
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
}
