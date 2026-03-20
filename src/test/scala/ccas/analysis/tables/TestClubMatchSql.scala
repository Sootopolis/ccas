package ccas.analysis.tables

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertCompletes, assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.api.misc.enums.{ClubMatchResult, ClubMatchStatus, GameResultDetail, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubUrlName, PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer

object TestClubMatchSql extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestClubMatchSql")(
    testCreateTables,
    testClubMatchUpsert,
    testClubMatchUpsertUpdate,
    testClubMatchSelectMatchIdsForClub,
    testClubMatchSelectStaleForClub,
    testClubMatchPlayerInsertAndSelect,
    testClubMatchPlayerNullableEnumRoundTrip,
    testClubMatchPlayerDeleteMatch,
    testHistoryPendingMatchInsert,
    testHistoryPendingMatchCount,
    testHistoryPendingMatchBatch,
    testHistoryPendingMatchSelectClubBatch,
    testHistoryPendingMatchDelete,
    testHistoryMemberQueryInsert,
    testHistoryMemberQueryDeleteClub,
    testHistoryRunInsertAndComplete
  ).provideShared(
    FreshSchemaLayer("test_club_match_sql", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object T {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(2))
    val t3: Instant = t0.plus(Duration.ofDays(30))
    val t4: Instant = t0.plus(Duration.ofDays(120)) // unused — reserved for future stale-window tests
  }

  private val clubA = Club(ClubId(300), T.t0, ClubUrlName("club-a"))
  private val clubB = Club(ClubId(301), T.t0, ClubUrlName("club-b"))

  private val player0 = Player(PlayerId(50), T.t0)
  private val player1 = Player(PlayerId(51), T.t0)

  private val snap0 = PlayerSnapshot(player0.playerId, T.t0, Username("p0"), ccas.api.misc.enums.PlayerStatusCategory.Active, None)
  private val snap1 = PlayerSnapshot(player1.playerId, T.t0, Username("p1"), ccas.api.misc.enums.PlayerStatusCategory.Active, None)

  private val matchFinished = ClubMatch(
    matchId = ClubMatchId(1001),
    name = "Club A vs Club B",
    url = "https://www.chess.com/club/matches/1001",
    status = ClubMatchStatus.Finished,
    timeClass = TimeClass.Daily,
    startTime = Some(T.t0),
    endTime = Some(T.t1),
    boards = 5,
    team1ClubId = Some(clubA.clubId),
    team1Name = "Club A",
    team1Score = 6.0,
    team1Result = Some(ClubMatchResult.Win),
    team2ClubId = Some(clubB.clubId),
    team2Name = "Club B",
    team2Score = 4.0,
    team2Result = Some(ClubMatchResult.Lose),
    fetchedAt = T.t2
  )

  private val matchInProgress = ClubMatch(
    matchId = ClubMatchId(1002),
    name = "Club A vs Unknown",
    url = "https://www.chess.com/club/matches/1002",
    status = ClubMatchStatus.InProgress,
    timeClass = TimeClass.Daily,
    startTime = Some(T.t1),
    endTime = None,
    boards = 3,
    team1ClubId = Some(clubA.clubId),
    team1Name = "Club A",
    team1Score = 2.0,
    team1Result = None,
    team2ClubId = None,
    team2Name = "Unknown Club",
    team2Score = 1.0,
    team2Result = None,
    fetchedAt = T.t2
  )

  // --- ClubMatch tests ---

  private def testCreateTables = test("createTables") {
    for {
      _ <- Club.upsertBatch(List(clubA, clubB))
      _ <- Player.insertBatch(List(player0, player1))
      _ <- PlayerSnapshot.insertBatch(List(snap0, snap1))
    } yield assertCompletes
  }

  private def testClubMatchUpsert = test("ClubMatch upsert insert") {
    for {
      _      <- ClubMatch.upsert(matchFinished)
      result <- ClubMatch.selectId(matchFinished.matchId)
    } yield assertTrue(result.contains(matchFinished))
  }

  private def testClubMatchUpsertUpdate = test("ClubMatch upsert update") {
    val updated = matchFinished.copy(team1Score = 7.0, fetchedAt = T.t3)
    for {
      _      <- ClubMatch.upsert(updated)
      result <- ClubMatch.selectId(matchFinished.matchId)
    } yield assertTrue(
      result.contains(updated),
      result.get.team1Score == 7.0,
      result.get.fetchedAt == T.t3
    )
  }

  private def testClubMatchSelectMatchIdsForClub = test("selectMatchIdsForClub") {
    for {
      _     <- ClubMatch.upsert(matchFinished.copy(fetchedAt = T.t2)) // restore original
      _     <- ClubMatch.upsert(matchInProgress)
      idsA  <- ClubMatch.selectMatchIdsForClub(clubA.clubId)
      idsB  <- ClubMatch.selectMatchIdsForClub(clubB.clubId)
      idsNone <- ClubMatch.selectMatchIdsForClub(ClubId(999))
    } yield assertTrue(
      idsA == Set(ClubMatchId(1001), ClubMatchId(1002)),
      idsB == Set(ClubMatchId(1001)),
      idsNone.isEmpty
    )
  }

  private def testClubMatchSelectStaleForClub = test("selectStaleForClub returns non-finished and stale finished") {
    // matchFinished: end_time=T.t1, fetched_at=T.t2. Stale if fetched_at < end_time + 90 days.
    // T.t2 < T.t1 + 90 days → stale.
    // matchInProgress: not finished → always stale.
    for {
      staleA <- ClubMatch.selectStaleForClub(clubA.clubId)
    } yield assertTrue(
      staleA.toSet == Set(ClubMatchId(1001), ClubMatchId(1002))
    )
  }

  // --- ClubMatchPlayer tests ---

  private val cmpA = ClubMatchPlayer(
    matchId = matchFinished.matchId,
    playerId = player0.playerId,
    teamIdx = 1,
    username = Username("p0"),
    board = Some(1),
    playedAsWhite = Some(GameResultDetail.Win),
    playedAsBlack = Some(GameResultDetail.Checkmated),
    scoreX2 = 2, // 1.0 + 0.0 = 1.0 → 2
    fairPlayRemoval = false
  )

  private val cmpB = ClubMatchPlayer(
    matchId = matchFinished.matchId,
    playerId = player1.playerId,
    teamIdx = 2,
    username = Username("p1"),
    board = Some(1),
    playedAsWhite = Some(GameResultDetail.Resigned),
    playedAsBlack = Some(GameResultDetail.Win),
    scoreX2 = 2,
    fairPlayRemoval = false
  )

  private def testClubMatchPlayerInsertAndSelect = test("ClubMatchPlayer insertBatch and selectMatch") {
    for {
      _       <- ClubMatchPlayer.insertBatch(List(cmpA, cmpB))
      results <- ClubMatchPlayer.selectMatch(matchFinished.matchId)
    } yield assertTrue(results.toSet == Set(cmpA, cmpB))
  }

  private def testClubMatchPlayerNullableEnumRoundTrip = test("ClubMatchPlayer with null played_as fields round-trips") {
    // Simulates a registration-state player: no board, no results
    val regPlayer = ClubMatchPlayer(
      matchId = matchFinished.matchId,
      playerId = player0.playerId,
      teamIdx = 1,
      username = Username("p0"),
      board = None,
      playedAsWhite = None,
      playedAsBlack = None,
      scoreX2 = 0,
      fairPlayRemoval = true
    )
    for {
      _       <- ClubMatchPlayer.deleteMatch(matchFinished.matchId)
      _       <- ClubMatchPlayer.insert(regPlayer)
      results <- ClubMatchPlayer.selectMatch(matchFinished.matchId)
    } yield assertTrue(
      results == List(regPlayer),
      results.head.playedAsWhite.isEmpty,
      results.head.playedAsBlack.isEmpty,
      results.head.board.isEmpty,
      results.head.fairPlayRemoval
    )
  }

  private def testClubMatchPlayerDeleteMatch = test("ClubMatchPlayer deleteMatch") {
    for {
      // Re-insert both players for a clean delete test
      _       <- ClubMatchPlayer.deleteMatch(matchFinished.matchId)
      _       <- ClubMatchPlayer.insertBatch(List(cmpA, cmpB))
      deleted <- ClubMatchPlayer.deleteMatch(matchFinished.matchId)
      results <- ClubMatchPlayer.selectMatch(matchFinished.matchId)
    } yield assertTrue(
      deleted == 2,
      results.isEmpty
    )
  }

  // --- HistoryPendingMatch tests ---

  private def testHistoryPendingMatchInsert = test("HistoryPendingMatch insert with ON CONFLICT DO NOTHING") {
    for {
      r1 <- HistoryPendingMatch.insert(HistoryPendingMatch(clubA.clubId, ClubMatchId(2001)))
      r2 <- HistoryPendingMatch.insert(HistoryPendingMatch(clubA.clubId, ClubMatchId(2001))) // duplicate
      ids <- HistoryPendingMatch.selectClub(clubA.clubId)
    } yield assertTrue(
      r1 == 1,
      r2 == 0,
      ids == List(ClubMatchId(2001))
    )
  }

  private def testHistoryPendingMatchCount = test("HistoryPendingMatch count") {
    for {
      count <- HistoryPendingMatch.count(clubA.clubId)
    } yield assertTrue(count == 1L)
  }

  private def testHistoryPendingMatchBatch = test("HistoryPendingMatch insertBatch") {
    for {
      _ <- HistoryPendingMatch.insertBatch(List(
        HistoryPendingMatch(clubA.clubId, ClubMatchId(2002)),
        HistoryPendingMatch(clubA.clubId, ClubMatchId(2003)),
        HistoryPendingMatch(clubA.clubId, ClubMatchId(2001)) // duplicate, should be ignored
      ))
      count <- HistoryPendingMatch.count(clubA.clubId)
      ids   <- HistoryPendingMatch.selectClub(clubA.clubId)
    } yield assertTrue(
      count == 3L,
      ids.toSet == Set(ClubMatchId(2001), ClubMatchId(2002), ClubMatchId(2003))
    )
  }

  private def testHistoryPendingMatchSelectClubBatch = test("HistoryPendingMatch selectClubBatch respects limit") {
    for {
      batch <- HistoryPendingMatch.selectClubBatch(clubA.clubId, 2)
    } yield assertTrue(batch.size == 2)
  }

  private def testHistoryPendingMatchDelete = test("HistoryPendingMatch delete") {
    for {
      deleted  <- HistoryPendingMatch.delete(clubA.clubId, ClubMatchId(2001))
      remaining <- HistoryPendingMatch.count(clubA.clubId)
    } yield assertTrue(
      deleted == 1,
      remaining == 2L
    )
  }

  // --- HistoryMemberQuery tests ---

  private def testHistoryMemberQueryInsert = test("HistoryMemberQuery insert and upsert") {
    for {
      _ <- HistoryMemberQuery.insert(HistoryMemberQuery(clubA.clubId, player0.playerId, T.t1))
      _ <- HistoryMemberQuery.insert(HistoryMemberQuery(clubA.clubId, player1.playerId, T.t1))
      ids <- HistoryMemberQuery.selectClubPlayerIds(clubA.clubId)
      // Upsert same player with new timestamp
      _ <- HistoryMemberQuery.insert(HistoryMemberQuery(clubA.clubId, player0.playerId, T.t2))
      idsAfter <- HistoryMemberQuery.selectClubPlayerIds(clubA.clubId)
    } yield assertTrue(
      ids == Set(player0.playerId, player1.playerId),
      idsAfter == Set(player0.playerId, player1.playerId) // same set, timestamp updated
    )
  }

  private def testHistoryMemberQueryDeleteClub = test("HistoryMemberQuery deleteClub") {
    for {
      deleted <- HistoryMemberQuery.deleteClub(clubA.clubId)
      ids     <- HistoryMemberQuery.selectClubPlayerIds(clubA.clubId)
    } yield assertTrue(
      deleted == 2,
      ids.isEmpty
    )
  }

  // --- HistoryRun tests ---

  private def testHistoryRunInsertAndComplete = test("HistoryRun insert and complete") {
    for {
      runId <- HistoryRun.insert(clubA.clubId, T.t0)
      _     <- HistoryRun.complete(runId, T.t1, matchesProcessed = 42, playersDiscovered = 7)
    } yield assertTrue(runId > 0L)
  }
}
