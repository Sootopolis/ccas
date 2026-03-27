package ccas.analysis.tables

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertCompletes, assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.api.misc.enums.{BoardGameWinner, ClubMatchResult, ClubMatchStatus, GameResultDetail, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer

object TestClubMatchSql extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestClubMatchSql")(
    testCreateTables,
    testClubMatchUpsert,
    testClubMatchUpsertUpdate,
    testClubMatchSelectMatchIdsForClub,
    testClubMatchSelectStaleForClub,
    testClubMatchBoardInsertAndSelect,
    testClubMatchBoardNullableGameFields,
    testClubMatchBoardDeleteMatch,
    testHistoryPendingMatchInsert,
    testHistoryPendingMatchCount,
    testHistoryPendingMatchBatch,
    testHistoryPendingMatchSelectClubBatch,
    testHistoryPendingMatchDelete,
    testHistoryPendingMatchUpdateStatus,
    testHistoryPendingMatchCountNew,
    testHistoryPendingMatchSelectClubBatchFiltersStatus,
    testHistoryPendingMatchResetStatuses,
    testHistoryMemberQueryUpsert,
    testHistoryMemberQueryDeleteClub,
    testHistoryRunInsertAndComplete,
    testUnresolvedBoardPlayerInsertAndSelect,
    testUnresolvedBoardPlayerDoNothing,
    testUnresolvedBoardPlayerDelete,
    testUnresolvedMatchClubInsertAndSelect,
    testUnresolvedMatchClubDoNothing,
    testUnresolvedMatchClubDelete
  ).provideShared(
    FreshSchemaLayer("test_club_match_sql", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(2))
    val t3: Instant = t0.plus(Duration.ofDays(30))
    val t4: Instant = t0.plus(Duration.ofDays(120)) // unused — reserved for future stale-window tests
  }

  private val clubA = Club(ClubId(300), Times.t0, ClubSlug("club-a"), "Club A")
  private val clubB = Club(ClubId(301), Times.t0, ClubSlug("club-b"), "Club B")

  private val player0 = Player(PlayerId(50), Times.t0)
  private val player1 = Player(PlayerId(51), Times.t0)

  private val snap0 =
    PlayerSnapshot(player0.playerId, Times.t0, Username("p0"), ccas.api.misc.enums.PlayerStatusCategory.Active, None)
  private val snap1 =
    PlayerSnapshot(player1.playerId, Times.t0, Username("p1"), ccas.api.misc.enums.PlayerStatusCategory.Active, None)

  private val matchFinished = ClubMatch(
    matchId = ClubMatchId(1001),
    name = "Club A vs Club B",
    url = "https://www.chess.com/club/matches/1001",
    status = ClubMatchStatus.Finished,
    timeClass = TimeClass.Daily,
    startTime = Some(Times.t0),
    endTime = Some(Times.t1),
    boards = 5,
    team1ClubId = Some(clubA.clubId),
    team1Score = 6.0,
    team1Result = Some(ClubMatchResult.Win),
    team2ClubId = Some(clubB.clubId),
    team2Score = 4.0,
    team2Result = Some(ClubMatchResult.Lose),
    fetchedAt = Times.t2
  )

  private val matchInProgress = ClubMatch(
    matchId = ClubMatchId(1002),
    name = "Club A vs Unknown",
    url = "https://www.chess.com/club/matches/1002",
    status = ClubMatchStatus.InProgress,
    timeClass = TimeClass.Daily,
    startTime = Some(Times.t1),
    endTime = None,
    boards = 3,
    team1ClubId = Some(clubA.clubId),
    team1Score = 2.0,
    team1Result = None,
    team2ClubId = None,
    team2Score = 1.0,
    team2Result = None,
    fetchedAt = Times.t2
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
    val updated = matchFinished.copy(team1Score = 7.0, fetchedAt = Times.t3)
    for {
      _      <- ClubMatch.upsert(updated)
      result <- ClubMatch.selectId(matchFinished.matchId)
    } yield assertTrue(
      result.contains(updated),
      result.get.team1Score == 7.0,
      result.get.fetchedAt == Times.t3
    )
  }

  private def testClubMatchSelectMatchIdsForClub = test("selectMatchIdsForClub") {
    for {
      _       <- ClubMatch.upsert(matchFinished.copy(fetchedAt = Times.t2)) // restore original
      _       <- ClubMatch.upsert(matchInProgress)
      idsA    <- ClubMatch.selectMatchIdsForClub(clubA.clubId)
      idsB    <- ClubMatch.selectMatchIdsForClub(clubB.clubId)
      idsNone <- ClubMatch.selectMatchIdsForClub(ClubId(999))
    } yield assertTrue(
      idsA == Set(ClubMatchId(1001), ClubMatchId(1002)),
      idsB == Set(ClubMatchId(1001)),
      idsNone.isEmpty
    )
  }

  private def testClubMatchSelectStaleForClub = test("selectStaleForClub returns non-finished and stale finished") {
    // matchFinished: end_time=Times.t1, fetched_at=Times.t2. Stale if fetched_at < end_time + 90 days.
    // Times.t2 < Times.t1 + 90 days → stale.
    // matchInProgress: not finished → always stale.
    for {
      staleA <- ClubMatch.selectStaleForClub(clubA.clubId)
    } yield assertTrue(
      staleA.toSet == Set(ClubMatchId(1001), ClubMatchId(1002))
    )
  }

  // --- ClubMatchBoard tests ---

  private val boardA = ClubMatchBoard(
    matchId = matchFinished.matchId,
    board = 1,
    team1PlayerId = Some(player0.playerId),
    team1FairPlay = false,
    team2PlayerId = Some(player1.playerId),
    team2FairPlay = false,
    game1Winner = Some(BoardGameWinner.Team1),
    game1Detail = Some(GameResultDetail.Checkmated),
    game2Winner = Some(BoardGameWinner.Team2),
    game2Detail = Some(GameResultDetail.Resigned),
    team1ScoreX2 = 2,
    team2ScoreX2 = 2
  )

  private val boardB = ClubMatchBoard(
    matchId = matchFinished.matchId,
    board = 2,
    team1PlayerId = Some(player1.playerId),
    team1FairPlay = false,
    team2PlayerId = None,
    team2FairPlay = false,
    game1Winner = Some(BoardGameWinner.Draw),
    game1Detail = Some(GameResultDetail.Stalemate),
    game2Winner = None,
    game2Detail = None,
    team1ScoreX2 = 1,
    team2ScoreX2 = 1
  )

  private def testClubMatchBoardInsertAndSelect = test("ClubMatchBoard insertBatch and selectMatch") {
    for {
      _       <- ClubMatchBoard.insertBatch(List(boardA, boardB))
      results <- ClubMatchBoard.selectMatch(matchFinished.matchId)
    } yield assertTrue(results.toSet == Set(boardA, boardB))
  }

  private def testClubMatchBoardNullableGameFields = test("ClubMatchBoard with null game fields round-trips") {
    val noGames = ClubMatchBoard(
      matchId = matchFinished.matchId,
      board = 3,
      team1PlayerId = None,
      team1FairPlay = true,
      team2PlayerId = None,
      team2FairPlay = false,
      game1Winner = None,
      game1Detail = None,
      game2Winner = None,
      game2Detail = None,
      team1ScoreX2 = 0,
      team2ScoreX2 = 0
    )
    for {
      _       <- ClubMatchBoard.insert(noGames)
      results <- ClubMatchBoard.selectMatch(matchFinished.matchId)
      board3 = results.find(_.board == 3).get
    } yield assertTrue(
      board3 == noGames,
      board3.game1Winner.isEmpty,
      board3.game1Detail.isEmpty,
      board3.team1PlayerId.isEmpty,
      board3.team1FairPlay
    )
  }

  private def testClubMatchBoardDeleteMatch = test("ClubMatchBoard deleteMatch") {
    for {
      deleted <- ClubMatchBoard.deleteMatch(matchFinished.matchId)
      results <- ClubMatchBoard.selectMatch(matchFinished.matchId)
    } yield assertTrue(
      deleted == 3,
      results.isEmpty
    )
  }

  // --- HistoryPendingMatch tests ---

  private def testHistoryPendingMatchInsert = test("HistoryPendingMatch insert with ON CONFLICT DO NOTHING") {
    for {
      r1  <- HistoryPendingMatch.insert(HistoryPendingMatch(clubA.clubId, ClubMatchId(2001), isLive = false))
      r2  <- HistoryPendingMatch.insert(HistoryPendingMatch(clubA.clubId, ClubMatchId(2001), isLive = false)) // duplicate
      ids <- HistoryPendingMatch.selectClub(clubA.clubId)
    } yield assertTrue(
      r1 == 1,
      r2 == 0,
      ids == List(HistoryPendingMatch(clubA.clubId, ClubMatchId(2001), isLive = false, PendingMatchStatus.New))
    )
  }

  private def testHistoryPendingMatchCount = test("HistoryPendingMatch count") {
    for {
      count <- HistoryPendingMatch.count(clubA.clubId)
    } yield assertTrue(count == 1L)
  }

  private def testHistoryPendingMatchBatch = test("HistoryPendingMatch insertBatch") {
    for {
      _ <- HistoryPendingMatch.insertBatch(
        List(
          HistoryPendingMatch(clubA.clubId, ClubMatchId(2002), isLive = false),
          HistoryPendingMatch(clubA.clubId, ClubMatchId(2003), isLive = false),
          HistoryPendingMatch(clubA.clubId, ClubMatchId(2001), isLive = false) // duplicate, should be ignored
        )
      )
      count <- HistoryPendingMatch.count(clubA.clubId)
      ids   <- HistoryPendingMatch.selectClub(clubA.clubId)
    } yield assertTrue(
      count == 3L,
      ids.map(_.matchId).toSet == Set(ClubMatchId(2001), ClubMatchId(2002), ClubMatchId(2003))
    )
  }

  private def testHistoryPendingMatchSelectClubBatch = test("HistoryPendingMatch selectClubBatch respects limit") {
    for {
      batch <- HistoryPendingMatch.selectClubBatch(clubA.clubId, 2)
    } yield assertTrue(batch.size == 2)
  }

  private def testHistoryPendingMatchDelete = test("HistoryPendingMatch delete") {
    for {
      deleted   <- HistoryPendingMatch.delete(clubA.clubId, ClubMatchId(2001), isLive = false)
      remaining <- HistoryPendingMatch.count(clubA.clubId)
    } yield assertTrue(
      deleted == 1,
      remaining == 2L
    )
  }

  // After the delete test, 2 entries remain: matchId 2002 and 2003, both New.

  private def testHistoryPendingMatchUpdateStatus = test("HistoryPendingMatch updateStatus") {
    for {
      updated <- HistoryPendingMatch.updateStatus(clubA.clubId, ClubMatchId(2002), isLive = false, PendingMatchStatus.ApiError)
      entries <- HistoryPendingMatch.selectClub(clubA.clubId)
      statuses = entries.map(e => (e.matchId, e.status)).toMap
    } yield assertTrue(
      updated == 1,
      statuses(ClubMatchId(2002)) == PendingMatchStatus.ApiError,
      statuses(ClubMatchId(2003)) == PendingMatchStatus.New
    )
  }

  private def testHistoryPendingMatchCountNew = test("HistoryPendingMatch countNew excludes non-New") {
    for {
      total  <- HistoryPendingMatch.count(clubA.clubId)
      newOnly <- HistoryPendingMatch.countNew(clubA.clubId)
    } yield assertTrue(
      total == 2L,
      newOnly == 1L // only 2003 is New; 2002 is ApiError
    )
  }

  private def testHistoryPendingMatchSelectClubBatchFiltersStatus =
    test("HistoryPendingMatch selectClubBatch only returns New") {
      for {
        batch <- HistoryPendingMatch.selectClubBatch(clubA.clubId, 10)
      } yield assertTrue(
        batch.size == 1,
        batch.head.matchId == ClubMatchId(2003)
      )
    }

  private def testHistoryPendingMatchResetStatuses = test("HistoryPendingMatch resetStatuses resets all to New") {
    for {
      _ <- HistoryPendingMatch.updateStatus(clubA.clubId, ClubMatchId(2003), isLive = false, PendingMatchStatus.Unidentified)
      beforeNew <- HistoryPendingMatch.countNew(clubA.clubId)
      reset     <- HistoryPendingMatch.resetStatuses(clubA.clubId)
      afterNew  <- HistoryPendingMatch.countNew(clubA.clubId)
    } yield assertTrue(
      beforeNew == 0L, // both 2002=ApiError, 2003=Unidentified
      reset == 2,
      afterNew == 2L
    )
  }

  // --- HistoryMemberQuery tests ---

  private def testHistoryMemberQueryUpsert = test("HistoryMemberQuery upsert") {
    for {
      _   <- HistoryMemberQuery.upsert(HistoryMemberQuery(clubA.clubId, player0.playerId, Times.t1))
      _   <- HistoryMemberQuery.upsert(HistoryMemberQuery(clubA.clubId, player1.playerId, Times.t1))
      ids <- HistoryMemberQuery.selectClubPlayerIds(clubA.clubId)
      // Upsert same player with new timestamp
      _        <- HistoryMemberQuery.upsert(HistoryMemberQuery(clubA.clubId, player0.playerId, Times.t2))
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
      runId <- HistoryRun.insert(clubA.clubId, RunTrigger.Cli, Times.t0)
      _     <- HistoryRun.complete(runId, Times.t1, matchesProcessed = 42, playersDiscovered = 7)
    } yield assertTrue(runId > 0L)
  }

  // --- UnresolvedBoardPlayer tests ---

  private def testUnresolvedBoardPlayerInsertAndSelect = test("UnresolvedBoardPlayer insert and selectAll") {
    for {
      r1  <- UnresolvedBoardPlayer.insert(ClubMatchId(1001), 1, isTeam1 = false, Username("opp1"))
      r2  <- UnresolvedBoardPlayer.insert(ClubMatchId(1001), 2, isTeam1 = true, Username("opp2"))
      all <- UnresolvedBoardPlayer.selectAll
    } yield assertTrue(
      r1 == 1,
      r2 == 1,
      all.size == 2,
      all.exists(_._4 == Username("opp1")),
      all.exists(_._4 == Username("opp2"))
    )
  }

  private def testUnresolvedBoardPlayerDoNothing = test("UnresolvedBoardPlayer ON CONFLICT DO NOTHING") {
    for {
      r <- UnresolvedBoardPlayer.insert(ClubMatchId(1001), 1, isTeam1 = false, Username("opp1-updated"))
    } yield assertTrue(r == 0)
  }

  private def testUnresolvedBoardPlayerDelete = test("UnresolvedBoardPlayer delete") {
    for {
      deleted   <- UnresolvedBoardPlayer.delete(ClubMatchId(1001), 1, isTeam1 = false)
      remaining <- UnresolvedBoardPlayer.selectAll
    } yield assertTrue(
      deleted == 1,
      remaining.size == 1,
      remaining.head._4 == Username("opp2")
    )
  }

  // --- UnresolvedMatchClub tests ---

  private def testUnresolvedMatchClubInsertAndSelect = test("UnresolvedMatchClub insert and selectAll") {
    for {
      r1  <- UnresolvedMatchClub.insert(ClubMatchId(1001), isTeam1 = false, ClubSlug("unknown-club"))
      all <- UnresolvedMatchClub.selectAll
    } yield assertTrue(
      r1 == 1,
      all.size == 1,
      all.head._3 == ClubSlug("unknown-club")
    )
  }

  private def testUnresolvedMatchClubDoNothing = test("UnresolvedMatchClub ON CONFLICT DO NOTHING") {
    for {
      r <- UnresolvedMatchClub.insert(ClubMatchId(1001), isTeam1 = false, ClubSlug("different-slug"))
    } yield assertTrue(r == 0)
  }

  private def testUnresolvedMatchClubDelete = test("UnresolvedMatchClub delete") {
    for {
      deleted   <- UnresolvedMatchClub.delete(ClubMatchId(1001), isTeam1 = false)
      remaining <- UnresolvedMatchClub.selectAll
    } yield assertTrue(
      deleted == 1,
      remaining.isEmpty
    )
  }
}
