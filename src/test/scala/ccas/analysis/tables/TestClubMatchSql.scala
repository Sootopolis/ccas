package ccas.analysis.tables

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertCompletes, assertTrue, Spec, TestAspect, ZIOSpecDefault}

import com.augustnagro.magnum.sql

import ccas.api.misc.enums.{ClubMatchStatus, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug, PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.connectZIO

object TestClubMatchSql extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestClubMatchSql")(
    testCreateTables,
    testClubMatchUpsert,
    testClubMatchUpsertUpdate,
    testClubMatchSelectMatchIdsForClub,
    testClubMatchSelectLatestActivity,
    testClubMatchSelectStaleForClub,
    testClubMatchSelectSettledForClub,
    testClubMatchSelectSettledForRefresh,
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
    testUnresolvedMatchClubDelete,
    testClubMatchUpdateTeamClubId,
    testClubMatchBoardUpdatePlayerId,
    testPlayerMatchRefUpsert
  ).provideShared(
    FreshSchemaLayer("test_club_match_sql", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(2))
    val t3: Instant = t0.plus(Duration.ofDays(30))
    val t4: Instant = t0.plus(Duration.ofDays(120))
  }

  private val clubA = Club(ClubId(300), Times.t0, ClubSlug("club-a"), "Club A", None, None)
  private val clubB = Club(ClubId(301), Times.t0, ClubSlug("club-b"), "Club B", None, None)

  private val player0 =
    Player(PlayerId(50), Times.t0, Username("p0"), ccas.api.misc.enums.PlayerStatusCategory.Active, None, Times.t0)
  private val player1 =
    Player(PlayerId(51), Times.t0, Username("p1"), ccas.api.misc.enums.PlayerStatusCategory.Active, None, Times.t0)

  private val matchFinished = ClubMatch(
    matchId = ClubMatchId(1001),
    name = "Club A vs Club B",
    status = ClubMatchStatus.Finished,
    timeClass = TimeClass.Daily,
    startTime = Some(Times.t0),
    endTime = Some(Times.t1),
    boards = 5,
    team1ClubId = Some(clubA.clubId),
    team1ScoreX2 = 12,
    team2ClubId = Some(clubB.clubId),
    team2ScoreX2 = 8,
    fetchedAt = Times.t2
  )

  private val matchInProgress = ClubMatch(
    matchId = ClubMatchId(1002),
    name = "Club A vs Unknown",
    status = ClubMatchStatus.InProgress,
    timeClass = TimeClass.Daily,
    startTime = Some(Times.t1),
    endTime = None,
    boards = 3,
    team1ClubId = Some(clubA.clubId),
    team1ScoreX2 = 4,
    team2ClubId = None,
    team2ScoreX2 = 2,
    fetchedAt = Times.t2
  )

  // --- ClubMatch tests ---

  private def testCreateTables = test("createTables") {
    for {
      _ <- Club.upsertBatch(List(clubA, clubB))
      _ <- Player.insertBatch(List(player0, player1))
    } yield assertCompletes
  }

  private def testClubMatchUpsert = test("ClubMatch upsert insert") {
    for {
      _      <- ClubMatch.upsert(matchFinished)
      result <- ClubMatch.selectId(matchFinished.matchId)
    } yield assertTrue(result.contains(matchFinished))
  }

  private def testClubMatchUpsertUpdate = test("ClubMatch upsert update") {
    val updated = matchFinished.copy(team1ScoreX2 = 14, fetchedAt = Times.t3)
    for {
      _      <- ClubMatch.upsert(updated)
      result <- ClubMatch.selectId(matchFinished.matchId)
    } yield assertTrue(
      result.contains(updated),
      result.get.team1ScoreX2 == 14,
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

  private def testClubMatchSelectLatestActivity =
    test("selectLatestActivityForClub returns max start_time across all matches; Registration counts as now") {
      // Club A has matchFinished (Times.t0) and matchInProgress (Times.t1). Latest = t1.
      // Club B has only matchFinished (t0). Latest = t0.
      // Add a Registration match for clubB to verify it's treated as "now".
      val matchRegistered = matchFinished.copy(
        matchId = ClubMatchId(1003),
        status = ClubMatchStatus.Registration,
        startTime = None,
        endTime = None,
        team1ClubId = Some(clubB.clubId),
        team2ClubId = None
      )
      val before = Instant.now()
      for {
        // Without a registration match: max start_time
        latestANoReg <- ClubMatch.selectLatestActivityForClub(clubA.clubId)
        latestBNoReg <- ClubMatch.selectLatestActivityForClub(clubB.clubId)
        // With a registration match for clubB: should jump to ~now
        _            <- ClubMatch.upsert(matchRegistered)
        latestBReg   <- ClubMatch.selectLatestActivityForClub(clubB.clubId)
        // Unknown club returns None
        latestNone   <- ClubMatch.selectLatestActivityForClub(ClubId(999))
        // Cleanup
        _ <- connectZIO(sql"DELETE FROM club_match WHERE match_id = 1003".update.run())
      } yield assertTrue(
        latestANoReg.contains(Times.t1),
        latestBNoReg.contains(Times.t0),
        latestBReg.exists(_.isAfter(before.minusSeconds(1))),
        latestNone.isEmpty
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

  private def testClubMatchSelectSettledForClub =
    test("selectSettledMatchIdsForClub returns only finished matches fetched past stale window") {
      // Upsert matchFinished with fetchedAt=t4 (day 120). end_time=t1 (day 1).
      // t4 >= t1 + 90 days (day 91) → settled.
      // matchInProgress is not finished → never settled.
      val settled = matchFinished.copy(fetchedAt = Times.t4)
      for {
        _           <- ClubMatch.upsert(settled)
        settledA    <- ClubMatch.selectSettledMatchIdsForClub(clubA.clubId)
        settledB    <- ClubMatch.selectSettledMatchIdsForClub(clubB.clubId)
        settledNone <- ClubMatch.selectSettledMatchIdsForClub(ClubId(999))
        // Also verify it's no longer stale
        staleA <- ClubMatch.selectStaleForClub(clubA.clubId)
        // Restore original fetchedAt for subsequent tests
        _ <- ClubMatch.upsert(matchFinished)
      } yield assertTrue(
        settledA == Set(ClubMatchId(1001)),
        settledB == Set(ClubMatchId(1001)),
        settledNone.isEmpty,
        staleA.toSet == Set(ClubMatchId(1002)) // only matchInProgress is stale now
      )
    }

  private def testClubMatchSelectSettledForRefresh =
    test("selectSettledForRefreshBatch and countSettledForRefresh filter by cutoffTime") {
      // Make matchFinished settled: fetchedAt=t4 (day 120) >= endTime(day 1) + 90 days
      val settled = matchFinished.copy(fetchedAt = Times.t4)
      val cutoffAfter = Times.t4.plus(Duration.ofSeconds(1))
      for {
        _ <- ClubMatch.upsert(settled)
        // cutoffTime after fetchedAt → match is included
        count    <- ClubMatch.countSettledForRefresh(clubA.clubId, cutoffAfter)
        batch    <- ClubMatch.selectSettledForRefreshBatch(clubA.clubId, cutoffAfter, 100, ClubMatchId(0))
        // cutoffTime equal to fetchedAt → match is NOT included (fetchedAt < cutoffTime is false)
        countExact <- ClubMatch.countSettledForRefresh(clubA.clubId, Times.t4)
        batchExact <- ClubMatch.selectSettledForRefreshBatch(clubA.clubId, Times.t4, 100, ClubMatchId(0))
        // matchInProgress is not settled → never included
        // Cursor advancement: afterMatchId = 1001 should exclude match 1001
        batchCursor <- ClubMatch.selectSettledForRefreshBatch(clubA.clubId, cutoffAfter, 100, ClubMatchId(1001))
        _ <- ClubMatch.upsert(matchFinished) // restore
      } yield assertTrue(
        count == 1L,
        batch == List(ClubMatchId(1001)),
        countExact == 0L,
        batchExact.isEmpty,
        batchCursor.isEmpty // cursor past 1001 excludes the only settled match
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
    team1ScoreX2 = 1,
    team2ScoreX2 = 1
  )

  private def testClubMatchBoardInsertAndSelect = test("ClubMatchBoard insertBatch and selectMatch") {
    for {
      _       <- ClubMatchBoard.insertBatch(List(boardA, boardB))
      results <- ClubMatchBoard.selectMatch(matchFinished.matchId)
    } yield assertTrue(results.toSet == Set(boardA, boardB))
  }

  private def testClubMatchBoardNullableGameFields = test("ClubMatchBoard with null player IDs round-trips") {
    val noPlayers = ClubMatchBoard(
      matchId = matchFinished.matchId,
      board = 3,
      team1PlayerId = None,
      team1FairPlay = true,
      team2PlayerId = None,
      team2FairPlay = false,
      team1ScoreX2 = 0,
      team2ScoreX2 = 0
    )
    for {
      _       <- ClubMatchBoard.insert(noPlayers)
      results <- ClubMatchBoard.selectMatch(matchFinished.matchId)
      board3 = results.find(_.board == 3).get
    } yield assertTrue(
      board3 == noPlayers,
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
      r1 <- HistoryPendingMatch.insert(HistoryPendingMatch(clubA.clubId, ClubMatchId(2001), isLive = false))
      r2 <- HistoryPendingMatch.insert(
        HistoryPendingMatch(clubA.clubId, ClubMatchId(2001), isLive = false)
      ) // duplicate
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
      updated <- HistoryPendingMatch.updateStatus(
        clubA.clubId,
        ClubMatchId(2002),
        isLive = false,
        PendingMatchStatus.ApiError
      )
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
      total   <- HistoryPendingMatch.count(clubA.clubId)
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
      _ <- HistoryPendingMatch.updateStatus(
        clubA.clubId,
        ClubMatchId(2003),
        isLive = false,
        PendingMatchStatus.Unidentified
      )
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
      all.exists(_.username == Username("opp1")),
      all.exists(_.username == Username("opp2"))
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
      remaining.head.username == Username("opp2")
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
      all.head.slug == ClubSlug("unknown-club")
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

  // --- updateTeamClubId / updatePlayerId tests ---

  private def testClubMatchUpdateTeamClubId = test("ClubMatch updateTeamClubId patches correct team column") {
    // matchInProgress has team2ClubId = None
    for {
      before <- ClubMatch.selectId(matchInProgress.matchId)
      _ = assert(before.get.team2ClubId.isEmpty)
      updated <- ClubMatch.updateTeamClubId(matchInProgress.matchId, isTeam1 = false, clubB.clubId)
      after   <- ClubMatch.selectId(matchInProgress.matchId)
      // team1 unchanged
      noOp <- ClubMatch.updateTeamClubId(ClubMatchId(9999), isTeam1 = true, clubA.clubId)
    } yield assertTrue(
      updated == 1,
      after.get.team2ClubId.contains(clubB.clubId),
      after.get.team1ClubId == before.get.team1ClubId,
      noOp == 0
    )
  }

  // PlayerMatchRef has a single row per player_id. upsert refreshes all non-PK columns so that
  // parallel resolution/recruitment fibers writing a ref for the same player don't crash with a
  // duplicate-key error, and the most recent successful resolution wins.
  private def testPlayerMatchRefUpsert = test("PlayerMatchRef upsert refreshes on conflict") {
    val initial   = PlayerMatchRef(player0.playerId, ClubMatchId(1001), isLive = false, isTeam1 = true, 1)
    val refreshed = initial.copy(matchId = ClubMatchId(1002), isLive = true, isTeam1 = false, boardIdx = 3)
    for {
      _     <- PlayerMatchRef.upsert(initial)
      first <- PlayerMatchRef.selectId(player0.playerId)
      _     <- PlayerMatchRef.upsert(refreshed)
      after <- PlayerMatchRef.selectId(player0.playerId)
    } yield assertTrue(
      first.contains(initial),
      after.contains(refreshed)
    )
  }

  private def testClubMatchBoardUpdatePlayerId = test("ClubMatchBoard updatePlayerId patches correct team column") {
    // Re-insert boards since testClubMatchBoardDeleteMatch removed them
    for {
      _ <- ClubMatchBoard.insertBatch(List(boardA, boardB))
      // boardB has team2PlayerId = None — patch it
      updated <- ClubMatchBoard.updatePlayerId(boardB.matchId, boardB.board, isTeam1 = false, player0.playerId)
      boards  <- ClubMatchBoard.selectMatch(matchFinished.matchId)
      patched  = boards.find(_.board == boardB.board).get
      original = boards.find(_.board == boardA.board).get
      // non-existent board returns 0
      noOp <- ClubMatchBoard.updatePlayerId(ClubMatchId(9999), 1, isTeam1 = true, player0.playerId)
    } yield assertTrue(
      updated == 1,
      patched.team2PlayerId.contains(player0.playerId),
      patched.team1PlayerId == boardB.team1PlayerId,
      original == boardA,
      noOp == 0
    )
  }
}
