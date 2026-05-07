package ccas.analysis.tables

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertCompletes, assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.Chunk

import ccas.api.misc.enums.PlayerStatusCategory.{Active, Fairplay}
import ccas.api.misc.enums.Title.{CM, IM}
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer

object TestPlayerSql extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestPlayerSql")(
    testInsert,
    testInsertBatch,
    testSelect,
    testUpdate,
    testArchiveAndUpdate,
    testSelectByUsername,
    testSelectByIds,
    testInsertBatchIdempotent,
    testPlayerSnapshotInsertIdempotent,
    testResolveUsernames,
    testUpdateCurrentStateOptimistic,
    testSelectLatestPlayerIdByUsername
  ).provideShared(
    FreshSchemaLayer("test_player_sql", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 1, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(2))
    val t3: Instant = t0.plus(Duration.ofDays(3))
  }

  private val player0 = Player(PlayerId(0), Times.t0, Username("player0_0"), Active, None, Times.t2)
  private val player1 = Player(PlayerId(1), Times.t1, Username("player1"), Active, Some(CM), Times.t1)

  // Historical snapshots — represent past states that were archived
  private val player0Snapshot0 = PlayerSnapshot(player0.playerId, Times.t0, Username("player0_old"), Active, None)
  private val player0Snapshot1 =
    PlayerSnapshot(player0.playerId, Times.t1, Username("player0_mid"), Fairplay, None)

  private def testInsert = test("testInsert") {
    for {
      _ <- Player.insert(player0)
      _ <- PlayerSnapshot.insert(player0Snapshot0)
    } yield assertCompletes
  }

  private def testInsertBatch = test("testInsertBatch") {
    for {
      _ <- Player.insertBatch(Chunk(player1))
      _ <- PlayerSnapshot.insertBatch(Chunk(player0Snapshot1))
    } yield assertCompletes
  }

  private def testSelect = test("testSelect") {
    for {
      players     <- Player.selectAll
      nonExistent <- Player.selectId(PlayerId(3))
      p0          <- Player.selectId(player0.playerId)
      p0History   <- PlayerSnapshot.selectId(player0.playerId)
    } yield assertTrue(
      players.toSet == Set(player0, player1),
      nonExistent.isEmpty,
      p0.contains(player0),
      p0History.toSet == Set(player0Snapshot0, player0Snapshot1)
    )
  }

  private def testUpdate = test("testUpdate") {
    for {
      _       <- PlayerSnapshot.update(player0Snapshot0.copy(title = Some(IM)))
      results <- PlayerSnapshot.selectId(player0.playerId).map(_.sortBy(_.since).flatMap(_.title))
    } yield assertTrue(results == List(IM))
  }

  private def testArchiveAndUpdate = test("testArchiveAndUpdate") {
    for {
      // Archive player0's current state and update to new username
      existing <- Player.selectId(player0.playerId).map(_.get)
      archive = PlayerSnapshot(existing.playerId, existing.since, existing.username, existing.status, existing.title)
      _ <- PlayerSnapshot.insert(archive)
      updated = existing.copy(username = Username("player0_new"), since = Times.t3)
      rows <- Player.updateCurrentState(updated)
      // Verify
      current <- Player.selectId(player0.playerId)
      history <- PlayerSnapshot.selectId(player0.playerId)
    } yield assertTrue(
      rows == 1,
      current.exists(_.username == Username("player0_new")),
      current.exists(_.since == Times.t3),
      history.size == 3 // player0Snapshot0 + player0Snapshot1 + the archive we just created
    )
  }

  private def testSelectByUsername = test("testSelectByUsername") {
    for {
      found    <- Player.selectByUsername(Username("player0_new")) // updated in previous test
      notFound <- Player.selectByUsername(Username("nonexistent"))
    } yield assertTrue(
      found.exists(_.playerId == player0.playerId),
      notFound.isEmpty
    )
  }

  private def testSelectByIds = test("testSelectByIds") {
    for {
      empty       <- Player.selectByIds(Nil)
      single      <- Player.selectByIds(List(player0.playerId))
      both        <- Player.selectByIds(List(player0.playerId, player1.playerId))
      nonExistent <- Player.selectByIds(List(PlayerId(999)))
      mixed       <- Player.selectByIds(List(player0.playerId, PlayerId(999)))
    } yield assertTrue(
      empty.isEmpty,
      single.size == 1,
      single.head.playerId == player0.playerId,
      both.toSet.map(_.playerId) == Set(player0.playerId, player1.playerId),
      nonExistent.isEmpty,
      mixed.size == 1,
      mixed.head.playerId == player0.playerId
    )
  }

  // Re-inserting a player with the same player_id must silently no-op (ON CONFLICT DO NOTHING),
  // not raise a unique-key violation. Otherwise two concurrent MembershipApp runs against different
  // clubs that share a member would abort their persist transactions.
  private def testInsertBatchIdempotent = test("testInsertBatchIdempotent") {
    val altered = player1.copy(username = Username("player1_altered"))
    for {
      _       <- Player.insertBatch(Chunk(altered))
      current <- Player.selectId(player1.playerId)
    } yield assertTrue(
      // First insert wins: the altered row is ignored.
      current.exists(_.username == player1.username)
    )
  }

  // PK is (player_id, since). Duplicate snapshots must silently no-op so that two concurrent jobs
  // that both observe the same stale (player_id, since) can't crash each other's transaction.
  private def testPlayerSnapshotInsertIdempotent = test("testPlayerSnapshotInsertIdempotent") {
    val altered = player0Snapshot0.copy(username = Username("player0_altered"))
    for {
      r      <- PlayerSnapshot.insert(altered)
      stored <- PlayerSnapshot.selectId(player0.playerId).map(_.find(_.since == player0Snapshot0.since))
      // insertBatch with one duplicate + one fresh row
      freshSince = Times.t3.plusSeconds(1)
      fresh      = PlayerSnapshot(player0.playerId, freshSince, Username("player0_future"), Active, None)
      _     <- PlayerSnapshot.insertBatch(Chunk(altered, fresh))
      later <- PlayerSnapshot.selectId(player0.playerId)
    } yield assertTrue(
      r == 0, // ON CONFLICT DO NOTHING: 0 rows affected
      // Existing row retained (earlier test already updated title to IM via PlayerSnapshot.update)
      stored.exists(_.title.contains(IM)),
      stored.exists(_.username == Username("player0_old")), // original username, not "altered"
      later.exists(_.since == freshSince)                   // fresh row was inserted
    )
  }

  // Simulates the cross-club MembershipApp race: two classifications from the same stale state
  // both try to update the same player. The later-committing one must no-op rather than overwrite
  // the winner's fresher data. With optimistic `AND since < newSince`, a second call with the
  // same newSince (or older) returns 0 rows affected.
  private def testUpdateCurrentStateOptimistic = test("testUpdateCurrentStateOptimistic") {
    // player1 was inserted with since = Times.t1 and untouched by earlier tests.
    val t4 = Times.t3.plusSeconds(1)
    val t5 = Times.t3.plusSeconds(2)
    val firstWriter  = player1.copy(username = Username("player1_A"), since = t4)
    val staleWriter  = player1.copy(username = Username("player1_B"), since = t4) // same since as firstWriter
    val olderWriter  = player1.copy(username = Username("player1_C"), since = Times.t1) // same as DB start
    val newerWriter  = player1.copy(username = Username("player1_D"), since = t5)
    for {
      r1      <- Player.updateCurrentState(firstWriter)  // applies: t1 < t4
      r2      <- Player.updateCurrentState(staleWriter)  // no-op: t4 < t4 is false
      r3      <- Player.updateCurrentState(olderWriter)  // no-op: t4 < t1 is false
      current <- Player.selectId(player1.playerId)
      r4      <- Player.updateCurrentState(newerWriter)  // applies: t4 < t5
      after   <- Player.selectId(player1.playerId)
    } yield assertTrue(
      r1 == 1,
      r2 == 0,
      r3 == 0,
      current.exists(_.username == Username("player1_A")),
      r4 == 1,
      after.exists(_.username == Username("player1_D")),
      after.exists(_.since == t5)
    )
  }

  private def testSelectLatestPlayerIdByUsername = test("testSelectLatestPlayerIdByUsername") {
    // After earlier tests:
    //   player0 (id=0) snapshots: player0_old @ t0, player0_mid @ t1, player0_0 @ Times.t2 (archived in testArchiveAndUpdate)
    //   player0 current username: player0_new
    //   player1 (id=1) current username: player1_D, no snapshots in this test data
    for {
      // Single match — the snapshot for player0_old maps to player_id = 0
      old <- PlayerSnapshot.selectLatestPlayerIdByUsername(Username("player0_old"))
      // No snapshot ever held this username
      missing <- PlayerSnapshot.selectLatestPlayerIdByUsername(Username("never_existed"))
      // Synthesize an ambiguous case: insert a snapshot for player1 holding username player0_old
      _ <- PlayerSnapshot.insert(
        PlayerSnapshot(player1.playerId, Times.t3.plusSeconds(10), Username("player0_old"), Active, None)
      )
      ambiguous <- PlayerSnapshot.selectLatestPlayerIdByUsername(Username("player0_old"))
    } yield assertTrue(
      old == List(player0.playerId),
      missing.isEmpty,
      ambiguous.size == 2,
      // Ordered by MAX(since) DESC — player1's snapshot at Times.t3+10s is the most recent
      ambiguous.head == player1.playerId
    )
  }

  private def testResolveUsernames = test("testResolveUsernames") {
    for {
      empty       <- Player.resolveUsernames(Nil)
      single      <- Player.resolveUsernames(List(player1.playerId))
      both        <- Player.resolveUsernames(List(player0.playerId, player1.playerId))
      nonExistent <- Player.resolveUsernames(List(PlayerId(999)))
      mixed       <- Player.resolveUsernames(List(player0.playerId, PlayerId(999)))
    } yield assertTrue(
      empty.isEmpty,
      single == Map(player1.playerId -> player1.username),
      both.size == 2,
      both(player0.playerId) == Username("player0_new"), // updated in earlier test
      both(player1.playerId) == player1.username,
      nonExistent.isEmpty,
      mixed.size == 1,
      mixed(player0.playerId) == Username("player0_new")
    )
  }
}
