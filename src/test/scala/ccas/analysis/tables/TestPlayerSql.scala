package ccas.analysis.tables

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.test.{assertCompletes, assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.Chunk

import ccas.api.misc.enums.PlayerStatusCategory.{Active, Fairplay}
import ccas.api.misc.enums.Title.{CM, GM, IM}
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer

object TestPlayerSql extends ZIOSpecDefault {
  override def spec: Spec[Any, Throwable] = suite("TestPlayerSql")(
    testCreateTables,
    testDeleteAll,
    testInsert,
    testInsertBatch,
    testSelect,
    testUpdate,
    testArchiveAndUpdate,
    testSelectByUsername,
    testSelectByIds,
    testResolveUsernames
  ).provideShared(
    FreshSchemaLayer("test_player_sql", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object Timestamps {
    val t0: Instant = LocalDateTime.of(2025, 1, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(2))
    val t3: Instant = t0.plus(Duration.ofDays(3))
  }

  private val player0 = Player(PlayerId(0), Timestamps.t0, Username("player0_0"), Active, None, Timestamps.t2)
  private val player1 = Player(PlayerId(1), Timestamps.t1, Username("player1"), Active, Some(CM), Timestamps.t1)

  // Historical snapshots — represent past states that were archived
  private val player0Snapshot0 = PlayerSnapshot(player0.playerId, Timestamps.t0, Username("player0_old"), Active, None)
  private val player0Snapshot1 =
    PlayerSnapshot(player0.playerId, Timestamps.t1, Username("player0_mid"), Fairplay, None)

  private def testCreateTables = test("testCreateTables") {
    assertCompletes
  }

  private def testDeleteAll = test("testDeleteAll") {
    for {
      _ <- PlayerSnapshot.deleteAll
      _ <- Player.deleteAll
    } yield assertCompletes
  }

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
      updated = existing.copy(username = Username("player0_new"), since = Timestamps.t3)
      rows <- Player.updateCurrentState(updated)
      // Verify
      current <- Player.selectId(player0.playerId)
      history <- PlayerSnapshot.selectId(player0.playerId)
    } yield assertTrue(
      rows == 1,
      current.exists(_.username == Username("player0_new")),
      current.exists(_.since == Timestamps.t3),
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
