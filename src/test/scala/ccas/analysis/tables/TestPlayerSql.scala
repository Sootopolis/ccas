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
    testUpdate
  ).provideShared(
    FreshSchemaLayer("test_player_sql", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private object Timestamps {
    val t0: Instant = LocalDateTime.of(2025, 1, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(2))
  }

  private val player0          = Player(PlayerId(0), Timestamps.t0)
  private val player1          = Player(PlayerId(1), Timestamps.t1)
  private val player0Snapshot0 = PlayerSnapshot(player0.playerId, Timestamps.t0, Username("player0_0"), Active, None)
  private val player0Snapshot1 = player0Snapshot0.copy(since = Timestamps.t1, username = Username("player0_1"))
  private val player0Snapshot2 = player0Snapshot1.copy(since = Timestamps.t2, status = Fairplay)
  private val player1Snapshot1 = player0Snapshot1.copy(playerId = player1.playerId, username = Username("player1"))
  private val player1Snapshot2 = player1Snapshot1.copy(since = Timestamps.t2, title = Some(CM))

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
      _ <- PlayerSnapshot.insertBatch(Chunk(player0Snapshot1, player0Snapshot2, player1Snapshot1, player1Snapshot2))
    } yield assertCompletes
  }

  private def testSelect = test("testSelect") {
    for {
      players     <- Player.selectAll
      nonExistent <- Player.selectId(PlayerId(3))
      latest      <- PlayerSnapshot.selectLatest
      p0Latest    <- PlayerSnapshot.selectIdLatest(player0.playerId)
      p0All       <- PlayerSnapshot.selectId(player0.playerId)
      sinceT1     <- PlayerSnapshot.selectSince(Timestamps.t1)
    } yield assertTrue(
      players.toSet == Set(player0, player1),
      nonExistent.isEmpty,
      latest.toSet == Set(player0Snapshot2, player1Snapshot2),
      p0Latest.contains(player0Snapshot2),
      p0All.toSet == Set(player0Snapshot0, player0Snapshot1, player0Snapshot2),
      sinceT1.toSet == Set(player0Snapshot1, player1Snapshot1, player0Snapshot2, player1Snapshot2)
    )
  }

  private def testUpdate = test("testUpdate") {
    for {
      _       <- PlayerSnapshot.update(player0Snapshot0.copy(title = Some(IM)))
      _       <- PlayerSnapshot.updateBatch(Chunk(player0Snapshot1, player0Snapshot2).map(_.copy(title = Some(GM))))
      results <- PlayerSnapshot.selectId(player0.playerId).map(_.sortBy(_.since).flatMap(_.title))
    } yield assertTrue(results == List(IM, GM, GM))
  }
}
