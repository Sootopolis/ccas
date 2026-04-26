package ccas.analysis.apps

import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.{Player, PlayerSnapshot, Tables}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.withTransaction

object TestPlayerUpdater extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestPlayerUpdater")(
    testNoUsernameChange,
    testUsernameRenameNoConflict,
    testUsernameRenameRecursesIntoConflictingPlayer
  ).provideShared(
    FreshSchemaLayer("test_player_updater", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // Postgres TIMESTAMPTZ has microsecond precision; Instant.now() has nanos. Truncate so
  // direct equality with stored values works.
  private def nowMicros: Instant = Instant.now().truncatedTo(ChronoUnit.MICROS)

  private def testNoUsernameChange = test("status change without rename: snapshot prior state, update player") {
    val pidA = PlayerId(7001)
    val now  = nowMicros
    for {
      client <- fakeChessComClient(Map.empty)
      _ <- Player.insert(Player(pidA, Times.t0, Username("alice-7001"), PlayerStatusCategory.Active, None, Times.t0))
      existing <- Player.selectId(pidA).someOrFailException
      _ <- withTransaction {
        PlayerUpdater.archiveAndUpdate(
          existing,
          Username("alice-7001"),
          PlayerStatusCategory.Closed,
          None,
          now,
          client
        )
      }
      updated   <- Player.selectId(pidA).someOrFailException
      snapshots <- PlayerSnapshot.selectId(pidA)
    } yield assertTrue(
      updated.username == Username("alice-7001"),
      updated.status == PlayerStatusCategory.Closed,
      updated.since == now,
      snapshots.size == 1,
      snapshots.head.username == Username("alice-7001"),
      snapshots.head.status == PlayerStatusCategory.Active,
      snapshots.head.since == Times.t0
    )
  }

  private def testUsernameRenameNoConflict = test("rename with no other player at the new username") {
    val pidA = PlayerId(7011)
    val now  = nowMicros
    for {
      client <- fakeChessComClient(Map.empty)
      _ <- Player.insert(Player(pidA, Times.t0, Username("alice-7011"), PlayerStatusCategory.Active, None, Times.t0))
      existing <- Player.selectId(pidA).someOrFailException
      _ <- withTransaction {
        PlayerUpdater.archiveAndUpdate(
          existing,
          Username("alice-7011-renamed"),
          PlayerStatusCategory.Active,
          None,
          now,
          client
        )
      }
      updated   <- Player.selectId(pidA).someOrFailException
      snapshots <- PlayerSnapshot.selectId(pidA)
    } yield assertTrue(
      updated.username == Username("alice-7011-renamed"),
      updated.since == now,
      snapshots.size == 1,
      snapshots.head.username == Username("alice-7011")
    )
  }

  private def testUsernameRenameRecursesIntoConflictingPlayer = test(
    "rename triggers recursive archive of conflicting player when API confirms drift"
  ) {
    // Bob is in DB with username "bob-7022". The API now reports Bob's username is "bob-7022-new".
    // Alice tries to rename to "bob-7022" → conflict resolver fetches API for "bob-7022" → sees drift →
    // recursively archives Bob, freeing the username for Alice. Both updates land in one transaction
    // (the username unique constraint is DEFERRABLE INITIALLY DEFERRED).
    val pidA = PlayerId(7021)
    val pidB = PlayerId(7022)
    val responses = Map("player/bob-7022" -> apiPlayerJson(pidB.value, "bob-7022-new"))
    val now       = nowMicros
    for {
      client <- fakeChessComClient(responses)
      _ <- Player.insert(Player(pidA, Times.t0, Username("alice-7021"), PlayerStatusCategory.Active, None, Times.t0))
      _ <- Player.insert(Player(pidB, Times.t0, Username("bob-7022"), PlayerStatusCategory.Active, None, Times.t0))
      existingAlice <- Player.selectId(pidA).someOrFailException
      _ <- withTransaction {
        PlayerUpdater.archiveAndUpdate(
          existingAlice,
          Username("bob-7022"),
          PlayerStatusCategory.Active,
          None,
          now,
          client
        )
      }
      aliceUpdated   <- Player.selectId(pidA).someOrFailException
      bobUpdated     <- Player.selectId(pidB).someOrFailException
      aliceSnapshots <- PlayerSnapshot.selectId(pidA)
      bobSnapshots   <- PlayerSnapshot.selectId(pidB)
    } yield assertTrue(
      aliceUpdated.username == Username("bob-7022"),
      bobUpdated.username == Username("bob-7022-new"),
      aliceSnapshots.size == 1,
      aliceSnapshots.head.username == Username("alice-7021"),
      bobSnapshots.size == 1,
      bobSnapshots.head.username == Username("bob-7022")
    )
  }
}
