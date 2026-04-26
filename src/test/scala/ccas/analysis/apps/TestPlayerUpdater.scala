package ccas.analysis.apps

import java.time.Instant
import java.time.temporal.ChronoUnit

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.{Player, PlayerSnapshot, Tables}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.{connectZIO, withTransaction}

// `withLiveClock` stays because PlayerUpdater's transitive call into ApiPlayerArchive.getUrl
// rejects year=1970 (the TestClock default), and the rate-limiter Clock.sleep would park.
// Removing it requires either advancing TestClock or excising those code paths — out of scope.
object TestPlayerUpdater extends ZIOSpecDefault {

  private val pidA = PlayerId(7001)
  private val pidB = PlayerId(7002)

  // Postgres TIMESTAMPTZ has microsecond precision; Instant.now() has nanos. Truncate so
  // direct equality with stored values works.
  private def nowMicros: Instant = Instant.now().truncatedTo(ChronoUnit.MICROS)

  // Snapshot rows have an FK to player; delete in dependency order.
  private val resetPlayerTables = for {
    _ <- connectZIO { val _ = sql"DELETE FROM player_snapshot".update.run() }
    _ <- connectZIO { val _ = sql"DELETE FROM player".update.run() }
  } yield ()

  override def spec: Spec[Any, Throwable] = suite("TestPlayerUpdater")(
    testNoUsernameChange,
    testUsernameRenameNoConflict,
    testUsernameRenameRecursesIntoConflictingPlayer
  ).provideShared(
    FreshSchemaLayer("test_player_updater", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private def testNoUsernameChange = test("status change without rename: snapshot prior state, update player") {
    val now = nowMicros
    for {
      _      <- resetPlayerTables
      client <- fakeChessComClient(Map.empty)
      _ <- Player.insert(Player(pidA, Times.t0, Username("alice"), PlayerStatusCategory.Active, None, Times.t0))
      existing <- Player.selectId(pidA).someOrFailException
      _ <- withTransaction {
        PlayerUpdater.archiveAndUpdate(
          existing,
          Username("alice"),
          PlayerStatusCategory.Closed,
          None,
          now,
          client
        )
      }
      updated   <- Player.selectId(pidA).someOrFailException
      snapshots <- PlayerSnapshot.selectId(pidA)
    } yield assertTrue(
      updated.username == Username("alice"),
      updated.status == PlayerStatusCategory.Closed,
      updated.since == now,
      snapshots.size == 1,
      snapshots.head.username == Username("alice"),
      snapshots.head.status == PlayerStatusCategory.Active,
      snapshots.head.since == Times.t0
    )
  }

  private def testUsernameRenameNoConflict = test("rename with no other player at the new username") {
    val now = nowMicros
    for {
      _      <- resetPlayerTables
      client <- fakeChessComClient(Map.empty)
      _ <- Player.insert(Player(pidA, Times.t0, Username("alice"), PlayerStatusCategory.Active, None, Times.t0))
      existing <- Player.selectId(pidA).someOrFailException
      _ <- withTransaction {
        PlayerUpdater.archiveAndUpdate(
          existing,
          Username("alice-renamed"),
          PlayerStatusCategory.Active,
          None,
          now,
          client
        )
      }
      updated   <- Player.selectId(pidA).someOrFailException
      snapshots <- PlayerSnapshot.selectId(pidA)
    } yield assertTrue(
      updated.username == Username("alice-renamed"),
      updated.since == now,
      snapshots.size == 1,
      snapshots.head.username == Username("alice")
    )
  }

  private def testUsernameRenameRecursesIntoConflictingPlayer = test(
    "rename triggers recursive archive of conflicting player when API confirms drift"
  ) {
    // Bob is in DB with username "bob". The API now reports Bob's username is "bob-new".
    // Alice tries to rename to "bob" → conflict resolver fetches API for "bob" → sees drift →
    // recursively archives Bob, freeing the username for Alice. Both updates land in one transaction
    // (the username unique constraint is DEFERRABLE INITIALLY DEFERRED).
    val responses = Map("player/bob" -> apiPlayerJson(pidB.value, "bob-new"))
    val now       = nowMicros
    for {
      _      <- resetPlayerTables
      client <- fakeChessComClient(responses)
      _ <- Player.insert(Player(pidA, Times.t0, Username("alice"), PlayerStatusCategory.Active, None, Times.t0))
      _ <- Player.insert(Player(pidB, Times.t0, Username("bob"), PlayerStatusCategory.Active, None, Times.t0))
      existingAlice <- Player.selectId(pidA).someOrFailException
      _ <- withTransaction {
        PlayerUpdater.archiveAndUpdate(
          existingAlice,
          Username("bob"),
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
      aliceUpdated.username == Username("bob"),
      bobUpdated.username == Username("bob-new"),
      aliceSnapshots.size == 1,
      aliceSnapshots.head.username == Username("alice"),
      bobSnapshots.size == 1,
      bobSnapshots.head.username == Username("bob")
    )
  }
}
