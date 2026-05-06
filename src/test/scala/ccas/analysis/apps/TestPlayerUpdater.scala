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
import ccas.utils.sql.PostgresClient.{transactZIO, withTransaction}

object TestPlayerUpdater extends ZIOSpecDefault {

  private val pidA = PlayerId(7001)
  private val pidB = PlayerId(7002)

  // Postgres TIMESTAMPTZ has microsecond precision; Instant.now() has nanos. Truncate so
  // direct equality with stored values works.
  private def nowMicros: Instant = Instant.now().truncatedTo(ChronoUnit.MICROS)

  // Snapshot rows have an FK to player; delete in dependency order. One transaction = one
  // round-trip; atomicity isn't strictly required here but is the correct shape for batched
  // mutations.
  private val resetPlayerTables = transactZIO {
    val _ = sql"DELETE FROM player_snapshot".update.run()
    sql"DELETE FROM player".update.run()
  }

  // `withLiveClock` stays because PlayerUpdater's transitive code path through
  // ApiPlayerArchive.getUrl rejects year=1970 (the TestClock default), and the rate-limiter
  // Clock.sleep would park. Removing it requires either advancing TestClock or excising those
  // code paths — out of scope.
  override def spec: Spec[Any, Throwable] = (suite("TestPlayerUpdater")(
    testNoUsernameChange,
    testUsernameRenameNoConflict,
    testUsernameRenameRecursesIntoConflictingPlayer,
    testRecycledHandleTombstonesConflicting
  ) @@ TestAspect.before(resetPlayerTables)).provideShared(
    FreshSchemaLayer("test_player_updater", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private def testNoUsernameChange = test("status change without rename: snapshot prior state, update player") {
    val now = nowMicros
    for {
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

  private def testRecycledHandleTombstonesConflicting = test(
    "recycled handle: API serves conflicting username for a different player → tombstone the stale row"
  ) {
    // Bob is in DB with username "bob". Bob renamed away to something we don't yet know. Some new account
    // (different playerId) has registered "bob" since. Alice now wants to rename to "bob".
    // The API for "bob" returns playerId != Bob's. Resolver tombstones Bob's row to free the slot, Alice's
    // update lands in the same transaction (UNIQUE constraint deferred to commit).
    val recycledPlayerId = pidB.value + 1000
    val responses = Map("player/bob" -> apiPlayerJson(recycledPlayerId, "bob"))
    val now       = nowMicros
    for {
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
      bobSnapshots   <- PlayerSnapshot.selectId(pidB)
      aliceSnapshots <- PlayerSnapshot.selectId(pidA)
    } yield assertTrue(
      aliceUpdated.username == Username("bob"),
      bobUpdated.username == Username(s"_stale_${pidB.value}"),
      bobUpdated.isTombstoned,
      bobSnapshots.size == 1,
      bobSnapshots.head.username == Username("bob"),
      aliceSnapshots.size == 1,
      aliceSnapshots.head.username == Username("alice")
    )
  }
}
