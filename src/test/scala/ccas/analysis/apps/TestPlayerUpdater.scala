package ccas.analysis.apps

import java.time.Instant
import java.time.temporal.ChronoUnit

import zio.ZIO
import zio.json.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.{Player, PlayerName, PlayerSnapshot, Tables}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.api.player.ApiPlayer
import ccas.utils.sql.{FreshSchemaLayer, TestDbCleanup}
import ccas.utils.sql.PostgresClient.withTransaction

object TestPlayerUpdater extends ZIOSpecDefault {

  private val pidA = PlayerId(7001)
  private val pidB = PlayerId(7002)

  // Postgres TIMESTAMPTZ has microsecond precision; Instant.now() has nanos. Truncate so
  // direct equality with stored values works.
  private def nowMicros: Instant = Instant.now().truncatedTo(ChronoUnit.MICROS)

  override def spec: Spec[Any, Throwable] = (suite("TestPlayerUpdater")(
    testNoUsernameChange,
    testUsernameRenameNoConflict,
    testRenameOntoAnotherPlayersStoredName,
    testReconcileTakesBackLastName
  ) @@ TestAspect.before(TestDbCleanup.clearPlayer)).provideShared(
    FreshSchemaLayer("test_player_updater", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  private def testNoUsernameChange = test("status change without rename: snapshot prior state, update player") {
    val now = nowMicros
    for {
      _ <- Player.insert(Player(pidA, Times.t0, Username("alice"), PlayerStatusCategory.Active, None, Times.t0))
      existing <- Player.selectId(pidA).someOrFailException
      _ <- withTransaction {
        PlayerUpdater.archiveAndUpdate(existing, Username("alice"), PlayerStatusCategory.Closed, None, now)
      }
      updated   <- Player.selectId(pidA).someOrFailException
      snapshots <- PlayerSnapshot.selectId(pidA)
    } yield assertTrue(
      updated.username == Username("alice"),
      updated.status == PlayerStatusCategory.Closed,
      updated.since == now,
      snapshots.size == 1,
      snapshots.head.status == PlayerStatusCategory.Active,
      snapshots.head.since == Times.t0
    )
  }

  // A pure rename still archives the prior state: the update moves `since`, and without the snapshot the report would
  // date the prior status from the rename.
  private def testUsernameRenameNoConflict = test("rename with no other player at the new username") {
    val now = nowMicros
    for {
      _ <- Player.insert(Player(pidA, Times.t0, Username("alice"), PlayerStatusCategory.Active, None, Times.t0))
      existing <- Player.selectId(pidA).someOrFailException
      _ <- withTransaction {
        PlayerUpdater.archiveAndUpdate(existing, Username("alice-renamed"), PlayerStatusCategory.Active, None, now)
      }
      updated   <- Player.selectId(pidA).someOrFailException
      snapshots <- PlayerSnapshot.selectId(pidA)
      names     <- PlayerName.selectPlayer(pidA)
    } yield assertTrue(
      updated.username == Username("alice-renamed"),
      updated.since == now,
      snapshots.map(snap => (snap.since, snap.status)) == List((Times.t0, PlayerStatusCategory.Active)),
      names.map(n => (n.username, n.until.isEmpty)) ==
        List((Username("alice"), false), (Username("alice-renamed"), true))
    )
  }

  // What used to tombstone the other row (#254 step 5b): Bob renamed away to a name we have not seen and someone —
  // here Alice — now answers to "bob". Nothing is asked of Chess.com and Bob's row is left as it was; Bob just holds
  // no name until his new one surfaces.
  private def testRenameOntoAnotherPlayersStoredName = test(
    "renaming onto a name another player's row stores takes it, and leaves that player holding none"
  ) {
    val now = nowMicros
    for {
      _ <- Player.insert(Player(pidA, Times.t0, Username("alice"), PlayerStatusCategory.Active, None, Times.t0))
      _ <- Player.insert(Player(pidB, Times.t0, Username("bob"), PlayerStatusCategory.Active, None, Times.t0))
      existingAlice <- Player.selectId(pidA).someOrFailException
      _ <- withTransaction {
        PlayerUpdater.archiveAndUpdate(existingAlice, Username("bob"), PlayerStatusCategory.Active, None, now)
      }
      aliceUpdated <- Player.selectId(pidA).someOrFailException
      bobAfter     <- Player.selectId(pidB).someOrFailException
      bobSnapshots <- PlayerSnapshot.selectId(pidB)
      currentNames <- PlayerName.selectCurrentNames(List(pidA, pidB))
      bobNames     <- PlayerName.selectPlayer(pidB)
    } yield assertTrue(
      aliceUpdated.username == Username("bob"),
      bobAfter == Player(pidB, Times.t0, Username("bob"), PlayerStatusCategory.Active, None, Times.t0),
      bobSnapshots.isEmpty,
      currentNames == Map(pidA -> Username("bob")),
      bobNames.map(n => (n.username, n.until.isDefined)) == List((Username("bob"), true))
    )
  }

  // A player holding no name keeps the one it lost as its row's display label. Observed under that very name, it takes
  // the name back: the label matching the observation is not the player holding it.
  private def testReconcileTakesBackLastName =
    test("reconcile gives a player holding no name the name it is observed under, even the one it lost") {
      for {
        _ <- Player.insert(Player(pidA, Times.t0, Username("bob"), PlayerStatusCategory.Active, None, Times.t0))
        _ <- Player.insert(Player(pidB, Times.t0, Username("bob"), PlayerStatusCategory.Active, None, Times.t0))
        observed <- ZIO.fromEither(apiPlayerJson(pidA.value, "bob").fromJson[ApiPlayer])
          .mapError(IllegalStateException(_))
        _      <- withTransaction(PlayerUpdater.reconcile(observed))
        holder <- PlayerName.selectCurrentHolder(Username("bob"))
        bob    <- PlayerName.selectCurrentName(pidB)
      } yield assertTrue(holder.contains(pidA), bob.isEmpty)
    }
}
