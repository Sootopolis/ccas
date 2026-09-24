package ccas.analysis.tables

import java.sql.SQLException
import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.ZIO

import ccas.analysis.apps.UsernameRenameResolver
import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient.connectZIO

/** `player_name` is kept in step with every write of `player.username`, seeded from the history `player_snapshot`
  * holds, and its constraints hold the current-slice invariants ADR 0016 puts in the database. Each test uses its own
  * player ids, so the sequential suite shares one schema.
  */
object TestPlayerNameSql extends ZIOSpecDefault {

  private val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  private def at(days: Int): Instant = t0.plusSeconds(days.toLong * 86400)

  private def player(id: Long, username: String, since: Instant = t0): Player =
    Player(PlayerId(id), t0, Username(username), Active, None, since)

  private def windows(id: Long): ZIO[PostgresClient, SQLException, List[(Username, Boolean)]] =
    PlayerName.selectPlayer(PlayerId(id)).map(_.map(n => (n.username, n.until.isEmpty)))

  override def spec: Spec[Any, Throwable] = suite("TestPlayerNameSql")(
    test("an insert opens a current name, and rewriting the same name opens no second row") {
      val p = player(100, "first-player")
      for {
        _     <- Player.insert(p)
        _     <- Player.updateCurrentState(p.copy(since = at(1)))
        names <- windows(100)
      } yield assertTrue(names == List((p.username, true)))
    },
    test("a rename closes the old name and opens the new one at the same instant") {
      val p = player(110, "before-rename")
      for {
        _     <- Player.insert(p)
        _     <- Player.updateCurrentState(p.copy(username = Username("after-rename"), since = at(1)))
        names <- PlayerName.selectPlayer(p.playerId)
      } yield assertTrue(
        names.map(_.username) == List(Username("before-rename"), Username("after-rename")),
        names.head.until.contains(names(1).since),
        names(1).until.isEmpty
      )
    },
    test("an update that loses the since guard records nothing, so the name the row still stores stands") {
      val p = player(120, "kept-name", since = at(5))
      for {
        _      <- Player.insert(p)
        rows   <- Player.updateCurrentState(p.copy(username = Username("stale-write"), since = at(1)))
        names  <- windows(120)
        holder <- PlayerName.selectCurrentHolder(Username("stale-write"))
      } yield assertTrue(rows == 0, names == List((p.username, true)), holder.isEmpty)
    },
    // The tombstone is what 5a still writes to free `player_username_unique`; it holds no name, and the player that
    // takes the freed one becomes its only holder.
    test("a tombstoned player holds no name, and the player taking its name is the only holder") {
      val losing = player(130, "handed-over")
      for {
        _ <- Player.insert(losing)
        tomb = UsernameRenameResolver.stalePlaceholder(losing.playerId)
        _       <- Player.updateCurrentState(losing.copy(username = tomb, since = at(1)))
        _       <- Player.insert(player(131, "handed-over"))
        names   <- windows(130)
        current <- PlayerName.selectCurrentName(losing.playerId)
        holder  <- PlayerName.selectCurrentHolder(losing.username)
        holders <- PlayerName.selectHolders(losing.username)
      } yield assertTrue(
        names == List((losing.username, false)),
        current.isEmpty,
        holder.contains(PlayerId(131)),
        holders == List(PlayerId(130), PlayerId(131))
      )
    },
    // MembershipApp persists a roster in one batch, so two players trading names land in one statement batch; the
    // deferred `player_username_unique` tolerates the intermediate state, and the name tables must as well.
    test("two players swapping names in one batch each end holding the other's") {
      for {
        _     <- Player.insertBatch(List(player(140, "swap-a"), player(141, "swap-b")))
        _     <- Player.updateCurrentStateBatch(List(player(140, "swap-b", at(1)), player(141, "swap-a", at(1))))
        names <- PlayerName.selectCurrentNames(List(PlayerId(140), PlayerId(141)))
        open  <- openHolders(Username("swap-a")) <*> openHolders(Username("swap-b"))
      } yield assertTrue(
        names == Map(PlayerId(140) -> Username("swap-b"), PlayerId(141) -> Username("swap-a")),
        open == (1, 1)
      )
    },
    test("selectCurrentHolders answers with the names some player holds now, not the ones given up") {
      val p = player(150, "held-before")
      for {
        _     <- Player.insert(p)
        _     <- Player.updateCurrentState(p.copy(username = Username("held-now"), since = at(1)))
        empty <- PlayerName.selectCurrentHolders(Nil)
        held  <- PlayerName.selectCurrentHolders(List("held-before", "held-now", "never-held").map(Username(_)))
      } yield assertTrue(empty.isEmpty, held == Map(Username("held-now") -> p.playerId))
    },
    test("a current name dated at or after the observation is dropped, not closed into an empty window") {
      for {
        _     <- insertRawPlayer(160, "clock-skewed", t0)
        _     <- rawName(160, "clock-skewed", "2999-01-01T00:00:00Z", None)
        _     <- Player.updateCurrentState(player(160, "observed-now", at(1)))
        names <- windows(160)
      } yield assertTrue(names == List((Username("observed-now"), true)))
    },
    test("backfill turns snapshot history into windows, one per run of a name, and is idempotent") {
      for {
        _ <- insertRawPlayer(170, "third", at(3))
        _ <- rawSnapshot(170, "first", at(0))
        _ <- rawSnapshot(170, "first", at(1))
        _ <- rawSnapshot(170, "second", at(2))
        // Dated after the row it would precede: the name held now comes from `player`, never from such a snapshot.
        _      <- rawSnapshot(170, "from-the-future", at(4))
        first  <- PlayerName.backfill
        second <- PlayerName.backfill
        names  <- PlayerName.selectPlayer(PlayerId(170))
      } yield assertTrue(
        first == 3,
        second == 0,
        names.map(n => (n.username.value, n.since, n.until)) == List(
          ("first", at(0), Some(at(2))),
          ("second", at(2), Some(at(3))),
          ("third", at(3), None)
        )
      )
    },
    // The one tombstone on Neon (2026-09-23) is this shape: its last real name is held by another player now, so
    // opening it for both — the runbook's first draft — would fail the boot on `player_name_current`.
    test("backfill closes a tombstoned player's last name at the tombstone, beside the player holding it now") {
      val tomb = UsernameRenameResolver.stalePlaceholder(PlayerId(180)).value
      for {
        _       <- insertRawPlayer(180, tomb, at(5))
        _       <- rawSnapshot(180, "was-mine", at(1))
        _       <- rawSnapshot(180, "interim", at(2))
        _       <- rawSnapshot(180, UsernameRenameResolver.stalePlaceholder(PlayerId(180)).value, at(3))
        _       <- rawSnapshot(180, "interim", at(4))
        _       <- insertRawPlayer(181, "was-mine", at(1))
        _       <- PlayerName.backfill
        names   <- PlayerName.selectPlayer(PlayerId(180))
        current <- PlayerName.selectCurrentName(PlayerId(180))
        holder  <- PlayerName.selectCurrentHolder(Username("was-mine"))
      } yield assertTrue(
        names.map(n => (n.username.value, n.since, n.until)) == List(
          ("was-mine", at(1), Some(at(2))),
          ("interim", at(2), Some(at(3))),
          ("interim", at(4), Some(at(5)))
        ),
        current.isEmpty,
        holder.contains(PlayerId(181))
      )
    },
    test("backfill leaves a player holding none when another is already recorded holding its name") {
      for {
        _ <- Player.insert(player(190, "recorded-first"))
        _ <- insertRawPlayer(191, "stored-later", t0)
        // The recorded holder's row has since moved on without a record — what a pre-`player_name` binary leaves.
        _      <- rawRename(190, "moved-on")
        _      <- rawRename(191, "recorded-first")
        _      <- PlayerName.backfill
        second <- PlayerName.selectCurrentName(PlayerId(191))
        holder <- PlayerName.selectCurrentHolder(Username("recorded-first"))
      } yield assertTrue(second.isEmpty, holder.contains(PlayerId(190)))
    },
    test("the exclusion constraint rejects two open names for one player") {
      for {
        _    <- Player.insert(player(200, "open-one"))
        exit <- rawName(200, "open-two", "2030-01-01T00:00:00Z", None).either
      } yield assertTrue(violates(exit, "player_name_no_overlap"))
    },
    test("the partial unique index rejects two current holders of one name") {
      for {
        _    <- Player.insert(player(210, "held-once"))
        _    <- insertRawPlayer(211, "other-name", t0)
        exit <- rawName(211, "held-once", "2030-01-01T00:00:00Z", None).either
      } yield assertTrue(violates(exit, "player_name_current"))
    },
    test("the CHECK rejects an empty window") {
      for {
        _    <- insertRawPlayer(220, "empty-window", t0)
        exit <- rawName(220, "empty-window", "2030-01-01T00:00:00Z", Some("2030-01-01T00:00:00Z")).either
      } yield assertTrue(violates(exit, "player_name_window"))
    }
  ).provideShared(FreshSchemaLayer("test_player_name_sql", onInit = Tables.ensureTables)) @@ TestAspect.sequential

  // Names the constraint, so a failure for any other reason (a codec, a missing player row) cannot pass for it.
  private def violates(result: Either[SQLException, Int], constraint: String): Boolean =
    result.left.exists(error => Option(error.getMessage).exists(_.contains(constraint)))

  private def openHolders(username: Username): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"SELECT count(*) FROM player_name WHERE username = $username AND until IS NULL".query[Int].run().head
    }

  // Bypasses the recording writers, as a row written before `player_name` existed would.
  private def insertRawPlayer(id: Long, username: String, since: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player (player_id, joined, username, status, title, since)
            VALUES (${PlayerId(id)}, $t0, ${Username(username)}, 'Active', NULL, $since)""".update.run()
    }

  private def rawRename(id: Long, username: String): ZIO[PostgresClient, SQLException, Int] =
    connectZIO(sql"UPDATE player SET username = ${Username(username)} WHERE player_id = ${PlayerId(id)}".update.run())

  private def rawSnapshot(id: Long, username: String, since: Instant): ZIO[PostgresClient, SQLException, Int] =
    PlayerSnapshot.insert(PlayerSnapshot(PlayerId(id), since, Username(username), Active, None))

  private def rawName(
    id: Long,
    username: String,
    since: String,
    until: Option[String]
  ): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      val sinceAt = Instant.parse(since)
      val untilAt = until.map(Instant.parse)
      sql"""INSERT INTO player_name (player_id, username, since, until)
            VALUES (${PlayerId(id)}, ${Username(username)}, $sinceAt, $untilAt)""".update.run()
    }
}
