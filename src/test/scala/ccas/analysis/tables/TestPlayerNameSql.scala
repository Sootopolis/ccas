package ccas.analysis.tables

import java.sql.SQLException
import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.ZIO

import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.{ForcedOverlap, FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient.{connectZIO, withTransaction}

/** `player_name` is kept in step with every write of `player.username`, backfilled from what `player` stores, and its
  * constraints hold the current-slice invariants ADR 0016 puts in the database — the only ones left since #254 dropped
  * `player_username_unique`. Each test uses its own player ids, so the sequential suite shares one schema.
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
    // What the `_stale_<id>` tombstone used to stand in for (#254 step 5b): the loser of a name holds none we know of,
    // while its display cache keeps the name it last held.
    test("a player whose name another takes holds none, and the taker is the only holder") {
      val losing = player(130, "handed-over")
      for {
        _       <- Player.insert(losing)
        _       <- Player.insert(player(131, "handed-over"))
        names   <- windows(130)
        current <- PlayerName.selectCurrentName(losing.playerId)
        holder  <- PlayerName.selectCurrentHolder(losing.username)
        holders <- PlayerName.selectHolders(losing.username)
        stored  <- Player.selectId(losing.playerId)
      } yield assertTrue(
        names == List((losing.username, false)),
        current.isEmpty,
        holder.contains(PlayerId(131)),
        holders == List(PlayerId(130), PlayerId(131)),
        stored.exists(_.username == losing.username)
      )
    },
    // A player holding no name still stores the name it lost, so a write that changes nothing must not read that back
    // as an observation: an insert that finds the row there, or an update that loses its `since` race.
    test("a write that lands on nothing gives a player holding no name nothing back") {
      val losing = player(135, "lost-for-good", since = at(1))
      for {
        _        <- Player.insert(losing)
        _        <- Player.insert(player(136, "lost-for-good"))
        inserted <- Player.insertIfNew(player(135, "some-other-name", since = at(3)))
        _        <- Player.writeBatch(inserted = List(player(135, "batch-other-name", since = at(3))), updated = Nil)
        // The row's own `since`, as an inactive player's unchanging `lastOnlineAt` gives: the name still differs.
        sameSince <- Player.insertIfNew(player(135, "same-since-name", since = at(1)))
        updated  <- Player.updateCurrentState(losing.copy(since = t0))
        current  <- PlayerName.selectCurrentName(losing.playerId)
        holder   <- PlayerName.selectCurrentHolder(losing.username)
      } yield assertTrue(
        inserted == 0,
        sameSince == 0,
        updated == 0,
        current.isEmpty,
        holder.contains(PlayerId(136))
      )
    },
    test("a write that lands on a player holding no name records what it stores, even the name it last held") {
      val losing = player(137, "taken-back")
      for {
        _       <- Player.insert(losing)
        _       <- Player.insert(player(138, "taken-back"))
        _       <- Player.updateCurrentState(losing.copy(since = at(1)))
        current <- PlayerName.selectCurrentName(losing.playerId)
        holder  <- PlayerName.selectCurrentHolder(losing.username)
      } yield assertTrue(current.contains(losing.username), holder.contains(losing.playerId))
    },
    // #298: `player_name_current` refused whichever of two overlapping claims committed second (ADR 0020).
    test("a player claiming a name another player is claiming at that moment waits, then takes the name over") {
      val contested = Username("claimed-at-once")
      for {
        _ <- Player.insert(player(139, "claims-first"))
        _ <- Player.insert(player(140, "claims-second"))
        overlap <- ForcedOverlap.run(
          first = Player.updateCurrentState(player(139, contested.value, at(1))),
          second = Player.updateCurrentState(player(140, contested.value, at(1)))
        )
        first  <- windows(139)
        second <- PlayerName.selectPlayer(PlayerId(140))
      } yield assertTrue(
        overlap.waitedOn == "advisory",
        first == List((Username("claims-first"), false), (contested, false)),
        second.map(n => (n.username, n.until.isEmpty)) == List((Username("claims-second"), false), (contested, true)),
        second.last.since.isAfter(overlap.releasedAt)
      )
    },
    // Unless a player moving off a name queues with a player claiming it, its new window can open before the claim
    // closes its old one, which `player_name_no_overlap` refuses.
    test("a player moving off a name another player is claiming at that moment waits, then opens its new one") {
      for {
        _ <- Player.insert(player(143, "given-up"))
        _ <- Player.insert(player(144, "taker-before"))
        overlap <- ForcedOverlap.run(
          first = Player.updateCurrentState(player(144, "given-up", at(1))),
          second = Player.updateCurrentState(player(143, "moved-to", at(1)))
        )
        giver <- PlayerName.selectPlayer(PlayerId(143))
        taker <- windows(144)
      } yield assertTrue(
        overlap.waitedOn == "advisory",
        giver.map(n => (n.username.value, n.until.isEmpty)) == List(("given-up", false), ("moved-to", true)),
        taker == List((Username("taker-before"), false), (Username("given-up"), true)),
        giver.last.since.isAfter(overlap.releasedAt)
      )
    },
    // A membership batch's first run for a large club moves a name per member.
    test("a call locks each name up to the bound, and the whole space in one lock past it") {
      val within = List.tabulate(NameLock.MaxNameKeys)(i => player(300L + i, s"within-bound-$i"))
      val past   = List.tabulate(NameLock.MaxNameKeys + 1)(i => player(400L + i, s"past-bound-$i"))
      for {
        withinLocks <- withTransaction(Player.writeBatch(inserted = within, updated = Nil) *> heldAdvisoryLocks)
        pastLocks   <- withTransaction(Player.writeBatch(inserted = past, updated = Nil) *> heldAdvisoryLocks)
      } yield assertTrue(withinLocks == NameLock.MaxNameKeys + 1, pastLocks == 1)
    },
    test("a player claiming a name a past-bound batch is moving waits for the batch, then takes the name over") {
      val batch = List.tabulate(NameLock.MaxNameKeys + 1)(i => player(500L + i, s"escalated-$i"))
      for {
        overlap <- ForcedOverlap.run(
          first = Player.writeBatch(inserted = batch, updated = Nil),
          second = Player.insert(player(600, "escalated-0"))
        )
        batched <- windows(500)
        holder  <- PlayerName.selectCurrentHolder(Username("escalated-0"))
      } yield assertTrue(
        overlap.waitedOn == "advisory",
        batched == List((Username("escalated-0"), false)),
        holder.contains(PlayerId(600))
      )
    },
    // MembershipApp persists a roster in one batch, so two players trading names land in one statement batch, and the
    // name tables must tolerate the intermediate state.
    test("two players swapping names in one batch each end holding the other's") {
      val swapped = List(player(141, "swap-b", at(1)), player(142, "swap-a", at(1)))
      for {
        _     <- Player.writeBatch(inserted = List(player(141, "swap-a"), player(142, "swap-b")), updated = Nil)
        _     <- Player.writeBatch(inserted = Nil, updated = swapped)
        names <- PlayerName.selectCurrentNames(List(PlayerId(141), PlayerId(142)))
        open  <- openHolders(Username("swap-a")) <*> openHolders(Username("swap-b"))
      } yield assertTrue(
        names == Map(PlayerId(141) -> Username("swap-b"), PlayerId(142) -> Username("swap-a")),
        open == (1, 1)
      )
    },
    test("selectPlayers returns every window of each asked-for player, in order, and no one else's") {
      for {
        _     <- Player.insert(player(145, "windows-first"))
        _     <- Player.updateCurrentState(player(145, "windows-second", at(1)))
        _     <- Player.insert(player(146, "windows-other"))
        _     <- Player.insert(player(147, "windows-unasked"))
        empty <- PlayerName.selectPlayers(Nil)
        found <- PlayerName.selectPlayers(List(PlayerId(146), PlayerId(145)))
      } yield assertTrue(
        empty.isEmpty,
        found.map(n => (n.playerId, n.username.value, n.until.isEmpty)) == List(
          (PlayerId(145), "windows-first", false),
          (PlayerId(145), "windows-second", true),
          (PlayerId(146), "windows-other", true)
        )
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
    // From the migration instant, not the row's `since`: nothing observed the name any earlier (ADR 0016).
    test("backfill opens the stored name, from the migration instant, for a player with none, and is idempotent") {
      for {
        _      <- insertRawPlayer(170, "written-by-old-binary", at(3))
        first  <- PlayerName.backfill
        second <- PlayerName.backfill
        names  <- PlayerName.selectPlayer(PlayerId(170))
      } yield assertTrue(
        first == 1,
        second == 0,
        names.map(n => (n.username.value, n.until)) == List(("written-by-old-binary", None)),
        names.forall(_.since.isAfter(at(3)))
      )
    },
    // Two `player` rows can store one name now that `player_username_unique` is gone. The partial unique index picks
    // one holder, and the backfill must skip the other rather than fail the boot it runs in.
    test("backfill gives a name two players store to one of them, and leaves the other holding none") {
      val shared = Username("shared-at-backfill")
      for {
        _      <- insertRawPlayer(180, shared.value, t0)
        _      <- insertRawPlayer(181, shared.value, t0)
        rows   <- PlayerName.backfill
        first  <- PlayerName.selectCurrentName(PlayerId(180))
        second <- PlayerName.selectCurrentName(PlayerId(181))
        holder <- PlayerName.selectCurrentHolder(shared)
      } yield assertTrue(
        rows == 1,
        List(first, second).flatten == List(shared),
        holder.exists(Set(PlayerId(180), PlayerId(181)).contains)
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

  private val heldAdvisoryLocks: ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()".query[Int].run().head
    }

  // Bypasses the recording writers, as a row written before `player_name` existed would.
  private def insertRawPlayer(id: Long, username: String, since: Instant): ZIO[PostgresClient, SQLException, Int] =
    connectZIO {
      sql"""INSERT INTO player (player_id, joined, username, status, title, since)
            VALUES (${PlayerId(id)}, $t0, ${Username(username)}, 'Active', NULL, $since)""".update.run()
    }

  private def rawRename(id: Long, username: String): ZIO[PostgresClient, SQLException, Int] =
    connectZIO(sql"UPDATE player SET username = ${Username(username)} WHERE player_id = ${PlayerId(id)}".update.run())

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
