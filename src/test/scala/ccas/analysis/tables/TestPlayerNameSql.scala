package ccas.analysis.tables

import java.sql.SQLException
import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.ZIO

import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{PlayerId, Username}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient.connectZIO

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
        _        <- Player.insertBatch(List(player(135, "batch-other-name", since = at(3))))
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
    // Nothing locks across players, so two observations of one name for two players race; `player_name_current` is
    // the only thing refusing the loser now that `player_username_unique` is gone. Whatever the interleaving, the name
    // ends with exactly one current holder and any failure names the index that said so.
    test("two players claiming one name at once leave exactly one holder") {
      val contested = Username("claimed-at-once")
      for {
        _       <- Player.insert(player(139, "claims-first"))
        _       <- Player.insert(player(140, "claims-second"))
        results <- ZIO.foreachPar(List(139L, 140L))(id =>
          Player.updateCurrentState(player(id, contested.value, at(1))).either
        )
        open   <- openHolders(contested)
        holder <- PlayerName.selectCurrentHolder(contested)
      } yield assertTrue(
        open == 1,
        results.exists(_.isRight),
        results.collect { case Left(error) => error }.forall(violatesConstraint(_, "player_name_current")),
        holder.exists(Set(PlayerId(139), PlayerId(140)).contains)
      )
    },
    // MembershipApp persists a roster in one batch, so two players trading names land in one statement batch, and the
    // name tables must tolerate the intermediate state.
    test("two players swapping names in one batch each end holding the other's") {
      for {
        _     <- Player.insertBatch(List(player(141, "swap-a"), player(142, "swap-b")))
        _     <- Player.updateCurrentStateBatch(List(player(141, "swap-b", at(1)), player(142, "swap-a", at(1))))
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
    result.left.exists(violatesConstraint(_, constraint))

  private def violatesConstraint(error: SQLException, constraint: String): Boolean =
    Option(error.getMessage).exists(_.contains(constraint))

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
