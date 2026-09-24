package ccas.analysis.apps

import com.augustnagro.magnum.sql
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}
import zio.{RIO, ZIO}
import zio.http.*

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.*
import ccas.analysis.tables.*
import ccas.api.misc.enums.{ClubMatchStatus, PlayerStatusCategory, TimeClass}
import ccas.api.misc.subtypes.{ClubMatchId, PlayerId, Username}
import ccas.utils.client.{BodyStore, ChessComClient, TestChessComClientSupport}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.transactZIO

object TestUsernameRenameResolver extends ZIOSpecDefault {

  private val pidA = PlayerId(9101)
  private val pidB = PlayerId(9102)

  private val resetTables = transactZIO {
    sql"DELETE FROM player_match_ref".update.run()
    sql"DELETE FROM club_match_board".update.run()
    sql"DELETE FROM club_match WHERE match_id = 90001".update.run()
    sql"DELETE FROM player_name".update.run()
    sql"DELETE FROM player_snapshot".update.run()
    sql"DELETE FROM player".update.run()
  }

  override def spec: Spec[Any, Throwable] = (suite("TestUsernameRenameResolver")(
    testStalePlaceholderIsATombstone,
    testTierADeletionWhenHintHoldsStale,
    testTierARecycledHandle,
    testTierASnapshotResolves,
    testTierAAmbiguousSnapshotWithoutHint,
    testTierATombstoneSkipped,
    testVerificationFailureReturnsNone,
    testVerificationPlayerIdMismatchReturnsNone,
    testTierBBoardEndpointResolves,
    testTierBFallsBackToMatchEndpointWhenOpponentUnlinked,
    testResolveAndReconcileUpdatesPlayer
  ) @@ TestAspect.before(resetTables)).provideShared(
    FreshSchemaLayer("test_username_rename_resolver", onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private def insertPlayer(pid: PlayerId, username: String, status: PlayerStatusCategory = PlayerStatusCategory.Active) =
    Player.insert(Player(pid, TestTimes.t0, Username(username), status, None, TestTimes.t0))

  /** Local fake client that adds a `/pub/match/{id}/{board}` route on top of the shared `RecruitmentTestSupport`
    * routes. Used by Tier B tests since `buildRoutes` doesn't expose the board endpoint.
    */
  private def fakeClientWithBoard(responses: Map[String, String]): RIO[PostgresClient & BodyStore, ChessComClient] = {
    val routes: Routes[Any, Response] = Routes(
      Method.GET / "pub" / "player" / string("username") -> handler { (username: String, _: Request) =>
        responses.get(s"player/$username") match {
          case Some(json) => Response.json(json)
          case None       => notFoundResponse
        }
      },
      Method.GET / "pub" / "match" / long("id") / int("board") -> handler { (id: Long, board: Int, _: Request) =>
        responses.get(s"match/$id/$board") match {
          case Some(json) => Response.json(json)
          case None       => notFoundResponse
        }
      },
      Method.GET / "pub" / "match" / long("id") -> handler { (id: Long, _: Request) =>
        responses.get(s"match/$id") match {
          case Some(json) => Response.json(json)
          case None       => notFoundResponse
        }
      }
    )
    TestChessComClientSupport.fakeClient(routes)
  }

  // Holds `former`, then `current` from a later write: the history Tier A reads, recorded the way production does.
  private def insertRenamed(pid: PlayerId, former: String, current: String) =
    insertPlayer(pid, former) *>
      Player.updateCurrentState(
        Player(pid, TestTimes.t0, Username(current), PlayerStatusCategory.Active, None, TestTimes.t1)
      )

  private def testStalePlaceholderIsATombstone = test("stalePlaceholder writes the tombstone format") {
    ZIO.succeed(assertTrue(
      Player.isTombstoneUsername(Username("_stale_42")),
      Player.isTombstoneUsername(Username("_stale_9999999")),
      !Player.isTombstoneUsername(Username("_stale_")),
      !Player.isTombstoneUsername(Username("stale_42")),
      !Player.isTombstoneUsername(Username("alice")),
      !Player.isTombstoneUsername(Username("_stale_42a")),
      UsernameRenameResolver.stalePlaceholder(pidA) == Username("_stale_9101"),
      Player.isTombstoneUsername(UsernameRenameResolver.stalePlaceholder(pidA))
    ))
  }

  private def testTierADeletionWhenHintHoldsStale =
    test("Tier A: hint matches current holder of stale name → None (deletion, not rename)") {
      for {
        client <- fakeChessComClient(Map.empty)
        _      <- insertPlayer(pidA, "alpha")
        result <- UsernameRenameResolver.resolveCurrentUsername(client, Username("alpha"), Some(pidA))
      } yield assertTrue(result.isEmpty)
    }

  private def testTierARecycledHandle =
    test("Tier A: stale name now held by different player AND hint exists with new name → returns hint's current name") {
      // Player A renamed: alpha → newA. Player B took the freed handle: ??? → alpha.
      // DB state: pidA=newA (verified ApiPlayer matches hint), pidB=alpha.
      val responses = Map(
        s"player/newa" -> apiPlayerJson(PlayerId.unwrap(pidA), "newa")
      )
      for {
        client <- fakeChessComClient(responses)
        _      <- insertPlayer(pidA, "newa")
        _      <- insertPlayer(pidB, "alpha")
        result <- UsernameRenameResolver.resolveCurrentUsername(client, Username("alpha"), Some(pidA))
      } yield assertTrue(result.contains(Username("newa")))
    }

  private def testTierASnapshotResolves =
    test("Tier A: a former name no one holds now resolves to its sole former holder's current name") {
      // pidA renamed: alpha → newA, and nobody has taken alpha since.
      val responses = Map(
        s"player/newa" -> apiPlayerJson(PlayerId.unwrap(pidA), "newa")
      )
      for {
        client <- fakeChessComClient(responses)
        _      <- insertRenamed(pidA, "alpha", "newa")
        result <- UsernameRenameResolver.resolveCurrentUsername(client, Username("alpha"), Some(pidA))
      } yield assertTrue(result.contains(Username("newa")))
    }

  private def testTierAAmbiguousSnapshotWithoutHint =
    test("Tier A: no hint AND several former holders of the name → None (ambiguous)") {
      for {
        client <- fakeChessComClient(Map.empty)
        _      <- insertRenamed(pidA, "shared", "currentA")
        _      <- insertRenamed(pidB, "shared", "currentB")
        result <- UsernameRenameResolver.resolveCurrentUsername(client, Username("shared"), None)
      } yield assertTrue(result.isEmpty)
    }

  private def testTierATombstoneSkipped =
    test("Tier A: the sole former holder holds no name now → None, never the tombstone") {
      // pidA held alpha, then was tombstoned, so it holds no name. Tier A must NOT return the tombstone.
      val tombstone = UsernameRenameResolver.stalePlaceholder(pidA).value
      for {
        client <- fakeChessComClient(Map.empty)
        _      <- insertRenamed(pidA, "alpha", tombstone)
        // No hint, no Tier B.
        result <- UsernameRenameResolver.resolveCurrentUsername(client, Username("alpha"), None)
      } yield assertTrue(result.isEmpty)
    }

  private def testVerificationFailureReturnsNone =
    test("Verification: 404 on candidate → resolver returns None and original error propagates") {
      // Tier A finds "newA" candidate from snapshot, but verification fetch 404s.
      for {
        client <- fakeChessComClient(Map.empty, failures = Set("newa"))
        _      <- insertRenamed(pidA, "alpha", "newa")
        result <- UsernameRenameResolver.resolveCurrentUsername(client, Username("alpha"), Some(pidA))
      } yield assertTrue(result.isEmpty)
    }

  private def testVerificationPlayerIdMismatchReturnsNone =
    test("Verification: candidate's playerId differs from hint → None") {
      // Tier A would return "newA" pointing to pidA, but ApiPlayer for newA returns pidB → mismatch.
      val responses = Map(
        s"player/newa" -> apiPlayerJson(PlayerId.unwrap(pidB), "newa")
      )
      for {
        client <- fakeChessComClient(responses)
        _      <- insertRenamed(pidA, "alpha", "newa")
        result <- UsernameRenameResolver.resolveCurrentUsername(client, Username("alpha"), Some(pidA))
      } yield assertTrue(result.isEmpty)
    }

  private def testTierBBoardEndpointResolves =
    test("Tier B: board endpoint identifies the renamed player by eliminating opponent") {
      // pidA renamed alpha → newA. Player table doesn't reflect it (tombstoned), no snapshot of "alpha" → Tier A None.
      // PlayerMatchRef(pidA, match=90001, board=1, isTeam1=true) exists. Board endpoint returns newA vs opponent.
      // Opponent (pidB, "opponent") is resolved via ClubMatchBoard.team2PlayerIdOption.
      val matchId = ClubMatchId(90001L)
      val tombstone = UsernameRenameResolver.stalePlaceholder(pidA).value
      val responses = Map(
        s"match/90001/1" -> apiMatchBoardJson(90001L, 1, "newa", "opponent"),
        s"player/newa" -> apiPlayerJson(PlayerId.unwrap(pidA), "newa")
      )
      for {
        client <- fakeClientWithBoard(responses)
        _      <- insertPlayer(pidA, tombstone)
        _      <- insertPlayer(pidB, "opponent")
        _ <- ClubMatch.upsert(
          ClubMatch(
            matchId, "Test Match",
            ClubMatchStatus.Finished, TimeClass.Daily,
            Some(TestTimes.t0), Some(TestTimes.t1), 1,
            None, 20, None, 10, TestTimes.t0, None
          )
        )
        _ <- ClubMatchBoard.insertBatch(
          List(ClubMatchBoard(matchId, 1, Some(pidA), false, Some(pidB), false, 2, 0))
        )
        _ <- PlayerMatchRef.insert(PlayerMatchRef(pidA, matchId, isLive = false, isTeam1 = true, boardIdx = 1))
        result <- UsernameRenameResolver.resolveCurrentUsername(client, Username("alpha"), Some(pidA))
      } yield assertTrue(result.contains(Username("newa")))
    }

  // Regression for #258: opposingCurrentUsername prefers the DB link (club_match_board.team2PlayerIdOption), so the
  // above test never actually reaches RefHelpers.fetchTeamMatchTeamsOptional. This forces the opposing side
  // unlinked so the fallback to the full match endpoint is what resolves the rename.
  private def testTierBFallsBackToMatchEndpointWhenOpponentUnlinked =
    test("Tier B: opposing side not linked in club_match_board falls back to the match endpoint") {
      val matchId   = ClubMatchId(90002L)
      val tombstone = UsernameRenameResolver.stalePlaceholder(pidA).value
      val responses = Map(
        s"match/90002" -> apiDailyMatchJson(
          90002L, "clubx", "cluby",
          team1Players = List(("newa", 1)),
          team2Players = List(("opponent", 1))
        ),
        s"match/90002/1" -> apiMatchBoardJson(90002L, 1, "newa", "opponent"),
        s"player/newa" -> apiPlayerJson(PlayerId.unwrap(pidA), "newa")
      )
      for {
        client <- fakeClientWithBoard(responses)
        _      <- insertPlayer(pidA, tombstone)
        _ <- ClubMatch.upsert(
          ClubMatch(
            matchId, "Test Match",
            ClubMatchStatus.Finished, TimeClass.Daily,
            Some(TestTimes.t0), Some(TestTimes.t1), 1,
            None, 20, None, 10, TestTimes.t0, None
          )
        )
        _ <- ClubMatchBoard.insertBatch(
          List(ClubMatchBoard(matchId, 1, Some(pidA), false, None, false, 2, 0))
        )
        _ <- PlayerMatchRef.insert(PlayerMatchRef(pidA, matchId, isLive = false, isTeam1 = true, boardIdx = 1))
        result <- UsernameRenameResolver.resolveCurrentUsername(client, Username("alpha"), Some(pidA))
      } yield assertTrue(result.contains(Username("newa")))
    }

  private def testResolveAndReconcileUpdatesPlayer =
    test("resolveAndReconcile returns the verified ApiPlayer and leaves Player table consistent") {
      // Terminal state we want to test against:
      //   pidA: current=newa (already reflects the rename in the DB)
      //   pidB: current=alpha (recycled-handle holder)
      //   player_name: pidA held alpha before newa — historical evidence of the rename
      // resolveAndReconcile should return Some((newa, ApiPlayer)) and reconcile should be a no-op since
      // Player(pidA).username already matches.
      val responses = Map(
        s"player/newa" -> apiPlayerJson(PlayerId.unwrap(pidA), "newa")
      )
      for {
        client <- fakeChessComClient(responses)
        _      <- insertRenamed(pidA, "alpha", "newa")
        _      <- insertPlayer(pidB, "alpha")
        result <- UsernameRenameResolver.resolveAndReconcile(client, Username("alpha"), Some(pidA))
        post   <- Player.selectId(pidA).someOrFailException
      } yield assertTrue(
        result.exists((u, ap) => u == Username("newa") && ap.playerId == pidA),
        post.username == Username("newa")
      )
    }
}
