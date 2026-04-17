package ccas.analysis.apps.history

import java.time.{Instant, LocalDateTime, ZoneOffset}

import zio.{Ref, RIO, ZIO}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.*
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.*
import ccas.utils.client.{ChessComClient, TestChessComClientSupport}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}

object TestSeedMatchesForPlayer extends ZIOSpecDefault {

  private val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  private val clubId   = ClubId(700)
  private val clubSlug = ClubSlug("test-club")
  private val club     = Club(clubId, t0, clubSlug, "Test Club", None, None, None)

  private val playerId = PlayerId(7001)
  private val username = Username("alice")
  private val player   = Player(playerId, t0, username, PlayerStatusCategory.Active, None, t0)

  private def playerMatchesJson(matchIds: List[Long]): String = {
    val entries = matchIds.map { id =>
      s"""{"name":"M","url":"https://www.chess.com/club/matches/$id","@id":"https://api.chess.com/pub/match/$id","club":"https://api.chess.com/pub/club/${clubSlug.value}","results":null,"board":null}"""
    }
    s"""{"finished":[${entries.mkString(",")}],"in_progress":[],"registered":[]}"""
  }

  /** Adds `Cache-Control: max-age=3600` + ETag and increments `counter` on every route hit. Lets the test prove
    * the cache layer served a second call from cache without touching the fake.
    */
  private def fakeChessComClientCounting(
    json: String,
    counter: Ref[Int]
  ): RIO[PostgresClient, ChessComClient] = {
    val routes: Routes[Any, Response] = Routes(
      Method.GET / "pub" / "player" / string("username") / "matches" -> handler {
        (_: String, _: Request) =>
          counter.update(_ + 1).as(
            Response.json(json)
              .addHeader(Header.CacheControl.MaxAge(3600))
              .addHeader(Header.ETag.Strong("v1"))
          )
      }
    )
    TestChessComClientSupport.fakeClient(routes)
  }

  override def spec: Spec[Any, Throwable] = suite("seedMatchesForPlayer")(
    testUnchangedResponseSkipsInsertsButStillStamps
  ).provideShared(
    FreshSchemaLayer("test_seed_matches_player", Tables.ensureTables)
  ) @@ TestAspect.sequential

  /** The `isUnchanged` branch must **skip** the per-player insert pipeline but **still** stamp
    * `HistoryMemberQuery` — otherwise the wave loop would re-query the player every iteration. This test wipes
    * both side effects after the first call so the second call's behaviour is visible on its own.
    */
  private def testUnchangedResponseSkipsInsertsButStillStamps =
    test("second call with unchanged response skips inserts but still stamps HistoryMemberQuery") {
      val json = playerMatchesJson(List(5001, 5002, 5003))
      for {
        _       <- Club.upsert(club)
        _       <- Player.insertIfNew(player)
        counter <- Ref.make(0)
        client  <- fakeChessComClientCounting(json, counter)
        first   <- HistorySeeding.seedMatchesForPlayer(client, clubId, clubSlug, playerId, username, Set.empty)
        pendingAfterFirst <- HistoryPendingMatch.selectClub(clubId)
        // Clear both side effects so the second call has to re-materialise anything it does.
        _ <- ZIO.foreachDiscard(pendingAfterFirst)(p => HistoryPendingMatch.delete(clubId, p.matchId, p.isLive))
        _ <- HistoryMemberQuery.deleteClub(clubId)
        second             <- HistorySeeding.seedMatchesForPlayer(client, clubId, clubSlug, playerId, username, Set.empty)
        pendingAfterSecond <- HistoryPendingMatch.selectClub(clubId)
        queriedAfterSecond <- HistoryMemberQuery.selectClubPlayerIds(clubId)
        netCalls           <- counter.get
      } yield assertTrue(
        first == 3,
        second == 0,
        pendingAfterSecond.isEmpty,            // unchanged branch took the skip — no re-insert
        queriedAfterSecond.contains(playerId), // stamp still fired — bookkeeping preserved
        netCalls == 1                          // within max-age → Fresh; second call never hit the route
      )
    }
}
