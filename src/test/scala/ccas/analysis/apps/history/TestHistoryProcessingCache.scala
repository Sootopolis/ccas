package ccas.analysis.apps.history

import java.time.{Instant, LocalDateTime, ZoneOffset}

import zio.{Ref, RIO}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.history.HistoryUtils.{ProcessingContext, RunStats}
import ccas.analysis.tables.{Club, HistoryPendingMatch, Tables}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug}
import ccas.utils.ProgressDisplay
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.client.TestChessComClientSupport.reportedNotFoundBody
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}

/** `fetchMatch`'s cache (`ProcessingContext.matchCache`) is scoped to the whole club run, not just one wave, so a
  * match that resurfaces as pending later in the same run must reuse the first fetch's outcome — including a
  * cached `ReportedNotFound` failure, replayed to the second waiter without a second network call — rather than
  * re-hitting the network. Considered moving this cache to carry `FetchResult[T]` during #258 and reverted:
  * `fetchMatch` has one consumer that always wants "fail on absence," so there was nothing to fold differently,
  * and caching `FetchResult`'s unmemoized `Unchanged#getValue` would have made every waiter redo the decode.
  */
object TestHistoryProcessingCache extends ZIOSpecDefault {

  private val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  private val clubId   = ClubId(720)
  private val clubSlug = ClubSlug("cache-test-club")
  private val club     = Club(clubId, t0, clubSlug, "Cache Test Club", None, None, None)
  private val matchId  = ClubMatchId(9001)

  private def runWave(ctx: ProcessingContext): RIO[PostgresClient, RunStats] =
    HistoryProcessing.processWaves(ctx, excludeMatchIds = Set.empty)
      .provideSomeEnvironment[PostgresClient](_.add[ProgressDisplay](ProgressDisplay.make(enabled = false)))

  override def spec: Spec[Any, Throwable] = suite("HistoryProcessing match-fetch cache")(
    test("a match re-queued later in the same run reuses the first fetch's outcome, not a second network call") {
      for {
        _ <- Club.upsert(club)
        _ <- HistoryPendingMatch.insert(HistoryPendingMatch(clubId, matchId, isLive = false))
        netCalls <- Ref.make(0)
        routes = Routes(
          Method.GET / "pub" / "match" / long("matchId") -> handler { (_: Long, _: Request) =>
            netCalls.update(_ + 1).as(Response.json(reportedNotFoundBody).status(Status.NotFound))
          }
        )
        client <- TestChessComClientSupport.fakeClient(routes)
        ctx    <- ProcessingContext.make(client, clubId, clubSlug, Map.empty)

        _                 <- runWave(ctx)
        callsAfterFirst   <- netCalls.get
        abortedAfterFirst <- ctx.matchesAborted.get
        pendingAfterFirst <- HistoryPendingMatch.selectClub(clubId)

        // Simulate the match resurfacing as pending later in the same club run (e.g. re-discovered via another
        // club's match list) — same clubId, same matchId, fresh row since the first wave deleted it on abort.
        _ <- HistoryPendingMatch.insert(HistoryPendingMatch(clubId, matchId, isLive = false))
        _                  <- runWave(ctx)
        callsAfterSecond   <- netCalls.get
        abortedAfterSecond <- ctx.matchesAborted.get
        pendingAfterSecond <- HistoryPendingMatch.selectClub(clubId)
      } yield assertTrue(
        callsAfterFirst == 1,
        abortedAfterFirst == 1,
        pendingAfterFirst.isEmpty,
        callsAfterSecond == 1, // still 1: the second wave's fetchMatch hit the cached Promise, not the network
        abortedAfterSecond == 2, // processMatchBatch's catchAll still fires from the cached Missing outcome
        pendingAfterSecond.isEmpty
      )
    }
  ).provideShared(
    FreshSchemaLayer("test_history_processing_cache", Tables.ensureTables)
  ) @@ TestAspect.withLiveClock
}
