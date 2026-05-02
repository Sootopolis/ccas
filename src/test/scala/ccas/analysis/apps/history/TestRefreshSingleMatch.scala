package ccas.analysis.apps.history

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.{Ref, RIO}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, TestResult, ZIOSpecDefault}

import ccas.analysis.apps.history.HistoryUtils.ProcessingContext
import ccas.analysis.tables.{
  ApiResponseCache,
  Club,
  ClubMatch,
  ClubMatchBoard,
  ClubMatchGame,
  ClubMatchRef,
  Tables,
  UnresolvedBoardPlayer,
  UnresolvedMatchClub
}
import ccas.api.clubmatch.ApiDailyMatch
import ccas.api.misc.enums.{ClubMatchStatus, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug}
import ccas.utils.ProgressDisplay
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.connectZIO

object TestRefreshSingleMatch extends ZIOSpecDefault {

  private val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  private val clubId       = ClubId(700)
  private val opponentId   = ClubId(999)
  private val clubSlug     = ClubSlug("test-club")
  private val club         = Club(clubId, t0, clubSlug, "Test Club", None, None, None)
  private val opponentClub = Club(opponentId, t0, ClubSlug("other"), "Other Club", None, None, None)

  /** Opaque body string. Never decoded on the Unchanged path so it doesn't need to satisfy `ApiDailyMatch`'s
    * schema — only the bytes matter (for SHA-256 dedupe in the `IdenticalBody` case).
    */
  private val cannedBody = """{"placeholder":true}"""

  /** Initial `fetched_at` on the seeded `ClubMatch`. `Instant.EPOCH` is decoupled from any fixture date so
    * `isAfter(EPOCH)` proves the refresh wrote a current-time value regardless of when the suite runs.
    */
  private val initialFetchedAt: Instant = Instant.EPOCH

  private def clubMatchRow(matchId: Long): ClubMatch =
    ClubMatch(
      matchId      = ClubMatchId(matchId),
      name         = s"Match $matchId",
      status       = ClubMatchStatus.Finished,
      timeClass    = TimeClass.Daily,
      startTime    = Some(t0),
      endTime      = Some(t0.plus(Duration.ofDays(7))),
      boards       = 10,
      team1ClubId  = Some(clubId),
      team1ScoreX2 = 10,
      team2ClubId  = Some(opponentId),
      team2ScoreX2 = 10,
      fetchedAt    = initialFetchedAt
    )

  private def seedFixtures(matchId: Long): RIO[PostgresClient, Unit] =
    Club.upsert(club) *> Club.upsert(opponentClub) *> ClubMatch.upsert(clubMatchRow(matchId)).unit

  private def runRefresh(ctx: ProcessingContext, matchId: ClubMatchId): RIO[PostgresClient, Unit] =
    HistoryProcessing.refreshSingleMatch(ctx, matchId)
      .provideSomeEnvironment[PostgresClient](_.add[ProgressDisplay](ProgressDisplay.make(enabled = false)))

  private def countPlayerMatchRef: RIO[PostgresClient, Long] =
    connectZIO(sql"SELECT COUNT(*) FROM player_match_ref".query[Long].run().head)

  private def assertNoDownstreamWrites(matchId: ClubMatchId): RIO[PostgresClient, TestResult] =
    for {
      boards            <- ClubMatchBoard.selectMatch(matchId)
      games             <- ClubMatchGame.selectMatch(matchId)
      clubRef           <- ClubMatchRef.selectId(clubId)
      pmrCount          <- countPlayerMatchRef
      unresolvedClubs   <- UnresolvedMatchClub.selectAll
      unresolvedPlayers <- UnresolvedBoardPlayer.selectAll
    } yield assertTrue(
      boards.isEmpty,
      games.isEmpty,
      clubRef.isEmpty,
      pmrCount == 0L,
      unresolvedClubs.isEmpty,
      unresolvedPlayers.isEmpty
    )

  /** Drives one Unchanged-variant scenario end-to-end: seed fixtures, pre-seed cache, build a counting fake client
    * around the supplied route response, run `refreshSingleMatch` once, assert counter / fetched_at / no downstream
    * writes / expected route hit count.
    */
  private def runUnchangedCase(
    matchId: ClubMatchId,
    cacheMaxAge: Long,
    cacheFetchedAt: Instant,
    expectedCalls: Int
  )(routeResponse: => Response): RIO[PostgresClient, TestResult] = {
    val url = ApiDailyMatch.getUrl(matchId).encode
    for {
      _ <- seedFixtures(matchId.value)
      _ <- ApiResponseCache.upsertWithBody(
        url           = url,
        body          = cannedBody,
        etag          = Some("\"v1\""),
        lastModified  = None,
        maxAgeSeconds = Some(cacheMaxAge),
        contentType   = Some("application/json"),
        fetchedAt     = cacheFetchedAt
      )
      netCalls <- Ref.make(0)
      routes = Routes(
        Method.GET / "pub" / "match" / long("matchId") -> handler { (_: Long, _: Request) =>
          netCalls.update(_ + 1).as(routeResponse)
        }
      )
      client <- TestChessComClientSupport.fakeClient(routes)
      ctx    <- ProcessingContext.make(client, clubId, clubSlug, Map.empty)
      _      <- runRefresh(ctx, matchId)

      calls          <- netCalls.get
      unchangedCount <- ctx.refreshMatchUnchanged.get
      boardsUpdated  <- ctx.matchesBoardsUpdated.get
      rowAfter       <- ClubMatch.selectId(matchId)
      downstream     <- assertNoDownstreamWrites(matchId)
    } yield assertTrue(
      calls == expectedCalls,
      unchangedCount == 1,
      boardsUpdated == 0,
      rowAfter.exists(_.fetchedAt.isAfter(initialFetchedAt))
    ) && downstream
  }

  override def spec: Spec[Any, Throwable] = suite("refreshSingleMatch unchanged path")(
    testFreshSkipsNetworkAndPipeline,
    testRevalidatedSkipsPipeline,
    testIdenticalBodySkipsPipeline
  ).provideShared(
    FreshSchemaLayer("test_refresh_single_match", Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private def testFreshSkipsNetworkAndPipeline =
    test("Fresh: cache hit within max-age skips network and pipeline, bumps counter and fetched_at") {
      // Cache row is within max-age, so the route should never be reached. Returning a normal 200 keeps the failure
      // mode a missed `calls == 0` assertion rather than a propagated HTTP exception.
      runUnchangedCase(
        matchId        = ClubMatchId(8001),
        cacheMaxAge    = 3600,
        cacheFetchedAt = Instant.now(),
        expectedCalls  = 0
      ) {
        Response.json(cannedBody)
          .addHeader(Header.CacheControl.MaxAge(3600))
          .addHeader(Header.ETag.Strong("v1"))
      }
    }

  private def testRevalidatedSkipsPipeline =
    test("Revalidated: stale entry + 304 response skips pipeline, bumps counter and fetched_at") {
      runUnchangedCase(
        matchId        = ClubMatchId(8002),
        cacheMaxAge    = 0,
        cacheFetchedAt = Instant.now().minus(Duration.ofHours(1)),
        expectedCalls  = 1
      ) {
        Response(status = Status.NotModified)
      }
    }

  private def testIdenticalBodySkipsPipeline =
    test("IdenticalBody: stale entry + 200 with byte-identical body skips pipeline, bumps counter and fetched_at") {
      // Distinct ETag so the route doesn't coincidentally match a 304 path; identical body bytes so SHA-256 dedupe
      // in `ApiResponseBody.ensureBody` returns the same `body_id`, yielding `IdenticalBody`.
      runUnchangedCase(
        matchId        = ClubMatchId(8003),
        cacheMaxAge    = 0,
        cacheFetchedAt = Instant.now().minus(Duration.ofHours(1)),
        expectedCalls  = 1
      ) {
        Response.json(cannedBody)
          .addHeader(Header.CacheControl.MaxAge(3600))
          .addHeader(Header.ETag.Strong("v2"))
      }
    }
}
