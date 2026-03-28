package ccas.analysis.apps.history

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.Transactor
import zio.{durationInt, Chunk, Fiber, RIO, Ref, Scope, Semaphore, Trace, ZIO}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, ClubMatch, HistoryPendingMatch, Tables}
import ccas.api.misc.enums.{ClubMatchResult, ClubMatchStatus, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug}
import ccas.utils.{CcasLogger, TestCcasLogger}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.FreshSchemaLayer

object TestSeedFromClubMatches extends ZIOSpecDefault {

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(30))
  }

  private val clubId       = ClubId(500)
  private val clubSlug     = ClubSlug("test-club")
  private val club         = Club(clubId, Times.t0, clubSlug, "Test Club")
  private val opponentId   = ClubId(999)
  private val opponentClub = Club(opponentId, Times.t0, ClubSlug("other"), "Other Club")

  private def clubMatchRow(matchId: Long, status: ClubMatchStatus = ClubMatchStatus.Finished): ClubMatch =
    ClubMatch(
      matchId = ClubMatchId(matchId),
      name = s"Match $matchId",
      url = s"https://www.chess.com/club/matches/$matchId",
      status = status,
      timeClass = TimeClass.Daily,
      startTime = Some(Times.t0),
      endTime = if (status == ClubMatchStatus.Finished) Some(Times.t1) else None,
      boards = 10,
      team1ClubId = Some(clubId),
      team1Score = 5.0,
      team1Result = if (status == ClubMatchStatus.Finished) Some(ClubMatchResult.Win) else None,
      team2ClubId = Some(ClubId(999)),
      team2Score = 5.0,
      team2Result = if (status == ClubMatchStatus.Finished) Some(ClubMatchResult.Lose) else None,
      fetchedAt = Times.t1
    )

  private def apiClubMatchesJson(finishedIds: List[Long], inProgressIds: List[Long] = Nil): String = {
    def finishedEntry(id: Long): String =
      s"""{"name":"Match $id","@id":"https://api.chess.com/pub/match/$id","opponent":"https://api.chess.com/pub/club/other","time_class":"daily","start_time":${Times.t0.getEpochSecond},"result":"win"}"""
    def inProgressEntry(id: Long): String =
      s"""{"name":"Match $id","@id":"https://api.chess.com/pub/match/$id","opponent":"https://api.chess.com/pub/club/other","time_class":"daily","start_time":${Times.t0.getEpochSecond}}"""
    s"""{"finished":[${finishedIds.map(finishedEntry).mkString(",")}],"in_progress":[${inProgressIds.map(inProgressEntry).mkString(",")}],"registered":[]}"""
  }

  private def fakeChessComClient(
    clubMatchesJson: String
  ): RIO[Transactor, ChessComClient] =
    for {
      transactor    <- ZIO.service[Transactor]
      semaphore     <- Semaphore.make(1)
      stateRef      <- Ref.make(ChessComClient.ThrottleState(1, 0, Vector.empty))
      reserveRef    <- Ref.make(Chunk.empty[Fiber.Runtime[Nothing, Nothing]])
      adjustMutex   <- Semaphore.make(1)
      activeRef     <- Ref.make(0)
      rateLimitGate <- Semaphore.make(1)
      lastReqRef    <- Ref.make(0L)
      ema           <- Ref.make(0.0)
      bar           <- TestCcasLogger.noopBar
    } yield {
      val routes: Routes[Any, Response] = Routes(
        Method.GET / "pub" / "club" / string("club") / "matches" -> handler { (_: String, _: Request) =>
          Response.json(clubMatchesJson)
        }
      )
      val driver = new ZClient.Driver[Any, Scope, Throwable] {
        override def request(
          version: Version,
          method: Method,
          url: URL,
          headers: Headers,
          body: Body,
          sslConfig: Option[ClientSSLConfig],
          proxy: Option[Proxy]
        )(implicit trace: Trace): ZIO[Scope, Throwable, Response] =
          routes.runZIO(Request(method = method, url = url, headers = headers, body = body))

        override def socket[Env1 <: Any](
          version: Version,
          url: URL,
          headers: Headers,
          app: WebSocketApp[Env1]
        )(implicit
          trace: Trace,
          ev: Scope =:= Scope
        ): ZIO[Env1 & Scope, Throwable, Response] =
          ZIO.die(new UnsupportedOperationException)
      }
      val refs = ChessComClient.ThrottleRefs(semaphore, stateRef, reserveRef, adjustMutex, activeRef, rateLimitGate, lastReqRef, ema)
      ChessComClient(
        ZClient.fromDriver(driver),
        transactor,
        Headers.empty,
        TestCcasLogger.noop,
        refs,
        bar,
        ChessComClient.ThrottleConfig(1, 30.seconds, 1.second, 5.seconds, 10.seconds, 20, 0.2, 10)
      )
    }

  private val layer = FreshSchemaLayer("test_seed_club", Tables.ensureTables)

  override def spec: Spec[Any, Throwable] = suite("seedFromClubMatches")(
    testSkipsKnownMatches,
    testSeedsAllWhenNoneKnown,
    testSeedsNewAlongsideKnown
  ).provideLayerShared(layer) @@ TestAspect.sequential

  private def testSkipsKnownMatches = test("returns 0 and seeds nothing when all matches are already known") {
    val json = apiClubMatchesJson(List(1001, 1002))
    for {
      _      <- Club.upsert(club)
      _      <- Club.upsert(opponentClub)
      _      <- ClubMatch.upsert(clubMatchRow(1001))
      _      <- ClubMatch.upsert(clubMatchRow(1002))
      client <- fakeChessComClient(json)
      count  <- HistorySeeding.seedFromClubMatches(client, clubId, clubSlug)
        .provideSomeEnvironment[Transactor](_.add[CcasLogger](TestCcasLogger.noop))
      pending <- HistoryPendingMatch.selectClub(clubId)
    } yield assertTrue(count == 0, pending.isEmpty)
  }

  private def testSeedsAllWhenNoneKnown = test("seeds all matches when none are in club_match") {
    val json = apiClubMatchesJson(List(2001, 2002, 2003))
    for {
      _       <- Club.upsert(club)
      _       <- Club.upsert(opponentClub)
      client  <- fakeChessComClient(json)
      count   <- HistorySeeding.seedFromClubMatches(client, clubId, clubSlug)
        .provideSomeEnvironment[Transactor](_.add[CcasLogger](TestCcasLogger.noop))
      pending <- HistoryPendingMatch.selectClub(clubId)
      _       <- ZIO.foreachDiscard(pending)(p => HistoryPendingMatch.delete(clubId, p.matchId, p.isLive))
    } yield assertTrue(
      count == 3,
      pending.map(_.matchId).toSet == Set(ClubMatchId(2001), ClubMatchId(2002), ClubMatchId(2003))
    )
  }

  private def testSeedsNewAlongsideKnown = test("seeds only new matches when some are already known") {
    val json = apiClubMatchesJson(List(3001, 3002, 3003))
    for {
      _       <- Club.upsert(club)
      _       <- Club.upsert(opponentClub)
      _       <- ClubMatch.upsert(clubMatchRow(3001))
      client  <- fakeChessComClient(json)
      count   <- HistorySeeding.seedFromClubMatches(client, clubId, clubSlug)
        .provideSomeEnvironment[Transactor](_.add[CcasLogger](TestCcasLogger.noop))
      pending <- HistoryPendingMatch.selectClub(clubId)
      _       <- ZIO.foreachDiscard(pending)(p => HistoryPendingMatch.delete(clubId, p.matchId, p.isLive))
    } yield assertTrue(
      count == 2,
      pending.map(_.matchId).toSet == Set(ClubMatchId(3002), ClubMatchId(3003))
    )
  }
}
