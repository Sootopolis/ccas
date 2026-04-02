package ccas.analysis.apps.history

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import ccas.utils.sql.PostgresClient
import zio.{RIO, ZIO}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, ClubMatch, HistoryPendingMatch, Tables}
import ccas.api.misc.enums.{ClubMatchStatus, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug}
import ccas.utils.{CcasLogger, TestCcasLogger}
import ccas.utils.client.{ChessComClient, TestChessComClientSupport}
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
      status = status,
      timeClass = TimeClass.Daily,
      startTime = Some(Times.t0),
      endTime = if (status == ClubMatchStatus.Finished) Some(Times.t1) else None,
      boards = 10,
      team1ClubId = Some(clubId),
      team1ScoreX2 = 10,
      team2ClubId = Some(ClubId(999)),
      team2ScoreX2 = 10,
      fetchedAt = Times.t1
    )

  private def apiClubMatchesJson(finishedIds: List[Long], inProgressIds: List[Long] = Nil): String = {
    def finishedEntry(id: Long): String =
      s"""{"name":"Match $id","@id":"https://api.chess.com/pub/match/$id","opponent":"https://api.chess.com/pub/club/other","time_class":"daily","start_time":${Times.t0.getEpochSecond},"result":"win"}"""
    def inProgressEntry(id: Long): String =
      s"""{"name":"Match $id","@id":"https://api.chess.com/pub/match/$id","opponent":"https://api.chess.com/pub/club/other","time_class":"daily","start_time":${Times.t0.getEpochSecond}}"""
    s"""{"finished":[${finishedIds.map(finishedEntry).mkString(",")}],"in_progress":[${inProgressIds.map(
        inProgressEntry
      ).mkString(",")}],"registered":[]}"""
  }

  private def fakeChessComClient(
    clubMatchesJson: String
  ): RIO[PostgresClient, ChessComClient] = {
    val routes: Routes[Any, Response] = Routes(
      Method.GET / "pub" / "club" / string("club") / "matches" -> handler { (_: String, _: Request) =>
        Response.json(clubMatchesJson)
      }
    )
    TestChessComClientSupport.fakeClient(routes)
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
      count <- HistorySeeding.seedFromClubMatches(client, clubId, clubSlug)
        .provideSomeEnvironment[PostgresClient](_.add[CcasLogger](TestCcasLogger.noop))
      pending <- HistoryPendingMatch.selectClub(clubId)
    } yield assertTrue(count == 0, pending.isEmpty)
  }

  private def testSeedsAllWhenNoneKnown = test("seeds all matches when none are in club_match") {
    val json = apiClubMatchesJson(List(2001, 2002, 2003))
    for {
      _      <- Club.upsert(club)
      _      <- Club.upsert(opponentClub)
      client <- fakeChessComClient(json)
      count <- HistorySeeding.seedFromClubMatches(client, clubId, clubSlug)
        .provideSomeEnvironment[PostgresClient](_.add[CcasLogger](TestCcasLogger.noop))
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
      _      <- Club.upsert(club)
      _      <- Club.upsert(opponentClub)
      _      <- ClubMatch.upsert(clubMatchRow(3001))
      client <- fakeChessComClient(json)
      count <- HistorySeeding.seedFromClubMatches(client, clubId, clubSlug)
        .provideSomeEnvironment[PostgresClient](_.add[CcasLogger](TestCcasLogger.noop))
      pending <- HistoryPendingMatch.selectClub(clubId)
      _       <- ZIO.foreachDiscard(pending)(p => HistoryPendingMatch.delete(clubId, p.matchId, p.isLive))
    } yield assertTrue(
      count == 2,
      pending.map(_.matchId).toSet == Set(ClubMatchId(3002), ClubMatchId(3003))
    )
  }
}
