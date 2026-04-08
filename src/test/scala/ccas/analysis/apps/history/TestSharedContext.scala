package ccas.analysis.apps.history

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.{RIO, ZIO}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.history.HistoryUtils.*
import ccas.analysis.tables.*
import ccas.api.misc.enums.{ClubMatchStatus, PlayerStatusCategory, TimeClass}
import ccas.api.misc.subtypes.*
import ccas.utils.{CcasLogger, TestCcasLogger}
import ccas.utils.client.{ChessComClient, TestChessComClientSupport}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}

object TestSharedContext extends ZIOSpecDefault {

  private object Times {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(30))
  }

  private val clubAId   = ClubId(100)
  private val clubASlug = ClubSlug("club-a")
  private val clubBId   = ClubId(200)
  private val clubBSlug = ClubSlug("club-b")
  private val clubA     = Club(clubAId, Times.t0, clubASlug, "Club A")
  private val clubB     = Club(clubBId, Times.t0, clubBSlug, "Club B")

  private val player1Id = PlayerId(1001)
  private val player1   = Player(player1Id, Times.t0, Username("alice"), PlayerStatusCategory.Active, None, Times.t0)
  private val player2Id = PlayerId(1002)
  private val player2   = Player(player2Id, Times.t0, Username("bob"), PlayerStatusCategory.Active, None, Times.t0)

  private def clubMatchRow(matchId: Long): ClubMatch =
    ClubMatch(
      matchId = ClubMatchId(matchId),
      name = s"Match $matchId",
      status = ClubMatchStatus.Finished,
      timeClass = TimeClass.Daily,
      startTime = Some(Times.t0),
      endTime = Some(Times.t1),
      boards = 10,
      team1ClubId = Some(clubAId),
      team1ScoreX2 = 10,
      team2ClubId = Some(clubBId),
      team2ScoreX2 = 10,
      fetchedAt = Times.t1
    )

  override def spec: Spec[Any, Throwable] = suite("SharedContext")(
    testSeedStaleFiltersProcessedMatches,
    testSeedFromMembersSkipsSharedQueried,
    testSeedFromMembersWritesHistoryMemberQueryForSkipped
  ).provideShared(
    FreshSchemaLayer("test_shared_ctx", Tables.ensureTables)
  ) @@ TestAspect.sequential

  // --- seedStaleMatches ---

  private def testSeedStaleFiltersProcessedMatches =
    test("seedStaleMatches excludes matches in shared.processedMatches") {
      for {
        _ <- Club.upsert(clubA)
        _ <- Club.upsert(clubB)
        _ <- ClubMatch.upsert(clubMatchRow(5001))
        _ <- ClubMatch.upsert(clubMatchRow(5002))
        _ <- ClubMatch.upsert(clubMatchRow(5003))

        shared <- SharedContext.make
        _      <- shared.processedMatches.set(Set(ClubMatchId(5001), ClubMatchId(5003)))

        // --refresh re-queues all matches for club A; shared should filter out 5001 and 5003
        count <- HistorySeeding.seedStaleMatches(clubAId, refresh = true, Some(shared))
        pending <- HistoryPendingMatch.selectClub(clubAId)
        _ <- ZIO.foreachDiscard(pending)(p => HistoryPendingMatch.delete(clubAId, p.matchId, p.isLive))
      } yield assertTrue(
        count == 1,
        pending.map(_.matchId) == List(ClubMatchId(5002))
      )
    }

  // --- seedFromMemberMatches ---

  private def fakeChessComClient(
    playerMatchJson: Map[String, String]
  ): RIO[PostgresClient, ChessComClient] = {
    val routes: Routes[Any, Response] = Routes(
      Method.GET / "pub" / "player" / string("username") / "matches" -> handler {
        (username: String, _: Request) =>
          playerMatchJson.get(username) match {
            case Some(json) => Response.json(json)
            case None       => Response.status(Status.NotFound)
          }
      }
    )
    TestChessComClientSupport.fakeClient(routes)
  }

  private def playerMatchesJson(clubSlug: String, matchIds: List[Long]): String = {
    val entries = matchIds.map { id =>
      s"""{"name":"M","url":"https://www.chess.com/club/matches/$id","@id":"https://api.chess.com/pub/match/$id","club":"https://api.chess.com/pub/club/$clubSlug","results":null,"board":null}"""
    }
    s"""{"finished":[${entries.mkString(",")}],"in_progress":[],"registered":[]}"""
  }

  private def testSeedFromMembersSkipsSharedQueried =
    test("seedFromMemberMatches skips players already in shared.queriedPlayers") {
      for {
        _ <- Club.upsert(clubA)
        _ <- Club.upsert(clubB)
        _ <- Player.insertIfNew(player1)
        _ <- Player.insertIfNew(player2)
        _ <- ClubMember.insert(ClubMember(clubAId, player1Id, Times.t0, None, sinceApproximate = false))
        _ <- ClubMember.insert(ClubMember(clubAId, player2Id, Times.t0, None, sinceApproximate = false))

        allMembers <- ClubMember.selectClub(clubAId)
        playerById = Map(player1Id -> player1, player2Id -> player2)

        // alice was already queried by a prior club
        shared <- SharedContext.make
        _      <- shared.queriedPlayers.set(Set(player1Id))

        // Only bob's match list should be fetched
        client <- fakeChessComClient(Map("bob" -> playerMatchesJson("club-a", List(6001))))
        result <- HistorySeeding
          .seedFromMemberMatches(client, clubAId, clubASlug, allMembers, Set.empty, playerById, Set.empty, Some(shared))
          .provideSomeEnvironment[PostgresClient](_.add[CcasLogger](TestCcasLogger.noop))
        _ <- ZIO.foreachDiscard(List(ClubMatchId(6001)))(id =>
          HistoryPendingMatch.delete(clubAId, id, isLive = false)
        )
      } yield assertTrue(
        result.queried == 1,
        result.seeded == 1
      )
    }

  private def testSeedFromMembersWritesHistoryMemberQueryForSkipped =
    test("seedFromMemberMatches writes HistoryMemberQuery for shared-skipped members") {
      for {
        _ <- Club.upsert(clubA)
        _ <- Club.upsert(clubB)
        _ <- Player.insertIfNew(player1)
        _ <- Player.insertIfNew(player2)
        _ <- ClubMember.insert(ClubMember(clubBId, player1Id, Times.t0, None, sinceApproximate = false))

        allMembers <- ClubMember.selectClub(clubBId)
        playerById = Map(player1Id -> player1)

        // alice was already queried by club A
        shared <- SharedContext.make
        _      <- shared.queriedPlayers.set(Set(player1Id))

        client <- fakeChessComClient(Map.empty) // no API calls expected
        _ <- HistorySeeding
          .seedFromMemberMatches(client, clubBId, clubBSlug, allMembers, Set.empty, playerById, Set.empty, Some(shared))
          .provideSomeEnvironment[PostgresClient](_.add[CcasLogger](TestCcasLogger.noop))

        // HistoryMemberQuery should be recorded for club B even though alice was skipped
        queriedForB <- HistoryMemberQuery.selectClubPlayerIds(clubBId)
      } yield assertTrue(queriedForB.contains(player1Id))
    }
}
