package ccas.analysis.apps.history

import java.net.UnknownHostException
import java.time.Instant

import zio.{durationInt, Exit, Promise, RIO, Ref, Scope, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.history.HistoryUtils.{DiscoveredPlayer, ProcessingContext}
import ccas.analysis.apps.recruitment.RecruitmentTestSupport.apiPlayerJson
import ccas.analysis.tables.{Club, ClubMember, Player, Tables}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.utils.client.{BodyStore, ChessComClient, NetworkUnavailableException, TestChessComClientSupport}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.ProgressDisplay

/** What History records for a player it finds on a match board, and whose match lists it crawls. */
object TestDiscoverPlayer extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestDiscoverPlayer")(
    testNewTeammateIsCrawledWithNoMemberRow,
    testNewOpponentIsNotCrawled,
    testFormerOpponentIsCrawledOnOurTeam,
    testOneWaitingOnAnOpponentsDiscoveryIsCrawled,
    testATeammateIsCrawledOnce,
    testAMemberTheRunStartsFromIsNotCrawledAgain,
    testProfileOutageWritesNothing
  ).provideShared(
    FreshSchemaLayer("test_discover_player", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false)),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private val t0 = Instant.parse("2024-01-01T00:00:00Z")

  /** One club and one player per test, so tests sharing the schema never see each other's rows. */
  private final case class Fixture(clubId: ClubId, clubSlug: ClubSlug, playerId: PlayerId, username: Username) {
    val discovered: DiscoveredPlayer = DiscoveredPlayer(playerId, username)

    def context(client: ChessComClient): RIO[PostgresClient, ProcessingContext] =
      contextKnowing(client, Map.empty, Set.empty)

    def contextKnowing(
      client: ChessComClient,
      knownPlayers: Map[String, PlayerId],
      crawled: Set[PlayerId]
    ): RIO[PostgresClient, ProcessingContext] =
      Club.upsert(Club(clubId, t0, clubSlug, "Discover test", None, None, None)) *>
        ProcessingContext.make(client, clubId, clubSlug, knownPlayers, crawled)

    def resolve(ctx: ProcessingContext, isOurTeam: Boolean): RIO[ProgressDisplay & PostgresClient, Option[PlayerId]] =
      HistoryProcessing.resolvePlayerId(ctx, username, isOurTeam)
  }

  /** Serves `f`'s active profile, counting every request made of Chess.com. */
  private def playerClient(f: Fixture, requests: Ref[Int]): RIO[PostgresClient & BodyStore, ChessComClient] =
    TestChessComClientSupport.fakeClient(
      Routes(
        Method.GET / "pub" / "player" / string("username") -> handler { (_: String, _: Request) =>
          requests.update(_ + 1).as(Response.json(apiPlayerJson(f.playerId.value, f.username.value)))
        }
      )
    )

  private def testNewTeammateIsCrawledWithNoMemberRow =
    test("a new player on our team is recorded and crawled, with no member row and no request for their clubs") {
      val f = Fixture(ClubId(930_100), ClubSlug("discover-new"), PlayerId(930_101), Username("new-teammate"))
      for {
        requests   <- Ref.make(0)
        client     <- playerClient(f, requests)
        ctx        <- f.context(client)
        resolved   <- f.resolve(ctx, isOurTeam = true)
        player     <- Player.selectId(f.playerId)
        members    <- ClubMember.selectClub(f.clubId)
        discovered <- ctx.playersDiscovered.get
        newPlayers <- ctx.newPlayers.get
        made       <- requests.get
      } yield assertTrue(
        resolved.contains(f.playerId),
        player.nonEmpty,
        members.isEmpty,
        discovered == 1,
        newPlayers == Set(f.discovered),
        made == 1
      )
    }

  private def testNewOpponentIsNotCrawled =
    test("a new player on the other team is recorded but not crawled") {
      val f = Fixture(ClubId(930_200), ClubSlug("discover-opponent"), PlayerId(930_201), Username("new-opponent"))
      for {
        requests   <- Ref.make(0)
        client     <- playerClient(f, requests)
        ctx        <- f.context(client)
        resolved   <- f.resolve(ctx, isOurTeam = false)
        newPlayers <- ctx.newPlayers.get
      } yield assertTrue(resolved.contains(f.playerId), newPlayers.isEmpty)
    }

  /** The lookup meets the name it cached when it recorded them as an opponent (#302). */
  private def testFormerOpponentIsCrawledOnOurTeam =
    test("a player first met as an opponent is crawled when met on our team later in the run") {
      val f = Fixture(ClubId(930_300), ClubSlug("discover-former"), PlayerId(930_301), Username("former-opponent"))
      for {
        requests   <- Ref.make(0)
        client     <- playerClient(f, requests)
        ctx        <- f.context(client)
        _          <- f.resolve(ctx, isOurTeam = false)
        _          <- f.resolve(ctx, isOurTeam = true)
        newPlayers <- ctx.newPlayers.get
        known      <- ctx.playersKnown.get
        made       <- requests.get
      } yield assertTrue(newPlayers == Set(f.discovered), known == 1, made == 1)
    }

  /** The discovery another board started, on the other team, is still under way when this lookup meets the name. */
  private def testOneWaitingOnAnOpponentsDiscoveryIsCrawled =
    test("a lookup on our team that waits for one discovering the player as an opponent still crawls them") {
      val f = Fixture(ClubId(930_400), ClubSlug("discover-waiting"), PlayerId(930_401), Username("raced-player"))
      for {
        requests   <- Ref.make(0)
        client     <- playerClient(f, requests)
        ctx        <- f.context(client)
        asOpponent <- Promise.make[Throwable, Option[DiscoveredPlayer]]
        _          <- ctx.discoveryCache.set(Map(f.username.value -> asOpponent))
        onOurTeam  <- f.resolve(ctx, isOurTeam = true).fork
        _          <- asOpponent.succeed(Some(f.discovered))
        resolved   <- onOurTeam.join
        newPlayers <- ctx.newPlayers.get
        made       <- requests.get
      } yield assertTrue(resolved.contains(f.playerId), newPlayers == Set(f.discovered), made == 0)
    }

  private def testATeammateIsCrawledOnce =
    test("a player met on our team on several boards in a run is crawled once") {
      val f = Fixture(ClubId(930_500), ClubSlug("discover-once"), PlayerId(930_501), Username("busy-teammate"))
      for {
        requests   <- Ref.make(0)
        client     <- playerClient(f, requests)
        ctx        <- f.context(client)
        _          <- f.resolve(ctx, isOurTeam = true)
        _          <- ctx.newPlayers.set(Set.empty) // as the next wave starts
        _          <- f.resolve(ctx, isOurTeam = true)
        newPlayers <- ctx.newPlayers.get
      } yield assertTrue(newPlayers.isEmpty)
    }

  /** Phase 2 queries the match list of every member it starts from, unless an earlier run did. */
  private def testAMemberTheRunStartsFromIsNotCrawledAgain =
    test("a member the run started from is not crawled again when met on our team") {
      val f = Fixture(ClubId(930_600), ClubSlug("discover-member"), PlayerId(930_601), Username("known-member"))
      for {
        requests   <- Ref.make(0)
        client     <- playerClient(f, requests)
        ctx        <- f.contextKnowing(client, Map(f.username.value -> f.playerId), Set(f.playerId))
        resolved   <- f.resolve(ctx, isOurTeam = true)
        newPlayers <- ctx.newPlayers.get
        made       <- requests.get
      } yield assertTrue(resolved.contains(f.playerId), newPlayers.isEmpty, made == 0)
    }

  private def testProfileOutageWritesNothing =
    test("a network outage on the profile fetch re-raises, recording and crawling no one") {
      val f = Fixture(ClubId(930_700), ClubSlug("discover-outage"), PlayerId(930_701), Username("outage-teammate"))
      for {
        clientAndRefs <- TestChessComClientSupport.makeClient(
          handler = _ => ZIO.fail(UnknownHostException("api.chess.com: Temporary failure in name resolution")),
          retryBase = 10.millis,
          maxConnectionRetries = 2
        )
        ctx        <- f.context(clientAndRefs._1)
        exit       <- f.resolve(ctx, isOurTeam = true).exit
        player     <- Player.selectId(f.playerId)
        newPlayers <- ctx.newPlayers.get
      } yield assertTrue(isNetworkUnavailable(exit), player.isEmpty, newPlayers.isEmpty)
    }

  private def isNetworkUnavailable(exit: Exit[Throwable, Any]): Boolean =
    exit match {
      case Exit.Failure(cause) => cause.failures.exists(_.isInstanceOf[NetworkUnavailableException])
      case _                   => false
    }
}
