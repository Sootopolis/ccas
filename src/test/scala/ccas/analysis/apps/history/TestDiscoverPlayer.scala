package ccas.analysis.apps.history

import java.net.UnknownHostException
import java.time.Instant

import zio.{durationInt, Exit, RIO, Ref, Scope, UIO, ZEnvironment, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.history.HistoryUtils.{DiscoveredPlayer, ProcessingContext}
import ccas.analysis.apps.recruitment.RecruitmentTestSupport.apiPlayerJson
import ccas.analysis.tables.{Club, ClubMember, Player, Tables}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.utils.client.{BodyStore, ChessComClient, NetworkUnavailableException, TestChessComClientSupport}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.ProgressDisplay

/** The player and member rows History writes for a player it finds on a match board. */
object TestDiscoverPlayer extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestDiscoverPlayer")(
    testNewTeammateGetsExactMembership,
    testInactiveTeammatesMembershipEndsAtLastVisit,
    testFormerOpponentGetsMembershipOnOurTeam,
    testRowWrittenDuringFetchIsNotDuplicated,
    testClubsFetchErrorFallsBackToApproximate,
    testClubsOutageWritesNothing
  ).provideShared(
    FreshSchemaLayer("test_discover_player", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false)),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private val t0         = Instant.parse("2024-01-01T00:00:00Z")
  private val joinedClub = Instant.parse("2024-06-01T00:00:00Z")
  private val matchStart = Instant.parse("2025-03-01T00:00:00Z")
  private val lastVisit  = Instant.parse("2025-05-01T00:00:00Z")

  /** One club and one player per test, so tests sharing the schema never see each other's rows. */
  private final case class Fixture(clubId: ClubId, clubSlug: ClubSlug, playerId: PlayerId, username: Username) {
    val activeProfile: String = apiPlayerJson(playerId.value, username.value)

    def clubsJson(listsOurClub: Boolean): String = {
      val slug = if (listsOurClub) { clubSlug.value }
      else { "some-other-club" }
      s"""{"clubs": [{"name": "Club", "last_activity": 0, "url": "https://www.chess.com/club/$slug",
         |  "joined": ${joinedClub.getEpochSecond}}]}""".stripMargin
    }

    def member(since: Instant, until: Option[Instant], sinceApproximate: Boolean): ClubMember =
      ClubMember(
        clubId = clubId,
        playerId = playerId,
        since = since,
        until = until,
        sinceApproximate = sinceApproximate
      )

    def context(client: ChessComClient): RIO[PostgresClient, ProcessingContext] =
      Club.upsert(Club(clubId, t0, clubSlug, "Discover test", None, None, None)) *>
        ProcessingContext.make(client, clubId, clubSlug, Map.empty)

    def resolve(ctx: ProcessingContext, isOurTeam: Boolean): RIO[ProgressDisplay & PostgresClient, Option[PlayerId]] =
      HistoryProcessing.resolvePlayerId(ctx, username, isOurTeam, Some(matchStart))
  }

  /** Serves `profile` as any player's profile, and runs `clubs` to answer for their club list. */
  private def playerClient(profile: String, clubs: UIO[Response]): RIO[PostgresClient & BodyStore, ChessComClient] =
    TestChessComClientSupport.fakeClient(
      Routes(
        Method.GET / "pub" / "player" / string("username") -> handler { (_: String, _: Request) =>
          Response.json(profile)
        },
        Method.GET / "pub" / "player" / string("username") / "clubs" -> handler((_: String, _: Request) => clubs)
      )
    )

  // With the client refusing fetches inside a transaction, this is also the regression test for #300: the club-list
  // fetch used to run inside the transaction that records the player.
  private def testNewTeammateGetsExactMembership =
    test("a new player on our team gets a member row dated from when they joined the club") {
      val f = Fixture(ClubId(930_100), ClubSlug("discover-new"), PlayerId(930_101), Username("new-teammate"))
      for {
        client     <- playerClient(f.activeProfile, ZIO.succeed(Response.json(f.clubsJson(listsOurClub = true))))
        ctx        <- f.context(client)
        resolved   <- f.resolve(ctx, isOurTeam = true)
        members    <- ClubMember.selectClub(f.clubId)
        discovered <- ctx.playersDiscovered.get
        newPlayers <- ctx.newPlayers.get
      } yield assertTrue(
        resolved.contains(f.playerId),
        members == List(f.member(since = joinedClub, until = None, sinceApproximate = false)),
        discovered == 1,
        newPlayers == Set(DiscoveredPlayer(f.playerId, f.username))
      )
    }

  private def testInactiveTeammatesMembershipEndsAtLastVisit =
    test("a closed account's member row ends when it was last online") {
      val f = Fixture(ClubId(930_600), ClubSlug("discover-closed"), PlayerId(930_601), Username("closed-teammate"))
      val profile = apiPlayerJson(
        playerId = f.playerId.value,
        username = f.username.value,
        status = "closed",
        joined = t0.getEpochSecond,
        country = "US",
        lastOnline = Some(lastVisit.getEpochSecond)
      )
      for {
        client  <- playerClient(profile, ZIO.succeed(Response.json(f.clubsJson(listsOurClub = true))))
        ctx     <- f.context(client)
        _       <- f.resolve(ctx, isOurTeam = true)
        members <- ClubMember.selectClub(f.clubId)
      } yield assertTrue(
        members == List(f.member(since = joinedClub, until = Some(lastVisit), sinceApproximate = false))
      )
    }

  private def testFormerOpponentGetsMembershipOnOurTeam =
    test("a player first recorded as an opponent gets a member row when later seen on our team") {
      val f = Fixture(ClubId(930_200), ClubSlug("discover-former"), PlayerId(930_201), Username("former-opponent"))
      for {
        clubsCalls <- Ref.make(0)
        client <- playerClient(
          f.activeProfile,
          clubsCalls.update(_ + 1).as(Response.json(f.clubsJson(listsOurClub = false)))
        )
        asOpponent      <- f.context(client)
        _               <- f.resolve(asOpponent, isOurTeam = false)
        membersAsOpp    <- ClubMember.selectClub(f.clubId)
        clubsCallsAsOpp <- clubsCalls.get
        onOurTeam       <- f.context(client) // a later run, which knows only the club's members
        _               <- f.resolve(onOurTeam, isOurTeam = true)
        members         <- ClubMember.selectClub(f.clubId)
        discovered      <- onOurTeam.playersDiscovered.get
        newPlayers      <- onOurTeam.newPlayers.get
      } yield assertTrue(
        membersAsOpp.isEmpty,
        clubsCallsAsOpp == 0,
        members == List(f.member(since = matchStart, until = Some(matchStart), sinceApproximate = true)),
        discovered == 0,
        newPlayers == Set(DiscoveredPlayer(f.playerId, f.username))
      )
    }

  private def testRowWrittenDuringFetchIsNotDuplicated =
    test("a member row written while the club list is fetched is kept, not joined by a second") {
      val f = Fixture(ClubId(930_500), ClubSlug("discover-race"), PlayerId(930_501), Username("raced-teammate"))
      // What a membership run would write between History's check and its insert.
      val concurrent = f.member(since = t0, until = None, sinceApproximate = false)
      for {
        pg         <- ZIO.service[PostgresClient]
        clubsCalls <- Ref.make(0)
        clubsAfterConcurrentWrite = for {
          _ <- clubsCalls.update(_ + 1)
          _ <- ClubMember.insert(concurrent).provideEnvironment(ZEnvironment(pg)).orDie
        } yield Response.json(f.clubsJson(listsOurClub = true))
        client     <- playerClient(f.activeProfile, clubsAfterConcurrentWrite)
        asOpponent <- f.context(client)
        _          <- f.resolve(asOpponent, isOurTeam = false)
        onOurTeam  <- f.context(client)
        _          <- f.resolve(onOurTeam, isOurTeam = true)
        members    <- ClubMember.selectClub(f.clubId)
        calls      <- clubsCalls.get
        newPlayers <- onOurTeam.newPlayers.get
      } yield assertTrue(calls == 1, members == List(concurrent), newPlayers.isEmpty)
    }

  private def testClubsFetchErrorFallsBackToApproximate =
    test("a failed club-list fetch still gives a player on our team an approximate member row") {
      val f = Fixture(ClubId(930_300), ClubSlug("discover-error"), PlayerId(930_301), Username("unlisted-teammate"))
      for {
        client  <- playerClient(f.activeProfile, ZIO.succeed(Response.status(Status.InternalServerError)))
        ctx     <- f.context(client)
        _       <- f.resolve(ctx, isOurTeam = true)
        members <- ClubMember.selectClub(f.clubId)
      } yield assertTrue(members == List(f.member(since = matchStart, until = None, sinceApproximate = true)))
    }

  private def testClubsOutageWritesNothing =
    test("a network outage on the club-list fetch writes neither the player nor a member row") {
      val f = Fixture(ClubId(930_400), ClubSlug("discover-outage"), PlayerId(930_401), Username("outage-teammate"))
      for {
        clientAndRefs <- TestChessComClientSupport.makeClient(
          handler = req =>
            if (req.url.path.segments.lastOption.contains("clubs")) {
              ZIO.fail(UnknownHostException("api.chess.com: Temporary failure in name resolution"))
            } else { ZIO.succeed(Response.json(f.activeProfile)) },
          retryBase = 10.millis,
          maxConnectionRetries = 2
        )
        ctx     <- f.context(clientAndRefs._1)
        exit    <- f.resolve(ctx, isOurTeam = true).exit
        player  <- Player.selectId(f.playerId)
        members <- ClubMember.selectClub(f.clubId)
      } yield assertTrue(isNetworkUnavailable(exit), player.isEmpty, members.isEmpty)
    }

  private def isNetworkUnavailable(exit: Exit[Throwable, Any]): Boolean =
    exit match {
      case Exit.Failure(cause) => cause.failures.exists(_.isInstanceOf[NetworkUnavailableException])
      case _                   => false
    }
}
