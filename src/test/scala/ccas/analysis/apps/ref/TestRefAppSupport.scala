package ccas.analysis.apps.ref

import java.time.{Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.{durationInt, RIO, Scope, ZIO, ZLayer}
import zio.http.*

import ccas.analysis.tables.*
import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.utils.ProgressDisplay
import ccas.utils.client.{ChessComClient, TestChessComClientSupport}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.PostgresClient.connectZIO

object TestRefAppSupport {

  // --- Timestamps ---

  val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  // --- IDs ---

  val pid0 = PlayerId(300)
  val pid1 = PlayerId(301)
  val pid2 = PlayerId(302)

  val clubId0   = ClubId(700)
  val clubId1   = ClubId(701)
  val clubSlug0 = ClubSlug("our-club")
  val clubSlug1 = ClubSlug("other-club")

  val matchId1 = 9001L
  val matchId2 = 9002L

  // --- JSON builders ---

  def apiPlayerMatchesJson(finished: List[(Long, Option[Int])]): String = {
    val items = finished.map { (matchId, boardOpt) =>
      val boardField = boardOpt.fold("")(b => s""", "board": "https://api.chess.com/pub/match/$matchId/$b"""")
      s"""{
        "name": "Match $matchId",
        "url": "https://www.chess.com/club/matches/$matchId",
        "@id": "https://api.chess.com/pub/match/$matchId",
        "club": "https://api.chess.com/pub/club/some-club"$boardField
      }"""
    }
    s"""{"finished": [${items.mkString(",")}], "in_progress": [], "registered": []}"""
  }

  def apiClubMatchesJson(finishedIds: List[Long]): String = {
    val items = finishedIds.map { matchId =>
      s"""{
        "name": "Match $matchId",
        "@id": "https://api.chess.com/pub/match/$matchId",
        "opponent": "https://api.chess.com/pub/club/opponent-club",
        "time_class": "daily",
        "start_time": ${t0.getEpochSecond},
        "result": "win"
      }"""
    }
    s"""{"finished": [${items.mkString(",")}], "in_progress": [], "registered": []}"""
  }

  def apiPlayerTournamentsJson(finished: List[(String, Int)]): String = {
    val items = finished.map { (slug, totalPlayers) =>
      s"""{
        "url": "https://www.chess.com/tournament/$slug",
        "@id": "https://api.chess.com/pub/tournament/$slug",
        "wins": 1, "losses": 0, "draws": 0,
        "points_awarded": 1.0, "placement": 1, "status": "eliminated", "total_players": $totalPlayers
      }"""
    }
    s"""{"finished": [${items.mkString(",")}], "in_progress": [], "registered": []}"""
  }

  def apiTournamentRoundJson(players: List[String]): String = {
    val items = players.map(u => s"""{"username": "$u"}""")
    s"""{"groups": [], "players": [${items.mkString(",")}]}"""
  }

  val emptyPlayerMatchesJson     = """{"finished": [], "in_progress": [], "registered": []}"""
  val emptyPlayerTournamentsJson = """{"finished": [], "in_progress": [], "registered": []}"""
  val emptyClubMatchesJson       = """{"finished": [], "in_progress": [], "registered": []}"""

  /** Canonical Chess.com 404 body: matches `HttpStatusException.classify`'s `not found."` substring,
    * stamps as `ReportedNotFound` at the throw site. Wire shape mirrors prod, e.g.
    * `reportedNotFoundBody("Player", "alice")` → `{"code": 0, "message": "Player \"alice\" not found."}`.
    */
  def reportedNotFoundBody(kind: String, id: String): String =
    s"""{"code": 0, "message": "$kind \\"$id\\" not found."}"""

  /** Transient Chess.com 404 body (code 3024 / "internal error"): does NOT trigger `ReportedNotFound`;
    * stays as base `HttpStatusException`, so callers handling it route the failure to `RefSkipReason.ApiError`
    * rather than `NotFound` (issue #3).
    */
  val transientInternalErrorBody: String =
    """{"code": 3024, "message": "An internal error has occurred. Please contact admin."}"""

  // --- Fake client ---

  def fakeChessComClient(
    responses: Map[String, String],
    failures: Set[String] = Set.empty,
    notFound: Map[String, String] = Map.empty
  ): RIO[PostgresClient, ChessComClient] =
    TestChessComClientSupport.fakeClient(refRoutes(responses, failures, notFound), permits = 5)

  /** DNS/network failure as it surfaces from the JVM resolver (`UnknownHostException`, the exact prod message). The
    * real `ChessComClient` wraps this into `NetworkUnavailableException` after its retry schedule exhausts.
    */
  private def networkDownCause: Throwable =
    new java.net.UnknownHostException("api.chess.com: Temporary failure in name resolution")

  /** A client whose every request fails with a DNS error — simulates a machine-wide network outage. Built on
    * `makeClient` (not the routes-based `fakeClient`) because only a failing `handler` can produce a transport-level
    * connection error; small retry knobs keep the exhaustion fast under live-clock tests.
    */
  def networkDownChessComClient: RIO[Scope & PostgresClient, ChessComClient] =
    TestChessComClientSupport
      .makeClient(handler = _ => ZIO.fail(networkDownCause), retryBase = 10.millis, maxConnectionRetries = 2, permits = 5)
      .map(_._1)

  /** A client that serves `responses` normally but fails the requests matching `isDown` with a DNS error. Lets a test
    * exercise the outage reaching a deeper fetch (e.g. a served match-listing whose per-match fetch is down).
    */
  def chessComClientWithOutage(
    responses: Map[String, String],
    isDown: Request => Boolean,
    failures: Set[String] = Set.empty,
    notFound: Map[String, String] = Map.empty
  ): RIO[Scope & PostgresClient, ChessComClient] = {
    val routes = refRoutes(responses, failures, notFound)
    TestChessComClientSupport
      .makeClient(
        handler = req => if (isDown(req)) ZIO.fail(networkDownCause) else ZIO.scoped(routes.runZIO(req)),
        retryBase = 10.millis,
        maxConnectionRetries = 2,
        permits = 5
      )
      .map(_._1)
  }

  private def refRoutes(
    responses: Map[String, String],
    failures: Set[String],
    notFound: Map[String, String]
  ): Routes[Any, Response] = {
    def maybeNotFound(key: String): Option[Response] =
      notFound.get(key).map(body => Response.json(body).copy(status = Status.NotFound))
    Routes(
      Method.GET / "pub" / "player" / string("username") / "tournaments" -> handler { (username: String, _: Request) =>
        if (failures.contains(username)) Response(status = Status.InternalServerError)
        else maybeNotFound(s"player/$username/tournaments").getOrElse(
          responses.get(s"player/$username/tournaments").fold(Response.json(emptyPlayerTournamentsJson))(
            Response.json(_)
          )
        )
      },
      Method.GET / "pub" / "player" / string("username") / "matches" -> handler { (username: String, _: Request) =>
        if (failures.contains(username)) Response(status = Status.InternalServerError)
        else maybeNotFound(s"player/$username/matches").getOrElse(
          responses.get(s"player/$username/matches").fold(Response.json(emptyPlayerMatchesJson))(Response.json(_))
        )
      },
      Method.GET / "pub" / "player" / string("username") -> handler { (username: String, _: Request) =>
        if (failures.contains(username)) Response(status = Status.InternalServerError)
        else maybeNotFound(s"player/$username").getOrElse {
          val pid = playerIdByUsername.getOrElse(username.toLowerCase, 0L)
          Response.json(apiPlayerJson(username, pid))
        }
      },
      Method.GET / "pub" / "club" / string("club") / "matches" -> handler { (clubName: String, _: Request) =>
        maybeNotFound(s"club/$clubName/matches").getOrElse(
          responses.get(s"club/$clubName/matches").fold(Response.json(emptyClubMatchesJson))(Response.json(_))
        )
      },
      Method.GET / "pub" / "match" / long("matchId") -> handler { (matchId: Long, _: Request) =>
        responses.get(s"match/$matchId").fold(Response.json("""{"code": 0, "message": "Resource \"\" not found."}""").copy(status = Status.NotFound))(Response.json(_))
      },
      Method.GET / "pub" / "tournament" / string("slug") / int("round") -> handler {
        (slug: String, round: Int, _: Request) =>
          responses.get(s"tournament/$slug/$round").fold(Response.json("""{"code": 0, "message": "Resource \"\" not found."}""").copy(status = Status.NotFound))(Response.json(_))
      }
    )
  }

  // --- DB helpers ---

  private val playerIdByUsername: Map[String, Long] = Map(
    "alice"   -> PlayerId.unwrap(pid0),
    "bob"     -> PlayerId.unwrap(pid1),
    "charlie" -> PlayerId.unwrap(pid2)
  )

  private def apiPlayerJson(username: String, playerId: Long): String =
    s"""{"player_id":$playerId,"username":"$username","country":"https://api.chess.com/pub/country/XX",
       |"status":"premium","joined":1000000000,"last_online":1000000000,"followers":0,"is_streamer":false,"verified":false}""".stripMargin

  private val testPlayerIds = List(pid0, pid1, pid2)
  private val testClubIds   = List(clubId0, clubId1)

  def seedDb: RIO[PostgresClient, Unit] =
    for {
      // Clean in FK-safe order
      _ <- connectZIO(sql"DELETE FROM api_fetch_failure".update.run())
      _ <- connectZIO(sql"DELETE FROM api_response_cache".update.run())
      _ <- connectZIO(sql"DELETE FROM api_response_body".update.run())
      _ <- connectZIO(sql"DELETE FROM player_ref_skip".update.run())
      _ <- connectZIO(sql"DELETE FROM club_ref_skip".update.run())
      _ <- connectZIO(sql"DELETE FROM player_match_ref".update.run())
      _ <- connectZIO(sql"DELETE FROM player_tournament_ref".update.run())
      _ <- connectZIO(sql"DELETE FROM club_match_ref".update.run())
      _ <- ZIO.foreachDiscard(testPlayerIds) { pid =>
        connectZIO(sql"DELETE FROM player_snapshot WHERE player_id = $pid".update.run())
      }
      _ <- ZIO.foreachDiscard(testPlayerIds) { pid =>
        connectZIO(sql"DELETE FROM player WHERE player_id = $pid".update.run())
      }
      _ <- ZIO.foreachDiscard(testClubIds) { cid =>
        connectZIO(sql"DELETE FROM club WHERE club_id = $cid".update.run())
      }
      // Insert test data
      _ <- Player.insert(Player(pid0, t0, Username("alice"), Active, None, t0))
      _ <- Player.insert(Player(pid1, t0, Username("bob"), Active, None, t0))
      _ <- Player.insert(Player(pid2, t0, Username("charlie"), Active, None, t0))
      _ <- Club.upsert(Club(clubId0, t0, clubSlug0, "Club 0", None, None, None))
      _ <- Club.upsert(Club(clubId1, t0, clubSlug1, "Club 1", None, None, None))
    } yield ()

  def runPopulate(
    client: ChessComClient,
    forceSkipped: Boolean,
    upgradeRefs: Boolean
  ): RIO[Scope & PostgresClient, Unit] =
    RefApp
      .populate(forceSkipped = forceSkipped, upgradeRefs = upgradeRefs)
      .unit
      .provideSomeLayer(ZLayer.succeed(client) ++ ZLayer.succeed(ProgressDisplay.make(enabled = false)))
}
