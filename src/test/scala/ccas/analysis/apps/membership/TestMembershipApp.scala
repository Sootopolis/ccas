package ccas.analysis.apps.membership

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.{sql, Transactor}
import zio.{Chunk, RIO, Ref, Scope, Semaphore, Trace, UIO, ZIO}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.membership.MembershipApp.{PhaseBResult, PhaseCResult}
import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.tables.{Club, ClubMember, Player, PlayerSnapshot, Tables}
import ccas.api.misc.enums.PlayerStatusCategory.{Active, Closed}
import ccas.api.misc.subtypes.{ClubId, ClubUrlName, PlayerId, Username}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.{DataSourceLayer, SqlZioTypes}

object TestMembershipApp extends ZIOSpecDefault {

  // --- Timestamps ---

  private object T {
    val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)
    val t1: Instant = t0.plus(Duration.ofDays(1))
    val t2: Instant = t0.plus(Duration.ofDays(30))
    val t3: Instant = t0.plus(Duration.ofDays(60))
  }

  // --- IDs ---

  private val pid0 = PlayerId(100)
  private val pid1 = PlayerId(101)
  private val pid2 = PlayerId(102)
  private val pid3 = PlayerId(103)
  private val pid4 = PlayerId(104)
  private val pid5 = PlayerId(105)

  private val clubId = ClubId(500)
  private val club   = Club(clubId, T.t0, ClubUrlName("test-club"))

  // --- Helpers ---

  private def apiPlayerJson(
      playerId: Long,
      username: String,
      status: String = "basic",
      joined: Long = T.t0.getEpochSecond
    ): String = {
    val fields = List(
      s""""player_id": $playerId""",
      s""""username": "$username"""",
      s""""country": "https://api.chess.com/pub/country/US"""",
      s""""status": "$status"""",
      s""""joined": $joined""",
      s""""last_online": $joined""",
      s""""followers": 0""",
      s""""is_streamer": false""",
      s""""verified": false""",
      s""""league": "wood""""
    )
    fields.mkString("{\n", ",\n", "\n}")
  }

  private def fakeChessComClient(
      responses: Map[String, String],
      failures: Set[String] = Set.empty
    ): UIO[ChessComClient] =
    (for {
      semaphore <- Semaphore.make(1)
      mutex     <- Semaphore.make(1)
      throttled <- Ref.make(false)
    } yield (semaphore, mutex, throttled)).map { (semaphore, mutex, throttled) =>
      val routes: Routes[Any, Response] = Routes(
        Method.GET / "pub" / "player" / string("username") -> handler { (username: String, _: Request) =>
          if failures.contains(username) then Response(status = Status.NotFound)
          else responses.get(username).fold(Response(status = Status.NotFound))(Response.json(_))
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
          )(implicit trace: Trace
          ): ZIO[Scope, Throwable, Response] =
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
      ChessComClient(
        ZClient.fromDriver(driver),
        Headers.empty,
        semaphore,
        mutex,
        throttled,
        zio.Duration.fromSeconds(30)
      )
    }

  private val testPlayerIds = List(pid0, pid1, pid2, pid3, pid4, pid5)

  private def seedDb(
      players: List[Player] = Nil,
      snapshots: List[PlayerSnapshot] = Nil,
      members: List[ClubMember] = Nil
    ): RIO[Transactor, Unit] =
    for {
      _ <- SqlZioTypes.connectZIO(sql"DELETE FROM club_member WHERE club_id = $clubId".update.run())
      _ <- ZIO.foreachDiscard(testPlayerIds) { pid =>
        SqlZioTypes.connectZIO(sql"DELETE FROM player_snapshot WHERE player_id = $pid".update.run()) *>
          SqlZioTypes.connectZIO(sql"DELETE FROM player WHERE player_id = $pid".update.run())
      }
      _ <- Club.upsert(club)
      _ <- ZIO.whenDiscard(players.nonEmpty)(Player.insertBatch(players))
      _ <- ZIO.whenDiscard(snapshots.nonEmpty)(PlayerSnapshot.insertBatch(snapshots))
      _ <- ZIO.whenDiscard(members.nonEmpty)(ClubMember.insertBatch(members))
    } yield ()

  // --- Spec ---

  override def spec: Spec[Any, Throwable] = suite("TestMembershipApp")(
    suiteClassifyFromDb,
    suiteMergeResults,
    suiteBuildDbState,
    suiteClassifyApiMembers,
    suiteClassifyDisappeared
  ).provideShared(
    DataSourceLayer.liveFromPrefix(schema = Some("test_membership_app"), onInit = Tables.ensureTables)
  ) @@ TestAspect.sequential

  // ==========================================================================
  // Suite A: classifyFromDb (pure)
  // ==========================================================================

  private def suiteClassifyFromDb = suite("classifyFromDb")(
    test("empty inputs") {
      val result = MembershipApp.classifyFromDb(clubId, Nil, Nil, T.t0, T.t2)
      assertTrue(result.isEmpty)
    },
    test("member since in range, no prior snaps → NewMember") {
      val member = ClubMember(clubId, pid0, T.t1, None)
      val result = MembershipApp.classifyFromDb(clubId, List(member), Nil, T.t0, T.t2)
      assertTrue(
        result.size == 1,
        result.head.playerId == pid0,
        result.head.changes.exists(_.isInstanceOf[NewMember])
      )
    },
    test("member since in range, prior snaps exist → JoinedClub") {
      val member = ClubMember(clubId, pid0, T.t1, None)
      val snap   = PlayerSnapshot(pid0, T.t0, Username("alice"), Active, None)
      val result = MembershipApp.classifyFromDb(clubId, List(member), List(snap), T.t0, T.t2)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[JoinedClub])
      )
    },
    test("member since in range, prior closed membership → Rejoined") {
      val oldMember = ClubMember(clubId, pid0, T.t0, Some(T.t1))
      val newMember = ClubMember(clubId, pid0, T.t2, None)
      val result    = MembershipApp.classifyFromDb(clubId, List(oldMember, newMember), Nil, T.t1, T.t3)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[Rejoined])
      )
    },
    test("member until in range, latest snap Active → LeftClub") {
      val member = ClubMember(clubId, pid0, T.t0, Some(T.t1))
      val snap   = PlayerSnapshot(pid0, T.t0, Username("alice"), Active, None)
      val result = MembershipApp.classifyFromDb(clubId, List(member), List(snap), T.t0, T.t2)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[LeftClub])
      )
    },
    test("member until in range, latest snap Closed → AccountClosed") {
      val member = ClubMember(clubId, pid0, T.t0, Some(T.t1))
      val snap   = PlayerSnapshot(pid0, T.t0, Username("alice"), Closed, None)
      val result = MembershipApp.classifyFromDb(clubId, List(member), List(snap), T.t0, T.t2)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[AccountClosed])
      )
    },
    test("member until in range, no snapshot → Unresolvable") {
      val member = ClubMember(clubId, pid0, T.t0, Some(T.t1))
      val result = MembershipApp.classifyFromDb(clubId, List(member), Nil, T.t0, T.t2)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[Unresolvable])
      )
    },
    test("two snaps in range, different usernames → UsernameChange") {
      val member = ClubMember(clubId, pid0, T.t0, None)
      val snap1  = PlayerSnapshot(pid0, T.t0, Username("alice-old"), Active, None)
      val snap2  = PlayerSnapshot(pid0, T.t1, Username("alice-new"), Active, None)
      val result = MembershipApp.classifyFromDb(clubId, List(member), List(snap1, snap2), T.t0, T.t2)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[UsernameChange])
      )
    },
    test("two snaps in range, different statuses → StatusChange") {
      val member = ClubMember(clubId, pid0, T.t0, None)
      val snap1  = PlayerSnapshot(pid0, T.t0, Username("alice"), Active, None)
      val snap2  = PlayerSnapshot(pid0, T.t1, Username("alice"), Closed, None)
      val result = MembershipApp.classifyFromDb(clubId, List(member), List(snap1, snap2), T.t0, T.t2)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[StatusChange])
      )
    },
    test("all dates outside range → empty list") {
      val member = ClubMember(clubId, pid0, T.t0, None)
      val snap   = PlayerSnapshot(pid0, T.t0, Username("alice"), Active, None)
      val result = MembershipApp.classifyFromDb(clubId, List(member), List(snap), T.t2, T.t3)
      assertTrue(result.isEmpty)
    }
  )

  // ==========================================================================
  // Suite B: mergeResults (pure)
  // ==========================================================================

  private def suiteMergeResults = suite("mergeResults")(
    test("concatenates PhaseBResult and PhaseCResult fields") {
      val bChange = MemberChangeSummary(pid0, Username("alice"), Chunk(NewMember(T.t1)))
      val cChange = MemberChangeSummary(pid1, Username("bob"), Chunk(LeftClub(T.t1)))
      val bPlayer = Player(pid0, T.t0)
      val bSnap   = PlayerSnapshot(pid0, T.t1, Username("alice"), Active, None)
      val cSnap   = PlayerSnapshot(pid1, T.t1, Username("bob"), Active, None)
      val bMember = ClubMember(clubId, pid0, T.t1, None)
      val bClosed = ClubMember(clubId, pid2, T.t0, Some(T.t1))
      val cClosed = ClubMember(clubId, pid1, T.t0, Some(T.t1))

      val b      = PhaseBResult(Set(pid0), Chunk(bChange), Chunk(bPlayer), Chunk(bSnap), Chunk(bMember), Chunk(bClosed))
      val c      = PhaseCResult(Chunk(cChange), Chunk(cSnap), Chunk(cClosed))
      val result = MembershipApp.mergeResults(b, c)

      assertTrue(
        result.changes == Chunk(bChange, cChange),
        result.newPlayers == Chunk(bPlayer),
        result.newSnapshots == Chunk(bSnap, cSnap),
        result.newMemberships == Chunk(bMember),
        result.closedMemberships == Chunk(bClosed, cClosed)
      )
    }
  )

  // ==========================================================================
  // Suite C: buildDbState (DB)
  // ==========================================================================

  private def suiteBuildDbState = suite("buildDbState")(
    test("builds correct DbState maps") {
      val player0 = Player(pid0, T.t0)
      val player1 = Player(pid1, T.t0)
      val snap0   = PlayerSnapshot(pid0, T.t1, Username("alice"), Active, None)
      val snap1   = PlayerSnapshot(pid1, T.t1, Username("bob"), Active, None)
      val mem0    = ClubMember(clubId, pid0, T.t1, None)
      val mem1    = ClubMember(clubId, pid1, T.t1, None)

      for {
        _ <- seedDb(
          players = List(player0, player1),
          snapshots = List(snap0, snap1),
          members = List(mem0, mem1)
        )
        dbState <- MembershipApp.buildDbState(clubId)
      } yield assertTrue(
        dbState.membersByPlayerId.size == 2,
        dbState.membersByPlayerId.contains(pid0),
        dbState.membersByPlayerId.contains(pid1),
        dbState.membersByPlayerId(pid0).player == snap0,
        dbState.membersByPlayerId(pid0).member == mem0,
        dbState.membersByUsername.size == 2,
        dbState.membersByUsername.contains(Username("alice")),
        dbState.membersByUsername.contains(Username("bob")),
        dbState.knownPlayersByUsername.contains(Username("alice")),
        dbState.knownPlayersByUsername.contains(Username("bob")),
        dbState.knownPlayersByUsername(Username("alice")) == snap0,
        dbState.knownPlayersByUsername(Username("bob")) == snap1
      )
    },
    test("excludes former members from DbState") {
      val player0   = Player(pid0, T.t0)
      val snap0     = PlayerSnapshot(pid0, T.t1, Username("alice"), Active, None)
      val formerMem = ClubMember(clubId, pid0, T.t0, Some(T.t1))

      for {
        _ <- seedDb(
          players = List(player0),
          snapshots = List(snap0),
          members = List(formerMem)
        )
        dbState <- MembershipApp.buildDbState(clubId)
      } yield assertTrue(
        dbState.membersByPlayerId.isEmpty,
        dbState.knownPlayersByUsername.contains(Username("alice"))
      )
    }
  )

  // ==========================================================================
  // Suite D: classifyApiMembers (DB + fake HTTP)
  // ==========================================================================

  private def suiteClassifyApiMembers = suite("classifyApiMembers")(
    test("unchanged member — matching since") {
      val snap = PlayerSnapshot(pid0, T.t1, Username("alice"), Active, None)
      val mem  = ClubMember(clubId, pid0, T.t1, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid0 -> MemberState(snap, mem)),
        membersByUsername = Map(Username("alice") -> MemberState(snap, mem))
      )
      val apiMap = Map(Username("alice") -> T.t1.getEpochSecond)

      for {
        client <- fakeChessComClient(Map.empty)
        result <- MembershipApp.classifyApiMembers(client, clubId, apiMap, dbState, T.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid0),
        result.changes.isEmpty
      )
    },
    test("different since → Rejoined") {
      val snap = PlayerSnapshot(pid1, T.t0, Username("bob"), Active, None)
      val mem  = ClubMember(clubId, pid1, T.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid1 -> MemberState(snap, mem)),
        membersByUsername = Map(Username("bob") -> MemberState(snap, mem))
      )
      val apiMap = Map(Username("bob") -> T.t1.getEpochSecond)

      for {
        client <- fakeChessComClient(Map.empty)
        result <- MembershipApp.classifyApiMembers(client, clubId, apiMap, dbState, T.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid1),
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[Rejoined]),
        result.closedMemberships.nonEmpty,
        result.newMemberships.nonEmpty
      )
    },
    test("username change — same player ID, different username") {
      val snap = PlayerSnapshot(pid2, T.t0, Username("charlie-old"), Active, None)
      val mem  = ClubMember(clubId, pid2, T.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid2 -> MemberState(snap, mem)),
        membersByUsername = Map(Username("charlie-old") -> MemberState(snap, mem))
      )
      val apiMap    = Map(Username("charlie-new") -> T.t0.getEpochSecond)
      val responses = Map("charlie-new" -> apiPlayerJson(102, "charlie-new"))

      for {
        client <- fakeChessComClient(responses)
        result <- MembershipApp.classifyApiMembers(client, clubId, apiMap, dbState, T.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid2),
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[UsernameChange]),
        result.newSnapshots.nonEmpty
      )
    },
    test("new player — not in DB") {
      val dbState   = DbState(Map.empty, Map.empty)
      val apiMap    = Map(Username("diana") -> T.t0.getEpochSecond)
      val responses = Map("diana" -> apiPlayerJson(103, "diana"))

      for {
        _      <- seedDb()
        client <- fakeChessComClient(responses)
        result <- MembershipApp.classifyApiMembers(client, clubId, apiMap, dbState, T.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid3),
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[NewMember]),
        result.newPlayers.nonEmpty,
        result.newSnapshots.nonEmpty,
        result.newMemberships.nonEmpty
      )
    },
    test("existing player joins club") {
      val player4   = Player(pid4, T.t0)
      val snap4     = PlayerSnapshot(pid4, T.t0, Username("eve"), Active, None)
      val dbState   = DbState(Map.empty, Map.empty)
      val apiMap    = Map(Username("eve") -> T.t1.getEpochSecond)
      val responses = Map("eve" -> apiPlayerJson(104, "eve"))

      for {
        _      <- seedDb(players = List(player4), snapshots = List(snap4))
        client <- fakeChessComClient(responses)
        result <- MembershipApp.classifyApiMembers(client, clubId, apiMap, dbState, T.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid4),
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[JoinedClub]),
        result.newMemberships.nonEmpty
      )
    },
    test("username change + status change") {
      val snap = PlayerSnapshot(pid5, T.t0, Username("frank-old"), Active, None)
      val mem  = ClubMember(clubId, pid5, T.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid5 -> MemberState(snap, mem)),
        membersByUsername = Map(Username("frank-old") -> MemberState(snap, mem))
      )
      val apiMap    = Map(Username("frank-new") -> T.t0.getEpochSecond)
      val responses = Map("frank-new" -> apiPlayerJson(105, "frank-new", status = "closed"))

      for {
        client <- fakeChessComClient(responses)
        result <- MembershipApp.classifyApiMembers(client, clubId, apiMap, dbState, T.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid5),
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[UsernameChange]),
        result.changes.head.changes.exists(_.isInstanceOf[StatusChange])
      )
    },
    test("trust-mode: known player joins club without API call") {
      val snap = PlayerSnapshot(pid3, T.t0, Username("diana"), Active, None)
      val dbState = DbState(
        membersByPlayerId = Map.empty,
        membersByUsername = Map.empty,
        knownPlayersByUsername = Map(Username("diana") -> snap)
      )
      val apiMap = Map(Username("diana") -> T.t1.getEpochSecond)

      for {
        client <- fakeChessComClient(Map.empty)
        result <- MembershipApp.classifyApiMembers(client, clubId, apiMap, dbState, T.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid3),
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[JoinedClub]),
        result.newMemberships.nonEmpty,
        result.newSnapshots.isEmpty
      )
    },
    test("trust-mode: username change detected without API call") {
      val oldSnap = PlayerSnapshot(pid2, T.t0, Username("charlie-old"), Active, None)
      val mem     = ClubMember(clubId, pid2, T.t0, None)
      val newSnap = PlayerSnapshot(pid2, T.t1, Username("charlie-new"), Active, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid2 -> MemberState(oldSnap, mem)),
        membersByUsername = Map(Username("charlie-old") -> MemberState(oldSnap, mem)),
        knownPlayersByUsername = Map(Username("charlie-new") -> newSnap)
      )
      val apiMap = Map(Username("charlie-new") -> T.t0.getEpochSecond)

      for {
        client <- fakeChessComClient(Map.empty)
        result <- MembershipApp.classifyApiMembers(client, clubId, apiMap, dbState, T.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid2),
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[UsernameChange]),
        result.newSnapshots.nonEmpty
      )
    },
    test("trustUsernames=false bypasses known player lookup") {
      val snap = PlayerSnapshot(pid3, T.t0, Username("diana"), Active, None)
      val dbState = DbState(
        membersByPlayerId = Map.empty,
        membersByUsername = Map.empty,
        knownPlayersByUsername = Map(Username("diana") -> snap)
      )
      val apiMap = Map(Username("diana") -> T.t0.getEpochSecond)

      for {
        client <- fakeChessComClient(Map.empty)
        result <- MembershipApp.classifyApiMembers(client, clubId, apiMap, dbState, T.t2, trustUsernames = false).exit
      } yield assertTrue(result.isFailure)
    }
  )

  // ==========================================================================
  // Suite E: classifyDisappeared (fake HTTP)
  // ==========================================================================

  private def suiteClassifyDisappeared = suite("classifyDisappeared")(
    test("active player left club → LeftClub") {
      val snap = PlayerSnapshot(pid0, T.t0, Username("alice"), Active, None)
      val mem  = ClubMember(clubId, pid0, T.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid0 -> MemberState(snap, mem)),
        membersByUsername = Map(Username("alice") -> MemberState(snap, mem))
      )
      val responses = Map("alice" -> apiPlayerJson(100, "alice"))

      for {
        client <- fakeChessComClient(responses)
        result <- MembershipApp.classifyDisappeared(client, dbState, Set.empty, Map.empty, ClubUrlName("test-club"), T.t2)
      } yield assertTrue(
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[LeftClub]),
        result.closedMemberships.nonEmpty
      )
    },
    test("closed player → AccountClosed") {
      val snap = PlayerSnapshot(pid1, T.t0, Username("bob"), Active, None)
      val mem  = ClubMember(clubId, pid1, T.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid1 -> MemberState(snap, mem)),
        membersByUsername = Map(Username("bob") -> MemberState(snap, mem))
      )
      val responses = Map("bob" -> apiPlayerJson(101, "bob", status = "closed"))

      for {
        client <- fakeChessComClient(responses)
        result <- MembershipApp.classifyDisappeared(client, dbState, Set.empty, Map.empty, ClubUrlName("test-club"), T.t2)
      } yield assertTrue(
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[AccountClosed]),
        result.newSnapshots.nonEmpty,
        result.closedMemberships.nonEmpty
      )
    },
    test("API 404 → Unresolvable") {
      val snap = PlayerSnapshot(pid2, T.t0, Username("charlie"), Active, None)
      val mem  = ClubMember(clubId, pid2, T.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid2 -> MemberState(snap, mem)),
        membersByUsername = Map(Username("charlie") -> MemberState(snap, mem))
      )

      for {
        client <- fakeChessComClient(Map.empty, failures = Set("charlie"))
        result <- MembershipApp.classifyDisappeared(client, dbState, Set.empty, Map.empty, ClubUrlName("test-club"), T.t2)
      } yield assertTrue(
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[Unresolvable]),
        result.closedMemberships.nonEmpty
      )
    },
    test("different player ID at same username → Unresolvable") {
      val snap = PlayerSnapshot(pid3, T.t0, Username("diana"), Active, None)
      val mem  = ClubMember(clubId, pid3, T.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid3 -> MemberState(snap, mem)),
        membersByUsername = Map(Username("diana") -> MemberState(snap, mem))
      )
      val responses = Map("diana" -> apiPlayerJson(999, "diana"))

      for {
        client <- fakeChessComClient(responses)
        result <- MembershipApp.classifyDisappeared(client, dbState, Set.empty, Map.empty, ClubUrlName("test-club"), T.t2)
      } yield assertTrue(
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[Unresolvable]),
        result.closedMemberships.nonEmpty
      )
    },
    test("all resolved → empty results") {
      val snap = PlayerSnapshot(pid0, T.t0, Username("alice"), Active, None)
      val mem  = ClubMember(clubId, pid0, T.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid0 -> MemberState(snap, mem)),
        membersByUsername = Map(Username("alice") -> MemberState(snap, mem))
      )

      for {
        client <- fakeChessComClient(Map.empty)
        result <- MembershipApp.classifyDisappeared(client, dbState, Set(pid0), Map.empty, ClubUrlName("test-club"), T.t2)
      } yield assertTrue(
        result.changes.isEmpty,
        result.newSnapshots.isEmpty,
        result.closedMemberships.isEmpty
      )
    }
  )
}
