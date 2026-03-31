package ccas.analysis.apps.membership

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.augustnagro.magnum.{sql, Transactor}
import zio.{durationInt, Chunk, Fiber, RIO, Ref, Scope, Semaphore, Trace, ZIO}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.membership.MembershipClassify.{PhaseBResult, PhaseCResult}
import ccas.analysis.apps.membership.MembershipChange.*
import ccas.analysis.apps.membership.MembershipChange.MemberChange.*
import ccas.analysis.tables.{Club, ClubMember, Player, PlayerSnapshot, Tables}
import ccas.api.misc.enums.PlayerStatusCategory.{Active, Closed}
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.utils.{CcasLogger, TestCcasLogger}
import ccas.utils.client.ChessComClient
import ccas.utils.sql.{FreshSchemaLayer, SqlZioTypes}

object TestMembershipApp extends ZIOSpecDefault {

  // --- Timestamps ---

  private object Times {
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
  private val club   = Club(clubId, Times.t0, ClubSlug("test-club"), "Test Club")

  // --- Helpers ---

  private def apiPlayerJson(
    playerId: Long,
    username: String,
    status: String = "basic",
    joined: Long = Times.t0.getEpochSecond,
    lastOnline: Long = Times.t0.getEpochSecond
  ): String = {
    val fields = List(
      s""""player_id": $playerId""",
      s""""username": "$username"""",
      s""""country": "https://api.chess.com/pub/country/US"""",
      s""""status": "$status"""",
      s""""joined": $joined""",
      s""""last_online": $lastOnline""",
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
  ): RIO[Transactor, ChessComClient] =
    for {
      transactor <- ZIO.service[Transactor]
      semaphore  <- Semaphore.make(1)
      stateRef   <- Ref.make(ChessComClient.ThrottleState(1, 0, Vector.empty))
      reserveRef  <- Ref.make(Chunk.empty[Fiber.Runtime[Nothing, Nothing]])
      adjustMutex <- Semaphore.make(1)
      activeRef   <- Ref.make(0)
      rateLimitGate <- Semaphore.make(1)
      lastReqRef  <- Ref.make(0L)
      ema         <- Ref.make(0.0)
      bar         <- TestCcasLogger.noopBar
    } yield {
      val routes: Routes[Any, Response] = Routes(
        Method.GET / "pub" / "player" / string("username") -> handler { (username: String, _: Request) =>
          if (failures.contains(username)) { Response(status = Status.NotFound) }
          else { responses.get(username).fold(Response(status = Status.NotFound))(Response.json(_)) }
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
    suiteFormatReport,
    suiteBuildDbState,
    suiteClassifyApiMembers,
    suiteClassifyDisappeared
  ).provideShared(
    FreshSchemaLayer("test_membership_app", onInit = Tables.ensureTables),
    Scope.default,
    CcasLogger.live(showProgress = false)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  // ==========================================================================
  // Suite A: classifyFromDb (pure)
  // ==========================================================================

  private def suiteClassifyFromDb = suite("classifyFromDb")(
    test("empty inputs") {
      val result = MembershipReport.classifyFromDb(clubId, Nil, Nil, Times.t0, Times.t2)
      assertTrue(result.isEmpty)
    },
    test("member since in range, no prior snaps → NewMember") {
      val member = ClubMember(clubId, pid0, Times.t1, None)
      val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, Times.t0, Times.t2)
      assertTrue(
        result.size == 1,
        result.head.playerId == pid0,
        result.head.changes.exists(_.isInstanceOf[NewMember])
      )
    },
    test("member since in range, prior snaps exist → JoinedClub") {
      val member = ClubMember(clubId, pid0, Times.t1, None)
      val snap   = PlayerSnapshot(pid0, Times.t0, Username("alice"), Active, None)
      val result = MembershipReport.classifyFromDb(clubId, List(member), List(snap), Times.t0, Times.t2)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[JoinedClub])
      )
    },
    test("member since in range, prior closed membership → Rejoined") {
      val oldMember = ClubMember(clubId, pid0, Times.t0, Some(Times.t1))
      val newMember = ClubMember(clubId, pid0, Times.t2, None)
      val result    = MembershipReport.classifyFromDb(clubId, List(oldMember, newMember), Nil, Times.t1, Times.t3)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[Rejoined])
      )
    },
    test("member until in range, latest snap Active → LeftClub") {
      val member = ClubMember(clubId, pid0, Times.t0, Some(Times.t1))
      val snap   = PlayerSnapshot(pid0, Times.t0, Username("alice"), Active, None)
      val result = MembershipReport.classifyFromDb(clubId, List(member), List(snap), Times.t0, Times.t2)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[LeftClub])
      )
    },
    test("member until in range, latest snap Closed → AccountClosed") {
      val member = ClubMember(clubId, pid0, Times.t0, Some(Times.t1))
      val snap   = PlayerSnapshot(pid0, Times.t0, Username("alice"), Closed, None)
      val result = MembershipReport.classifyFromDb(clubId, List(member), List(snap), Times.t0, Times.t2)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[AccountClosed])
      )
    },
    test("member until in range, no snapshot → Unresolvable") {
      val member = ClubMember(clubId, pid0, Times.t0, Some(Times.t1))
      val result = MembershipReport.classifyFromDb(clubId, List(member), Nil, Times.t0, Times.t2)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[Unresolvable])
      )
    },
    test("two snaps in range, different usernames → UsernameChange") {
      val member = ClubMember(clubId, pid0, Times.t0, None)
      val snap1  = PlayerSnapshot(pid0, Times.t0, Username("alice-old"), Active, None)
      val snap2  = PlayerSnapshot(pid0, Times.t1, Username("alice-new"), Active, None)
      val result = MembershipReport.classifyFromDb(clubId, List(member), List(snap1, snap2), Times.t0, Times.t2)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[UsernameChange])
      )
    },
    test("two snaps in range, different statuses → StatusChange") {
      val member = ClubMember(clubId, pid0, Times.t0, None)
      val snap1  = PlayerSnapshot(pid0, Times.t0, Username("alice"), Active, None)
      val snap2  = PlayerSnapshot(pid0, Times.t1, Username("alice"), Closed, None)
      val result = MembershipReport.classifyFromDb(clubId, List(member), List(snap1, snap2), Times.t0, Times.t2)
      assertTrue(
        result.size == 1,
        result.head.changes.exists(_.isInstanceOf[StatusChange])
      )
    },
    test("all dates outside range → empty list") {
      val member = ClubMember(clubId, pid0, Times.t0, None)
      val snap   = PlayerSnapshot(pid0, Times.t0, Username("alice"), Active, None)
      val result = MembershipReport.classifyFromDb(clubId, List(member), List(snap), Times.t2, Times.t3)
      assertTrue(result.isEmpty)
    }
  )

  // ==========================================================================
  // Suite B: mergeResults (pure)
  // ==========================================================================

  private def suiteMergeResults = suite("mergeResults")(
    test("concatenates PhaseBResult and PhaseCResult fields") {
      val bChange    = MemberChangeSummary(pid0, Username("alice"), Chunk(NewMember(Times.t1)))
      val cChange    = MemberChangeSummary(pid1, Username("bob"), Chunk(LeftClub(Times.t1)))
      val bPlayer    = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val bUpdated   = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t1)
      val bArchived  = PlayerSnapshot(pid0, Times.t0, Username("alice"), Active, None)
      val cUpdated   = Player(pid1, Times.t0, Username("bob"), Closed, None, Times.t1)
      val cArchived  = PlayerSnapshot(pid1, Times.t0, Username("bob"), Active, None)
      val bMember    = ClubMember(clubId, pid0, Times.t1, None)
      val bClosed    = ClubMember(clubId, pid2, Times.t0, Some(Times.t1))
      val cClosed    = ClubMember(clubId, pid1, Times.t0, Some(Times.t1))

      val phaseB = PhaseBResult(Set(pid0), Chunk(bChange), Chunk(bPlayer), Chunk(bUpdated), Chunk(bArchived), Chunk(bMember), Chunk(bClosed))
      val phaseC = PhaseCResult(Chunk(cChange), Chunk(cUpdated), Chunk(cArchived), Chunk(cClosed))
      val result = MembershipApp.mergeResults(phaseB, phaseC, 10, 8, Times.t0, Times.t1)

      assertTrue(
        result.changes == Chunk(bChange, cChange),
        result.newPlayers == Chunk(bPlayer),
        result.updatedPlayers == Chunk(bUpdated, cUpdated),
        result.archivedSnapshots == Chunk(bArchived, cArchived),
        result.newMemberships == Chunk(bMember),
        result.closedMemberships == Chunk(bClosed, cClosed)
      )
    }
  )

  // ==========================================================================
  // Suite C: formatReport (pure)
  // ==========================================================================

  private def suiteFormatReport = suite("formatReport")(
    test("empty summaries → 'No changes'") {
      val rr = MembershipReport.ReportResult(Nil, 10, 10)
      val output = MembershipReport.formatReport(rr)
      assertTrue(
        output.contains("Total members: 10 (+0)"),
        output.contains("No changes")
      )
    },
    test("groups changes by category, not by player") {
      val summaries = List(
        MemberChangeSummary(pid0, Username("alice"), Chunk(NewMember(Times.t1), UsernameChange(Times.t2, Username("alice-old")))),
        MemberChangeSummary(pid1, Username("bob"), Chunk(NewMember(Times.t1)))
      )
      val rr = MembershipReport.ReportResult(summaries, 8, 10)
      val output = MembershipReport.formatReport(rr)
      assertTrue(
        output.contains("[JOINED]\n  alice"),
        output.contains("[JOINED]\n  alice") && output.contains("  bob"),
        output.contains("[USERNAME CHANGE]\n  alice")
      )
    },
    test("categories appear in enum ordinal order") {
      val summaries = List(
        MemberChangeSummary(pid0, Username("alice"), Chunk(UsernameChange(Times.t2, Username("old")))),
        MemberChangeSummary(pid1, Username("bob"), Chunk(NewMember(Times.t1)))
      )
      val rr = MembershipReport.ReportResult(summaries, 8, 9)
      val output = MembershipReport.formatReport(rr)
      val newIdx = output.indexOf("[JOINED]")
      val usrIdx = output.indexOf("[USERNAME CHANGE]")
      assertTrue(
        newIdx >= 0,
        usrIdx >= 0,
        newIdx < usrIdx
      )
    },
    test("entries within a category are sorted by timestamp") {
      val summaries = List(
        MemberChangeSummary(pid0, Username("bob"), Chunk(NewMember(Times.t2))),
        MemberChangeSummary(pid1, Username("alice"), Chunk(NewMember(Times.t1)))
      )
      val rr = MembershipReport.ReportResult(summaries, 8, 10)
      val output = MembershipReport.formatReport(rr)
      val aliceIdx = output.indexOf("alice")
      val bobIdx   = output.indexOf("bob")
      assertTrue(aliceIdx < bobIdx)
    },
    test("shows member count delta") {
      val rr = MembershipReport.ReportResult(Nil, 12, 10)
      val output = MembershipReport.formatReport(rr)
      assertTrue(output.contains("Total members: 10 (-2)"))
    }
  )

  // ==========================================================================
  // Suite D: buildDbState (DB)
  // ==========================================================================

  private def suiteBuildDbState = suite("buildDbState")(
    test("builds correct DbState maps") {
      val player0 = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t1)
      val player1 = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t1)
      val mem0    = ClubMember(clubId, pid0, Times.t1, None)
      val mem1    = ClubMember(clubId, pid1, Times.t1, None)

      for {
        _ <- seedDb(
          players = List(player0, player1),
          members = List(mem0, mem1)
        )
        dbState <- MembershipApp.buildDbState(clubId)
      } yield assertTrue(
        dbState.membersByPlayerId.size == 2,
        dbState.membersByPlayerId.contains(pid0),
        dbState.membersByPlayerId.contains(pid1),
        dbState.membersByPlayerId(pid0).player == player0,
        dbState.membersByPlayerId(pid0).member == mem0,
        dbState.membersByUsername.size == 2,
        dbState.membersByUsername.contains(Username("alice")),
        dbState.membersByUsername.contains(Username("bob")),
        dbState.knownPlayersByUsername.contains(Username("alice")),
        dbState.knownPlayersByUsername.contains(Username("bob")),
        dbState.knownPlayersByUsername(Username("alice")) == player0,
        dbState.knownPlayersByUsername(Username("bob")) == player1
      )
    },
    test("excludes former members from DbState") {
      val player0   = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t1)
      val formerMem = ClubMember(clubId, pid0, Times.t0, Some(Times.t1))

      for {
        _ <- seedDb(
          players = List(player0),
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
      val player = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t1)
      val mem    = ClubMember(clubId, pid0, Times.t1, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid0 -> MemberState(player, mem)),
        membersByUsername = Map(Username("alice") -> MemberState(player, mem))
      )
      val apiMap = Map(Username("alice") -> Times.t1.getEpochSecond)

      for {
        client <- fakeChessComClient(Map.empty)
        result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid0),
        result.changes.isEmpty
      )
    },
    test("different since → Rejoined") {
      val player = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t0)
      val mem    = ClubMember(clubId, pid1, Times.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid1 -> MemberState(player, mem)),
        membersByUsername = Map(Username("bob") -> MemberState(player, mem))
      )
      val apiMap = Map(Username("bob") -> Times.t1.getEpochSecond)

      for {
        client <- fakeChessComClient(Map.empty)
        result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
      } yield {
        val change = result.changes.head.changes.head
        assertTrue(
          result.resolvedIds.contains(pid1),
          result.changes.size == 1,
          change.isInstanceOf[Rejoined],
          change.timestamp == Times.t1, // API join time, not reconciliation time
          result.closedMemberships.nonEmpty,
          result.newMemberships.nonEmpty
        )
      }
    },
    test("username change — same player ID, different username") {
      val player = Player(pid2, Times.t0, Username("charlie-old"), Active, None, Times.t0)
      val mem    = ClubMember(clubId, pid2, Times.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid2 -> MemberState(player, mem)),
        membersByUsername = Map(Username("charlie-old") -> MemberState(player, mem))
      )
      val apiMap    = Map(Username("charlie-new") -> Times.t0.getEpochSecond)
      val responses = Map("charlie-new" -> apiPlayerJson(102, "charlie-new"))

      for {
        client <- fakeChessComClient(responses)
        result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid2),
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[UsernameChange]),
        result.updatedPlayers.nonEmpty
      )
    },
    test("new player — not in DB") {
      val dbState   = DbState(Map.empty, Map.empty)
      val apiMap    = Map(Username("diana") -> Times.t0.getEpochSecond)
      val responses = Map("diana" -> apiPlayerJson(103, "diana"))

      for {
        _      <- seedDb()
        client <- fakeChessComClient(responses)
        result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
      } yield {
        val change = result.changes.head.changes.head
        assertTrue(
          result.resolvedIds.contains(pid3),
          result.changes.size == 1,
          change.isInstanceOf[NewMember],
          change.timestamp == Times.t0, // API join time, not reconciliation time
          result.newPlayers.nonEmpty,
          result.newMemberships.nonEmpty
        )
      }
    },
    test("existing player joins club") {
      val player4   = Player(pid4, Times.t0, Username("eve"), Active, None, Times.t0)
      val dbState   = DbState(Map.empty, Map.empty)
      val apiMap    = Map(Username("eve") -> Times.t1.getEpochSecond)
      val responses = Map("eve" -> apiPlayerJson(104, "eve"))

      for {
        _      <- seedDb(players = List(player4))
        client <- fakeChessComClient(responses)
        result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
      } yield {
        val change = result.changes.head.changes.head
        assertTrue(
          result.resolvedIds.contains(pid4),
          result.changes.size == 1,
          change.isInstanceOf[JoinedClub],
          change.timestamp == Times.t1, // API join time, not reconciliation time
          result.newMemberships.nonEmpty
        )
      }
    },
    test("username change + status change") {
      val player = Player(pid5, Times.t0, Username("frank-old"), Active, None, Times.t0)
      val mem    = ClubMember(clubId, pid5, Times.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid5 -> MemberState(player, mem)),
        membersByUsername = Map(Username("frank-old") -> MemberState(player, mem))
      )
      val apiMap    = Map(Username("frank-new") -> Times.t0.getEpochSecond)
      val responses = Map("frank-new" -> apiPlayerJson(105, "frank-new", status = "closed"))

      for {
        client <- fakeChessComClient(responses)
        result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid5),
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[UsernameChange]),
        result.changes.head.changes.exists(_.isInstanceOf[StatusChange])
      )
    },
    test("trust-mode: known player joins club without API call") {
      val player = Player(pid3, Times.t0, Username("diana"), Active, None, Times.t0)
      val dbState = DbState(
        membersByPlayerId = Map.empty,
        membersByUsername = Map.empty,
        knownPlayersByUsername = Map(Username("diana") -> player)
      )
      val apiMap = Map(Username("diana") -> Times.t1.getEpochSecond)

      for {
        client <- fakeChessComClient(Map.empty)
        result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
      } yield {
        val change = result.changes.head.changes.head
        assertTrue(
          result.resolvedIds.contains(pid3),
          result.changes.size == 1,
          change.isInstanceOf[JoinedClub],
          change.timestamp == Times.t1, // API join time, not reconciliation time
          result.newMemberships.nonEmpty,
          result.updatedPlayers.isEmpty
        )
      }
    },
    test("trust-mode: username change detected without API call") {
      val oldPlayer = Player(pid2, Times.t0, Username("charlie-old"), Active, None, Times.t0)
      val mem       = ClubMember(clubId, pid2, Times.t0, None)
      val newPlayer = Player(pid2, Times.t0, Username("charlie-new"), Active, None, Times.t1)
      val dbState = DbState(
        membersByPlayerId = Map(pid2 -> MemberState(oldPlayer, mem)),
        membersByUsername = Map(Username("charlie-old") -> MemberState(oldPlayer, mem)),
        knownPlayersByUsername = Map(Username("charlie-new") -> newPlayer)
      )
      val apiMap = Map(Username("charlie-new") -> Times.t0.getEpochSecond)

      for {
        client <- fakeChessComClient(Map.empty)
        result <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
      } yield assertTrue(
        result.resolvedIds.contains(pid2),
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[UsernameChange]),
        result.updatedPlayers.nonEmpty
      )
    },
    test("sinceApproximate member → replaceSince, not Rejoined") {
      val player = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val mem    = ClubMember(clubId, pid0, Times.t0, None, sinceApproximate = true)
      val dbState = DbState(
        membersByPlayerId = Map(pid0 -> MemberState(player, mem)),
        membersByUsername = Map(Username("alice") -> MemberState(player, mem))
      )
      val apiMap = Map(Username("alice") -> Times.t1.getEpochSecond)

      for {
        _ <- seedDb(
          players = List(player),
          members = List(mem)
        )
        client  <- fakeChessComClient(Map.empty)
        result  <- MembershipClassify.classifyApiMembers(client, clubId, apiMap, dbState, Times.t2)
        members <- ClubMember.selectClub(clubId)
      } yield assertTrue(
        result.resolvedIds.contains(pid0),
        result.changes.isEmpty,
        result.newMemberships.isEmpty,
        result.closedMemberships.isEmpty,
        members.size == 1,
        members.head.since == Times.t1,
        !members.head.sinceApproximate
      )
    },
    test("trustUsernames=false bypasses known player lookup") {
      val player = Player(pid3, Times.t0, Username("diana"), Active, None, Times.t0)
      val dbState = DbState(
        membersByPlayerId = Map.empty,
        membersByUsername = Map.empty,
        knownPlayersByUsername = Map(Username("diana") -> player)
      )
      val apiMap = Map(Username("diana") -> Times.t0.getEpochSecond)

      for {
        client <- fakeChessComClient(Map.empty)
        result <- MembershipClassify.classifyApiMembers(
          client,
          clubId,
          apiMap,
          dbState,
          Times.t2,
          trustUsernames = false
        ).exit
      } yield assertTrue(result.isFailure)
    }
  )

  // ==========================================================================
  // Suite E: classifyDisappeared (fake HTTP)
  // ==========================================================================

  private def suiteClassifyDisappeared = suite("classifyDisappeared")(
    test("active player left club → LeftClub with now timestamp") {
      val player = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val mem    = ClubMember(clubId, pid0, Times.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid0 -> MemberState(player, mem)),
        membersByUsername = Map(Username("alice") -> MemberState(player, mem))
      )
      val responses = Map("alice" -> apiPlayerJson(100, "alice"))

      for {
        client <- fakeChessComClient(responses)
        result <- MembershipClassify.classifyDisappeared(
          client,
          dbState,
          Set.empty,
          Map.empty,
          ClubSlug("test-club"),
          Times.t2
        )
      } yield {
        val change = result.changes.head.changes.head
        assertTrue(
          result.changes.size == 1,
          change.isInstanceOf[LeftClub],
          change.timestamp == Times.t2, // detection time — no authoritative departure time
          result.closedMemberships.nonEmpty,
          result.closedMemberships.head.until.contains(Times.t2)
        )
      }
    },
    test("closed player → AccountClosed with lastOnline timestamp") {
      val player = Player(pid1, Times.t0, Username("bob"), Active, None, Times.t0)
      val mem    = ClubMember(clubId, pid1, Times.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid1 -> MemberState(player, mem)),
        membersByUsername = Map(Username("bob") -> MemberState(player, mem))
      )
      val responses = Map("bob" -> apiPlayerJson(101, "bob", status = "closed", lastOnline = Times.t1.getEpochSecond))

      for {
        client <- fakeChessComClient(responses)
        result <- MembershipClassify.classifyDisappeared(
          client,
          dbState,
          Set.empty,
          Map.empty,
          ClubSlug("test-club"),
          Times.t2
        )
      } yield {
        val change = result.changes.head.changes.head
        assertTrue(
          result.changes.size == 1,
          change.isInstanceOf[AccountClosed],
          change.timestamp == Times.t1, // lastOnline, not reconciliation time
          result.updatedPlayers.nonEmpty,
          result.closedMemberships.nonEmpty,
          result.closedMemberships.head.until.contains(Times.t1) // until matches lastOnline
        )
      }
    },
    test("API 404 → Unresolvable") {
      val player = Player(pid2, Times.t0, Username("charlie"), Active, None, Times.t0)
      val mem    = ClubMember(clubId, pid2, Times.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid2 -> MemberState(player, mem)),
        membersByUsername = Map(Username("charlie") -> MemberState(player, mem))
      )

      for {
        client <- fakeChessComClient(Map.empty, failures = Set("charlie"))
        result <- MembershipClassify.classifyDisappeared(
          client,
          dbState,
          Set.empty,
          Map.empty,
          ClubSlug("test-club"),
          Times.t2
        )
      } yield assertTrue(
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[Unresolvable]),
        result.closedMemberships.nonEmpty
      )
    },
    test("different player ID at same username → Unresolvable") {
      val player = Player(pid3, Times.t0, Username("diana"), Active, None, Times.t0)
      val mem    = ClubMember(clubId, pid3, Times.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid3 -> MemberState(player, mem)),
        membersByUsername = Map(Username("diana") -> MemberState(player, mem))
      )
      val responses = Map("diana" -> apiPlayerJson(999, "diana"))

      for {
        client <- fakeChessComClient(responses)
        result <- MembershipClassify.classifyDisappeared(
          client,
          dbState,
          Set.empty,
          Map.empty,
          ClubSlug("test-club"),
          Times.t2
        )
      } yield assertTrue(
        result.changes.size == 1,
        result.changes.head.changes.exists(_.isInstanceOf[Unresolvable]),
        result.closedMemberships.nonEmpty
      )
    },
    test("all resolved → empty results") {
      val player = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val mem    = ClubMember(clubId, pid0, Times.t0, None)
      val dbState = DbState(
        membersByPlayerId = Map(pid0 -> MemberState(player, mem)),
        membersByUsername = Map(Username("alice") -> MemberState(player, mem))
      )

      for {
        client <- fakeChessComClient(Map.empty)
        result <- MembershipClassify.classifyDisappeared(
          client,
          dbState,
          Set(pid0),
          Map.empty,
          ClubSlug("test-club"),
          Times.t2
        )
      } yield assertTrue(
        result.changes.isEmpty,
        result.updatedPlayers.isEmpty,
        result.closedMemberships.isEmpty
      )
    }
  )
}
