package ccas.analysis.apps.history

import java.time.Instant

import zio.{Exit, Ref, Scope, ZLayer}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.tables.{Club, ClubMember, HistoryMemberQuery, Player, Tables}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.utils.ProgressDisplay
import ccas.utils.client.{NetworkUnavailableException, TestChessComClientSupport}
import ccas.utils.sql.FreshSchemaLayer

/** A systemic outage during per-member seeding must abort, not be recorded as a one-off failure — and crucially must
  * NOT stamp a `history_member_query` row that would suppress re-querying the member next run. Issue #119.
  */
object TestHistoryAppNetworkOutage extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestHistoryAppNetworkOutage")(
    testSeedFromMemberMatchesOutageReRaises
  ).provideShared(
    FreshSchemaLayer("test_history_app_network_outage", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false)),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private val t0 = Instant.parse("2025-01-01T00:00:00Z")

  private def isNetworkUnavailable(exit: Exit[Throwable, Any]): Boolean =
    exit match {
      case Exit.Failure(cause) => cause.failures.exists(_.isInstanceOf[NetworkUnavailableException])
      case _                   => false
    }

  private def testSeedFromMemberMatchesOutageReRaises =
    test("outage during seedFromMemberMatches re-raises and stamps no history_member_query") {
      val clubId   = ClubId(910_500)
      val clubSlug = ClubSlug("outage-test-club")
      val pid      = PlayerId(910_001)
      val username = Username("active-member")
      val member   = ClubMember(clubId, pid, t0, None, sinceApproximate = false)
      val player   = Player(pid, t0, username, PlayerStatusCategory.Active, None, t0)
      for {
        _            <- Club.upsert(Club(clubId, t0, clubSlug, "Outage test", None, None, None))
        _            <- Player.insertIfNew(player)
        _            <- ClubMember.insert(member)
        unchangedRef <- Ref.make(0)
        client       <- TestChessComClientSupport.networkDownClient
        exit <- HistorySeeding
          .seedFromMemberMatches(
            client,
            clubId,
            clubSlug,
            allMembers = List(member),
            queriedIds = Set.empty,
            playerById = Map(pid -> player),
            excludeMatchIds = Set.empty,
            includeFinished = false,
            shared = None,
            unchangedPlayerCounter = unchangedRef
          )
          .exit
        stamped <- HistoryMemberQuery.selectClubPlayerIds(clubId)
      } yield assertTrue(isNetworkUnavailable(exit), stamped.isEmpty)
    }
}
