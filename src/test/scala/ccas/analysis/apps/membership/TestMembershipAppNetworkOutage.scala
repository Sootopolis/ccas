package ccas.analysis.apps.membership

import zio.{Exit, Scope, ZLayer}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.membership.MembershipChange.{DbState, MemberState}
import ccas.analysis.tables.{ClubMember, Player, Tables}
import ccas.api.misc.enums.PlayerStatusCategory.Active
import ccas.api.misc.subtypes.Username
import ccas.utils.ProgressDisplay
import ccas.utils.client.{NetworkUnavailableException, TestChessComClientSupport}
import ccas.utils.sql.FreshSchemaLayer

import TestMembershipAppSupport.*

/** A systemic network outage during membership classification must abort, not be swallowed into a member
  * classification. A disappeared *active* member routes through `classifyOneDisappearedActive`, whose `ApiPlayer.get`
  * is the first network call; on an outage it must re-raise rather than fall through to the match-ref fallback (which
  * would misclassify the member as left / closed / unresolvable on incomplete data). Issue #119.
  */
object TestMembershipAppNetworkOutage extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestMembershipAppNetworkOutage")(
    testClassifyDisappearedOutageReRaises
  ).provideShared(
    FreshSchemaLayer("test_membership_app_network_outage", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false)),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private def isNetworkUnavailable(exit: Exit[Throwable, Any]): Boolean =
    exit match {
      case Exit.Failure(cause) => cause.failures.exists(_.isInstanceOf[NetworkUnavailableException])
      case _                   => false
    }

  private def testClassifyDisappearedOutageReRaises =
    test("outage during classifyDisappeared re-raises instead of misclassifying the member") {
      val player = Player(pid0, Times.t0, Username("alice"), Active, None, Times.t0)
      val mem    = ClubMember(clubId, pid0, Times.t0, None, sinceApproximate = false)
      val dbState = DbState(
        membersByPlayerId = Map(pid0 -> MemberState(player, mem)),
        membersByUsername = Map(Username("alice") -> MemberState(player, mem))
      )
      for {
        client <- TestChessComClientSupport.networkDownClient
        exit   <- MembershipClassify.classifyDisappeared(client, dbState, Set.empty, Map.empty, Times.t2).exit
      } yield assertTrue(isNetworkUnavailable(exit))
    }
}
