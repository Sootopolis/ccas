package ccas.analysis.apps.ref

import com.augustnagro.magnum.sql
import zio.{Exit, Scope}
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.apiDailyMatchJson
import ccas.analysis.tables.{ClubRefSkip, PlayerMatchRef, PlayerRefSkip, Tables}
import ccas.utils.client.NetworkUnavailableException
import ccas.utils.sql.FreshSchemaLayer
import ccas.utils.sql.PostgresClient.connectZIO

import TestRefAppSupport.*

/** A systemic network/DNS outage must abort the run and record NO skip rows — a local blip should not poison
  * resources for the 3-day `ApiError` retry window. The client surfaces the outage as `NetworkUnavailableException`
  * once its retry schedule exhausts; RefApp re-raises it instead of recording a per-resource skip.
  */
object TestRefAppNetworkOutage extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestRefAppNetworkOutage")(
    testOutageAbortsAndRecordsNoSkips,
    testOutageInDeeperFetchAbortsWithoutSkip,
    testNextRunRetriesAfterOutage
  ).provideShared(
    FreshSchemaLayer("test_ref_app_network_outage", onInit = Tables.ensureTables),
    Scope.default
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock

  private def isNetworkUnavailable(exit: Exit[Throwable, Any]): Boolean =
    exit match {
      case Exit.Failure(cause) => cause.failures.exists(_.isInstanceOf[NetworkUnavailableException])
      case _                   => false
    }

  private def testOutageAbortsAndRecordsNoSkips =
    test("systemic outage aborts the run and records no player/club skips") {
      for {
        _      <- seedDb
        client <- networkDownChessComClient
        exit   <- runPopulate(client, forceSkipped = false, upgradeRefs = false).exit
        p0     <- PlayerRefSkip.selectId(pid0)
        p1     <- PlayerRefSkip.selectId(pid1)
        p2     <- PlayerRefSkip.selectId(pid2)
        c0     <- ClubRefSkip.selectId(clubId0)
        c1     <- ClubRefSkip.selectId(clubId1)
      } yield assertTrue(
        isNetworkUnavailable(exit),
        p0.isEmpty,
        p1.isEmpty,
        p2.isEmpty,
        c0.isEmpty,
        c1.isEmpty
      )
    }

  // The outage reaches a deeper fetch: alice's match listing is served, but the per-match fetch is down. The
  // `fetchMatch` foldZIO arm must let the typed error propagate (abort) rather than swallow it into a NotFound that
  // would eventually skip alice. Clubs are removed so resolution starts with players and the abort is clearly the
  // player path.
  private def testOutageInDeeperFetchAbortsWithoutSkip =
    test("outage in a deeper per-match fetch aborts without skipping the player") {
      val matchJson = apiDailyMatchJson(
        matchId1,
        "our-club",
        "other-club",
        team1Players = List(("alice", 3)),
        team2Players = List(("opponent1", 1))
      )
      for {
        _ <- seedDb
        _ <- connectZIO(sql"DELETE FROM club".update.run())
        client <- chessComClientWithOutage(
          // bob/charlie default to empty matches in refRoutes; only alice's listing + the down match matter here.
          responses = Map(
            s"player/alice/matches" -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
            s"match/$matchId1"      -> matchJson
          ),
          isDown = _.url.encode.contains(s"/match/$matchId1")
        )
        exit  <- runPopulate(client, forceSkipped = false, upgradeRefs = false).exit
        ref   <- PlayerMatchRef.selectId(pid0)
        skip0 <- PlayerRefSkip.selectId(pid0)
      } yield assertTrue(
        isNetworkUnavailable(exit),
        ref.isEmpty,   // never resolved (aborted)
        skip0.isEmpty  // and crucially never skipped
      )
    }

  private def testNextRunRetriesAfterOutage =
    test("next run after an outage retries the resource (not suppressed)") {
      val matchJson = apiDailyMatchJson(
        matchId1,
        "our-club",
        "other-club",
        team1Players = List(("alice", 3)),
        team2Players = List(("opponent1", 1))
      )
      for {
        _          <- seedDb
        downClient <- networkDownChessComClient
        exit1      <- runPopulate(downClient, forceSkipped = false, upgradeRefs = false).exit
        skip0After <- PlayerRefSkip.selectId(pid0)
        okClient <- fakeChessComClient(
          Map(
            s"player/alice/matches"   -> apiPlayerMatchesJson(List((matchId1, Some(3)))),
            s"player/bob/matches"     -> emptyPlayerMatchesJson,
            s"player/charlie/matches" -> emptyPlayerMatchesJson,
            s"match/$matchId1"        -> matchJson
          )
        )
        _   <- runPopulate(okClient, forceSkipped = false, upgradeRefs = false)
        ref <- PlayerMatchRef.selectId(pid0)
      } yield assertTrue(
        isNetworkUnavailable(exit1),
        skip0After.isEmpty, // run 1 left no suppression
        ref.isDefined       // so run 2 resolves immediately
      )
    }
}
