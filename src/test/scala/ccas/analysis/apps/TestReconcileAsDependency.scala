package ccas.analysis.apps

import com.augustnagro.magnum.sql
import zio.{RIO, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.history.HistoryApp
import ccas.analysis.apps.recruitment.RecruitmentApp
import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{apiClubJson, apiClubMembersJson}
import ccas.analysis.tables.{RunTrigger, Tables}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.ProgressDisplay
import ccas.utils.client.{BodyStore, ChessComClient, TestChessComClientSupport}
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.connectZIO

/** History and Recruitment start with a reconcile, and record it as a run another job made (ADR 0021). */
object TestReconcileAsDependency extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestReconcileAsDependency")(
    test("a history run records its reconcile as a dependency run") {
      val clubId = ClubId(910_800)
      for {
        _    <- withClient(clubId)(HistoryApp.discover(clubSlug = ClubSlug("history-dep"), expectedClubIdOption = None))
        runs <- runsOf(clubId)
      } yield assertTrue(runs == List(RunTrigger.Dependency.toString -> true))
    },
    test("a recruitment run records its reconcile as a dependency run, even when it then fails") {
      val clubId = ClubId(910_810)
      val recruit = RecruitmentApp.recruit(
        clubSlug = ClubSlug("recruitment-dep"),
        expectedClubIdOption = None,
        alias = "no-such-alias"
      )
      for {
        exit <- withClient(clubId)(recruit).exit
        runs <- runsOf(clubId)
      } yield assertTrue(exit.isFailure, runs == List(RunTrigger.Dependency.toString -> true))
    }
  ).provideShared(
    FreshSchemaLayer("test_reconcile_as_dependency", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false))
  ) @@ TestAspect.withLiveClock @@ TestAspect.sequential

  private val emptyMatchesJson = """{"finished": [], "in_progress": [], "registered": []}"""

  /** Each of `clubId`'s runs, as its trigger and whether it completed. */
  private def runsOf(clubId: ClubId): RIO[PostgresClient, List[(String, Boolean)]] =
    connectZIO {
      sql"SELECT trigger, completed_at IS NOT NULL FROM membership_run WHERE club_id = $clubId"
        .query[(String, Boolean)].run().toList
    }

  /** Runs `effect` against a Chess.com where `clubId` has an empty roster and no matches. */
  private def withClient[A](clubId: ClubId)(
    effect: ZIO[ProgressDisplay & ChessComClient & PostgresClient, Throwable, A]
  ): RIO[ProgressDisplay & PostgresClient & BodyStore, A] = {
    val routes: Routes[Any, Response] = Routes(
      Method.GET / "pub" / "club" / string("slug") / "members" -> handler { (_: String, _: Request) =>
        Response.json(apiClubMembersJson(Nil))
      },
      Method.GET / "pub" / "club" / string("slug") / "matches" -> handler { (_: String, _: Request) =>
        Response.json(emptyMatchesJson)
      },
      Method.GET / "pub" / "club" / string("slug") -> handler { (slug: String, _: Request) =>
        Response.json(apiClubJson(ClubId.unwrap(clubId), slug))
      }
    )
    TestChessComClientSupport.fakeClient(routes).flatMap { client =>
      effect.provideSomeLayer[ProgressDisplay & PostgresClient](ZLayer.succeed(client))
    }
  }
}
