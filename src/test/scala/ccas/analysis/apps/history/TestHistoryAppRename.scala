package ccas.analysis.apps.history

import zio.{Chunk, Ref, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{apiClubJson, apiClubMembersJson}
import ccas.analysis.tables.Tables
import ccas.api.misc.subtypes.ClubSlug
import ccas.utils.ProgressDisplay
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}

/** A history run addressed by a club's former slug learns the current one during its membership reconcile, and must
  * seed from that — `isClubDailyMatch` compares match URLs against the slug, so a stale one silently matches nothing.
  */
object TestHistoryAppRename extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestHistoryAppRename")(
    test("a run addressed by a former slug seeds and reports under the current one") {
      val clubId = 910_600L
      for {
        requested <- Ref.make(Chunk.empty[String])
        routes: Routes[Any, Response] = Routes(
          Method.GET / "pub" / "club" / string("slug") / "members" -> handler { (slug: String, _: Request) =>
            requested.update(_ :+ s"members:$slug").as(Response.json(apiClubMembersJson(Nil)))
          },
          Method.GET / "pub" / "club" / string("slug") / "matches" -> handler { (slug: String, _: Request) =>
            requested.update(_ :+ s"matches:$slug").as(Response.json(emptyClubMatchesJson))
          },
          // Chess.com answers the old name with the club's current `@id`.
          Method.GET / "pub" / "club" / string("slug") -> handler { (_: String, _: Request) =>
            Response.json(apiClubJson(clubId, "current-name"))
          }
        )
        client <- TestChessComClientSupport.fakeClient(routes)
        result <- HistoryApp.discover(ClubSlug("former-name"), expectedClubIdOption = None)
          .provideSomeLayer[ProgressDisplay & PostgresClient](ZLayer.succeed(client))
        paths <- requested.get
      } yield assertTrue(
        result.clubSlug == ClubSlug("current-name"),
        paths.contains("matches:current-name"),
        !paths.contains("matches:former-name")
      )
    }
  ).provideShared(
    FreshSchemaLayer("test_history_app_rename", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false))
  ) @@ TestAspect.withLiveClock

  private val emptyClubMatchesJson = """{"finished": [], "in_progress": [], "registered": []}"""
}
