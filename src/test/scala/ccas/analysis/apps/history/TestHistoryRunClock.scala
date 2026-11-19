package ccas.analysis.apps.history

import java.time.{Clock as JClock, Instant, ZoneOffset}

import com.augustnagro.magnum.sql
import zio.{Clock, ZIO, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{apiClubJson, apiClubMembersJson}
import ccas.analysis.tables.{Club, Tables}
import ccas.api.misc.subtypes.{ClubId, ClubSlug}
import ccas.utils.ProgressDisplay
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient.connectZIO

/** A History run stamps its own row from the app clock, as `JobRunner` stamps the job's (#509). */
object TestHistoryRunClock extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestHistoryRunClock")(
    test("a history run's started_at and completed_at come from the app clock") {
      val clubId = ClubId(911_000)
      val slug   = ClubSlug("history-run-clock")
      val pinned = Instant.parse("2024-03-01T12:00:00Z")
      val routes = Routes(
        Method.GET / "pub" / "club" / string("slug") / "members" -> handler { (_: String, _: Request) =>
          Response.json(apiClubMembersJson(Nil))
        },
        Method.GET / "pub" / "club" / string("slug") / "matches" -> handler { (_: String, _: Request) =>
          Response.json(emptyMatchesJson)
        },
        Method.GET / "pub" / "club" / string("slug") -> handler { (name: String, _: Request) =>
          Response.json(apiClubJson(ClubId.unwrap(clubId), name))
        }
      )
      for {
        client <- TestChessComClientSupport.fakeClient(routes)
        _      <- Club.upsert(Club(clubId, pinned, slug, "History run clock", None, None, None))
        // A fixed java.time clock rather than TestClock: its sleeps stay real, so the client's pacing still runs.
        _ <- ZIO.withClock(Clock.ClockJava(JClock.fixed(pinned, ZoneOffset.UTC))) {
          HistoryApp
            .discover(clubSlug = slug, expectedClubIdOption = None)
            .provideSomeLayer[ProgressDisplay & PostgresClient](ZLayer.succeed(client))
        }
        times <- connectZIO {
          sql"SELECT started_at, completed_at FROM history_run WHERE club_id = $clubId"
            .query[(Instant, Option[Instant])].run().toList
        }
      } yield assertTrue(times == List((pinned, Some(pinned))))
    }
  ).provideShared(
    FreshSchemaLayer(schema = "test_history_run_clock", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false))
  ) @@ TestAspect.withLiveClock

  private val emptyMatchesJson = """{"finished": [], "in_progress": [], "registered": []}"""
}
