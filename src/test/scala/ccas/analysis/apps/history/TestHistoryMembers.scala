package ccas.analysis.apps.history

import java.time.Instant

import com.augustnagro.magnum.sql
import zio.{Chunk, Ref, ZLayer}
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.recruitment.RecruitmentTestSupport.{apiClubJson, apiClubMembersJson}
import ccas.analysis.tables.{Club, ClubMember, Player, Tables}
import ccas.api.misc.enums.PlayerStatusCategory
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId, Username}
import ccas.utils.ProgressDisplay
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}
import ccas.utils.sql.PostgresClient.connectZIO

/** The players a History run starts from, whose match lists it queries. */
object TestHistoryMembers extends ZIOSpecDefault {

  override def spec: Spec[Any, Throwable] = suite("TestHistoryMembers")(
    test("a run starts from a player who sat on the club's side of a board, though no row records them") {
      val clubId = ClubId(910_900)
      val slug   = ClubSlug("history-members")
      val t0     = Instant.parse("2024-01-01T00:00:00Z")
      val player = Player(PlayerId(910_901), t0, Username("board-only"), PlayerStatusCategory.Active, None, t0)
      val board = connectZIO {
        sql"""INSERT INTO club_match (match_id, name, status, time_class, boards, team1_club_id, team1_score_x2,
                team2_club_id, team2_score_x2, fetched_at)
              VALUES (910902, 'A match', 'Finished', 'Daily', 1, $clubId, 2, NULL, 2, now())""".update.run()
        sql"""INSERT INTO club_match_board (match_id, board, team1_player_id, team1_fair_play, team2_player_id,
                team2_fair_play, team1_score_x2, team2_score_x2)
              VALUES (910902, 1, ${player.playerId}, false, NULL, false, 2, 2)""".update.run()
      }
      for {
        queried <- Ref.make(Chunk.empty[String])
        routes = Routes(
          Method.GET / "pub" / "club" / string("slug") / "members" -> handler { (_: String, _: Request) =>
            Response.json(apiClubMembersJson(Nil))
          },
          Method.GET / "pub" / "club" / string("slug") / "matches" -> handler { (_: String, _: Request) =>
            Response.json(emptyMatchesJson)
          },
          Method.GET / "pub" / "club" / string("slug") -> handler { (name: String, _: Request) =>
            Response.json(apiClubJson(ClubId.unwrap(clubId), name))
          },
          Method.GET / "pub" / "player" / string("username") / "matches" -> handler { (username: String, _: Request) =>
            queried.update(_ :+ username).as(Response.json(emptyMatchesJson))
          }
        )
        client <- TestChessComClientSupport.fakeClient(routes)
        _      <- Club.upsert(Club(clubId, t0, slug, "History members", None, None, None))
        _      <- Player.writeBatch(inserted = List(player), updated = Nil)
        _      <- board
        _ <- HistoryApp
          .discover(clubSlug = slug, expectedClubIdOption = None)
          .provideSomeLayer[ProgressDisplay & PostgresClient](ZLayer.succeed(client))
        names   <- queried.get
        members <- ClubMember.selectClub(clubId)
      } yield assertTrue(names == Chunk(player.username.value), members.isEmpty)
    }
  ).provideShared(
    FreshSchemaLayer("test_history_members", onInit = Tables.ensureTables),
    ZLayer.succeed(ProgressDisplay.make(enabled = false))
  ) @@ TestAspect.withLiveClock

  private val emptyMatchesJson = """{"finished": [], "in_progress": [], "registered": []}"""
}
