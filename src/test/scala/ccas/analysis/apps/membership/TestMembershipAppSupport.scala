package ccas.analysis.apps.membership

import java.time.Instant

import com.augustnagro.magnum.sql
import zio.{RIO, ZIO}
import zio.http.*

import ccas.analysis.apps.TestTimes
import ccas.analysis.apps.recruitment.CandidateOutcome
import ccas.analysis.tables.{Club, ClubMember, Player, PlayerMatchRef, PlayerSnapshot, RecruitmentCandidate}
import ccas.analysis.tables.subtypes.RecruitmentRunId
import ccas.api.misc.subtypes.{ClubId, ClubSlug, PlayerId}
import ccas.utils.client.{ChessComClient, TestChessComClientSupport}
import ccas.utils.sql.PostgresClient
import ccas.utils.sql.DbCodecs.given
import ccas.utils.sql.PostgresClient.connectZIO

object TestMembershipAppSupport {

  // Back-compat alias so test files that wildcard-import this object still resolve `Times.t0`. New tests should
  // reference `TestTimes` directly.
  val Times = TestTimes

  // --- IDs ---

  val pid0 = PlayerId(100)
  val pid1 = PlayerId(101)
  val pid2 = PlayerId(102)
  val pid3 = PlayerId(103)
  val pid4 = PlayerId(104)
  val pid5 = PlayerId(105)

  val clubId = ClubId(500)
  val club   = Club(clubId, TestTimes.t0, ClubSlug("test-club"), "Test Club", None, None, None)

  val otherClubId = ClubId(501)
  val otherClub   = Club(otherClubId, TestTimes.t0, ClubSlug("other-club"), "Other Club", None, None, None)

  // --- Helpers ---

  def fakeChessComClient(
    responses: Map[String, String],
    failures: Set[String] = Set.empty,
    clubsResponses: Map[String, String] = Map.empty,
    matchResponses: Map[String, String] = Map.empty
  ): RIO[PostgresClient, ChessComClient] = {
    val routes: Routes[Any, Response] = Routes(
      Method.GET / "pub" / "player" / string("username") -> handler { (username: String, _: Request) =>
        if (failures.contains(username)) { Response(status = Status.NotFound) }
        else { responses.get(username).fold(Response(status = Status.NotFound))(Response.json(_)) }
      },
      Method.GET / "pub" / "player" / string("username") / "clubs" -> handler {
        (username: String, _: Request) =>
          clubsResponses.get(username).fold(Response(status = Status.NotFound))(Response.json(_))
      },
      Method.GET / "pub" / "match" / string("matchId") -> handler { (matchId: String, _: Request) =>
        matchResponses.get(matchId).fold(Response(status = Status.NotFound))(Response.json(_))
      }
    )
    TestChessComClientSupport.fakeClient(routes)
  }

  private val testPlayerIds = List(pid0, pid1, pid2, pid3, pid4, pid5)

  def seedDb(
    players: List[Player] = Nil,
    snapshots: List[PlayerSnapshot] = Nil,
    members: List[ClubMember] = Nil,
    matchRefs: List[PlayerMatchRef] = Nil
  ): RIO[PostgresClient, Unit] =
    for {
      _ <- connectZIO(sql"DELETE FROM club_member WHERE club_id = $clubId".update.run())
      _ <- ZIO.foreachDiscard(testPlayerIds) { pid =>
        connectZIO(sql"DELETE FROM recruitment_candidate WHERE player_id = $pid".update.run()) *>
          connectZIO(sql"DELETE FROM player_match_ref WHERE player_id = $pid".update.run()) *>
          connectZIO(sql"DELETE FROM player_snapshot WHERE player_id = $pid".update.run()) *>
          connectZIO(sql"DELETE FROM player WHERE player_id = $pid".update.run())
      }
      _ <- Club.upsert(club)
      _ <- ZIO.whenDiscard(players.nonEmpty)(Player.insertBatch(players))
      _ <- ZIO.whenDiscard(snapshots.nonEmpty)(PlayerSnapshot.insertBatch(snapshots))
      _ <- ZIO.whenDiscard(members.nonEmpty)(ClubMember.insertBatch(members))
      _ <- ZIO.whenDiscard(matchRefs.nonEmpty)(PlayerMatchRef.insertBatch(matchRefs))
    } yield ()

  def seedRecruitmentInvitation(
    forClubId: ClubId,
    playerId: PlayerId,
    evaluatedAt: Instant
  ): ZIO[PostgresClient, java.sql.SQLException, Unit] =
    for {
      criteriaId <- connectZIO {
        sql"""INSERT INTO recruitment_criteria (
                nationality_exclude, nationality_countries, exclude_clubs,
                exclude_source_admins, exclude_former_members
              ) VALUES (false, '{}', '{}', false, false)
              RETURNING criteria_id""".query[Long].run().head
      }
      runId <- connectZIO {
        sql"""INSERT INTO recruitment_run (club_id, criteria_id, trigger, started_at, candidates_found)
              VALUES ($forClubId, $criteriaId, 'Cli', $evaluatedAt, 1)
              RETURNING run_id""".query[RecruitmentRunId].run().head
      }
      _ <- RecruitmentCandidate.insert(
        RecruitmentCandidate(runId, playerId, evaluatedAt, CandidateOutcome.Invited, None)
      )
    } yield ()
}
