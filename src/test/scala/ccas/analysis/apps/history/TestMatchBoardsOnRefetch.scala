package ccas.analysis.apps.history

import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import zio.ZEnvironment
import zio.http.*
import zio.test.{assertTrue, Spec, TestAspect, ZIOSpecDefault}

import ccas.analysis.apps.history.HistoryUtils.ProcessingContext
import ccas.analysis.tables.*
import ccas.api.clubmatch.ApiDailyMatch
import ccas.api.misc.enums.{ClubMatchStatus, TimeClass}
import ccas.api.misc.subtypes.{ClubId, ClubMatchId, ClubSlug}
import ccas.utils.ProgressDisplay
import ccas.utils.client.TestChessComClientSupport
import ccas.utils.json.JsonDecodingException
import ccas.utils.sql.{FreshSchemaLayer, PostgresClient}

/** What a re-fetched match's body does to the boards already stored for it (#416). A body that can't be read must leave
  * them alone on both routes that can delete them, while a match nobody played still clears them.
  */
object TestMatchBoardsOnRefetch extends ZIOSpecDefault {

  private val t0: Instant = LocalDateTime.of(2025, 6, 1, 0, 0).toInstant(ZoneOffset.UTC)

  private val clubId       = ClubId(730)
  private val clubSlug     = ClubSlug("refetch-test-club")
  private val club         = Club(clubId, t0, clubSlug, "Refetch Test Club", None, None, None)
  private val opponentId   = ClubId(731)
  private val opponentSlug = ClubSlug("refetch-test-opponent")
  private val opponentClub = Club(opponentId, t0, opponentSlug, "Refetch Test Opponent", None, None, None)

  // `Instant.EPOCH` is no fixture's date, so comparing to it shows whether anything wrote the row.
  private val initialFetchedAt = Instant.EPOCH

  private def matchRow(matchId: ClubMatchId, processedBodyHash: Option[String]): ClubMatch =
    ClubMatch(
      matchId = matchId,
      name = s"Match $matchId",
      status = ClubMatchStatus.Finished,
      timeClass = TimeClass.Daily,
      startTime = Some(t0),
      endTime = Some(t0.plus(Duration.ofDays(7))),
      boards = 1,
      team1ClubIdOption = Some(clubId),
      team1ScoreX2 = 2,
      team2ClubIdOption = Some(opponentId),
      team2ScoreX2 = 2,
      fetchedAt = initialFetchedAt,
      processedBodyHash = processedBodyHash
    )

  private def boardRow(matchId: ClubMatchId): ClubMatchBoard =
    ClubMatchBoard(
      matchId = matchId,
      board = 1,
      team1PlayerIdOption = None,
      team1FairPlay = false,
      team2PlayerIdOption = None,
      team2FairPlay = false,
      team1ScoreX2 = 2,
      team2ScoreX2 = 2
    )

  private def seedMatch(matchId: ClubMatchId, processedBodyHash: Option[String]) =
    Club.upsert(club) *> Club.upsert(opponentClub) *> ClubMatch.upsert(matchRow(matchId, processedBodyHash)) *>
      ClubMatchBoard.insertBatch(List(boardRow(matchId)))

  private def teamBody(slug: ClubSlug, score: Int, players: String): String =
    s"""{
      "@id": "https://api.chess.com/pub/club/${slug.value}",
      "name": "${slug.value}",
      "url": "https://www.chess.com/club/${slug.value}",
      "score": $score,
      "result": "draw",
      "players": [$players],
      "fair_play_removals": []
    }"""

  private def matchBody(matchId: ClubMatchId, team1: String, team2: String): String =
    s"""{
      "@id": "https://api.chess.com/pub/match/$matchId",
      "name": "Match $matchId",
      "url": "https://www.chess.com/club/matches/$matchId",
      "start_time": ${t0.getEpochSecond},
      "end_time": ${t0.plus(Duration.ofDays(7)).getEpochSecond},
      "status": "finished",
      "boards": 1,
      "settings": {"rules": "chess", "time_class": "daily", "time_control": "1/259200", "min_required_games": 0},
      "teams": {"team1": $team1, "team2": $team2}
    }"""

  private def stats(username: String) = s"https://api.chess.com/pub/player/$username/stats"

  private def bodyWithUnknownResult(matchId: ClubMatchId): String = {
    def player(username: String, asWhite: String, asBlack: String) =
      s"""{
        "username": "$username",
        "stats": "${stats(username)}",
        "played_as_white": "$asWhite",
        "played_as_black": "$asBlack",
        "board": "https://api.chess.com/pub/match/$matchId/1"
      }"""
    matchBody(
      matchId = matchId,
      team1 = teamBody(clubSlug, 1, player("first-player", "no-such-result", "win")),
      team2 = teamBody(opponentSlug, 1, player("second-player", "no-such-result", "win"))
    )
  }

  private def cancelledBody(matchId: ClubMatchId): String = {
    def player(username: String) = s"""{"username": "$username", "stats": "${stats(username)}"}"""
    matchBody(
      matchId = matchId,
      team1 = teamBody(clubSlug, 0, player("first-player")),
      team2 = teamBody(opponentSlug, 0, player("second-player"))
    )
  }

  private def contextServing(body: String) = {
    val routes = Routes(
      Method.GET / "pub" / "match" / long("matchId") -> handler((_: Long, _: Request) => Response.json(body))
    )
    TestChessComClientSupport.fakeClient(routes)
      .flatMap(ProcessingContext.make(_, clubId, clubSlug, Map.empty, Set.empty))
  }

  private def withoutProgress(env: ZEnvironment[PostgresClient]) =
    env.add[ProgressDisplay](ProgressDisplay.make(enabled = false))

  private def runWaves(ctx: ProcessingContext) =
    HistoryProcessing.processWaves(ctx, Set.empty).provideSomeEnvironment[PostgresClient](withoutProgress)

  private def runRefresh(ctx: ProcessingContext, matchId: ClubMatchId) =
    HistoryProcessing.refreshSingleMatch(ctx, matchId).provideSomeEnvironment[PostgresClient](withoutProgress)

  private def pendingRow(matchId: ClubMatchId) =
    HistoryPendingMatch(clubId = clubId, matchId = matchId, isLive = false)

  override def spec: Spec[Any, Throwable] = suite("a re-fetched match and the boards stored for it")(
    test("an unknown game result on a re-queued match keeps its boards and leaves it pending as ApiError") {
      val matchId = ClubMatchId(9101)
      for {
        _       <- seedMatch(matchId, None)
        _       <- HistoryPendingMatch.insert(pendingRow(matchId))
        ctx     <- contextServing(bodyWithUnknownResult(matchId))
        _       <- runWaves(ctx)
        boards  <- ClubMatchBoard.selectMatch(matchId)
        row     <- ClubMatch.selectId(matchId)
        pending <- HistoryPendingMatch.selectClub(clubId)
        failed  <- ctx.failedMatches.get
      } yield assertTrue(
        boards == List(boardRow(matchId)),
        row.exists(_.fetchedAt == initialFetchedAt),
        pending.find(_.matchId == matchId).map(_.status).contains(PendingMatchStatus.ApiError),
        failed.map(_.error).exists(_.contains("played_as_white"))
      )
    },
    test("an unknown game result on a refresh keeps the match's boards, marker and fetched_at") {
      val matchId      = ClubMatchId(9102)
      val markerOption = Some("marker-from-an-earlier-body")
      for {
        _      <- seedMatch(matchId, markerOption)
        ctx    <- contextServing(bodyWithUnknownResult(matchId))
        result <- runRefresh(ctx, matchId).either
        boards <- ClubMatchBoard.selectMatch(matchId)
        row    <- ClubMatch.selectId(matchId)
      } yield assertTrue(
        result.left.exists(_.isInstanceOf[JsonDecodingException]),
        boards == List(boardRow(matchId)),
        row.exists(_.processedBodyHash == markerOption),
        row.exists(_.fetchedAt == initialFetchedAt)
      )
    },
    test("an unknown game result leaves a failure row naming the field") {
      val matchId = ClubMatchId(9103)
      for {
        _        <- seedMatch(matchId, None)
        ctx      <- contextServing(bodyWithUnknownResult(matchId))
        _        <- runRefresh(ctx, matchId).either
        failures <- ApiFetchFailure.selectRecent(Instant.EPOCH)
        ofMatch = failures.filter(_.url == ApiDailyMatch.getUrl(matchId).encode)
      } yield assertTrue(
        ofMatch.map(_.errorType) == List("JsonDecodingException"),
        ofMatch.exists(_.errorMessage.exists(_.contains("played_as_white")))
      )
    },
    test("a cancelled match's body on a refresh clears boards stored earlier and records its marker") {
      val matchId = ClubMatchId(9105)
      for {
        _      <- seedMatch(matchId, None)
        ctx    <- contextServing(cancelledBody(matchId))
        _      <- runRefresh(ctx, matchId)
        boards <- ClubMatchBoard.selectMatch(matchId)
        row    <- ClubMatch.selectId(matchId)
      } yield assertTrue(
        boards.isEmpty,
        row.exists(_.team1ScoreX2 == 0),
        row.exists(_.processedBodyHash.isDefined)
      )
    },
    test("a cancelled match's body on a re-queued match clears boards stored earlier") {
      val matchId = ClubMatchId(9104)
      for {
        _       <- seedMatch(matchId, None)
        _       <- HistoryPendingMatch.insert(pendingRow(matchId))
        ctx     <- contextServing(cancelledBody(matchId))
        _       <- runWaves(ctx)
        boards  <- ClubMatchBoard.selectMatch(matchId)
        row     <- ClubMatch.selectId(matchId)
        pending <- HistoryPendingMatch.selectClub(clubId)
        updated <- ctx.matchesBoardsUpdated.get
      } yield assertTrue(
        boards.isEmpty,
        row.exists(_.team1ScoreX2 == 0),
        !pending.exists(_.matchId == matchId),
        updated == 1
      )
    }
  ).provideShared(
    FreshSchemaLayer("test_match_boards_on_refetch", Tables.ensureTables)
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock
}
