package ccas.analysis.apps.history

import java.time.Instant

import zio.http.URL

import ccas.analysis.GameScoring
import ccas.analysis.tables.{ClubMatch, ClubMatchBoard, ClubMatchGame}
import ccas.api.clubmatch.ApiDailyMatch
import ccas.api.clubmatch.ApiDailyMatch.*
import ccas.api.clubmatch.ApiMatchBoard.{ApiBoardGame, ApiBoardPlayer}
import ccas.api.misc.enums.*
import ccas.api.misc.subtypes.*

/** Pure transformations from API match data to DB rows. No ZIO effects, no shared state — callable from both
  * `HistoryProcessing.processDailyMatch` (first-time ingest) and `HistoryProcessing.refreshSingleMatchWithBody`
  * (settled-match refresh).
  */
private[history] object HistoryBoardBuilder {

  /** Builds a ClubMatchGame row if there's a match-level outcome or board-level game data. Extracts game ID,
    * start/end times, and post-game ratings from the board API data when available.
    */
  private[history] def buildGameRow(
    matchId: ClubMatchId,
    board: Short,
    team1IsWhite: Boolean,
    winner: Option[BoardGameWinner],
    detail: Option[GameResultDetail],
    boardGame: Option[ApiBoardGame]
  ): Option[ClubMatchGame] =
    Option.unless(winner.isEmpty && boardGame.isEmpty) {
      val gameId = boardGame.map(g => g.url.path.segments.last.toLong)
      val startTime = boardGame.flatMap(_.startTime)
      val endTime = boardGame.flatMap(_.endTime)

      // team1 is white when team1IsWhite=true, so white=team1, black=team2
      // team1 is black when team1IsWhite=false, so white=team2, black=team1
      val (t1Rating, t2Rating) = boardGame match {
        case Some(g) if team1IsWhite => (finishedRating(g.white), finishedRating(g.black))
        case Some(g)                 => (finishedRating(g.black), finishedRating(g.white))
        case None                    => (None, None)
      }

      ClubMatchGame(
        matchId = matchId,
        board = board,
        team1IsWhite = team1IsWhite,
        gameId = gameId,
        startTime = startTime,
        endTime = endTime,
        winner = winner,
        detail = detail,
        team1Rating = t1Rating,
        team2Rating = t2Rating
      )
    }

  private[history] def finishedRating(p: Either[URL, ApiBoardPlayer]): Option[Elo] =
    p.toOption.filter(_.result.isDefined).map(_.rating)

  private[history] def normalizeGameOutcome(
    whiteResult: Option[GameResultDetail],
    blackResult: Option[GameResultDetail],
    whiteTeamIsTeam1: Boolean
  ): (Option[BoardGameWinner], Option[GameResultDetail]) =
    (whiteResult, blackResult) match {
      case (Some(GameResultDetail.Win), Some(loss)) =>
        val winner = if (whiteTeamIsTeam1) { BoardGameWinner.Team1 }
        else { BoardGameWinner.Team2 }
        (Some(winner), Some(loss))
      case (Some(loss), Some(GameResultDetail.Win)) =>
        val winner = if (whiteTeamIsTeam1) { BoardGameWinner.Team2 }
        else { BoardGameWinner.Team1 }
        (Some(winner), Some(loss))
      case (Some(draw), Some(_)) if draw.category == GameResult.Draw =>
        (Some(BoardGameWinner.Draw), Some(draw))
      case (None, None) =>
        (None, None)
      case _ =>
        // Mismatched state (e.g., one side played, other didn't) — treat as not played
        (None, None)
    }

  private[history] def computeScoreX2(
    game1Winner: Option[BoardGameWinner],
    game2Winner: Option[BoardGameWinner],
    team1FairPlay: Boolean,
    team2FairPlay: Boolean
  ): (Short, Short) = {
    def gameScore(winner: Option[BoardGameWinner]): (Int, Int) = {
      val t1 = GameScoring.classifyGame(winner, team1FairPlay, team2FairPlay).fold(0)(GameScoring.scoreX2)
      val t2 = winner.fold(0)(_ => 2 - t1)
      (t1, t2)
    }

    val (g1t1, g1t2) = gameScore(game1Winner)
    val (g2t1, g2t2) = gameScore(game2Winner)
    ((g1t1 + g2t1).toShort, (g1t2 + g2t2).toShort)
  }

  /** Computes expected board scores from match-level data alone (no board endpoint needed).
    * Returns a map of boardNum → (team1ScoreX2, team2ScoreX2).
    */
  private[history] def computeExpectedScores(dailyMatch: ApiDailyMatch): Map[Short, (Short, Short)] =
    dailyMatch match {
      case _: ApiDailyMatchRegistered => Map.empty
      case _ =>
        val teams   = dailyMatch.teams
        val team1Fp = teams.team1.fairPlayRemovals.map(_.value)
        val team2Fp = teams.team2.fairPlayRemovals.map(_.value)

        val team1ByBoard: Map[Short, MatchPlayerStarted] = teams.team1.players.collect { case p: MatchPlayerStarted =>
          p.board.path.segments.last.toShort -> p
        }.toMap
        val team2ByBoard: Map[Short, MatchPlayerStarted] = teams.team2.players.collect { case p: MatchPlayerStarted =>
          p.board.path.segments.last.toShort -> p
        }.toMap

        val allBoards = team1ByBoard.keySet ++ team2ByBoard.keySet
        allBoards.flatMap { boardNum =>
          for {
            t1 <- team1ByBoard.get(boardNum)
            t2 <- team2ByBoard.get(boardNum)
          } yield {
            val t1FairPlay = team1Fp.contains(t1.username.value)
            val t2FairPlay = team2Fp.contains(t2.username.value)
            val (g1Winner, _) = normalizeGameOutcome(t1.playedAsWhite, t2.playedAsBlack, whiteTeamIsTeam1 = true)
            val (g2Winner, _) = normalizeGameOutcome(t2.playedAsWhite, t1.playedAsBlack, whiteTeamIsTeam1 = false)
            boardNum -> computeScoreX2(g1Winner, g2Winner, t1FairPlay, t2FairPlay)
          }
        }.toMap
    }

  /** Returns true if the expected board scores match the existing DB rows exactly. */
  private[history] def scoresMatch(
    expected: Map[Short, (Short, Short)],
    existing: List[ClubMatchBoard]
  ): Boolean =
    expected.size == existing.size && existing.forall { b =>
      expected.get(b.board).contains((b.team1ScoreX2, b.team2ScoreX2))
    }

  private[history] def buildClubMatchRow(
    matchId: ClubMatchId,
    dailyMatch: ApiDailyMatch,
    team1ClubId: Option[ClubId],
    team2ClubId: Option[ClubId]
  ): ClubMatch = {
    val teams = dailyMatch.teams
    val (startTime, endTime) = dailyMatch match {
      case m: ApiDailyMatchFinished =>
        (Some(Instant.ofEpochSecond(m.startTime)), Some(Instant.ofEpochSecond(m.endTime)))
      case m: ApiDailyMatchCancelled =>
        (Some(Instant.ofEpochSecond(m.startTime)), Some(Instant.ofEpochSecond(m.endTime)))
      case m: ApiDailyMatchInProgress => (Some(Instant.ofEpochSecond(m.startTime)), None)
      case m: ApiDailyMatchRegistered => (m.startTime.map(Instant.ofEpochSecond), None)
    }
    ClubMatch(
      matchId = matchId,
      name = dailyMatch.name,
      status = dailyMatch.status,
      timeClass = dailyMatch.settings.timeClass,
      startTime = startTime,
      endTime = endTime,
      boards = dailyMatch.boards.toShort,
      team1ClubId = team1ClubId,
      team1ScoreX2 = (teams.team1.score * 2).toShort,
      team2ClubId = team2ClubId,
      team2ScoreX2 = (teams.team2.score * 2).toShort,
      fetchedAt = Instant.now()
    )
  }
}
