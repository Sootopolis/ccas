package ccas.analysis.apps.stats

import ccas.analysis.GameScoring
import ccas.api.misc.enums.{BoardGameWinner, GameResult}
import ccas.api.misc.subtypes.{PlayerId, Username}

object StatsUtils {

  /** Aggregated board statistics for a single player under one scoring interpretation.
    *
    * @param pointsX2
    *   total points in the doubled scale (Win=2, Draw=1, Loss=0 per game) to avoid half-point floats. Divide by 2.0 for
    *   actual points.
    */
  case class PlayerBoardStats(
    boards: Int,
    games: Int,
    wins: Int,
    draws: Int,
    losses: Int,
    pointsX2: Int
  ) {
    def points: Double = pointsX2 / 2.0

    def scoreRate: Double = if (games == 0) 0.0 else points / games

    def +(other: PlayerBoardStats): PlayerBoardStats =
      PlayerBoardStats(
        boards + other.boards,
        games + other.games,
        wins + other.wins,
        draws + other.draws,
        losses + other.losses,
        pointsX2 + other.pointsX2
      )
  }

  object PlayerBoardStats {
    val empty: PlayerBoardStats = PlayerBoardStats(0, 0, 0, 0, 0, 0)
  }

  /** Combined raw and fairplay-adjusted stats for one player.
    *
    * @param raw
    *   stats derived from game winners only, ignoring fairplay flags.
    * @param fairPlay
    *   stats with fairplay rules applied (cheater forfeits, etc.).
    */
  case class MemberContribution(
    playerId: PlayerId,
    username: Username,
    raw: PlayerBoardStats,
    fairPlay: PlayerBoardStats
  )

  /** Aggregate normalized board rows into per-player contribution stats with both raw and fairplay-adjusted views. */
  def aggregate(
    rows: List[ClubBoard],
    usernameMap: Map[PlayerId, Username]
  ): List[MemberContribution] = {
    val byPlayer = rows.groupBy(_.playerId)
    byPlayer.map { case (playerId, boards) =>
      val username = usernameMap.getOrElse(playerId, Username.wrap("unknown"))
      val raw      = computeStats(boards, (_, w) => GameScoring.classifyGameRaw(w))
      val fairPlay = computeStats(boards, (r, w) => GameScoring.classifyGame(w, r.ourFairPlay, r.oppFairPlay))
      MemberContribution(playerId, username, raw, fairPlay)
    }.toList.sortBy(_.username.value.toLowerCase)(using Ordering.String)
  }

  private def computeStats(
    boards: List[ClubBoard],
    classify: (ClubBoard, Option[BoardGameWinner]) => Option[GameResult]
  ): PlayerBoardStats = {
    var nBoards, nGames, nWins, nDraws, nLosses, ptsX2 = 0
    boards.foreach { row =>
      nBoards += 1
      List(row.game1Winner, row.game2Winner).foreach { gameWinner =>
        classify(row, gameWinner).foreach { result =>
          nGames += 1
          result match {
            case GameResult.Win  => nWins += 1
            case GameResult.Draw => nDraws += 1
            case GameResult.Loss => nLosses += 1
          }
          ptsX2 += GameScoring.scoreX2(result)
        }
      }
    }
    PlayerBoardStats(nBoards, nGames, nWins, nDraws, nLosses, ptsX2)
  }
}
