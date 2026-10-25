package ccas.analysis

import ccas.api.misc.enums.{BoardGameWinner, GameResult}

/** Shared game outcome classification for board-level scoring.
  *
  * All methods assume the caller has already normalized the team perspective so that `Team1` means "our team" and
  * `Team2` means the opponent. Returns `None` when the game was not played (winner is `None`).
  */
object GameScoring {

  /** Classify a game outcome ignoring fairplay flags — the raw result based solely on who won. */
  def classifyGameRaw(winner: Option[BoardGameWinner]): Option[GameResult] =
    winner.map(_.team1Result)

  /** Classify a game outcome applying fairplay rules:
    *   - Both flagged: Draw (each side scores 0.5)
    *   - Only our side flagged: Loss (we forfeit)
    *   - Only opponent flagged: Win (they forfeit)
    *   - Neither flagged: use the actual game winner
    */
  def classifyGame(
    winner: Option[BoardGameWinner],
    ourFairPlay: Boolean,
    oppFairPlay: Boolean
  ): Option[GameResult] =
    winner.map { w =>
      (ourFairPlay, oppFairPlay) match {
        case (true, true)   => GameResult.Draw
        case (true, false)  => GameResult.Loss
        case (false, true)  => GameResult.Win
        case (false, false) => w.team1Result
      }
    }
}
