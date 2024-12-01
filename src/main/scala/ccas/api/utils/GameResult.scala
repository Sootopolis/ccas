package ccas.api.utils

enum GameResult(val score: Float) {
  case WIN  extends GameResult(1)
  case DRAW extends GameResult(0.5)
  case LOSS extends GameResult(0)
}
