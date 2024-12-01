package ccas.api.utils

enum GameResultDetail(val category: GameResult) {
  val score: Float = category.score

  case win                 extends GameResultDetail(GameResult.WIN)
  case stalemate           extends GameResultDetail(GameResult.DRAW)
  case agreed              extends GameResultDetail(GameResult.DRAW)
  case repetition          extends GameResultDetail(GameResult.DRAW)
  case `50move`            extends GameResultDetail(GameResult.DRAW)
  case insufficient        extends GameResultDetail(GameResult.DRAW)
  case timevsinsufficient  extends GameResultDetail(GameResult.DRAW)
  case checkmated          extends GameResultDetail(GameResult.LOSS)
  case resigned            extends GameResultDetail(GameResult.LOSS)
  case timeout             extends GameResultDetail(GameResult.LOSS)
  case lose                extends GameResultDetail(GameResult.LOSS)
  case abandoned           extends GameResultDetail(GameResult.LOSS)
  case kingofthehill       extends GameResultDetail(GameResult.LOSS)
  case threecheck          extends GameResultDetail(GameResult.LOSS)
  case bughousepartnerlose extends GameResultDetail(GameResult.LOSS)
}
